//! Arrow segment [Reader] and [Writer].
//!
//! Arrow segments may be accompanied by up to two other files:
//!
//! - `{segment}.recovery`: In-progress recovery file for the segment. See
//!   [recover] for a description of the recovery process.
//! - `{segment}.recovered`: Fully recovered segment. Available after the
//!   recovery process succesfully completes. Contains all rows that are
//!   recoverable from any prior partially written segment file and cache.
use anyhow::Result;
use plateau_transport::SegmentChunk;
use std::ffi::OsStr;
use std::io::{Read, Seek, SeekFrom};
use std::{
    fs,
    ops::Range,
    path::{Path, PathBuf},
};
use tracing::{error, trace, warn};

use crate::arrow2::{
    datatypes::Schema,
    io::ipc::{
        read::{
            read_batch, read_file_dictionaries, read_file_metadata, read_stream_metadata,
            Dictionaries, FileMetadata, StreamReader, StreamState,
        },
        write::{FileWriter, WriteOptions},
    },
};

use super::{cache, SegmentIterator};

const ARROW_HEADER: &str = "ARROW1";

pub fn check_file(f: &mut fs::File) -> Result<bool> {
    let mut buffer = [0u8; 6];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut buffer)?;
    Ok(buffer.into_iter().eq(ARROW_HEADER.bytes()))
}

#[derive(Clone, Debug)]
pub struct Segment {
    path: PathBuf,
    recovery_path: PathBuf,
    recovered_path: PathBuf,
}

impl Segment {
    pub fn new(path: PathBuf) -> Result<Self> {
        anyhow::ensure!(path.extension() != Some(OsStr::new("recovery")));
        anyhow::ensure!(path.extension() != Some(OsStr::new("recovered")));

        let mut recovery_path = path.clone();
        recovery_path.set_extension("recovery");
        let mut recovered_path = path.clone();
        recovered_path.set_extension("recovery");

        Ok(Self {
            path,
            recovery_path,
            recovered_path,
        })
    }

    fn directory(&self) -> Result<fs::File> {
        let mut parent = self.path.clone();
        parent.pop();
        fs::File::open(&parent).map_err(anyhow::Error::from)
    }

    pub fn read(&self, cache: Option<cache::Data>) -> Result<Reader> {
        if self.recovered_path.exists() {
            trace!("reading from recovered file at {:?}", self.recovered_path);
            Reader::open(&self.recovered_path)
        } else {
            match Reader::open(&self.path) {
                Ok(reader) => Ok(reader),
                Err(e) => {
                    error!("error reading {:?}: {e:?}", self.path);
                    Ok(self.recover(cache)?.1)
                }
            }
        }
    }

    pub fn recover(&self, cache: Option<cache::Data>) -> Result<(usize, Reader)> {
        warn!("beginning recovery of {:?}", self.path);

        // Combine all readable data from a damaged (partially written) segment with
        // the data from cache into a new, "recovered" file.
        //
        // Arrow files ultimately wrap a stream of IPC framed "messages". Each
        // message is prefixed with its length, so we are able to recover all full
        // frames written to disk.
        let (reader, schema) = fs::File::open(&self.path)
            .map_err(anyhow::Error::from)
            .and_then(|mut damaged| {
                damaged.seek(std::io::SeekFrom::Start(8))?;
                let metadata = read_stream_metadata(&mut damaged)?;
                let schema = metadata.schema.clone();
                let reader = StreamReader::new(damaged, metadata, None);
                Ok((reader, schema))
            })
            .map_err(|e| error!("error reading {:?}: {e:?}", self.path))
            .ok()
            .unzip();

        let recovery = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.recovery_path)?;

        let chunk_ix = cache.as_ref().map(|cache| cache.chunk_ix);
        let (schema, cache) = match (schema, cache) {
            (Some(a), Some(cache)) => {
                if a != cache.rows.schema {
                    // This _should_ never happen. Regardless, we choose to discard
                    // the cache as it should always have fewer rows than even a
                    // single chunk.
                    warn!(
                        "segment schema does not match cache schema. discarding {} rows from cache",
                        cache.rows.len()
                    );
                    (a, None)
                } else {
                    (a, Some(cache.rows.chunk))
                }
            }
            (Some(a), None) => (a, None),
            // This can happen e.g. if the segment never had enough rows to fill a chunk
            (None, Some(cache)) => (cache.rows.schema, Some(cache.rows.chunk)),
            (None, None) => anyhow::bail!("no segment or cache present"),
        };

        let options = WriteOptions { compression: None };
        let mut writer = FileWriter::try_new(recovery, schema.clone(), None, options)?;

        let mut recovered_chunks = 0;
        let mut recovered_rows = 0;

        // First, copy over all readable chunks from the damaged file
        for result in reader
            .into_iter()
            .flatten()
            .take_while(|state| matches!(state, Ok(StreamState::Some(..)) | Err(..)))
        {
            match result {
                Ok(StreamState::Some(chunk)) => {
                    recovered_chunks += 1;
                    recovered_rows += chunk.len();
                    writer.write(&chunk, None)?;
                }
                Ok(StreamState::Waiting) => {
                    warn!("halting on unexpected waiting state");
                }
                Err(e) => {
                    error!(%e, "error reading cache segment");
                }
            }
        }

        // Finally, fold in any valid cached chunk.
        let mut cache_recovery = "(no cache)";
        if Some(recovered_chunks) == chunk_ix {
            if let Some(chunk) = cache {
                recovered_chunks += 1;
                recovered_rows += chunk.len();
                writer.write(&chunk, None)?;
                cache_recovery = "(including cache)";
            }
        } else if let Some(chunk_ix) = chunk_ix {
            warn!(
                "gap between chunks in file ({}) and cache index ({})",
                recovered_chunks, chunk_ix
            );
            cache_recovery = "(cache invalid)"
        }

        writer.finish()?;
        writer.into_inner().sync_all()?;
        warn!(
            "recovered {recovered_rows} rows in {recovered_chunks} chunks from {:?} {cache_recovery}",
            self.path,
        );

        // Now, we should be able to read normally from the recovered file.
        fs::rename(&self.recovery_path, &self.recovered_path)?;

        Ok((recovered_rows, Reader::open(&self.recovered_path)?))
    }

    pub fn parts(self) -> impl Iterator<Item = PathBuf> {
        [self.recovery_path, self.recovered_path].into_iter()
    }

    pub fn into_path(self) -> PathBuf {
        self.path
    }
}

pub struct Writer {
    writer: FileWriter<fs::File>,
    file: fs::File,
}

impl Writer {
    pub fn create(file: fs::File, schema: &Schema) -> Result<Self> {
        let options = WriteOptions { compression: None };
        Ok(Self {
            writer: FileWriter::try_new(file.try_clone()?, schema.clone(), None, options)?,
            file,
        })
    }

    pub fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        self.writer.write(&chunk, None).map_err(Into::into)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.file.sync_data().map_err(Into::into)
    }

    pub fn end(mut self) -> Result<()> {
        self.writer.finish()?;
        self.file.sync_data()?;

        Ok(())
    }
}

pub struct Reader {
    metadata: FileMetadata,
    file: fs::File,
    dictionaries: Dictionaries,
    message_scratch: Vec<u8>,
    data_scratch: Vec<u8>,

    iter_range: Range<usize>,
}

impl Reader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = fs::File::open(&path)?;

        let mut data_scratch = vec![];
        let message_scratch = vec![];
        let metadata = read_file_metadata(&mut file)?;
        let dictionaries = read_file_dictionaries(&mut file, &metadata, &mut data_scratch)?;

        Ok(Self {
            iter_range: 0..metadata.blocks.len(),

            metadata,
            dictionaries,
            file,
            message_scratch,
            data_scratch,
        })
    }

    fn read_ix(&mut self, ix: usize) -> Result<SegmentChunk> {
        read_batch(
            &mut self.file,
            &self.dictionaries,
            &self.metadata,
            None,
            None,
            ix,
            &mut self.message_scratch,
            &mut self.data_scratch,
        )
        .map_err(Into::into)
    }
}

impl Iterator for Reader {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_range.is_empty() {
            return None;
        }

        let ix = self.iter_range.start;
        self.iter_range.start += 1;

        Some(self.read_ix(ix))
    }
}

impl DoubleEndedIterator for Reader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.iter_range.is_empty() {
            return None;
        }

        self.iter_range.end -= 1;

        Some(self.read_ix(self.iter_range.end))
    }
}

impl SegmentIterator for Reader {
    fn schema(&self) -> &Schema {
        &self.metadata.schema
    }
}

#[cfg(test)]
pub mod test {
    use plateau_transport::SchemaChunk;
    use sample_arrow2::chunk::{ChainedChunk, ChainedMultiChunk};
    use sample_std::{Random, Regex, Sample};
    use sample_test::sample_test;
    use tempfile::tempdir;

    use super::*;
    use crate::arrow2::datatypes::{Field, Metadata};
    use crate::chunk::test::{inferences_nested, inferences_schema_a};
    use crate::chunk::{legacy_schema, LegacyRecords};
    use crate::segment::test::{build_records, collect_records, deep_chunk};

    impl Writer {
        fn create_path(path: impl AsRef<Path>, schema: &Schema) -> Result<Self> {
            Self::create(fs::File::create(path)?, schema)
        }
    }

    #[test]
    fn check_file_format() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let a = inferences_schema_a();
        let mut w = Writer::create_path(&path, &a.schema)?;
        w.write_chunk(a.chunk)?;

        assert!(check_file(&mut fs::File::open(path)?)?);
        Ok(())
    }

    #[test]
    fn can_iter_forward_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let records: Vec<_> = build_records((0..10).map(|i| (i, format!("m{i}"))));

        let schema = legacy_schema();
        let mut w = Writer::create_path(&path, &schema)?;
        for record in records.clone() {
            w.write_chunk(SchemaChunk::try_from(LegacyRecords([record].to_vec()))?.chunk)?;
        }
        w.end()?;

        let reader = Reader::open(&path)?;

        assert_eq!(collect_records(schema, reader), records);
        Ok(())
    }

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let mut records: Vec<_> = build_records((0..10).map(|i| (i, format!("m{i}"))));

        let schema = legacy_schema();
        let mut w = Writer::create_path(&path, &schema)?;
        for record in records.clone() {
            w.write_chunk(SchemaChunk::try_from(LegacyRecords([record].to_vec()))?.chunk)?;
        }
        w.end()?;

        records.reverse();

        let reader = Reader::open(path)?;
        assert_eq!(collect_records(schema, reader.rev()), records);
        Ok(())
    }

    #[test]
    fn schema_file_metadata() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let mut a = inferences_schema_a();
        a.schema
            .metadata
            .insert("pipeline.name".to_string(), "pied-piper".to_string());
        a.schema
            .metadata
            .insert("pipeline.version".to_string(), "3.1".to_string());
        let mut w = Writer::create_path(&path, &a.schema)?;
        w.write_chunk(a.chunk)?;
        w.end()?;

        let reader = Reader::open(&path)?;
        let schema = reader.metadata.schema;
        assert_eq!(schema.metadata.get("pipeline.name").unwrap(), "pied-piper");
        assert_eq!(schema.metadata.get("pipeline.version").unwrap(), "3.1");

        Ok(())
    }

    #[test]
    fn nested() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let a = inferences_nested();
        let mut w = Writer::create_path(path, &a.schema)?;
        w.write_chunk(a.chunk)?;
        Ok(())
    }

    #[test]
    fn large_records() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let large: String = (0..100 * 1024).map(|_| "x").collect();
        let records: Vec<_> =
            build_records((0..20).map(|ix| (ix, format!("message-{}-{}", ix, large))));

        let mut w = Writer::create_path(&path, &legacy_schema())?;
        w.write_chunk(SchemaChunk::try_from(LegacyRecords(records[0..10].to_vec()))?.chunk)?;
        w.write_chunk(SchemaChunk::try_from(LegacyRecords(records[10..].to_vec()))?.chunk)?;
        w.end()?;

        let reader = Reader::open(&path)?;
        assert_eq!(
            collect_records(reader.metadata.schema.clone(), reader),
            records
        );

        Ok(())
    }

    fn open_drop(root: impl AsRef<Path>, rows: SchemaChunk<Schema>) -> Result<Segment> {
        let path = root.as_ref().join("open-drop.arrow");
        let s = Segment::new(path.clone())?;

        let mut w = Writer::create_path(&path, &rows.schema)?;
        w.write_chunk(rows.chunk)?;
        drop(w);

        assert!(check_file(&mut fs::File::open(path)?)?);

        Ok(s)
    }

    #[test]
    fn test_open_drop_recovery() -> Result<()> {
        let root = tempdir()?;
        let a = inferences_schema_a();
        let s = open_drop(root.path(), a.clone())?;

        let (rows, mut r) = s.recover(None)?;
        assert_eq!(rows, a.chunk.len());
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(r.next().is_none());

        Ok(())
    }

    #[test]
    fn test_open_drop_read_reread() -> Result<()> {
        let root = tempdir()?;
        let a = inferences_schema_a();
        let s = open_drop(root.path(), a.clone())?;

        let mut r = s.read(None)?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert!(r.next().is_none());

        // read again to verify read from recovered
        let mut r = s.read(None)?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(r.next().is_none());

        Ok(())
    }

    pub fn partial_write(root: impl AsRef<Path>, rows: SchemaChunk<Schema>) -> Result<Segment> {
        let path = root.as_ref().join("partial-write.arrow");
        let s = Segment::new(path.clone())?;

        let mut w = Writer::create_path(&path, &rows.schema)?;
        w.write_chunk(rows.chunk.clone())?;
        w.write_chunk(rows.chunk.clone())?;
        drop(w);
        let len = fs::File::open(&path)?.metadata()?.len();

        std::fs::File::options()
            .append(true)
            .open(&path)?
            .set_len(len - 40)?;

        assert!(check_file(&mut fs::File::open(path)?)?);

        Ok(s)
    }

    #[test]
    fn test_partial_write_recovery() -> Result<()> {
        let root = tempdir()?;
        let a = inferences_schema_a();
        let s = partial_write(root.path(), a.clone())?;

        let (rows, mut iter) = s.recover(None)?;
        assert_eq!(rows, a.chunk.len());
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert!(iter.next().is_none());

        Ok(())
    }

    fn test_partial_write_read_reread() -> Result<()> {
        let root = tempdir()?;
        let a = inferences_schema_a();
        let s = partial_write(root.path(), a.clone())?;

        let mut iter = s.read(None)?;
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert!(iter.next().is_none());

        // assert we can re-read from the recovered file
        let mut iter = s.read(None)?;
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(iter.next().is_none());

        Ok(())
    }

    #[sample_test]
    fn arbitrary_chunk(#[sample(deep_chunk(3, 100, true).sample_one())] chunk: ChainedChunk) {
        let chunk = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.arrow");

        use sample_std::Sample;
        let mut name = sample_std::Regex::new("[a-z]{4, 8}");
        let mut g = sample_std::Random::new();

        let schema = Schema {
            fields: chunk
                .iter()
                .map(|arr| {
                    Field::new(
                        name.generate(&mut g),
                        arr.data_type().clone(),
                        arr.validity().is_some(),
                    )
                })
                .collect(),
            metadata: Metadata::default(),
        };
        let mut w = Writer::create_path(&path, &schema).unwrap();
        w.write_chunk(chunk.clone()).unwrap();
        w.end().unwrap();

        let r = Reader::open(&path).unwrap();
        let chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(chunks, vec![chunk]);
    }

    #[sample_test]
    fn arbitrary_many_chunk(
        #[sample(deep_chunk(5, 100, true).sample_many(2..10))] chunk: ChainedMultiChunk,
    ) {
        let chunks = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.arrow");

        let mut name = Regex::new("[a-z]{4, 8}");
        let mut g = Random::new();

        let schema = Schema {
            fields: chunks
                .first()
                .unwrap()
                .iter()
                .map(|arr| {
                    Field::new(
                        name.generate(&mut g),
                        arr.data_type().clone(),
                        arr.validity().is_some(),
                    )
                })
                .collect(),
            metadata: Metadata::default(),
        };
        let mut w = Writer::create_path(&path, &schema).unwrap();

        for chunk in &chunks {
            w.write_chunk(chunk.clone()).unwrap();
        }
        w.end().unwrap();

        let r = Reader::open(&path).unwrap();
        let actual_chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(actual_chunks, chunks);
    }
}

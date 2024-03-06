//! Parquet segment [Reader] and [Writer].
//!
//! Each parquet segment file may be accompanied by up to two other files:
//!
//! - `{segment}.header`: Recovery "header". Contains a fixed version identifier
//!   prefix, current offset, and parquet footer.  See `write_checkpoint` for
//!   internal details of this file, and `recover` for the recovery process.
//! - `{segment}.header.tmp`: A temporary file that the above data is written
//!   into before moving to the above file, to avoid issues occurring if we
//!   crash while writing the recovery header.
use super::{cache, validate_header, SegmentIterator, PLATEAU_HEADER};
use crate::arrow2::{
    datatypes::Schema,
    io::parquet::read::FileReader as FileReader2,
    io::parquet::read::{infer_schema, read_metadata},
    io::parquet::write::{
        add_arrow_schema, to_parquet_schema, transverse, CompressionOptions, Encoding,
        RowGroupIterator, Version, WriteOptions,
    },
};

use anyhow::Result;
use parquet2::{
    metadata::{FileMetaData, KeyValue},
    write::FileWriter,
};
use parquet_format_safe::thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use plateau_transport::SegmentChunk;
use std::{
    ffi::OsStr,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    iter::{self, Chain, Flatten},
    option,
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::{debug, error, warn};

const PARQUET_HEADER: &str = "PAR1";

fn checkpoint_path(path: impl AsRef<Path>) -> PathBuf {
    let mut path = PathBuf::from(path.as_ref());
    assert!(path.set_extension("header"));
    path
}

pub fn check_file(f: &mut fs::File) -> Result<bool> {
    let mut buffer = [0u8; 4];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut buffer)?;
    Ok(buffer.into_iter().eq(PARQUET_HEADER.bytes()))
}

pub struct Segment {
    path: PathBuf,
    checkpoint_path: PathBuf,
    checkpoint_tmp_path: PathBuf,
}

impl Segment {
    pub fn new(path: PathBuf) -> Result<Self> {
        let checkpoint_path = checkpoint_path(&path);
        let mut checkpoint_tmp_path = path.clone();

        anyhow::ensure!(checkpoint_tmp_path.extension() != Some(OsStr::new("header")));
        anyhow::ensure!(checkpoint_tmp_path.extension() != Some(OsStr::new("header.tmp")));
        anyhow::ensure!(checkpoint_tmp_path.set_extension("header.tmp"));

        Ok(Self {
            path,
            checkpoint_path,
            checkpoint_tmp_path,
        })
    }

    fn directory(&self) -> Result<fs::File> {
        let mut parent = self.checkpoint_path.clone();
        parent.pop();
        fs::File::open(&parent).map_err(anyhow::Error::from)
    }

    pub fn read(self, cache: Option<cache::Data>) -> Result<Reader> {
        Reader::open(self, cache)
    }

    fn clear_checkpoints(&self) -> Result<()> {
        if Path::exists(&self.checkpoint_path) {
            fs::remove_file(&self.checkpoint_path)?;
        }

        if Path::exists(&self.checkpoint_tmp_path) {
            fs::remove_file(&self.checkpoint_tmp_path)?;
        }

        Ok(())
    }

    pub fn destroy(&self) -> Result<()> {
        self.clear_checkpoints()?;

        fs::remove_file(&self.path)?;

        Ok(())
    }
}

pub(super) struct Writer {
    segment: Segment,
    file: fs::File,
    directory: fs::File,
    config: super::Config,

    pub(super) writer: FileWriter<fs::File>,
    key_value_metadata: Option<Vec<KeyValue>>,
    options: WriteOptions,
}

impl Writer {
    pub(super) fn create(
        path: PathBuf,
        file: fs::File,
        schema: &Schema,
        config: super::Config,
    ) -> Result<Self> {
        let segment = Segment::new(path)?;
        let created_by = Some("plateau v0.1.0 segment v1".to_string());

        // TODO: ideally we'd use compression, but this currently causes nasty dependency issues
        // between parquet2 and parquet
        let options = WriteOptions {
            data_pagesize_limit: None,
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
        };

        let parquet_schema = to_parquet_schema(schema)?;

        let self_file = file.try_clone()?;
        let writer = parquet2::write::FileWriter::new(
            file,
            parquet_schema,
            parquet2::write::WriteOptions {
                version: options.version,
                write_statistics: options.write_statistics,
            },
            created_by,
        );

        let key_value_metadata = add_arrow_schema(schema, None);
        let directory = segment.directory()?;

        Ok(Self {
            segment,
            file: self_file,
            directory,
            config,

            writer,
            key_value_metadata,
            options,
        })
    }

    pub(super) fn write_chunk(&mut self, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        let encodings: Vec<_> = schema
            .fields
            .iter()
            .map(|field| transverse(field.data_type(), |_| Encoding::Plain))
            .collect();

        let row_groups =
            RowGroupIterator::try_new(iter::once(Ok(chunk)), schema, self.options, encodings)?;

        for group in row_groups {
            self.writer.write(group?)?;
        }

        Ok(())
    }

    pub(super) fn checkpoint(&self) -> Result<()> {
        let metadata = self
            .writer
            .compute_metadata(self.key_value_metadata.clone())?;

        let mut checkpoint_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.segment.checkpoint_tmp_path)?;

        // Write header
        checkpoint_file.write_all(PLATEAU_HEADER.as_bytes())?;

        // Write offset
        let offset_bytes = self.writer.offset().to_le_bytes();
        checkpoint_file.write_all(&offset_bytes)?;

        // Then, write the header
        let mut protocol = TCompactOutputProtocol::new(&mut checkpoint_file);
        let metadata_len = metadata.write_to_out_protocol(&mut protocol)? as i32;
        protocol.flush()?;
        drop(protocol);
        checkpoint_file.write_all(&metadata_len.to_le_bytes())?;

        // Now, sync all the things
        if self.config.durable_checkpoints {
            let now = Instant::now();
            self.file.sync_data()?;
            // After syncing the data, sync the parquet footer. The data is
            // useless without the footer telling us how to read it, and the
            // footer is useless without the corresponding data it describes.
            checkpoint_file.sync_data()?;
            drop(checkpoint_file);
            debug!("file sync elapsed: {:?}", now.elapsed());
        }

        fs::rename(
            &self.segment.checkpoint_tmp_path,
            &self.segment.checkpoint_path,
        )?;

        if self.config.durable_checkpoints {
            // Now sync the directory metadata, which will capture the rename of
            // the checkpoint file
            let now = Instant::now();
            self.directory.sync_all()?;
            debug!("dir sync elapsed: {:?}", now.elapsed());
        }

        Ok(())
    }

    pub(super) fn end(&mut self) -> Result<()> {
        self.writer.end(self.key_value_metadata.clone())?;
        self.file.sync_data()?;

        self.segment.clear_checkpoints()?;

        Ok(())
    }
}

fn recover(path: impl AsRef<Path>, checkpoint_path: impl AsRef<Path>) -> Result<FileMetaData> {
    let path = path.as_ref();
    let checkpoint_path = checkpoint_path.as_ref();
    warn!("attempting to recover checkpoint {:?}", checkpoint_path);

    {
        let mut checkpoint = fs::File::open(checkpoint_path)?;
        let mut segment = fs::File::options().write(true).open(path)?;

        validate_header(&mut checkpoint)?;
        let mut buffer = [0u8; 8];
        checkpoint.read_exact(&mut buffer)?;
        let offset = u64::from_le_bytes(buffer);
        debug!(
            "found valid v1 checkpoint at offset {} (file length {})",
            offset,
            segment.metadata()?.len()
        );

        let rest: Vec<u8> = checkpoint.bytes().collect::<Result<_, _>>()?;

        segment.set_len(offset)?;
        segment.seek(SeekFrom::Start(offset))?;
        segment.write_all(&rest)?;
        segment.write_all(PARQUET_HEADER.as_bytes())?;
        warn!("successfully recovered checkpoint {:?}", checkpoint_path);
    }

    let mut f = fs::File::open(path)?;
    let metadata = read_metadata(&mut f)?;

    debug!("recovered {} rows", metadata.num_rows);

    Ok(metadata)
}

pub(super) struct Reader {
    schema: Schema,
    iter: Chain<Flatten<option::IntoIter<CachingReader>>, option::IntoIter<Result<SegmentChunk>>>,
}

impl Reader {
    pub(super) fn open(segment: Segment, cache: Option<cache::Data>) -> Result<Self> {
        match CachingReader::open(segment) {
            Ok((schema, reader)) => {
                let cache = cache
                    .filter(|cache| cache.chunk_ix == reader.len as u32)
                    .map(|cache| Ok(cache.rows.chunk));

                Ok(Self {
                    schema,
                    iter: Some(reader).into_iter().flatten().chain(cache),
                })
            }
            Err(err) => {
                error!("error opening segment file: {err:?}");
                cache
                    .map(|cache| Self {
                        schema: cache.rows.schema,
                        iter: None.into_iter().flatten().chain(Some(Ok(cache.rows.chunk))),
                    })
                    .ok_or_else(|| anyhow::anyhow!("missing cache and segment"))
            }
        }
    }
}

impl Iterator for Reader {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl DoubleEndedIterator for Reader {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl SegmentIterator for Reader {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub(super) struct CachingReader {
    reader: FileReader2<fs::File>,
    len: usize,
    next_ix: usize,
    next_back_ix: usize,
    chunks: Vec<SegmentChunk>,
}

impl CachingReader {
    fn open(segment: Segment) -> Result<(Schema, Self)> {
        let mut f = fs::File::open(&segment.path)?;

        let metadata = read_metadata(&mut f).or_else(|err| {
            debug!("error reading segment: {err:?}");
            drop(f);
            anyhow::ensure!(
                segment.checkpoint_path.exists(),
                "no checkpoint to recover from"
            );
            recover(&segment.path, segment.checkpoint_path)
        })?;

        let schema = infer_schema(&metadata)?;

        let file = fs::File::open(&segment.path)?;

        let len = metadata.row_groups.len();
        let reader = FileReader2::new(file, metadata.row_groups, schema.clone(), None, None, None);

        Ok((
            schema,
            Self {
                reader,
                len,
                next_ix: 0,
                next_back_ix: len,
                chunks: vec![],
            },
        ))
    }
}

/// Reads chunks in forward or reverse order, with lazy caching of chunk data
impl Iterator for CachingReader {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ix >= self.next_back_ix {
            None
        } else {
            let index = self.next_ix;
            self.next_ix += 1;
            if self.chunks.len() < self.next_ix {
                match self.reader.next() {
                    Some(Ok(chunk)) => self.chunks.push(chunk),
                    Some(Err(e)) => {
                        error!("failed to read chunk from segment: {e}");
                        return Some(Err(e.into()));
                    }
                    None => {}
                }
            }

            if index >= self.chunks.len() {
                return None;
            }

            Some(Ok(self.chunks[index].clone()))
        }
    }
}

impl DoubleEndedIterator for CachingReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.next_back_ix <= self.next_ix {
            None
        } else {
            let fwd = self.next_ix;
            if let Some(e) = self.into_iter().find_map(|r| r.err()) {
                return Some(Err(e));
            }
            self.next_ix = fwd;
            self.next_back_ix -= 1;
            Some(Ok(self.chunks[self.next_back_ix].clone()))
        }
    }
}

#[cfg(test)]
mod test {
    use plateau_transport::SchemaChunk;
    use sample_arrow2::chunk::{ChainedChunk, ChainedMultiChunk};
    use sample_std::{Random, Regex, Sample};
    use sample_test::sample_test;
    use tempfile::tempdir;

    use super::*;
    use crate::arrow2::datatypes::{Field, Metadata};
    use crate::chunk::test::{inferences_nested, inferences_schema_a, inferences_schema_b};
    use crate::chunk::{legacy_schema, LegacyRecords};
    use crate::segment::test::{build_records, collect_records, deep_chunk};
    use crate::segment::{Config, Segment};

    impl Writer {
        fn from_path(path: PathBuf, schema: &Schema) -> Result<Self> {
            Writer::create(
                path.clone(),
                fs::File::create(&path)?,
                schema,
                Config::parquet(),
            )
        }
    }

    #[test]
    fn check_file_format() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");

        let a = inferences_schema_a();
        let mut w = Writer::from_path(path.clone(), &a.schema)?;
        w.write_chunk(&a.schema, a.chunk)?;

        assert!(check_file(&mut fs::File::open(path)?)?);
        Ok(())
    }

    #[test]
    fn can_iter_forward_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create(legacy_schema(), Config::parquet())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(
                SchemaChunk::try_from(LegacyRecords([record].to_vec()))?,
                None,
            )?;
        }
        let _ = w.close()?;

        let reader = s.iter()?;

        assert_eq!(collect_records(schema, reader), records);
        Ok(())
    }

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let mut records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create(legacy_schema(), Config::parquet())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(
                SchemaChunk::try_from(LegacyRecords([record].to_vec()))?,
                None,
            )?;
        }
        let _ = w.close()?;

        records.reverse();

        let reader = s.iter()?;
        assert_eq!(collect_records(schema, reader.rev()), records);
        Ok(())
    }

    #[test]
    fn round_trip1_2() -> Result<()> {
        let path = PathBuf::from("tests/data/v1.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let r = s.iter()?;
        assert_eq!(collect_records(r.schema().clone(), r), records);
        Ok(())
    }

    #[test]
    fn round_trip2() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let schema = legacy_schema();
        let mut w = s.create(schema.clone(), Config::parquet())?;
        w.log_arrow(
            SchemaChunk::try_from(LegacyRecords(records[0..10].to_vec()))?,
            None,
        )?;
        w.log_arrow(
            SchemaChunk::try_from(LegacyRecords(records[10..].to_vec()))?,
            None,
        )?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.iter()?;
        assert_eq!(collect_records(r.schema().clone(), r), records);
        Ok(())
    }

    #[test]
    fn schema_change() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::parquet())?;
        w.log_arrow(a, None)?;
        let b = inferences_schema_b();
        assert!(!w.check_schema(&b.schema));
        assert!(w.log_arrow(b, None).is_err());
        Ok(())
    }

    #[test]
    fn schema_file_metadata() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let mut a = inferences_schema_a();
        a.schema
            .metadata
            .insert("pipeline.name".to_string(), "pied-piper".to_string());
        a.schema
            .metadata
            .insert("pipeline.version".to_string(), "3.1".to_string());
        let mut w = s.create(a.schema.clone(), Config::parquet())?;
        w.log_arrow(a, None)?;
        w.close()?;

        let schema = s.iter()?.schema().clone();
        assert_eq!(schema.metadata.get("pipeline.name").unwrap(), "pied-piper");
        assert_eq!(schema.metadata.get("pipeline.version").unwrap(), "3.1");

        Ok(())
    }

    #[test]
    fn nested() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_nested();
        let mut w = s.create(a.schema.clone(), Config::parquet())?;
        w.log_arrow(a, None)?;
        Ok(())
    }

    #[test]
    fn large_records() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let large: String = (0..100 * 1024).map(|_| "x").collect();
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}-{}", ix, large))),
        );

        let mut w = s.create(legacy_schema(), Config::parquet())?;
        w.log_arrow(
            SchemaChunk::try_from(LegacyRecords(records[0..10].to_vec()))?,
            None,
        )?;
        w.log_arrow(
            SchemaChunk::try_from(LegacyRecords(records[10..].to_vec()))?,
            None,
        )?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.iter()?;
        assert_eq!(collect_records(r.schema().clone(), r), records);

        Ok(())
    }

    #[test]
    fn test_open_drop_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("open-drop.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::parquet())?;
        w.log_arrow(a.clone(), None)?;
        drop(w);

        assert!(check_file(&mut fs::File::open(path)?)?);

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_partial_write_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = super::Segment::new(path.clone())?;

        let a = inferences_schema_a();
        let mut w = Writer::from_path(s.path.clone(), &a.schema)?;
        w.write_chunk(&a.schema, a.chunk.clone())?;
        w.checkpoint()?;
        let len = w.writer.offset();
        drop(w);

        std::fs::File::options()
            .append(true)
            .open(&path)?
            .set_len(len - 4)?;

        assert!(check_file(&mut fs::File::open(path)?)?);

        let mut iter = s.read(None)?;
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(iter.next().is_none());

        Ok(())
    }

    #[sample_test]
    fn arbitrary_chunk(#[sample(deep_chunk(3, 100).sample_one())] chunk: ChainedChunk) {
        let chunk = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        use sample_std::Sample;
        let name = sample_std::Regex::new("[a-z]{4, 8}");
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
        let mut w = s.create(schema.clone(), Config::nocommit()).unwrap();
        let expected = SchemaChunk {
            schema,
            chunk: chunk.clone(),
        };
        w.log_arrow(expected, None).unwrap();
        w.close().unwrap();

        let r = s.iter().unwrap();
        let chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(chunks, vec![chunk]);
    }

    #[sample_test]
    fn arbitrary_many_chunk(
        #[sample(deep_chunk(5, 100).sample_many(2..10))] chunk: ChainedMultiChunk,
    ) {
        let chunks = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let name = Regex::new("[a-z]{4, 8}");
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
        let mut w = s.create(schema.clone(), Config::nocommit()).unwrap();

        for chunk in &chunks {
            let expected = SchemaChunk {
                schema: schema.clone(),
                chunk: chunk.clone(),
            };
            w.log_arrow(expected, None).unwrap();
        }
        w.close().unwrap();

        let r = s.iter().unwrap();
        let actual_chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(actual_chunks, chunks);
    }
}

use anyhow::Result;
use plateau_transport::SegmentChunk;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::{fs, ops::Range};
use tracing::{error, warn};

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

struct Writer {
    writer: FileWriter<fs::File>,
}

impl Writer {
    pub(super) fn create(file: fs::File, schema: &Schema) -> Result<Self> {
        let options = WriteOptions { compression: None };
        Ok(Self {
            writer: FileWriter::try_new(file, schema.clone(), None, options)?,
        })
    }

    pub(super) fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        self.writer.write(&chunk, None).map_err(Into::into)
    }

    pub(super) fn end(&mut self) -> Result<()> {
        self.writer.finish().map_err(Into::into)
    }
}

fn recover(path: impl AsRef<Path>) -> Result<(fs::File, FileMetadata)> {
    let display_path = path.as_ref().display();
    warn!("attempting recovery of {display_path:?}");

    // Read the inner ipc stream, copy to new file, then close that file. This
    // should replicate all readable data from the file (which will be all IPC
    // messages before data loss) into a new file with a proper footer.
    let mut broken = fs::File::open(&path)?;
    broken.seek(std::io::SeekFrom::Start(8))?;

    let mut recovery_path = PathBuf::from(path.as_ref());
    recovery_path.set_extension("recovery");
    let recovery = fs::File::create(&recovery_path)?;

    let metadata = read_stream_metadata(&mut broken)?;
    let schema = metadata.schema.clone();
    let reader = StreamReader::new(broken, metadata, None);

    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::try_new(recovery, schema.clone(), None, options)?;

    let mut recovered_chunks = 0;
    let mut recovered_rows = 0;
    for result in reader.take_while(|state| matches!(state, Ok(StreamState::Some(..)) | Err(..))) {
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
                error!("error reading cache segment: {:?}", e);
            }
        }
    }
    writer.finish()?;
    writer.into_inner().sync_all()?;
    warn!("recovered {recovered_rows} rows in {recovered_chunks} chunks from {display_path:?}");

    // Now, we should be able to re-read a correct footer.
    let mut recovery = fs::File::open(&recovery_path)?;
    let metadata = read_file_metadata(&mut recovery)?;

    // Once we've validated the footer, move the recovered file back over the
    // broken one to speed future reads.
    fs::rename(recovery_path, path)?;

    Ok((recovery, metadata))
}

struct Reader {
    metadata: FileMetadata,
    file: fs::File,
    dictionaries: Dictionaries,
    message_scratch: Vec<u8>,
    data_scratch: Vec<u8>,

    iter_range: Range<usize>,
}

impl Reader {
    pub(super) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = fs::File::open(&path)?;

        let mut data_scratch = vec![];
        let message_scratch = vec![];
        let metadata = read_file_metadata(&mut file).or_else::<anyhow::Error, _>(|e| {
            let display_path = path.as_ref().display();
            error!("error reading {display_path:?}: {e:?}");
            let (new_file, metadata) = recover(path)?;
            file = new_file;
            Ok(metadata)
        })?;
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

#[cfg(test)]
mod test {
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
    use crate::segment::*;

    #[test]
    fn can_iter_forward_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create2(legacy_schema(), Config::default())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(
                SchemaChunk::try_from(LegacyRecords([record].to_vec()))?,
                None,
            )?;
        }
        let _ = w.close()?;

        let (_, reader) = s.iter()?;

        assert_eq!(collect_records(schema, reader), records);
        Ok(())
    }

    impl Writer {
        fn create_path(path: impl AsRef<Path>, schema: &Schema) -> Result<Self> {
            Self::create(fs::File::create(path)?, schema)
        }
    }

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let mut records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let schema = legacy_schema();
        let mut w = Writer::create_path(&path, &schema)?;
        for record in records.clone() {
            w.write_chunk(SchemaChunk::try_from(LegacyRecords([record].to_vec()))?.chunk)?;
        }
        let _ = w.end()?;

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
        let mut w = Writer::create_path(&path, &a.schema)?;
        w.write_chunk(a.chunk)?;
        Ok(())
    }

    #[test]
    fn large_records() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.arrow");

        let large: String = (0..100 * 1024).map(|_| "x").collect();
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}-{}", ix, large))),
        );

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

    #[test]
    fn test_open_drop_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("open-drop.arrow");

        let a = inferences_schema_a();
        let mut w = Writer::create_path(&path, &a.schema)?;
        w.write_chunk(a.chunk.clone())?;
        drop(w);

        let mut r = Reader::open(&path)?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_partial_write_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.arrow");

        let a = inferences_schema_a();
        let mut w = Writer::create_path(&path, &a.schema)?;
        w.write_chunk(a.chunk.clone())?;
        w.write_chunk(a.chunk.clone())?;
        drop(w);
        let len = fs::File::open(&path)?.metadata()?.len();

        std::fs::File::options()
            .append(true)
            .open(&path)?
            .set_len(len - 40)?;

        let mut iter = Reader::open(&path)?;
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(iter.next().is_none());

        Ok(())
    }

    #[sample_test]
    fn arbitrary_chunk(#[sample(deep_chunk(3, 100).sample_one())] chunk: ChainedChunk) {
        let chunk = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.arrow");

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
        let mut w = Writer::create_path(&path, &schema).unwrap();
        w.write_chunk(chunk.clone()).unwrap();
        w.end().unwrap();

        let r = Reader::open(&path).unwrap();
        let chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(chunks, vec![chunk]);
    }

    #[sample_test]
    fn arbitrary_many_chunk(
        #[sample(deep_chunk(5, 100).sample_many(2..10))] chunk: ChainedMultiChunk,
    ) {
        let chunks = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.arrow");

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

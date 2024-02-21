use super::{validate_header, PLATEAU_HEADER};
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
    fs,
    io::{Read, Seek, SeekFrom, Write},
    iter,
    path::Path,
};
use tracing::{debug, error, warn};

pub(super) struct Writer {
    pub(super) writer: FileWriter<fs::File>,
    key_value_metadata: Option<Vec<KeyValue>>,
    options: WriteOptions,
}

impl Writer {
    pub(super) fn create(file: fs::File, schema: &Schema) -> Result<Self> {
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

        Ok(Self {
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

    pub(super) fn write_checkpoint(&self, path: impl AsRef<Path>) -> Result<fs::File> {
        let metadata = self
            .writer
            .compute_metadata(self.key_value_metadata.clone())?;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write header
        file.write_all(PLATEAU_HEADER.as_bytes())?;

        // Write offset
        let offset_bytes = self.writer.offset().to_le_bytes();
        file.write_all(&offset_bytes)?;

        // Then, write the header
        let mut protocol = TCompactOutputProtocol::new(&mut file);
        let metadata_len = metadata.write_to_out_protocol(&mut protocol)? as i32;
        protocol.flush()?;
        drop(protocol);
        file.write_all(&metadata_len.to_le_bytes())?;

        Ok(file)
    }

    pub(super) fn end(&mut self) -> Result<()> {
        self.writer.end(self.key_value_metadata.clone())?;
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
        segment.write_all("PAR1".as_bytes())?;
        warn!("successfully recovered checkpoint {:?}", checkpoint_path);
    }

    let mut f = fs::File::open(path)?;
    read_metadata(&mut f).map_err(anyhow::Error::from)
}

/// Reads chunks in forward or reverse order, with lazy caching of chunk data
pub(super) struct Reader {
    pub(super) schema: Option<Schema>,
    reader: Option<FileReader2<fs::File>>,
    next_ix: usize,
    next_back_ix: usize,
    chunks: Vec<SegmentChunk>,
}

impl Reader {
    fn empty() -> Self {
        Self {
            schema: None,
            reader: None,
            next_ix: 0,
            next_back_ix: 0,
            chunks: vec![],
        }
    }

    pub(super) fn open(path: impl AsRef<Path>, checkpoint_path: impl AsRef<Path>) -> Result<Self> {
        let mut f = fs::File::open(&path)?;
        if !path.as_ref().exists() {
            return Ok(Self::empty());
        }

        let metadata = match read_metadata(&mut f) {
            Ok(data) => data,
            Err(e) => {
                debug!("error reading checkpoint: {e:?}");
                drop(f);
                if checkpoint_path.as_ref().exists() {
                    recover(&path, &checkpoint_path)?
                } else {
                    return Ok(Self::empty());
                }
            }
        };

        let schema = infer_schema(&metadata)?;
        let file = fs::File::open(&path)?;

        let len = metadata.row_groups.len();
        let reader = FileReader2::new(file, metadata.row_groups, schema.clone(), None, None, None);

        Ok(Self {
            schema: Some(schema),
            reader: Some(reader),
            next_ix: 0,
            next_back_ix: len,
            chunks: vec![],
        })
    }
}

impl Iterator for Reader {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ix >= self.next_back_ix {
            None
        } else {
            let index = self.next_ix;
            self.next_ix += 1;
            if self.chunks.len() < self.next_ix {
                match self.reader.as_mut().and_then(|r| r.next()) {
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

impl DoubleEndedIterator for Reader {
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

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let mut records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create2(legacy_schema(), Config::default())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(
                SchemaChunk::try_from(LegacyRecords([record].to_vec()))?,
                None,
            )?;
        }
        let _ = w.close()?;

        records.reverse();

        let (_, reader) = s.iter()?;
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

        let (schema, r) = s.iter()?;
        assert_eq!(collect_records(schema, r), records);
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
        let mut w = s.create2(schema.clone(), Config::default())?;
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

        let (schema, r) = s.iter()?;
        assert_eq!(collect_records(schema, r), records);
        Ok(())
    }

    #[test]
    fn schema_change() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
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
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a, None)?;
        w.close()?;

        let (schema, _) = s.iter()?;
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
        let mut w = s.create2(a.schema.clone(), Config::default())?;
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

        let mut w = s.create2(legacy_schema(), Config::default())?;
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

        let (schema, r) = s.iter()?;
        assert_eq!(collect_records(schema, r), records);

        Ok(())
    }

    #[test]
    fn test_open_drop_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("open-drop.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), None)?;
        drop(w);

        let (_, mut r) = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_partial_write_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        let len = w.writer.writer.offset();
        drop(w);

        std::fs::File::options()
            .append(true)
            .open(path)?
            .set_len(len - 4)?;

        let (_, mut iter) = s.iter()?;
        // XXX - this has to be a bug, right? we only write one chunk above...
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn test_partial_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        drop(w);

        let f = std::fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let (_, mut r) = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_cache_updates() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();

        let all_counts = vec![1, 3, 4, 2, 1];
        for ix in 1..all_counts.len() {
            let mut chunk = a.chunk.clone();
            let mut w = s.create2(a.schema.clone(), Config::default())?;

            for count in &all_counts[0..ix] {
                let new_parts: Vec<_> = std::iter::once(chunk.clone())
                    .chain(std::iter::repeat(a.chunk.clone()).take(*count))
                    .collect();
                chunk = crate::chunk::concatenate(&new_parts)?;
                w.update_cache(chunk.clone())?;
            }

            drop(w);

            let (_, mut r) = s.iter()?;
            assert_eq!(r.next().map(|v| v.unwrap()), Some(chunk));
        }

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
        let mut w = s.create2(schema.clone(), Config::nocommit()).unwrap();
        let expected = SchemaChunk {
            schema,
            chunk: chunk.clone(),
        };
        w.log_arrow(expected, None).unwrap();
        w.close().unwrap();

        let (_, r) = s.iter().unwrap();
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
        let mut w = s.create2(schema.clone(), Config::nocommit()).unwrap();

        for chunk in &chunks {
            let expected = SchemaChunk {
                schema: schema.clone(),
                chunk: chunk.clone(),
            };
            w.log_arrow(expected, None).unwrap();
        }
        w.close().unwrap();

        let (_, r) = s.iter().unwrap();
        let actual_chunks = r.collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(actual_chunks, chunks);
    }
}

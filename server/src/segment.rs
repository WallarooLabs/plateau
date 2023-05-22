//! A segment contains a bundle of time and logically indexed records.
//!
//! Currently, the only supported segment format is local Parquet files.
use crate::arrow2::datatypes::Schema;
use crate::arrow2::io::parquet::read::FileReader as FileReader2;
use crate::arrow2::io::parquet::read::{infer_schema, read_metadata};
use crate::arrow2::io::parquet::write::FileWriter as FileWriter2;
use crate::arrow2::io::parquet::write::{
    transverse, CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use anyhow::Result;
#[cfg(test)]
use chrono::{Duration, TimeZone, Utc};
use std::borrow::Borrow;
use std::convert::TryFrom;
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::iter;
#[cfg(test)]
use std::sync::Arc;
use std::{fs, path::Path, path::PathBuf};

#[cfg(test)]
use parquet::{
    column::reader::ColumnReader,
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};

pub use crate::chunk::Record;
use plateau_transport::{arrow2, SchemaChunk, SegmentChunk};

// these are incomplete; they are currently only used in testing
#[cfg(test)]
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.time.hash(state);
        let data_string = String::from_utf8(self.message.clone()).unwrap();
        data_string.hash(state);
    }
}

pub struct Segment {
    path: PathBuf,
}

impl Segment {
    pub(crate) fn at(path: PathBuf) -> Segment {
        Segment { path }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    fn file(&self) -> Result<fs::File> {
        Ok(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?)
    }

    #[cfg(test)]
    pub(crate) fn create(&self) -> Result<SegmentWriter> {
        // TODO: better schema for timestamps.
        // something like (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS));
        let message_type = "
        message schema {
            REQUIRED INT64 time;
            REQUIRED BYTE_ARRAY message (UTF8);
        }
        ";
        let schema = Arc::new(parse_message_type(message_type)?);
        let props = Arc::new(WriterProperties::builder().build());

        let file = self.file()?;
        let writer = SerializedFileWriter::new(file.try_clone()?, schema, props)?;

        Ok(SegmentWriter {
            path: self.path.clone(),
            file,
            writer,
        })
    }

    pub(crate) fn create2(&self, schema: Schema) -> Result<SegmentWriter2> {
        // TODO: ideally we'd use compression, but this currently causes nasty dependency issues
        // between parquet2 and parquet
        let options = WriteOptions {
            data_pagesize_limit: None,
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
        };

        let file = self.file()?;
        let writer = FileWriter2::try_new(file.try_clone()?, schema.clone(), options)?;
        Ok(SegmentWriter2 {
            path: self.path.clone(),
            file,
            writer,
            schema,
            options,
        })
    }

    #[cfg(test)]
    pub(crate) fn read(&self) -> Result<SegmentReader> {
        let file = fs::File::open(&self.path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(SegmentReader { reader })
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        fs::remove_file(self.path.as_path()).map_err(|e| e.into())
    }

    pub(crate) fn validate(&self) -> bool {
        self.read_double_ended().is_ok()
    }

    pub(crate) fn read_double_ended(&self) -> Result<DoubleEndedChunkReader> {
        DoubleEndedChunkReader::open(self.path.as_path())
    }
}

#[cfg(test)]
pub(crate) struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    writer: SerializedFileWriter<fs::File>,
}

#[cfg(test)]
impl SegmentWriter {
    fn log(&mut self, mut record: Vec<Record>) -> Result<()> {
        let writer = &mut self.writer;
        let mut row_group_writer = writer.next_row_group()?;

        let mut times = vec![];
        let mut messages = vec![];
        for r in record.drain(..) {
            let dt = r
                .time
                .signed_duration_since(Utc.timestamp_opt(0, 0).unwrap());
            times.push(dt.num_milliseconds());
            messages.push(ByteArray::from(r.message));
        }

        if let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&times, None, None)?;
                }
                _ => anyhow::bail!("invalid column type"),
            };
            row_group_writer.close_column(col_writer)?;
        }

        if let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&messages, None, None)?;
                }
                _ => anyhow::bail!("invalid column type"),
            };
            row_group_writer.close_column(col_writer)?;
        }

        writer.close_row_group(row_group_writer)?;
        Ok(())
    }
}

#[cfg(test)]
impl CloseArrow for SegmentWriter {
    fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    fn end(&mut self) -> Result<()> {
        self.writer.close()?;
        self.file.sync_data()?;
        Ok(())
    }
}

pub struct SegmentWriter2 {
    path: PathBuf,
    file: fs::File,
    writer: FileWriter2<fs::File>,
    schema: Schema,
    options: WriteOptions,
}

impl SegmentWriter2 {
    pub fn check_schema(&self, schema: &Schema) -> bool {
        &self.schema == schema
    }

    pub fn log_arrow<S: Borrow<Schema> + Clone + PartialEq>(
        &mut self,
        data: SchemaChunk<S>,
    ) -> Result<()> {
        if !self.check_schema(data.schema.borrow()) {
            anyhow::bail!("cannot use different schemas within the same segment");
        }

        let encodings: Vec<_> = data
            .schema
            .borrow()
            .fields
            .iter()
            .map(|field| transverse(field.data_type(), |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(
            iter::once(Ok(data.chunk)),
            &self.schema,
            self.options,
            encodings,
        )?;

        for group in row_groups {
            self.writer.write(group?)?;
        }

        Ok(())
    }
}

impl CloseArrow for SegmentWriter2 {
    fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    fn end(&mut self) -> Result<()> {
        self.writer.end(None)?;
        self.file.sync_data()?;
        Ok(())
    }
}

pub(crate) trait CloseArrow: Sized {
    fn get_path(&self) -> &Path;
    fn end(&mut self) -> Result<()>;

    /// Return an estimate of the on-disk size of this file. Note that this
    /// will _not_ include the final metadata footer.
    fn size_estimate(&self) -> Result<usize> {
        Ok(usize::try_from(fs::metadata(self.get_path())?.len())?)
    }

    fn close(mut self) -> Result<usize> {
        self.end()?;

        // NOTE: the file data is now synchronized, but the file itself may not appear in the
        // parent directory on crash unless we fsync that too.
        let mut parent = self.get_path().to_path_buf();
        parent.pop();
        let directory = fs::File::open(&parent)?;
        directory.sync_all()?;

        self.size_estimate()
    }
}

#[cfg(test)]
pub(crate) struct SegmentReader {
    reader: SerializedFileReader<fs::File>,
}

#[cfg(test)]
impl SegmentReader {
    pub(crate) fn into_chunk_iter(self) -> impl Iterator<Item = Result<Vec<Record>>> {
        let metadata = self.reader.metadata().clone();

        let mut def_levels = vec![0; 8];
        let mut rep_levels = vec![0; 8];

        (0..metadata.num_row_groups()).into_iter().map(move |i| {
            let row_group_reader = self.reader.get_row_group(i)?;
            let row_group_metadata = metadata.row_group(i);
            let rows = usize::try_from(row_group_metadata.num_rows())?;
            let mut tvs = vec![0; rows];

            // NOTE - this buffer is dynamically resized by the reader when the
            // on-disk size exceeds the allocated space, see the large_records
            // test below for proof
            let mut arrs: Vec<ByteArray> = vec![ByteArray::from(vec![0; 128])]
                .into_iter()
                .cycle()
                .take(rows)
                .collect();

            let mut column_reader = row_group_reader.get_column_reader(0)?;
            match column_reader {
                ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                    let mut read = 0;
                    while read < rows {
                        let batch = typed_reader.read_batch(
                            rows,
                            None,
                            None,
                            &mut tvs.as_mut_slice()[read..],
                        )?;
                        read += batch.0;
                    }
                }
                _ => anyhow::bail!("invalid column type"),
            };

            let mut column_reader = row_group_reader.get_column_reader(1)?;
            match column_reader {
                ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                    let mut read = 0;
                    while read < rows {
                        let batch = typed_reader.read_batch(
                            rows,
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut arrs.as_mut_slice()[read..],
                        )?;
                        read += batch.0;
                    }
                }
                _ => anyhow::bail!("invalid column type"),
            };

            let result: Vec<_> = tvs
                .into_iter()
                .zip(arrs.into_iter())
                .map(|(tv, message)| {
                    let time = Utc.timestamp_opt(0, 0).unwrap() + Duration::milliseconds(tv);
                    Record {
                        time,
                        message: message.data().to_vec(),
                    }
                })
                .collect();

            Ok(result)
        })
    }
}

pub(crate) struct DoubleEndedChunkReader {
    pub path: PathBuf,
    pub schema: Schema,
    metadata: arrow2::io::parquet::read::FileMetaData,
}
impl DoubleEndedChunkReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut f = fs::File::open(path)?;
        let metadata = read_metadata(&mut f)?;
        let schema = infer_schema(&metadata)?;

        Ok(Self {
            path: path.to_path_buf(),
            schema,
            metadata,
        })
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Result<SegmentChunk>> {
        let file = fs::File::open(self.path.clone()).unwrap();

        let rev_idx = self.metadata.row_groups.len();
        let fwd_rdr = FileReader2::new(
            file,
            self.metadata.row_groups.clone(),
            self.schema.clone(),
            None,
            None,
            None,
        );

        DoubleEndedChunkIterator {
            fwd_reader: fwd_rdr,
            fwd_position: 0,
            rev_position: rev_idx,
            chunks: vec![],
            full_read: false,
        }
    }
}

/// Reads chunks in forward or reverse order, with lazy caching of chunk data
pub(crate) struct DoubleEndedChunkIterator {
    fwd_reader: FileReader2<fs::File>,
    fwd_position: usize,
    rev_position: usize,
    chunks: Vec<Option<SegmentChunk>>,
    full_read: bool,
}
impl Iterator for DoubleEndedChunkIterator {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fwd_position >= self.rev_position {
            None
        } else {
            self.fwd_position += 1;
            if self.chunks.len() < self.fwd_position {
                match self.fwd_reader.next() {
                    Some(r) => self.chunks.push(r.ok()),
                    None => {
                        // we reached the end of the segment
                        self.full_read = true;
                        return None;
                    }
                }
            }

            Some(self.chunks[self.fwd_position - 1].clone().ok_or_else(|| {
                arrow2::error::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "chunk read failure",
                ))
                .into()
            }))
        }
    }
}
impl DoubleEndedIterator for DoubleEndedChunkIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.rev_position <= self.fwd_position {
            None
        } else {
            if !self.full_read {
                let fwd = self.fwd_position;
                let _ = self.into_iter().collect::<Vec<_>>();
                self.fwd_position = fwd;
            }
            self.rev_position -= 1;
            Some(self.chunks[self.rev_position].clone().ok_or_else(|| {
                arrow2::error::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "chunk read failure",
                ))
                .into()
            }))
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::chunk::test::{inferences_nested, inferences_schema_a, inferences_schema_b};
    use crate::chunk::{iter_legacy, legacy_schema, LegacyRecords};
    use tempfile::tempdir;

    pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
        it.map(|(ix, message)| Record {
            time: Utc.timestamp_opt(ix, 0).unwrap(),
            message: message.into_bytes(),
        })
        .collect()
    }
    #[test]
    fn round_trip() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let mut w = s.create()?;
        w.log(records[0..10].to_vec())?;
        w.log(records[10..].to_vec())?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read()?;
        assert_eq!(
            r.into_chunk_iter()
                .flat_map(|b| b.unwrap())
                .collect::<Vec<_>>(),
            records
        );
        Ok(())
    }

    fn collect_records(
        schema: Schema,
        iter: impl Iterator<Item = Result<SegmentChunk, anyhow::Error>>,
    ) -> Vec<Record> {
        iter_legacy(schema, iter).flat_map(Result::unwrap).collect()
    }

    #[test]
    fn can_iter_forward_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create()?;
        for record in records.clone() {
            w.log([record].to_vec())?;
        }
        let _ = w.close()?;

        let reader = DoubleEndedChunkReader::open(path.as_path())?;

        assert_eq!(
            collect_records(reader.schema.clone(), reader.iter()),
            records
        );
        Ok(())
    }

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let mut records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create()?;
        for record in records.clone() {
            w.log([record].to_vec())?;
        }
        let _ = w.close()?;

        records.reverse();

        let reader = DoubleEndedChunkReader::open(path.as_path())?;
        //let result: Vec<_> = r.map(|i| i.unwrap()).collect();
        //let (schema, iter) = r.into_chunk_iter();
        assert_eq!(
            collect_records(reader.schema.clone(), reader.iter().rev()),
            records
        );
        Ok(())
    }

    #[test]
    fn round_trip1_2() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let mut w = s.create()?;
        w.log(records[0..10].to_vec())?;
        w.log(records[10..].to_vec())?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read_double_ended()?;
        assert_eq!(collect_records(r.schema.clone(), r.iter()), records);
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
        let mut w = s.create2(schema)?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[0..10].to_vec(),
        ))?)?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[10..].to_vec(),
        ))?)?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read_double_ended()?;
        assert_eq!(collect_records(r.schema.clone(), r.iter()), records);
        Ok(())
    }

    #[test]
    fn schema_change() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone())?;
        w.log_arrow(a)?;
        let b = inferences_schema_b();
        assert!(!w.check_schema(&b.schema));
        assert!(w.log_arrow(b).is_err());
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
        let mut w = s.create2(a.schema.clone())?;
        w.log_arrow(a)?;
        w.close()?;

        let r = s.read_double_ended()?;
        assert_eq!(
            r.schema.metadata.get("pipeline.name").unwrap(),
            "pied-piper"
        );
        assert_eq!(r.schema.metadata.get("pipeline.version").unwrap(), "3.1");

        Ok(())
    }

    #[test]
    fn nested() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_nested();
        let mut w = s.create2(a.schema.clone())?;
        w.log_arrow(a)?;
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

        let mut w = s.create()?;
        w.log(records[0..10].to_vec())?;
        w.log(records[10..].to_vec())?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read()?;
        assert_eq!(
            r.into_chunk_iter()
                .flat_map(|b| b.unwrap())
                .collect::<Vec<_>>(),
            records
        );

        Ok(())
    }
}

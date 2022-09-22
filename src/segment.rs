//! A segment contains a bundle of time and logically indexed records.
//!
//! Currently, the only supported segment format is local Parquet files.
use anyhow::Result;
use arrow2::array::{
    MutableArray, MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray, Utf8Array,
};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read::FileReader as FileReader2;
use arrow2::io::parquet::read::{infer_schema, read_metadata};
use arrow2::io::parquet::write::FileWriter as FileWriter2;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::convert::TryFrom;
#[cfg(test)]
use std::hash::{Hash, Hasher};
#[cfg(test)]
use std::sync::Arc;
use std::{fs, path::PathBuf};

use parquet::data_type::ByteArray;
#[cfg(test)]
use parquet::{
    column::reader::ColumnReader,
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub time: DateTime<Utc>,
    pub message: ByteArray,
}

// these are incomplete; they are currently only used in testing
#[cfg(test)]
impl Eq for Record {}

#[cfg(test)]
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.time.hash(state);
        let data_string = String::from_utf8(self.message.data().to_vec()).unwrap();
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
            writer: Writer::Arrow(writer),
        })
    }

    pub(crate) fn create2(&self) -> Result<SegmentWriter> {
        // TODO: better schema for timestamps.
        // something like (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS));
        let time = Field::new("time", DataType::Int64, true);
        let message = Field::new("message", DataType::Utf8, true);
        let schema = Schema::from(vec![time, message]);

        // TODO: ideally we'd use compression, but this currently causes nasty dependency issues
        // between parquet2 and parquet
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
        };

        let file = self.file()?;
        let writer = FileWriter2::try_new(file.try_clone()?, schema.clone(), options)?;
        let writer = WriteArrow2 {
            writer,
            schema,
            options,
        };
        Ok(SegmentWriter {
            path: self.path.clone(),
            file,
            writer: Writer::Arrow2(writer),
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
        self.read2().is_ok()
    }

    pub(crate) fn read2(&self) -> Result<SegmentReader2> {
        let file = fs::File::open(&self.path)?;
        Ok(SegmentReader2::new(file)?)
    }
}

enum Writer {
    #[cfg(test)]
    Arrow(SerializedFileWriter<fs::File>),
    Arrow2(WriteArrow2),
}

struct WriteArrow2 {
    writer: FileWriter2<fs::File>,
    schema: Schema,
    options: WriteOptions,
}

pub(crate) struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    writer: Writer,
}

impl SegmentWriter {
    pub(crate) fn log(&mut self, record: Vec<Record>) -> Result<()> {
        match &mut self.writer {
            #[cfg(test)]
            Writer::Arrow(ref mut sfw) => Self::log_arrow(sfw, record),
            Writer::Arrow2(ref mut fw) => Self::log_arrow2(fw, record),
        }
    }

    #[cfg(test)]
    fn log_arrow(
        writer: &mut SerializedFileWriter<fs::File>,
        mut record: Vec<Record>,
    ) -> Result<()> {
        let mut row_group_writer = writer.next_row_group()?;

        let mut times = vec![];
        let mut messages = vec![];
        for r in record.drain(..) {
            let dt = r.time.signed_duration_since(Utc.timestamp(0, 0));
            times.push(dt.num_milliseconds());
            messages.push(r.message);
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

    fn log_arrow2(writer: &mut WriteArrow2, mut record: Vec<Record>) -> Result<()> {
        let mut times = MutablePrimitiveArray::<i64>::new();
        let mut messages = MutableUtf8Array::<i32>::new();

        for r in record.drain(..) {
            let dt = r.time.signed_duration_since(Utc.timestamp(0, 0));
            times.push(Some(dt.num_milliseconds()));
            messages.push(Some(r.message.as_utf8()?));
        }

        let iter = vec![Chunk::try_new(vec![times.as_box(), messages.as_box()])];

        let encodings = vec![vec![Encoding::Plain], vec![Encoding::Plain]];
        let row_groups =
            RowGroupIterator::try_new(iter.into_iter(), &writer.schema, writer.options, encodings)?;

        for group in row_groups {
            writer.writer.write(group?)?;
        }

        Ok(())
    }

    /// Return an estimate of the on-disk size of this file. Note that this
    /// will _not_ include the final metadata footer.
    pub(crate) fn size_estimate(&self) -> Result<usize> {
        Ok(usize::try_from(fs::metadata(&self.path)?.len())?)
    }

    pub(crate) fn close(mut self) -> Result<usize> {
        match &mut self.writer {
            #[cfg(test)]
            Writer::Arrow(ref mut w) => {
                w.close()?;
            }
            Writer::Arrow2(ref mut w2) => {
                w2.writer.end(None)?;
            }
        }
        self.file.sync_data()?;

        // NOTE: the file data is now synchronized, but the file itself may not appear in the
        // parent directory on crash unless we fsync that too.
        let mut parent = self.path.clone();
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
                    let time = Utc.timestamp(0, 0) + Duration::milliseconds(tv);
                    Record { time, message }
                })
                .collect();

            Ok(result)
        })
    }
}

pub(crate) struct SegmentReader2 {
    reader: FileReader2<fs::File>,
}

impl SegmentReader2 {
    fn new(mut f: fs::File) -> Result<Self> {
        let metadata = read_metadata(&mut f)?;
        let schema = infer_schema(&metadata)?;

        let schema = schema.filter(|_, f| f.name == "time" || f.name == "message");

        Ok(SegmentReader2 {
            reader: FileReader2::new(f, metadata.row_groups, schema, None, None, None),
        })
    }

    pub(crate) fn into_chunk_iter(self) -> impl Iterator<Item = Result<Vec<Record>>> {
        self.reader.into_iter().map(|chunk| {
            let arrays = chunk?.into_arrays();
            let time = arrays
                .get(0)
                .ok_or(anyhow::anyhow!("missing 'time' column"))
                .and_then(|arr| {
                    arr.as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or(anyhow::anyhow!("invalid 'time' array"))
                })?;
            let message = arrays
                .get(1)
                .ok_or(anyhow::anyhow!("missing 'message' column"))
                .and_then(|arr| {
                    arr.as_any()
                        .downcast_ref::<Utf8Array<i32>>()
                        .ok_or(anyhow::anyhow!("invalid 'message' array"))
                })?;

            Ok(time
                .values_iter()
                .zip(message.values_iter())
                .map(|(tv, m)| Record {
                    time: Utc.timestamp(0, 0) + Duration::milliseconds(*tv),
                    message: ByteArray::from(m),
                })
                .collect())
        })
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use tempfile::tempdir;

    pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
        it.map(|(ix, message)| Record {
            time: Utc.timestamp(ix, 0),
            message: ByteArray::from(message.as_str()),
        })
        .collect()
    }

    #[test]
    fn round_trip() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
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

    #[test]
    fn round_trip1_2() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
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

        let r = s.read2()?;
        assert_eq!(
            r.into_chunk_iter()
                .flat_map(|b| b.unwrap())
                .collect::<Vec<_>>(),
            records
        );
        Ok(())
    }

    #[test]
    fn round_trip2() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let mut w = s.create2()?;
        w.log(records[0..10].to_vec())?;
        w.log(records[10..].to_vec())?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read2()?;
        assert_eq!(
            r.into_chunk_iter()
                .flat_map(|b| b.unwrap())
                .collect::<Vec<_>>(),
            records
        );
        Ok(())
    }

    #[test]
    fn large_records() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
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

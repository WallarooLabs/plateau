//! A segment contains a bundle of time and logically indexed records.
//!
//! Currently, the only supported segment format is local Parquet files.
use anyhow::{bail, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::convert::TryFrom;
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::{fs, path::PathBuf, sync::Arc};

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

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        let writer = SerializedFileWriter::new(file.try_clone()?, schema, props)?;

        Ok(SegmentWriter {
            path: self.path.clone(),
            file,
            writer,
        })
    }

    pub(crate) fn read(&self) -> Result<SegmentReader> {
        let file = fs::File::open(&self.path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(SegmentReader { reader })
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        fs::remove_file(self.path.as_path()).map_err(|e| e.into())
    }

    pub(crate) fn validate(&self) -> bool {
        self.read().is_ok()
    }
}

pub(crate) struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    writer: SerializedFileWriter<std::fs::File>,
}

impl SegmentWriter {
    pub(crate) fn log(&mut self, mut record: Vec<Record>) -> Result<()> {
        let mut row_group_writer = self.writer.next_row_group()?;

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
                _ => bail!("invalid column type"),
            };
            row_group_writer.close_column(col_writer)?;
        }

        if let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&messages, None, None)?;
                }
                _ => bail!("invalid column type"),
            };
            row_group_writer.close_column(col_writer)?;
        }

        self.writer.close_row_group(row_group_writer)?;
        Ok(())
    }

    /// Return an estimate of the on-disk size of this file. Note that this
    /// will _not_ include the final metadata footer.
    pub(crate) fn size_estimate(&self) -> Result<usize> {
        Ok(usize::try_from(fs::metadata(&self.path)?.len())?)
    }

    pub(crate) fn close(mut self) -> Result<usize> {
        self.writer.close()?;
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

pub(crate) struct SegmentReader {
    reader: SerializedFileReader<fs::File>,
}

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
                _ => bail!("invalid column type"),
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
                _ => bail!("invalid column type"),
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

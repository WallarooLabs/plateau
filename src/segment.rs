//! A segment contains a bundle of time and logically indexed records.
//!
//! Currently, the only supported segment format is local Parquet files.
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::convert::{TryFrom, TryInto};
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

    pub(crate) fn create(&self) -> SegmentWriter {
        // TODO: better schema for timestamps.
        // something like (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS));
        let message_type = "
        message schema {
            REQUIRED INT64 time;
            REQUIRED BYTE_ARRAY message (UTF8);
        }
        ";
        // TODO: fix all these unwraps()
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .unwrap();
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();

        SegmentWriter {
            path: self.path.clone(),
            file,
            writer,
        }
    }

    pub(crate) fn read(&self) -> SegmentReader {
        let file = fs::File::open(&self.path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        SegmentReader { reader }
    }
}

pub(crate) struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    writer: SerializedFileWriter<std::fs::File>,
}

impl SegmentWriter {
    pub(crate) fn log(&mut self, mut record: Vec<Record>) {
        let mut row_group_writer = self.writer.next_row_group().unwrap();

        let mut times = vec![];
        let mut messages = vec![];
        for r in record.drain(..) {
            let dt = r.time.signed_duration_since(Utc.timestamp(0, 0));
            times.push(dt.num_milliseconds());
            messages.push(r.message);
        }

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&times, None, None).unwrap();
                }
                _ => panic!("invalid column type"),
            };
            row_group_writer.close_column(col_writer).unwrap();
        }

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&messages, None, None).unwrap();
                }
                _ => panic!("invalid column type"),
            };
            row_group_writer.close_column(col_writer).unwrap();
        }
        self.writer.close_row_group(row_group_writer).unwrap();
    }

    /// Return an estimate of the on-disk size of this file. Note that this
    /// will _not_ include the final metadata footer.
    pub(crate) fn size_estimate(&self) -> usize {
        fs::metadata(&self.path).unwrap().len() as usize
    }

    pub(crate) fn close(mut self) -> usize {
        self.writer.close().unwrap();
        self.file.sync_data().unwrap();
        self.size_estimate()
    }
}

pub(crate) struct SegmentReader {
    reader: SerializedFileReader<fs::File>,
}

impl SegmentReader {
    pub(crate) fn read_all(&self) -> Vec<Record> {
        let metadata = self.reader.metadata();

        let mut results = vec![];
        let mut def_levels = vec![0; 8];
        let mut rep_levels = vec![0; 8];

        for i in 0..metadata.num_row_groups() {
            let row_group_reader = self.reader.get_row_group(i).unwrap();
            let row_group_metadata = metadata.row_group(i);
            let rows = usize::try_from(row_group_metadata.num_rows()).unwrap();
            let mut tvs = vec![0; rows];

            // NOTE - this buffer is dynamically resized by the reader when the
            // on-disk size exceeds the allocated space, see the large_records
            // test below for proof
            let mut arrs: Vec<ByteArray> = vec![ByteArray::from(vec![0; 128])]
                .into_iter()
                .cycle()
                .take(rows)
                .collect();

            let mut column_reader = row_group_reader.get_column_reader(0).unwrap();
            match column_reader {
                ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                    let mut read = 0;
                    while read < rows {
                        let batch = typed_reader
                            .read_batch(rows, None, None, &mut tvs.as_mut_slice()[read..])
                            .expect("batch read");
                        read += batch.0;
                    }
                }
                _ => panic!("invalid column type"),
            };

            let mut column_reader = row_group_reader.get_column_reader(1).unwrap();
            match column_reader {
                ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                    let mut read = 0;
                    while read < rows {
                        let batch = typed_reader
                            .read_batch(
                                rows,
                                Some(&mut def_levels),
                                Some(&mut rep_levels),
                                &mut arrs.as_mut_slice()[read..],
                            )
                            .expect("batch read");
                        read += batch.0;
                    }
                }
                _ => panic!("invalid column type"),
            };

            results.extend(tvs.into_iter().zip(arrs.into_iter()).map(|(tv, message)| {
                let time = Utc.timestamp(0, 0) + Duration::milliseconds(tv);
                Record { time, message }
            }));
        }

        results
    }
}

mod test {
    use super::*;
    use tempfile::tempdir;

    fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
        it.map(|(ix, message)| Record {
            time: Utc.timestamp(ix, 0),
            message: ByteArray::from(message.as_str()),
        })
        .collect()
    }

    #[test]
    fn round_trip() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let mut w = s.create();
        w.log(records[0..10].to_vec());
        w.log(records[10..].to_vec());
        let size = w.close();
        assert!(size > 0);

        let r = s.read();
        assert_eq!(r.read_all(), records);
    }

    #[test]
    fn large_records() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
        let large: String = (0..100 * 1024).map(|_| "x").collect();
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}-{}", ix, large))),
        );

        let mut w = s.create();
        w.log(records[0..10].to_vec());
        w.log(records[10..].to_vec());
        let size = w.close();
        assert!(size > 0);

        let r = s.read();
        assert_eq!(r.read_all(), records);
    }
}

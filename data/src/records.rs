//! Helpers for the legacy "records" format plateau initially supported
//!
//! These are used extensively in tests, though the arrow format has completely
//! supplanted the records format in actual usage.
use std::borrow::Borrow;

use chrono::{DateTime, TimeZone, Utc};

use crate::{
    arrow2::{
        array::{MutableArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array},
        chunk::Chunk,
        datatypes::{DataType, Field, Metadata},
    },
    chunk::{get_time, parse_time, type_name_of_val, Schema},
    transport::{ChunkError, SchemaChunk, SegmentChunk},
    IndexedChunk, LimitedBatch, Ordering,
};

#[derive(Clone, Debug)]
pub struct LegacyRecords(pub Vec<Record>);

pub fn chunk_into_legacy(chunk: SegmentChunk, order: Ordering) -> Vec<Record> {
    let mut records = LegacyRecords::try_from(SchemaChunk {
        schema: legacy_schema(),
        chunk,
    })
    .unwrap()
    .0;
    if order.is_reverse() {
        records.reverse();
    }

    records
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Record {
    pub time: DateTime<Utc>,
    pub message: Vec<u8>,
}

impl<S: Borrow<Schema> + Clone + PartialEq> TryFrom<SchemaChunk<S>> for LegacyRecords {
    type Error = ChunkError;

    fn try_from(orig: SchemaChunk<S>) -> Result<Self, Self::Error> {
        let arrays = orig.chunk.into_arrays();

        let time = get_time(&arrays, orig.schema.borrow())?;
        if orig.schema.borrow().fields.get(1).map(|f| f.name.as_str()) != Some("message") {
            return Err(ChunkError::BadColumn(1, "message"));
        }

        let message = arrays
            .get(1)
            .ok_or(ChunkError::BadColumn(1, "message"))
            .and_then(|arr| {
                arr.as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .ok_or_else(|| {
                        ChunkError::InvalidColumnType("message", "utf8", type_name_of_val(arr))
                    })
            })?;

        Ok(Self(
            time.values_iter()
                .zip(message.values_iter())
                .map(|(tv, m)| Record {
                    time: parse_time(*tv),
                    message: m.bytes().collect(),
                })
                .collect(),
        ))
    }
}

impl TryFrom<LegacyRecords> for SchemaChunk<Schema> {
    type Error = ChunkError;

    fn try_from(value: LegacyRecords) -> Result<Self, Self::Error> {
        let mut records = value.0;
        let mut times = MutablePrimitiveArray::<i64>::new();
        let mut messages = MutableUtf8Array::<i32>::new();

        for r in records.drain(..) {
            let dt = r
                .time
                .signed_duration_since(Utc.timestamp_opt(0, 0).unwrap());
            times.push(Some(dt.num_milliseconds()));
            messages.push(Some(
                std::str::from_utf8(&r.message)
                    .map_err(|_| ChunkError::FailedEncoding)?
                    .to_string(),
            ));
        }

        Ok(Self {
            schema: legacy_schema(),
            chunk: Chunk::try_new(vec![times.as_box(), messages.as_box()])
                .map_err(|_| ChunkError::LengthMismatch)?,
        })
    }
}

pub fn iter_legacy(
    schema: Schema,
    iter: impl Iterator<Item = anyhow::Result<SegmentChunk>>,
) -> impl Iterator<Item = anyhow::Result<Vec<Record>>> {
    iter.map(move |chunk| {
        chunk.and_then(|chunk| {
            LegacyRecords::try_from(SchemaChunk {
                schema: &schema,
                chunk,
            })
            .map_err(Into::into)
            .map(|r| r.0)
        })
    })
}

pub fn legacy_schema() -> Schema {
    // TODO: better schema for timestamps.
    // something like (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS));
    Schema {
        fields: vec![
            Field::new("time", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
        ],
        metadata: Metadata::default(),
    }
}

pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
    it.map(|(ix, message)| Record {
        time: Utc.timestamp_opt(ix, 0).unwrap(),
        message: message.into_bytes(),
    })
    .collect()
}

pub fn collect_records(
    schema: Schema,
    iter: impl Iterator<Item = Result<SegmentChunk, anyhow::Error>>,
) -> Vec<Record> {
    iter_legacy(schema, iter).flat_map(Result::unwrap).collect()
}

pub trait IntoRecords {
    fn into_records(self, order: Ordering) -> Vec<Record>;
}

impl IntoRecords for SegmentChunk {
    fn into_records(self, order: Ordering) -> Vec<Record> {
        chunk_into_legacy(self, order)
    }
}

impl IntoRecords for IndexedChunk {
    fn into_records(self, order: Ordering) -> Vec<Record> {
        chunk_into_legacy(self.chunk, order)
    }
}

pub trait TryIntoRecords {
    fn try_into_records(self) -> anyhow::Result<Vec<Record>>;
}

impl TryIntoRecords for LimitedBatch {
    fn try_into_records(self) -> anyhow::Result<Vec<Record>> {
        if self.chunks.is_empty() {
            return Ok(vec![]);
        }

        if let Some(schema) = self.schema {
            use itertools::Itertools;
            iter_legacy(
                schema,
                self.chunks
                    .into_iter()
                    .map(|chunk| Ok(SegmentChunk::from(chunk))),
            )
            .flatten_ok()
            .collect()
        } else {
            Ok(vec![])
        }
    }
}

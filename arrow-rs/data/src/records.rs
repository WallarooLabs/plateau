//! Helpers for the legacy "records" format plateau initially supported
//!
//! These are used extensively in tests, though the arrow format has completely
//! supplanted the records format in actual usage.
use std::borrow::Borrow;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;

use crate::{
    chunk::{get_time, parse_time, type_name_of_val},
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
        let time = get_time(&orig.chunk, orig.schema.borrow())?;

        if orig.schema.borrow().fields()[1].name() != "message" {
            return Err(ChunkError::BadColumn(1, "message"));
        }

        let message = orig
            .chunk
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ChunkError::InvalidColumnType(
                    "message",
                    "utf8",
                    type_name_of_val(orig.chunk.column(1)),
                )
            })?;

        let mut records = Vec::with_capacity(time.len());
        for i in 0..time.len() {
            if time.is_null(i) || message.is_null(i) {
                continue;
            }

            records.push(Record {
                time: parse_time(time.value(i)),
                message: message.value(i).as_bytes().to_vec(),
            });
        }

        Ok(Self(records))
    }
}

impl TryFrom<LegacyRecords> for SchemaChunk<SchemaRef> {
    type Error = ChunkError;

    fn try_from(value: LegacyRecords) -> Result<Self, Self::Error> {
        let records = &value.0;
        let time_values: Vec<i64> = records.iter().map(|r| r.time.timestamp_millis()).collect();

        let message_values: Vec<&str> = records
            .iter()
            .map(|r| std::str::from_utf8(&r.message).map_err(|_| ChunkError::FailedEncoding))
            .collect::<Result<Vec<_>, _>>()?;

        let time_array = Arc::new(Int64Array::from(time_values));
        let message_array = Arc::new(StringArray::from(message_values));

        let schema = legacy_schema();
        let batch = RecordBatch::try_new(schema.clone(), vec![time_array, message_array])
            .map_err(|_| ChunkError::LengthMismatch)?;

        Ok(Self {
            schema,
            chunk: batch,
        })
    }
}

pub fn iter_legacy(
    schema: SchemaRef,
    iter: impl Iterator<Item = anyhow::Result<SegmentChunk>>,
) -> impl Iterator<Item = anyhow::Result<Vec<Record>>> {
    iter.map(move |chunk| {
        chunk.and_then(|chunk| {
            LegacyRecords::try_from(SchemaChunk {
                schema: schema.clone(),
                chunk,
            })
            .map_err(Into::into)
            .map(|r| r.0)
        })
    })
}

// Stub function for API compatibility
pub fn iter_legacy_schema(
    schema: Schema,
    iter: impl Iterator<Item = anyhow::Result<SegmentChunk>>,
) -> impl Iterator<Item = anyhow::Result<Vec<Record>>> {
    iter_legacy(Arc::new(schema), iter)
}

pub fn legacy_schema() -> SchemaRef {
    // Schema for timestamps and messages
    let fields = Fields::from(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]);

    Arc::new(Schema::new(fields))
}

pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
    it.map(|(ix, message)| Record {
        time: Utc.timestamp_opt(ix, 0).unwrap(),
        message: message.into_bytes(),
    })
    .collect()
}

pub fn collect_records(
    schema: SchemaRef,
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
                Arc::new(schema.clone()),
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

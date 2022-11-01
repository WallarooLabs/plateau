//! Utilities for working with the [arrow2::chunk::Chunk] type.
use crate::arrow2::array::{
    Array, BooleanArray, FixedSizeListArray, ListArray, MutableArray, MutablePrimitiveArray,
    MutableUtf8Array, PrimitiveArray, StructArray, Utf8Array,
};
use crate::arrow2::chunk::Chunk;
use crate::arrow2::compute::filter::filter_chunk;
pub use crate::arrow2::datatypes::Schema;
use crate::arrow2::datatypes::{DataType, Field, Metadata};
use chrono::{DateTime, Duration, TimeZone, Utc};
use parquet::data_type::ByteArray;
use plateau_transport::{SchemaChunk, SegmentChunk};
use std::borrow::Borrow;
use std::ops::RangeInclusive;
use thiserror::Error;

use crate::slog::RecordIndex;

// currently unstable; don't need a const fn
pub fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
    std::any::type_name::<T>()
}

#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("unsupported type: {0}")]
    Unsupported(String),
    #[error("column {0} must be '{1}'")]
    BadColumn(usize, &'static str),
    #[error("column {0} type {1} != {2}")]
    InvalidColumnType(&'static str, &'static str, &'static str),
    #[error("could not encode 'message' to utf8")]
    FailedEncoding,
    #[error("array lengths do not match")]
    LengthMismatch,
    // this should really never happen...
    #[error("datatype does not match actual type")]
    TypeMismatch,
}

pub(crate) struct LegacyRecords(pub(crate) Vec<Record>);

pub fn chunk_into_legacy(chunk: SegmentChunk) -> Vec<Record> {
    LegacyRecords::try_from(SchemaChunk {
        schema: legacy_schema(),
        chunk,
    })
    .unwrap()
    .0
}

pub fn parse_time(tv: i64) -> DateTime<Utc> {
    Utc.timestamp(0, 0) + Duration::milliseconds(tv)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub time: DateTime<Utc>,
    pub message: ByteArray,
}

impl<S: Borrow<Schema> + Clone> TryFrom<SchemaChunk<S>> for LegacyRecords {
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

        Ok(LegacyRecords(
            time.values_iter()
                .zip(message.values_iter())
                .map(|(tv, m)| Record {
                    time: parse_time(*tv),
                    message: ByteArray::from(m),
                })
                .collect(),
        ))
    }
}

fn get_time<'a>(
    arrays: &'a [Box<dyn Array>],
    schema: &Schema,
) -> Result<&'a PrimitiveArray<i64>, ChunkError> {
    if schema.borrow().fields.get(0).map(|f| f.name.as_str()) != Some("time") {
        Err(ChunkError::BadColumn(0, "time"))
    } else {
        arrays
            .get(0)
            .ok_or(ChunkError::BadColumn(0, "time"))
            .and_then(|arr| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .ok_or_else(|| {
                        ChunkError::InvalidColumnType("time", "i64", type_name_of_val(arr))
                    })
            })
    }
}

pub fn new_schema_chunk<S: Borrow<Schema> + Clone>(
    schema: S,
    chunk: SegmentChunk,
) -> Result<SchemaChunk<S>, ChunkError> {
    get_time(chunk.arrays(), schema.borrow())?;
    Ok(SchemaChunk { schema, chunk })
}

pub trait TimeRange {
    fn time_range(&self) -> Result<RangeInclusive<DateTime<Utc>>, ChunkError>;
}

impl TimeRange for SchemaChunk<Schema> {
    fn time_range(&self) -> Result<RangeInclusive<DateTime<Utc>>, ChunkError> {
        let times = self
            .chunk
            .arrays()
            .get(0)
            .ok_or(ChunkError::BadColumn(0, "time"))
            .and_then(|arr| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .ok_or_else(|| {
                        ChunkError::InvalidColumnType("time", "i64", type_name_of_val(arr))
                    })
            })?;

        let times = times.iter().map(|tv| tv.unwrap());
        let start = parse_time(*times.clone().min().unwrap());
        let end = parse_time(*times.max().unwrap());

        Ok(start..=end)
    }
}

impl TryFrom<LegacyRecords> for SchemaChunk<Schema> {
    type Error = ChunkError;

    fn try_from(value: LegacyRecords) -> Result<Self, Self::Error> {
        let mut records = value.0;
        let mut times = MutablePrimitiveArray::<i64>::new();
        let mut messages = MutableUtf8Array::<i32>::new();

        for r in records.drain(..) {
            let dt = r.time.signed_duration_since(Utc.timestamp(0, 0));
            times.push(Some(dt.num_milliseconds()));
            messages.push(Some(
                r.message
                    .as_utf8()
                    .map_err(|_| ChunkError::FailedEncoding)?,
            ));
        }

        Ok(SchemaChunk {
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

fn estimate_array_size(arr: &dyn Array) -> Result<usize, ChunkError> {
    match arr.data_type() {
        DataType::UInt8 => Ok(arr.len()),
        DataType::UInt16 => Ok(arr.len() * 2),
        DataType::UInt32 => Ok(arr.len() * 4),
        DataType::UInt64 => Ok(arr.len() * 8),
        DataType::Int8 => Ok(arr.len()),
        DataType::Int16 => Ok(arr.len() * 2),
        DataType::Int32 => Ok(arr.len() * 4),
        DataType::Int64 => Ok(arr.len() * 8),
        // XXX - in future versions of arrow2
        // DataType::Int128 => Ok(arr.len() * 16),
        // DataType::Int256 => Ok(arr.len() * 32),
        DataType::Float16 => Ok(arr.len() * 2),
        DataType::Float32 => Ok(arr.len() * 4),
        DataType::Float64 => Ok(arr.len() * 8),
        DataType::Timestamp(_, _) => Ok(arr.len() * 8),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or(ChunkError::TypeMismatch)
            .map(|arr| arr.values().len()),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<Utf8Array<i64>>()
            .ok_or(ChunkError::TypeMismatch)
            .map(|arr| arr.values().len()),
        DataType::List(_) => arr
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::FixedSizeList(_, _) => arr
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::LargeList(_) => arr
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::Struct(_) => arr
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| {
                arr.values()
                    .iter()
                    .map(|inner| estimate_array_size(inner.as_ref()))
                    .sum()
            }),
        t => Err(ChunkError::Unsupported(format!("{:?}", t))),
    }
}

/// Estimate the size of a [Chunk]. The estimate does not include null bitmaps or extent buffers.
pub fn estimate_size(chunk: &SegmentChunk) -> Result<usize, ChunkError> {
    chunk
        .arrays()
        .iter()
        .map(|a| estimate_array_size(a.as_ref()))
        .sum()
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexedChunk {
    pub(crate) inner_schema: Schema,
    pub(crate) chunk: SegmentChunk,
}

impl IndexedChunk {
    pub(crate) fn from_start(ix: RecordIndex, data: SchemaChunk<Schema>) -> IndexedChunk {
        assert!(!data.chunk.arrays().is_empty());
        let start = ix.0 as i32;
        let size = data.chunk.len() as i32;
        let mut arrays = data.chunk.into_arrays();
        let indices = PrimitiveArray::<i32>::from_values(start..(start + size));
        arrays.push(indices.boxed());
        IndexedChunk {
            inner_schema: data.schema,
            chunk: SegmentChunk::new(arrays),
        }
    }

    pub(crate) fn start(&self) -> Option<RecordIndex> {
        self.indices()
            .values()
            .iter()
            .next()
            .map(|i| RecordIndex(*i as usize))
    }

    pub(crate) fn end(&self) -> Option<RecordIndex> {
        self.indices()
            .values()
            .last()
            .map(|i| RecordIndex(*i as usize))
    }

    pub(crate) fn indices(&self) -> &PrimitiveArray<i32> {
        self.chunk
            .arrays()
            .last()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap()
    }

    pub(crate) fn times(&self) -> &PrimitiveArray<i64> {
        self.chunk.arrays()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap()
    }

    pub(crate) fn slice(&mut self, offset: usize, len: usize) {
        let arrays = std::mem::replace(&mut self.chunk, Chunk::new(vec![]))
            .into_arrays()
            .into_iter()
            .map(|arr| arr.slice(offset, len))
            .collect();

        self.chunk = Chunk::new(arrays);
    }

    pub(crate) fn filter(
        &self,
        filter: &BooleanArray,
    ) -> Result<IndexedChunk, crate::arrow2::error::Error> {
        Ok(IndexedChunk {
            inner_schema: self.inner_schema.clone(),
            chunk: filter_chunk(&self.chunk, filter)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn into_legacy(self) -> Vec<Record> {
        chunk_into_legacy(self.chunk)
    }
}

impl From<IndexedChunk> for SegmentChunk {
    fn from(indexed: IndexedChunk) -> SegmentChunk {
        let mut arrays = indexed.chunk.into_arrays();
        arrays.truncate(arrays.len() - 1);
        SegmentChunk::new(arrays)
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::arrow2::array::PrimitiveArray;
    use crate::arrow2::datatypes::{Field, Metadata};

    pub(crate) fn inferences_schema_a() -> SchemaChunk<Schema> {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
        let inputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let mul = PrimitiveArray::<f32>::from_values(vec![2.0, 2.0, 2.0, 2.0, 2.0]);
        let inner = PrimitiveArray::<f64>::from_values(vec![
            2.0, 2.0, 4.0, 4.0, 6.0, 6.0, 8.0, 8.0, 10.0, 10.0,
        ]);

        // TODO: we need fixed size list array support, which currently is not
        // in arrow2's parquet io module.
        /*
        let outputs = FixedSizeListArray::new(
            DataType::FixedSizeList(
                Box::new(Field::new("inner", inner.data_type().clone(), false)),
                2,
            ),
            inner.to_boxed(),
            None,
        );
        */
        let offsets = vec![0, 2, 4, 6, 8, 10];
        let outputs = ListArray::new(
            DataType::List(Box::new(Field::new(
                "inner",
                inner.data_type().clone(),
                false,
            ))),
            offsets.into(),
            inner.boxed(),
            None,
        );

        let schema = Schema {
            fields: vec![
                Field::new("time", time.data_type().clone(), false),
                Field::new("inputs", inputs.data_type().clone(), false),
                Field::new("mul", mul.data_type().clone(), false),
                Field::new("outputs", outputs.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        SchemaChunk {
            schema,
            chunk: Chunk::try_new(vec![
                time.boxed(),
                inputs.boxed(),
                mul.boxed(),
                outputs.boxed(),
            ])
            .unwrap(),
        }
    }

    pub(crate) fn inferences_schema_b() -> SchemaChunk<Schema> {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
        let inputs = Utf8Array::<i32>::from_trusted_len_values_iter(
            vec!["one", "two", "three", "four", "five"].into_iter(),
        );
        let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        let schema = Schema {
            fields: vec![
                Field::new("time", time.data_type().clone(), false),
                Field::new("inputs", inputs.data_type().clone(), false),
                Field::new("outputs", outputs.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        SchemaChunk {
            schema,
            chunk: Chunk::try_new(vec![time.boxed(), inputs.boxed(), outputs.boxed()]).unwrap(),
        }
    }

    pub(crate) fn inferences_nested() -> SchemaChunk<Schema> {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);

        let a = inferences_schema_a();
        let b = inferences_schema_b();

        let a_struct = StructArray::new(
            DataType::Struct(a.schema.fields),
            a.chunk.into_arrays(),
            None,
        );
        let b_struct = StructArray::new(
            DataType::Struct(b.schema.fields),
            b.chunk.into_arrays(),
            None,
        );

        let schema = Schema {
            fields: vec![
                Field::new("time", time.data_type().clone(), false),
                Field::new("input", a_struct.data_type().clone(), false),
                Field::new("output", b_struct.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        SchemaChunk {
            schema,
            chunk: Chunk::try_new(vec![time.boxed(), a_struct.boxed(), b_struct.boxed()]).unwrap(),
        }
    }

    #[test]
    fn test_size_estimates() -> Result<(), ChunkError> {
        let time_size = 5 * 8;
        let a_size = time_size + 5 * 4 + 5 * 4 + 10 * 8;
        assert_eq!(estimate_size(&inferences_schema_a().chunk)?, a_size);
        let numbers = 3 + 3 + 5 + 4 + 4;
        let b_size = time_size + numbers + 5 * 4;
        assert_eq!(estimate_size(&inferences_schema_b().chunk)?, b_size);

        assert_eq!(
            estimate_size(&inferences_nested().chunk)?,
            time_size + a_size + b_size
        );

        Ok(())
    }
}

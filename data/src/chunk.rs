//! Utilities for working with the [arrow2::chunk::Chunk] type.
pub use crate::arrow2::datatypes::Schema;
use crate::arrow2::{
    array::{
        Array, BooleanArray, MutableArray, MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray,
        Utf8Array,
    },
    chunk::Chunk,
    compute::{self, filter::filter_chunk},
    datatypes::{DataType, Field, Metadata},
};
use chrono::{DateTime, TimeZone, Utc};
use plateau_transport::{ChunkError, SchemaChunk, SegmentChunk};
use std::borrow::Borrow;
use std::ops::RangeInclusive;

use crate::{Ordering, RecordIndex};

// currently unstable; don't need a const fn
pub fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
    std::any::type_name::<T>()
}

pub(crate) struct LegacyRecords(pub(crate) Vec<Record>);

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

pub fn parse_time(tv: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(tv).unwrap()
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

fn get_time<'a>(
    arrays: &'a [Box<dyn Array>],
    schema: &Schema,
) -> Result<&'a PrimitiveArray<i64>, ChunkError> {
    if schema.borrow().fields.first().map(|f| f.name.as_str()) != Some("time") {
        Err(ChunkError::BadColumn(0, "time"))
    } else {
        arrays
            .first()
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

pub fn new_schema_chunk<S: Borrow<Schema> + Clone + PartialEq>(
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
            .first()
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

#[derive(Clone, Debug, PartialEq)]
pub struct IndexedChunk {
    pub inner_schema: Schema,
    pub chunk: SegmentChunk,
}

impl IndexedChunk {
    pub fn from_start(ix: RecordIndex, data: SchemaChunk<Schema>, order: Ordering) -> Self {
        assert!(!data.chunk.arrays().is_empty());
        let start = ix.0 as i32;
        let size = data.chunk.len() as i32;
        let mut arrays = data.chunk.into_arrays();
        let indices = match order {
            Ordering::Forward => PrimitiveArray::from_values(start..(start + size)),
            Ordering::Reverse => PrimitiveArray::from_values(((start - size)..start).rev()),
        };
        arrays.push(indices.boxed());
        Self {
            inner_schema: data.schema,
            chunk: SegmentChunk::new(arrays),
        }
    }

    #[cfg(test)]
    /// Formats each message within an [IndexedChunk] as a series of Strings,
    /// prefixed with the chunk index. If the record is not a [LegacyRecord]
    /// or col 1 is not Utf8, returns an empty Vec.
    pub fn display_vec(&self) -> Vec<String> {
        let l = self.inner_schema.fields.len(); // index is stored in an "unlisted" extra column
        if self.inner_schema.fields[1].data_type == DataType::Utf8 {
            let idx = (*self.chunk.arrays()[l])
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap()
                .values_iter();
            return (*self.chunk.arrays()[1])
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .iter()
                .map(|s| String::from_utf8(s.unwrap().bytes().collect()).unwrap())
                .zip(idx)
                .map(|(s, i)| format!("{i}: {s}"))
                .collect::<Vec<String>>(); //.clone();
        }
        Default::default()
    }

    /// The absolute index of the first record in the chunk. For a chunk fetched/iterated
    /// using [Ordering::Reverse], this will be the high index, otherwise it will be the low index.
    pub fn start(&self) -> Option<RecordIndex> {
        self.indices()
            .values()
            .iter()
            .next()
            .map(|i| RecordIndex(*i as usize))
    }

    /// The absolute index of the last record in the chunk. For a chunk fetched/iterated
    /// using [Ordering::Reverse], this will be the low index, otherwise it will be the high index.
    pub fn end(&self) -> Option<RecordIndex> {
        self.indices()
            .values()
            .last()
            .map(|i| RecordIndex(*i as usize))
    }

    pub fn indices(&self) -> &PrimitiveArray<i32> {
        self.chunk
            .arrays()
            .last()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .unwrap()
    }

    pub fn times(&self) -> &PrimitiveArray<i64> {
        self.chunk.arrays()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap()
    }

    pub fn slice(&mut self, offset: usize, len: usize) {
        let mut arrays = std::mem::replace(&mut self.chunk, Chunk::new(vec![])).into_arrays();

        for arr in arrays.iter_mut() {
            arr.slice(offset, len);
        }

        self.chunk = Chunk::new(arrays);
    }

    pub fn filter(&self, filter: &BooleanArray) -> Result<Self, crate::arrow2::error::Error> {
        Ok(Self {
            inner_schema: self.inner_schema.clone(),
            chunk: filter_chunk(&self.chunk, filter)?,
        })
    }

    #[cfg(test)]
    pub fn into_legacy(self, order: Ordering) -> Vec<Record> {
        chunk_into_legacy(self.chunk, order)
    }
}

impl From<IndexedChunk> for SegmentChunk {
    fn from(indexed: IndexedChunk) -> Self {
        let mut arrays = indexed.chunk.into_arrays();
        arrays.truncate(arrays.len() - 1);
        SegmentChunk::new(arrays)
    }
}

pub fn concatenate(chunks: &[SegmentChunk]) -> anyhow::Result<SegmentChunk> {
    let arrays_len = chunks
        .first()
        .map(|c| c.arrays().len())
        .ok_or_else(|| anyhow::anyhow!("cannot concat empty list"))?;
    let columns: Vec<_> = (0..arrays_len)
        .map(|_| Vec::with_capacity(chunks.len()))
        .collect();

    let transpose = chunks.iter().fold(columns, |mut rows, chunk| {
        for (row, array) in rows.iter_mut().zip(chunk.arrays()) {
            row.push(array.as_ref());
        }
        rows
    });

    Ok(SegmentChunk::new(
        transpose
            .into_iter()
            .map(|arrays| compute::concatenate::concatenate(&arrays))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

pub fn slice(chunk: SegmentChunk, offset: usize, len: usize) -> SegmentChunk {
    let mut arrays = chunk.into_arrays();
    for array in arrays.iter_mut() {
        array.slice(offset, len);
    }
    SegmentChunk::new(arrays)
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::arrow2::array::{ListArray, MutableListArray, StructArray, TryExtend};
    use crate::transport::estimate_size;

    use plateau_test::{inferences_nested, inferences_schema_a, inferences_schema_b};

    /*
    #[test]
    fn get_exact_chunk_size() {
        let a: SchemaChunk<Schema> = inferences_schema_a();
        let b = inferences_schema_b();

        let a_bytes = a.to_bytes().unwrap();
        let b_bytes = b.to_bytes().unwrap();

        assert_eq!(2159, a_bytes.len());
        assert_eq!(1611, b_bytes.len())
    }*/

    #[test]
    fn test_size_estimates() -> Result<(), ChunkError> {
        let time_size = 5 * 8;
        let unknown = 40;
        let a_size = time_size + 10 * 8 + 5 * 4 + 10 * 8 + 5 * 4 + unknown;
        assert_eq!(estimate_size(&inferences_schema_a().chunk)?, a_size);
        let numbers = 3 + 3 + 5 + 4 + 4;
        let unknown = 48;
        let b_size = time_size + numbers + 5 * 4 + unknown;
        assert_eq!(estimate_size(&inferences_schema_b().chunk)?, b_size);

        assert_eq!(
            estimate_size(&inferences_nested().chunk)?,
            time_size + a_size + b_size
        );

        Ok(())
    }
}

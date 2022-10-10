//! Utilities for working with the [arrow2::chunk::Chunk] type.
use arrow2::array::{Array, FixedSizeListArray, ListArray, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("unsupported type: {0}")]
    Unsupported(String),
    // this should really never happen...
    #[error("datatype does not match actual type")]
    TypeMismatch,
}

pub(crate) type SegmentChunk = Chunk<Box<dyn Array>>;

fn estimate_array_size(arr: &Box<dyn Array>) -> Result<usize, ChunkError> {
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
            .and_then(|arr| estimate_array_size(arr.values())),
        DataType::FixedSizeList(_, _) => arr
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values())),
        DataType::LargeList(_) => arr
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values())),
        t => Err(ChunkError::Unsupported(String::from(std::stringify!(t)))),
    }
}

/// Estimate the size of a [Chunk]. The estimate does not include null bitmaps or extent buffers.
pub fn estimate_size(chunk: &SegmentChunk) -> Result<usize, ChunkError> {
    chunk.arrays().iter().map(|a| estimate_array_size(&a)).sum()
}

#[cfg(test)]
pub mod test {
    use super::*;
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::{Field, Metadata, Schema};

    pub fn inferences_schema_a() -> (Schema, SegmentChunk) {
        let inputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let mul = PrimitiveArray::<f32>::from_values(vec![2.0, 2.0, 2.0, 2.0, 2.0]);
        let inner = PrimitiveArray::<f64>::from_values(vec![2.0, 4.0, 6.0, 8.0, 10.0]);
        let outputs = inner;

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

        let schema = Schema {
            fields: vec![
                Field::new("inputs", inputs.data_type().clone(), false),
                Field::new("mul", mul.data_type().clone(), false),
                Field::new("outputs", outputs.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        (
            schema,
            Chunk::try_new(vec![inputs.boxed(), mul.boxed(), outputs.boxed()]).unwrap(),
        )
    }

    pub fn inferences_schema_b() -> (Schema, SegmentChunk) {
        let inputs = Utf8Array::<i32>::from_trusted_len_values_iter(
            vec!["one", "two", "three", "four", "five"].into_iter(),
        );
        let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        let schema = Schema {
            fields: vec![
                Field::new("inputs", inputs.data_type().clone(), false),
                Field::new("outputs", outputs.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        (
            schema,
            Chunk::try_new(vec![inputs.boxed(), outputs.boxed()]).unwrap(),
        )
    }

    #[test]
    fn test_size_estimates() -> Result<(), ChunkError> {
        assert_eq!(
            estimate_size(&inferences_schema_a().1)?,
            5 * 4 + 5 * 4 + 5 * 8
        );
        let numbers = 3 + 3 + 5 + 4 + 4;
        assert_eq!(estimate_size(&inferences_schema_b().1)?, numbers + 5 * 4);
        Ok(())
    }
}

use plateau_transport_arrow_rs as transport;
use std::sync::Arc;
use transport::arrow_array::types::*;
use transport::arrow_array::Array;
use transport::arrow_array::FixedSizeListArray;
use transport::arrow_array::ListArray;
use transport::arrow_array::PrimitiveArray;
use transport::arrow_array::RecordBatch;
use transport::arrow_array::StringArray;
use transport::arrow_array::StructArray;
use transport::arrow_buffer::OffsetBuffer;
use transport::arrow_buffer::ScalarBuffer;
use transport::arrow_schema::DataType;
use transport::arrow_schema::Field;
use transport::arrow_schema::Fields;
use transport::arrow_schema::Schema;
use transport::SchemaChunk;

pub mod http;

pub fn inferences_large(blob_size: usize) -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<Int64Type>::from_iter_values([0, 1, 2, 3, 4]);
    let inputs = StringArray::from(vec!["one", "two", "three", "four", "five"]);
    let outputs = PrimitiveArray::<Float32Type>::from_iter_values([1.0, 2.0, 3.0, 4.0, 5.0]);

    // Create list array for failures
    let blob = "x".repeat(blob_size);
    let values: Vec<Option<Vec<Option<String>>>> = vec![
        Some(vec![Some(blob.clone())]),
        Some(vec![Some(blob.clone())]),
        Some(vec![Some(blob.clone()), Some(blob.clone())]),
        Some(vec![Some(blob.clone())]),
        Some(vec![Some(blob.clone())]),
    ];

    // Flatten all strings into a single array
    let mut all_strings = Vec::new();
    let mut list_offsets = vec![0];
    let mut offset = 0;

    for value in &values {
        if let Some(strings) = value {
            for s in strings {
                all_strings.push(s.as_deref());
            }
            offset += strings.len() as i32;
        }
        list_offsets.push(offset);
    }

    let flat_strings = StringArray::from(all_strings);
    let failures = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(list_offsets)),
        Arc::new(flat_strings),
        None,
    );

    let schema_copy = Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("inputs", DataType::Utf8, false),
        Field::new("outputs", DataType::Float32, false),
        Field::new("failures", failures.data_type().clone(), true),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema_copy.clone()),
        vec![
            Arc::new(time),
            Arc::new(inputs),
            Arc::new(outputs),
            Arc::new(failures),
        ],
    )
    .unwrap();

    SchemaChunk {
        schema: schema_copy,
        chunk: record_batch,
    }
}

pub fn inferences_large_extension(
    count: i64,
    inner_size: i64,
    _shape: &str,
) -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<Int64Type>::from_iter_values(0..count);

    let inner = PrimitiveArray::<Int64Type>::from_iter_values(
        (0..count).flat_map(|ix| (0..inner_size).map(move |_| ix)),
    );

    // In arrow-rs, FixedSizeListArray takes different parameters
    let field = Arc::new(Field::new("inner", inner.data_type().clone(), false));
    let tensor = FixedSizeListArray::new(field, inner_size as i32, Arc::new(inner), None);

    // Fields for struct arrays must be wrapped in Fields::from
    let fields = Fields::from(vec![Field::new("tensor", tensor.data_type().clone(), true)]);
    let out = StructArray::new(fields, vec![Arc::new(tensor)], None);

    let schema_copy = Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("out", out.data_type().clone(), false),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema_copy.clone()),
        vec![Arc::new(time), Arc::new(out)],
    )
    .unwrap();

    SchemaChunk {
        schema: schema_copy,
        chunk: record_batch,
    }
}

pub fn inferences_schema_a() -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<Int64Type>::from_iter_values(vec![0, 1, 2, 3, 4]);
    let inputs = PrimitiveArray::<Float32Type>::from_iter_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let mul = PrimitiveArray::<Float32Type>::from_iter_values(vec![2.0, 2.0, 2.0, 2.0, 2.0]);
    let inner = PrimitiveArray::<Float64Type>::from_iter_values(vec![
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
        std::sync::Arc::new(inner),
        None,
    );
    */
    let offsets = vec![0, 2, 2, 4, 6, 8];

    // Create Field with Arc wrapper
    let inner_field = Arc::new(Field::new("inner", inner.data_type().clone(), false));

    let tensor = ListArray::new(
        inner_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(inner.clone()),
        None,
    );

    // Fields for struct arrays must be wrapped in Fields::from
    let fields = Fields::from(vec![
        Field::new("mul", mul.data_type().clone(), false),
        Field::new("tensor", tensor.data_type().clone(), false),
    ]);

    let outputs = StructArray::new(
        fields,
        vec![Arc::new(mul.clone()), Arc::new(tensor.clone())],
        None,
    );

    let schema_copy = Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("tensor", tensor.data_type().clone(), false),
        Field::new("inputs", inputs.data_type().clone(), false),
        Field::new("outputs", outputs.data_type().clone(), false),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema_copy.clone()),
        vec![
            Arc::new(time),
            Arc::new(tensor),
            Arc::new(inputs),
            Arc::new(outputs),
        ],
    )
    .unwrap();

    SchemaChunk {
        schema: schema_copy,
        chunk: record_batch,
    }
}

pub fn inferences_schema_b() -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<Int64Type>::from_iter_values(vec![0, 1, 2, 3, 4]);
    let inputs = StringArray::from(vec!["one", "two", "three", "four", "five"]);
    let outputs = PrimitiveArray::<Float32Type>::from_iter_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

    // Create an empty list array
    let _values: Vec<Option<Vec<Option<String>>>> = vec![
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
    ];

    // Create an empty StringArray
    let string_array = StringArray::from(Vec::<&str>::new());
    let offsets = vec![0, 0, 0, 0, 0, 0];

    let failures = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(string_array),
        None,
    );

    let schema_copy = Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("inputs", DataType::Utf8, false),
        Field::new("outputs", DataType::Float32, false),
        Field::new("failures", failures.data_type().clone(), true),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema_copy.clone()),
        vec![
            Arc::new(time),
            Arc::new(inputs),
            Arc::new(outputs),
            Arc::new(failures),
        ],
    )
    .unwrap();

    SchemaChunk {
        schema: schema_copy,
        chunk: record_batch,
    }
}

pub fn inferences_nested() -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<Int64Type>::from_iter_values(vec![0, 1, 2, 3, 4]);

    let a = inferences_schema_a();
    let b = inferences_schema_b();

    let a_struct = StructArray::new(a.schema.fields.clone(), a.chunk.columns().to_vec(), None);
    let b_struct = StructArray::new(b.schema.fields.clone(), b.chunk.columns().to_vec(), None);

    let schema_copy = Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("input", a_struct.data_type().clone(), false),
        Field::new("output", b_struct.data_type().clone(), false),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema_copy.clone()),
        vec![Arc::new(time), Arc::new(a_struct), Arc::new(b_struct)],
    )
    .unwrap();

    SchemaChunk {
        schema: schema_copy,
        chunk: record_batch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::arrow_array::ArrayRef;

    #[test]
    fn test_inferences_large() {
        let blob_size = 10;
        let result = inferences_large(blob_size);

        // Test schema fields
        assert_eq!(result.schema.fields.len(), 4);
        assert_eq!(result.schema.field(0).name(), "time");
        assert_eq!(result.schema.field(1).name(), "inputs");
        assert_eq!(result.schema.field(2).name(), "outputs");
        assert_eq!(result.schema.field(3).name(), "failures");

        // Test data types
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(result.schema.field(2).data_type(), &DataType::Float32);

        // Test record batch
        assert_eq!(result.chunk.num_columns(), 4);
        assert_eq!(result.chunk.num_rows(), 5);

        // Test array values
        let time = result
            .chunk
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        let inputs = result
            .chunk
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let outputs = result
            .chunk
            .column(2)
            .as_any()
            .downcast_ref::<PrimitiveArray<Float32Type>>()
            .unwrap();

        assert_eq!(time.value(0), 0);
        assert_eq!(time.value(4), 4);
        assert_eq!(inputs.value(0), "one");
        assert_eq!(inputs.value(4), "five");
        assert_eq!(outputs.value(0), 1.0);
        assert_eq!(outputs.value(4), 5.0);

        // Test that the failures array is a ListArray
        let failures = result.chunk.column(3);
        assert!(failures.as_any().downcast_ref::<ListArray>().is_some());
    }

    #[test]
    fn test_inferences_large_extension() {
        let count = 3;
        let inner_size = 2;
        let shape = "[2]"; // not actually used in function but required

        let result = inferences_large_extension(count, inner_size, shape);

        // Test schema fields
        assert_eq!(result.schema.fields.len(), 2);
        assert_eq!(result.schema.field(0).name(), "time");
        assert_eq!(result.schema.field(1).name(), "out");

        // Test data types
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);

        // Test record batch
        assert_eq!(result.chunk.num_columns(), 2);
        assert_eq!(result.chunk.num_rows(), 3);

        // Test array values
        let time = result
            .chunk
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(time.value(0), 0);
        assert_eq!(time.value(1), 1);
        assert_eq!(time.value(2), 2);

        // Test that the out array is a StructArray
        let out = result.chunk.column(1);
        assert!(out.as_any().downcast_ref::<StructArray>().is_some());
    }

    #[test]
    fn test_inferences_schema_a() {
        let result = inferences_schema_a();

        // Test schema fields
        assert_eq!(result.schema.fields.len(), 4);
        assert_eq!(result.schema.field(0).name(), "time");
        assert_eq!(result.schema.field(1).name(), "tensor");
        assert_eq!(result.schema.field(2).name(), "inputs");
        assert_eq!(result.schema.field(3).name(), "outputs");

        // Test data types
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.schema.field(2).data_type(), &DataType::Float32);

        // Test record batch
        assert_eq!(result.chunk.num_columns(), 4);
        assert_eq!(result.chunk.num_rows(), 5);

        // Test array values
        let time = result
            .chunk
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        let inputs = result
            .chunk
            .column(2)
            .as_any()
            .downcast_ref::<PrimitiveArray<Float32Type>>()
            .unwrap();

        assert_eq!(time.value(0), 0);
        assert_eq!(time.value(4), 4);
        assert_eq!(inputs.value(0), 1.0);
        assert_eq!(inputs.value(4), 5.0);

        // Test that the tensor array is a ListArray and outputs is a StructArray
        let tensor = result.chunk.column(1);
        let outputs = result.chunk.column(3);
        assert!(tensor.as_any().downcast_ref::<ListArray>().is_some());
        assert!(outputs.as_any().downcast_ref::<StructArray>().is_some());
    }

    #[test]
    fn test_inferences_schema_b() {
        let result = inferences_schema_b();

        // Test schema fields
        assert_eq!(result.schema.fields.len(), 4);
        assert_eq!(result.schema.field(0).name(), "time");
        assert_eq!(result.schema.field(1).name(), "inputs");
        assert_eq!(result.schema.field(2).name(), "outputs");
        assert_eq!(result.schema.field(3).name(), "failures");

        // Test data types
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(result.schema.field(2).data_type(), &DataType::Float32);

        // Test record batch
        assert_eq!(result.chunk.num_columns(), 4);
        assert_eq!(result.chunk.num_rows(), 5);

        // Test array values
        let time = result
            .chunk
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        let inputs = result
            .chunk
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let outputs = result
            .chunk
            .column(2)
            .as_any()
            .downcast_ref::<PrimitiveArray<Float32Type>>()
            .unwrap();

        assert_eq!(time.value(0), 0);
        assert_eq!(time.value(4), 4);
        assert_eq!(inputs.value(0), "one");
        assert_eq!(inputs.value(4), "five");
        assert_eq!(outputs.value(0), 1.0);
        assert_eq!(outputs.value(4), 5.0);

        // Test that the failures array is a ListArray and it's empty
        let failures = result
            .chunk
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(failures.len(), 5);

        for i in 0..5 {
            let list_values = get_list_values(failures, i);
            assert_eq!(list_values.len(), 0, "Expected empty list at index {i}");
        }
    }

    #[test]
    fn test_inferences_nested() {
        let result = inferences_nested();

        // Test schema fields
        assert_eq!(result.schema.fields.len(), 3);
        assert_eq!(result.schema.field(0).name(), "time");
        assert_eq!(result.schema.field(1).name(), "input");
        assert_eq!(result.schema.field(2).name(), "output");

        // Test record batch
        assert_eq!(result.chunk.num_columns(), 3);
        assert_eq!(result.chunk.num_rows(), 5);

        // Test array values
        let time = result
            .chunk
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap();
        assert_eq!(time.value(0), 0);
        assert_eq!(time.value(4), 4);

        // Test that input and output are StructArrays
        let input = result.chunk.column(1);
        let output = result.chunk.column(2);
        assert!(input.as_any().downcast_ref::<StructArray>().is_some());
        assert!(output.as_any().downcast_ref::<StructArray>().is_some());

        // Check that the input struct has 4 fields from schema_a
        let input_struct = input.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(input_struct.num_columns(), 4);

        // Check that the output struct has 4 fields from schema_b
        let output_struct = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(output_struct.num_columns(), 4);
    }

    // Helper function to get values from a ListArray at a specific index
    fn get_list_values(list_array: &ListArray, index: usize) -> Vec<ArrayRef> {
        let value_offsets = list_array.value_offsets();
        let offset = value_offsets[index] as usize;
        let length = (value_offsets[index + 1] - value_offsets[index]) as usize;
        let values = list_array.values();

        let mut result = Vec::new();
        for i in 0..length {
            let value_index = offset + i;
            result.push(values.slice(value_index, 1));
        }
        result
    }
}

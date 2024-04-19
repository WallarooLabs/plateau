use arrow2::array::FixedSizeListArray;

use crate::{
    arrow2::{
        array::{
            Array, ListArray, MutableListArray, MutableUtf8Array, PrimitiveArray, TryExtend,
            Utf8Array,
        },
        chunk::Chunk,
        datatypes::{DataType, Field, Metadata, Schema},
    },
    SchemaChunk,
};

pub fn inferences_large(blob_size: usize) -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
    let inputs = Utf8Array::<i32>::from_trusted_len_values_iter(
        vec!["one", "two", "three", "four", "five"].into_iter(),
    );
    let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let mut failures = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
    let blob = Some("x".repeat(blob_size));
    let values: Vec<Option<Vec<Option<String>>>> = vec![
        Some(vec![blob.clone()]),
        Some(vec![blob.clone()]),
        Some(vec![blob.clone(), blob.clone()]),
        Some(vec![blob.clone()]),
        Some(vec![blob.clone()]),
    ];
    failures.try_extend(values).unwrap();
    let failures = ListArray::from(failures);

    let schema = Schema {
        fields: vec![
            Field::new("time", time.data_type().clone(), false),
            Field::new("inputs", inputs.data_type().clone(), false),
            Field::new("outputs", outputs.data_type().clone(), false),
            Field::new("failures", failures.data_type().clone(), false),
        ],
        metadata: Metadata::default(),
    };

    SchemaChunk {
        schema,
        chunk: Chunk::try_new(vec![
            time.boxed(),
            inputs.boxed(),
            outputs.boxed(),
            failures.boxed(),
        ])
        .unwrap(),
    }
}

pub fn inferences_large_extension(count: i64, inner_size: i64, shape: &str) -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<i64>::from_values(0..count);

    let inner = PrimitiveArray::<i64>::from_values(
        (0..count).flat_map(|ix| (0..inner_size).map(move |_| ix)),
    );
    let tensor = FixedSizeListArray::new(
        DataType::Extension(
            "arrow.fixed_shape_tensor".to_string(),
            Box::new(DataType::FixedSizeList(
                Box::new(Field::new("inner", inner.data_type().clone(), false)),
                inner_size as usize,
            )),
            Some(format!("{{\"shape\":{shape}}}")),
        ),
        inner.to_boxed(),
        None,
    );

    let schema = Schema {
        fields: vec![
            Field::new("time", time.data_type().clone(), false),
            Field::new("tensor", tensor.data_type().clone(), false),
        ],
        metadata: Metadata::default(),
    };

    SchemaChunk {
        schema,
        chunk: Chunk::try_new(vec![time.boxed(), tensor.boxed()]).unwrap(),
    }
}

use plateau_transport::arrow2::array::Array;
use plateau_transport::arrow2::array::FixedSizeListArray;
use plateau_transport::arrow2::array::ListArray;
use plateau_transport::arrow2::array::MutableListArray;
use plateau_transport::arrow2::array::MutableUtf8Array;
use plateau_transport::arrow2::array::PrimitiveArray;
use plateau_transport::arrow2::array::StructArray;
use plateau_transport::arrow2::array::TryExtend;
use plateau_transport::arrow2::array::Utf8Array;
use plateau_transport::arrow2::chunk::Chunk;
use plateau_transport::arrow2::datatypes::DataType;
use plateau_transport::arrow2::datatypes::Field;
use plateau_transport::arrow2::datatypes::Metadata;
use plateau_transport::arrow2::datatypes::Schema;
use plateau_transport::SchemaChunk;

pub use self::server::TestServer;

mod server;

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

pub fn inferences_schema_a() -> SchemaChunk<Schema> {
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
    let offsets = vec![0, 2, 2, 4, 6, 8];
    let tensor = ListArray::new(
        DataType::List(Box::new(Field::new(
            "inner",
            inner.data_type().clone(),
            false,
        ))),
        offsets.try_into().unwrap(),
        inner.boxed(),
        None,
    );

    let outputs = StructArray::new(
        DataType::Struct(vec![
            Field::new("mul", mul.data_type().clone(), false),
            Field::new("tensor", tensor.data_type().clone(), false),
        ]),
        vec![mul.clone().boxed(), tensor.clone().boxed()],
        None,
    );

    let schema = Schema {
        fields: vec![
            Field::new("time", time.data_type().clone(), false),
            Field::new("tensor", tensor.data_type().clone(), false),
            Field::new("inputs", inputs.data_type().clone(), false),
            Field::new("outputs", outputs.data_type().clone(), false),
        ],
        metadata: Metadata::default(),
    };

    SchemaChunk {
        schema,
        chunk: Chunk::try_new(vec![
            time.boxed(),
            tensor.boxed(),
            inputs.boxed(),
            outputs.boxed(),
        ])
        .unwrap(),
    }
}

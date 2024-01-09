use std::io::Cursor;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use arrow2::array::{
    Array, ListArray, MutableListArray, MutableUtf8Array, PrimitiveArray, StructArray, TryExtend,
    Utf8Array,
};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Metadata};
use arrow2::io::ipc::{read, write};
use plateau::chunk::Schema;
use plateau::config::PlateauConfig;
use plateau::http;
use plateau::http::TestServer;
use plateau_transport::{
    arrow2,
    headers::{ITERATION_STATUS_HEADER, MAX_REQUEST_SIZE_HEADER},
};
use reqwest::Client;
use serde_json::json;
use std::fs::File;
use tracing_subscriber::{fmt, EnvFilter};

use plateau_transport::{DataFocus, SchemaChunk, SegmentChunk, CONTENT_TYPE_ARROW};

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

pub(crate) fn inferences_schema_b() -> SchemaChunk<Schema> {
    let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
    let inputs = Utf8Array::<i32>::from_trusted_len_values_iter(
        vec!["one", "two", "three", "four", "five"].into_iter(),
    );
    let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let mut failures = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
    let values: Vec<Option<Vec<Option<String>>>> = vec![
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
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

pub(crate) fn inferences_large() -> SchemaChunk<Schema> {
    let large = "x".repeat(1_000_000);
    let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
    let inputs =
        Utf8Array::<i32>::from_trusted_len_values_iter(std::iter::repeat(large.as_str()).take(5));
    let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let mut failures = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
    let values: Vec<Option<Vec<Option<String>>>> = vec![
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
        Some(vec![]),
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

async fn repeat_append(client: &Client, url: &str, body: &str, count: usize) {
    let records: Vec<_> = vec![body.to_string()]
        .iter()
        .cycle()
        .take(count)
        .cloned()
        .collect();
    let message = json!({ "records": records });
    client
        .post(url)
        .json(&message)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

async fn chunk_append(client: &Client, url: &str, data: SchemaChunk<Schema>) -> Result<()> {
    let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(bytes, data.schema, None, options);

    writer.start()?;
    writer.write(&data.chunk, None)?;
    writer.finish()?;

    client
        .post(url)
        .header("Content-Type", CONTENT_TYPE_ARROW)
        .body(writer.into_inner().into_inner())
        .send()
        .await?
        .error_for_status()
        .map_err(Into::into)
        .map(|_| ())
}

async fn read_next_chunks(
    client: &Client,
    url: &str,
    iter: Option<serde_json::Value>,
    limit: impl Into<Option<usize>>,
    focus: DataFocus,
) -> Result<(Schema, Vec<SegmentChunk>)> {
    let mut response = client.post(url).json(&json!({}));

    if let Some(limit) = limit.into() {
        response = response.query(&[("page_size", limit)]);
    }

    if focus.is_some() {
        for ds in focus.dataset {
            response = response.query(&[("dataset[]", ds)]);
        }
        response = response.query(&[("dataset.separator", focus.dataset_separator)])
    }

    if let Some(it) = iter {
        response = response.json(&it);
    }

    response = response.header("Accept", CONTENT_TYPE_ARROW);
    let bytes = response.send().await?.error_for_status()?.bytes().await?;

    let mut cursor = Cursor::new(bytes);
    let metadata = read::read_file_metadata(&mut cursor)?;
    let schema = metadata.schema.clone();
    let reader = read::FileReader::new(cursor, metadata, None, None);
    let chunks = reader.collect::<Result<Vec<_>, _>>()?;

    Ok((schema, chunks))
}

fn next_from_schema(schema: &Schema) -> Result<serde_json::Value> {
    let status: serde_json::Value = serde_json::from_str(schema.metadata.get("status").unwrap())?;
    Ok(status.get("next").unwrap().clone())
}

fn schema_field_names(schema: &Schema) -> Vec<String> {
    schema.fields.iter().map(|f| f.name.to_string()).collect()
}

async fn fetch_topic_records(
    client: &Client,
    url: &str,
    limit: impl Into<Option<usize>>,
) -> serde_json::Value {
    let mut response = client.post(url).json(&json!({}));
    if let Some(limit) = limit.into() {
        response = response.query(&[("page_size", limit)]);
    }
    let result: serde_json::Value = response
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    result
}

async fn fetch_partition_records(
    client: &Client,
    url: &str,
    limit: impl Into<Option<usize>>,
) -> serde_json::Value {
    let mut response = client.get(url).json(&json!({})).query(&[("start", 0)]);
    if let Some(limit) = limit.into() {
        response = response.query(&[("page_size", limit)]);
    }
    let result = response.send().await;
    let result: serde_json::Value = result
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    result
}

async fn get_topics(client: &Client, url: &str) -> Result<serde_json::Value> {
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json::<serde_json::Value>().await?)
}

fn topic_url(server: &TestServer) -> String {
    format!("{}/topics", server.base(),)
}

fn append_url(
    server: &TestServer,
    topic_name: impl AsRef<str>,
    partition_name: impl AsRef<str>,
) -> String {
    format!(
        "{}/topic/{}/partition/{}",
        server.base(),
        topic_name.as_ref(),
        partition_name.as_ref()
    )
}

fn topic_records_url(server: &TestServer, topic_name: impl AsRef<str>) -> String {
    format!("{}/topic/{}/records", server.base(), topic_name.as_ref())
}

fn partition_records_url(
    server: &TestServer,
    topic_name: impl AsRef<str>,
    partition_name: impl AsRef<str>,
) -> String {
    format!(
        "{}/topic/{}/partition/{}/records",
        server.base(),
        topic_name.as_ref(),
        partition_name.as_ref()
    )
}

fn assert_status(json_response: &serde_json::Value, expected: &str) {
    let status = json_response
        .as_object()
        .expect("expected object in JSON response")
        .get("status")
        .expect("expected 'status' key in JSON response")
        .as_str()
        .expect("expected 'status' value to be string");
    assert_eq!(status, expected);
}

fn assert_response_length(json_response: &serde_json::Value, expected: usize) {
    let records = json_response
        .as_object()
        .expect("expected object in JSON response")
        .get("records")
        .expect("expected 'records' key in JSON response")
        .as_array()
        .expect("expected 'records' value to be array");
    assert_eq!(records.len(), expected);
}

const PARTITION_NAME: &str = "partition-1";
const TEST_MESSAGE: &str = "this is my test message. it's not that long but it's fine. \
it just needs to be long enough that we can start hitting the byte limit before we hit \
the default record limit.";

async fn setup() -> (Client, String, http::TestServer) {
    setup_with_config(Default::default()).await
}

async fn setup_with_config(config: http::Config) -> (Client, String, http::TestServer) {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .ok(); // called multiple times, so ignore errors

    (
        Client::new(),
        format!("topic-{}", uuid::Uuid::new_v4()),
        http::TestServer::new_with_config(PlateauConfig {
            http: http::Config {
                bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..config
            },
            ..PlateauConfig::default()
        })
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn topic_status_all() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    assert_eq!(
        get_topics(&client, &topic_url(&server)).await?,
        json!({"topics": []})
    );
    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        10,
    )
    .await;

    server.catalog.checkpoint().await;
    // hack until we have a true commit mechanism (requires partial parquet file support)
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        get_topics(&client, &topic_url(&server)).await?,
        json!({"topics": [{"name": topic_name.clone()}]})
    );

    // test unlimited request, should get all records
    let json_response = fetch_topic_records(
        &client,
        topic_records_url(&server, &topic_name).as_str(),
        None,
    )
    .await;
    assert_status(&json_response, "All");
    assert_response_length(&json_response, 10);

    Ok(())
}

#[tokio::test]
async fn topic_status_record_limited() {
    let (client, topic_name, server) = setup().await;

    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        10,
    )
    .await;

    // test record-limited request, should get 'RecordLimited' response and fewer results
    let json_response =
        fetch_topic_records(&client, topic_records_url(&server, &topic_name).as_str(), 5).await;
    assert_status(&json_response, "RecordLimited");
    assert_response_length(&json_response, 5);
}

#[tokio::test]
async fn topic_status_byte_limited() {
    let (client, topic_name, server) = setup().await;

    const DEFAULT_MAX_BYTES: usize = 100 * 1024; // 100KB
    let test_messsage_bytelen = TEST_MESSAGE.as_bytes().len();
    // find the upper limit of messages we can store, accounting for the 10 records we already added
    let message_limit = DEFAULT_MAX_BYTES / test_messsage_bytelen;
    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        // add one more message so that we're beyond the limit
        message_limit + 1,
    )
    .await;
    let json_response = fetch_topic_records(
        &client,
        topic_records_url(&server, &topic_name).as_str(),
        None,
    )
    .await;
    assert_status(&json_response, "ByteLimited");
    // plateau always finishes the current record when we hit the byte limit, so we expect to have
    // one more than the actual hard limit caculated above
    assert_response_length(&json_response, message_limit + 1);
}

#[tokio::test]
async fn stored_schema_metadata() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    let mut chunk_a = inferences_schema_a();

    chunk_a
        .schema
        .metadata
        .insert("pipeline.name".to_string(), "pied-piper".to_string());
    chunk_a
        .schema
        .metadata
        .insert("pipeline.version".to_string(), "3.1".to_string());

    for _ in 0..10 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            chunk_a.clone(),
        )
        .await?;
    }

    // test record-limited request, should get 'RecordLimited' response and fewer results
    let topic_url = topic_records_url(&server, &topic_name);
    let (schema, _): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(json!({})),
        29,
        DataFocus::default(),
    )
    .await?;

    assert_eq!(schema.metadata.get("pipeline.name").unwrap(), "pied-piper");
    assert_eq!(schema.metadata.get("pipeline.version").unwrap(), "3.1");

    Ok(())
}

#[tokio::test]
async fn max_request_header() -> Result<()> {
    let max = 1234;

    let (client, topic_name, server) = setup_with_config(http::Config {
        max_append_bytes: max,
        ..Default::default()
    })
    .await;

    let req = client
        .post(append_url(&server, &topic_name, PARTITION_NAME))
        .header("content-length", max * 10);
    let resp = req.send().await?;

    assert_eq!(413, resp.status());
    assert_eq!(
        &max.to_string(),
        resp.headers().get(MAX_REQUEST_SIZE_HEADER).unwrap()
    );

    Ok(())
}

#[tokio::test]
async fn large_append() -> Result<()> {
    let large = inferences_large();

    let (client, topic_name, server) = setup_with_config(http::Config {
        max_append_bytes: 20,
        ..Default::default()
    })
    .await;

    let err = chunk_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        large.clone(),
    )
    .await;

    assert_eq!(
        err.err()
            .unwrap()
            .downcast::<reqwest::Error>()
            .unwrap()
            .status(),
        Some(reqwest::StatusCode::PAYLOAD_TOO_LARGE)
    );

    let (client, topic_name, server) = setup().await;

    for _ in 0..10 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            large.clone(),
        )
        .await?;
    }

    let topic_url = topic_records_url(&server, &topic_name);
    let (_, chunks): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(json!({})),
        29,
        DataFocus::default(),
    )
    .await?;

    assert_eq!(chunks[0].len(), 5);

    Ok(())
}

#[tokio::test]
async fn topic_iterate_schema_change() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    let chunk_a = inferences_schema_a();
    let chunk_b = inferences_schema_b();

    for _ in 0..10 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            chunk_a.clone(),
        )
        .await?;
    }

    for _ in 0..5 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            chunk_b.clone(),
        )
        .await?;
    }

    // test record-limited request, should get 'RecordLimited' response and fewer results
    let topic_url = topic_records_url(&server, &topic_name);
    let (schema, response): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(json!({})),
        29,
        DataFocus::default(),
    )
    .await?;
    assert_eq!(
        response.into_iter().map(|c| c.len()).collect::<Vec<_>>(),
        vec![5 + 5 + 5 + 5 + 5 + 4]
    );
    assert_eq!(
        schema_field_names(&schema),
        schema_field_names(&chunk_a.schema)
    );

    let next = next_from_schema(&schema)?;
    let (schema, response): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(next),
        29,
        DataFocus::default(),
    )
    .await?;
    assert_eq!(
        response.into_iter().map(|c| c.len()).collect::<Vec<_>>(),
        vec![1 + 5 + 5 + 5 + 5]
    );
    assert_eq!(
        schema_field_names(&schema),
        schema_field_names(&chunk_a.schema)
    );

    let next = next_from_schema(&schema)?;
    let (schema, response): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(next),
        29,
        DataFocus::default(),
    )
    .await?;
    assert_eq!(
        response.into_iter().map(|c| c.len()).collect::<Vec<_>>(),
        vec![5, 5, 5, 5, 5]
    );
    assert_eq!(
        schema_field_names(&schema),
        schema_field_names(&chunk_b.schema)
    );

    let next = next_from_schema(&schema)?;
    let (_, response): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(next),
        29,
        DataFocus::default(),
    )
    .await?;
    assert_eq!(
        response
            .into_iter()
            .map(|c| c.len())
            .collect::<Vec<_>>()
            .len(),
        0
    );

    // this is a horrible hack that resolves a race condition where the slog threads are still
    // writing but the tempdir is deleted, resulting in intermittent test failures.
    //TODO: graceful shutdown of test server
    tokio::time::sleep(Duration::from_millis(300)).await;

    Ok(())
}

#[tokio::test]
async fn topic_iterate_data_focus() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    let chunk_a = inferences_schema_a();

    for _ in 0..10 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            chunk_a.clone(),
        )
        .await?;
    }

    // test record-limited request, should get 'RecordLimited' response and fewer results
    let topic_url = topic_records_url(&server, &topic_name);
    let (schema, chunk): (Schema, Vec<SegmentChunk>) = read_next_chunks(
        &client,
        topic_url.as_str(),
        Some(json!({})),
        29,
        DataFocus {
            dataset: vec!["time".to_string()],
            dataset_separator: Some(".".to_string()),
            ..DataFocus::default()
        },
    )
    .await?;

    assert_eq!(
        chunk.iter().map(|c| c.len()).collect::<Vec<_>>(),
        vec![5, 5, 5, 5, 5, 4]
    );
    assert_eq!(schema_field_names(&schema), vec!["time"]);

    // this is a horrible hack that resolves a race condition where the slog threads are still
    // writing but the tempdir is deleted, resulting in intermittent test failures.
    //TODO: graceful shutdown of test server
    tokio::time::sleep(Duration::from_millis(300)).await;

    Ok(())
}

#[tokio::test]
async fn topic_time_query() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    let mut file = File::open("./tests/data/timed.arrow")?;

    let metadata = read::read_file_metadata(&mut file)?;
    let schema = metadata.schema.clone();
    let reader = read::FileReader::new(file, metadata, None, None);
    let chunks = reader.collect::<arrow2::error::Result<Vec<_>>>()?;

    let chunk_a = SchemaChunk {
        chunk: chunks[0].clone(),
        schema: schema.clone(),
    };

    chunk_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        chunk_a.clone(),
    )
    .await?;

    let topic_url = topic_records_url(&server, &topic_name);
    let mut response = client.post(&topic_url).json(&json!({}));
    response = response.header("Accept", CONTENT_TYPE_ARROW);
    response = response.query(&[("time.start", "2023-11-15T19:00:00+00:00")]);
    response = response.query(&[("time.end", "2023-11-17T21:00:00+00:00")]);
    let bytes = response.send().await?.error_for_status()?.bytes().await?;

    let mut cursor = Cursor::new(bytes);
    let metadata = read::read_file_metadata(&mut cursor)?;
    let reader = read::FileReader::new(cursor, metadata, None, None);

    let chunks = reader.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(chunks.len(), 1);

    Ok(())
}

#[tokio::test]
async fn topic_iterate_pandas_records() -> Result<()> {
    let (client, topic_name, server) = setup().await;

    let chunk_a = inferences_schema_a();

    for _ in 0..10 {
        chunk_append(
            &client,
            append_url(&server, &topic_name, PARTITION_NAME).as_str(),
            chunk_a.clone(),
        )
        .await?;
    }

    let topic_url = topic_records_url(&server, &topic_name);
    let request = client
        .post(&topic_url)
        .json(&json!({}))
        .query(&[("page_size", 3)])
        .query(&[("dataset[]", "inputs")])
        .query(&[("dataset[]", "outputs")])
        .query(&[("dataset.separator", ".")])
        .header("Accept", "application/json; format=pandas-records");

    let result = request.send().await?.error_for_status()?;

    assert_eq!(
        result
            .headers()
            .get(ITERATION_STATUS_HEADER)
            .unwrap()
            .to_str()?,
        "{\"status\":\"RecordLimited\",\"next\":{\"partition-1\":3}}"
    );

    let json = result.json::<serde_json::Value>().await?;

    assert_eq!(
        json,
        json!([
            {"inputs": 1.0, "outputs.mul": 2.0, "outputs.tensor": [2.0, 2.0]},
            {"inputs": 2.0, "outputs.mul": 2.0, "outputs.tensor": []},
            {"inputs": 3.0, "outputs.mul": 2.0, "outputs.tensor": [4.0, 4.0]},
        ])
    );

    let request = client
        .post(&topic_url)
        .json(&json!({}))
        .query(&[("page_size", 100)])
        .query(&[("dataset[]", "inputs")])
        .query(&[("dataset[]", "outputs")])
        .query(&[("dataset.separator", ".")])
        .header("Accept", "application/json; format=pandas-records");

    let result = request.send().await?.error_for_status()?;
    let status: serde_json::Value = serde_json::from_str(
        result
            .headers()
            .get(ITERATION_STATUS_HEADER)
            .unwrap()
            .to_str()?,
    )?;

    let request = client
        .post(&topic_url)
        .json(status.get("next").unwrap())
        .query(&[("page_size", 100)])
        .query(&[("dataset[]", "inputs")])
        .query(&[("dataset[]", "outputs")])
        .query(&[("dataset.separator", ".")])
        .header("Accept", "application/json; format=pandas-records");

    let result = request.send().await?.error_for_status()?;
    let json = result.json::<serde_json::Value>().await?;
    assert_eq!(json, json!([]));

    // this is a horrible hack that resolves a race condition where the slog threads are still
    // writing but the tempdir is deleted, resulting in intermittent test failures.
    //TODO: graceful shutdown of test server
    tokio::time::sleep(Duration::from_millis(300)).await;

    Ok(())
}

#[tokio::test]
async fn partition_status_all() {
    let (client, topic_name, server) = setup().await;

    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        10,
    )
    .await;

    let json_response = fetch_partition_records(
        &client,
        partition_records_url(&server, &topic_name, PARTITION_NAME).as_str(),
        None,
    )
    .await;
    assert_status(&json_response, "All");
    assert_response_length(&json_response, 10);
}

#[tokio::test]
async fn partition_status_record_limited() {
    let (client, topic_name, server) = setup().await;

    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        10,
    )
    .await;

    // test record-limited request, should get 'RecordLimited' response and fewer results
    let json_response = fetch_partition_records(
        &client,
        partition_records_url(&server, &topic_name, PARTITION_NAME).as_str(),
        5,
    )
    .await;
    assert_status(&json_response, "RecordLimited");
    assert_response_length(&json_response, 5);
}

#[tokio::test]
async fn partition_status_byte_limited() {
    let (client, topic_name, server) = setup().await;

    const DEFAULT_MAX_BYTES: usize = 100 * 1024; // 100KB
    let test_messsage_bytelen = TEST_MESSAGE.as_bytes().len();
    // find the upper limit of messages we can store, accounting for the 10 records we already added
    let message_limit = DEFAULT_MAX_BYTES / test_messsage_bytelen;
    repeat_append(
        &client,
        append_url(&server, &topic_name, PARTITION_NAME).as_str(),
        TEST_MESSAGE,
        // add one more message so that we're beyond the limit
        message_limit + 1,
    )
    .await;
    let json_response = fetch_partition_records(
        &client,
        partition_records_url(&server, &topic_name, PARTITION_NAME).as_str(),
        None,
    )
    .await;
    assert_status(&json_response, "ByteLimited");
    // plateau always finishes the current record when we hit the byte limit, so we expect to have
    // one more than the actual hard limit caculated above
    assert_response_length(&json_response, message_limit + 1);
}

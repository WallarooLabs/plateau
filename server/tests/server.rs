use anyhow::Result;
use arrow2::array::{Array, ListArray, PrimitiveArray, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Metadata};
use arrow2::io::ipc::{read, write};
use plateau::chunk::Schema;
use plateau::http;
use plateau::http::TestServer;
use plateau_transport::arrow2;
use reqwest::Client;
use serde_json::json;
use std::io::Cursor;
use std::time::Duration;

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
        response = response.query(&[("limit", limit)]);
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
        response = response.query(&[("limit", limit)]);
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
        response = response.query(&[("limit", limit)]);
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
    pretty_env_logger::try_init().ok(); // called multiple times, so ignore errors
    (
        Client::new(),
        format!("topic-{}", uuid::Uuid::new_v4()),
        http::TestServer::new().await.unwrap(),
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
        vec![5, 5, 5, 5, 5, 4]
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
        vec![1, 5, 5, 5, 5]
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
        .query(&[("limit", 3)])
        .query(&[("dataset[]", "inputs")])
        .query(&[("dataset[]", "outputs")])
        .query(&[("dataset.separator", ".")])
        .header("Accept", "application/json; format=pandas-records");

    let result = request.send().await?.error_for_status()?;

    assert_eq!(
        result
            .headers()
            .get("X-Iteration-Status")
            .unwrap()
            .to_str()?,
        "{\"status\":\"RecordLimited\",\"next\":{\"partition-1\":3}}"
    );

    let json = result.json::<serde_json::Value>().await?;

    assert_eq!(
        json,
        json!([
            {"inputs": 1.0, "outputs": [2.0, 2.0]},
            {"inputs": 2.0, "outputs": [4.0, 4.0]},
            {"inputs": 3.0, "outputs": [6.0, 6.0]},
        ])
    );

    let request = client
        .post(&topic_url)
        .json(&json!({}))
        .query(&[("limit", 100)])
        .query(&[("dataset[]", "inputs")])
        .query(&[("dataset[]", "outputs")])
        .query(&[("dataset.separator", ".")])
        .header("Accept", "application/json; format=pandas-records");

    let result = request.send().await?.error_for_status()?;
    let status: serde_json::Value = serde_json::from_str(
        result
            .headers()
            .get("X-Iteration-Status")
            .unwrap()
            .to_str()?,
    )?;

    let request = client
        .post(&topic_url)
        .json(status.get("next").unwrap())
        .query(&[("limit", 100)])
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

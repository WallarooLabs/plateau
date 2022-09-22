use anyhow::Result;
use plateau::http;
use plateau::http::TestServer;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

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

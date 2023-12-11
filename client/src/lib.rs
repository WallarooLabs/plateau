//! General-use client library for accessing plateau.
use std::fmt::Formatter;
use std::time::{Duration, Instant};
use std::{io::Cursor, pin::Pin, str::FromStr};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStream, TryStreamExt};
use plateau_transport::headers::ITERATION_STATUS_HEADER;
use plateau_transport::CONTENT_TYPE_JSON;
pub use plateau_transport::{
    self as transport, arrow2,
    arrow2::io::ipc,
    arrow2::io::ipc::read::stream_async::{read_stream_metadata_async, AsyncStreamReader},
    estimate_array_size, estimate_size, is_variable_len, ArrowError, ArrowSchema, ChunkError,
    DataFocus, Insert, InsertQuery, Inserted, MultiChunk, PartitionFilter, PartitionSelector,
    Partitions, RecordQuery, RecordStatus, Records, SchemaChunk, Span, TopicIterationOrder,
    TopicIterationQuery, TopicIterationReply, TopicIterationStatus, TopicIterator, Topics,
    CONTENT_TYPE_ARROW,
};
pub use reqwest;
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Body, RequestBuilder, Response, Url,
};
use thiserror::Error;

#[cfg(feature = "batch")]
pub mod batch;

pub mod replicate;

#[derive(Debug)]
pub struct MaxRequestSize(pub Option<usize>);
impl std::fmt::Display for MaxRequestSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(s) => f.write_str(s.to_string().as_str()),
            None => f.write_str("Unknown"),
        }
    }
}

/// Plateau errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("URL parse error for {1}: {0}")]
    UrlParse(url::ParseError, String),
    #[error("URL join error for '{1}'.join('{2}'): {0}")]
    UrlJoin(url::ParseError, Url, String),
    #[error("Query serialization error: {0}")]
    QuerySerialization(#[from] serde_qs::Error),
    #[error("Error sending request: {0}")]
    SendingRequest(reqwest::Error),
    #[error("The request body is too long. Max request size: {0}")]
    RequestTooLong(MaxRequestSize),
    #[error("The request failed: {0}")]
    RequestFailed(String),
    #[error("Error from server: {0}")]
    Server(reqwest::Error),
    #[error("Error deserializing server response: {0}")]
    Deserialize(reqwest::Error),
    #[error("Error streaming server request: {0}")]
    ArrowSerialize(ArrowError),
    #[error("Error streaming server response: {0}")]
    ArrowDeserialize(ArrowError),
    #[error("Error parsing json: {0}")]
    BadJson(#[from] serde_json::Error),
    #[error("Server unhealthy")]
    Unhealthy,
    #[error("Empty stream from server")]
    EmptyStream,
    #[error("Schemas do not match")]
    SchemaMismatch,
    #[cfg(feature = "polars")]
    #[error("Failed polars parse: {0}")]
    PolarsParse(polars::error::PolarsError),
}

/// Plateau client. Creation options:
/// ```
/// use plateau_client::Client;
///
/// // Client pointed at 'localhost:3030'.
/// let client = Client::default();
///
/// // Client pointed at an alternate URL.
/// let client = Client::new("plateau.my-wallaroo-cluster.dev:1234");
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    server_url: Url,
    http_client: reqwest::Client,
}

const DEFAULT_PLATEAU_PORT: u16 = 3030;

pub fn localhost() -> Url {
    format!("http://localhost:{DEFAULT_PLATEAU_PORT}")
        .parse()
        .expect("unexpected URL parse failure")
}

impl Default for Client {
    fn default() -> Client {
        Client {
            server_url: localhost(),
            http_client: reqwest::Client::new(),
        }
    }
}

impl From<Url> for Client {
    fn from(orig: Url) -> Client {
        Client {
            server_url: orig,
            http_client: reqwest::Client::new(),
        }
    }
}

impl FromStr for Client {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Client {
            server_url: s.parse()?,
            http_client: reqwest::Client::new(),
        })
    }
}

// send request to server and perform basic erorr handling
async fn process_request(r: RequestBuilder) -> Result<Response, Error> {
    let response = r.send().await.map_err(Error::SendingRequest)?;

    if response.status() == 413 {
        let max = response
            .headers()
            .get(plateau_transport::headers::MAX_REQUEST_SIZE_HEADER)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| usize::from_str(s).ok());
        return Err(Error::RequestTooLong(MaxRequestSize(max)));
    }

    response.error_for_status().map_err(Error::Server)
}

// process request and deserialize JSON response
async fn process_deserialize_request<T>(r: RequestBuilder) -> Result<T, Error>
where
    T: for<'de> serde::de::Deserialize<'de>,
{
    process_request(r)
        .await?
        .json::<T>()
        .await
        .map_err(Error::Deserialize)
}

async fn process_request_into_stream(
    r: RequestBuilder,
) -> Result<Pin<Box<dyn ArrowStream>>, Error> {
    Ok(Box::pin(process_request(r).await?.bytes_stream().map_err(
        |e| std::io::Error::new(std::io::ErrorKind::Other, e),
    )))
}

fn add_trailing_slash(s: impl AsRef<str>) -> String {
    format!("{}/", s.as_ref())
}

trait TryJoin {
    fn try_join(&self, s: impl AsRef<str>) -> Result<Self, Error>
    where
        Self: Sized;
}
impl TryJoin for Url {
    fn try_join(&self, s: impl AsRef<str>) -> Result<Url, Error> {
        self.join(s.as_ref())
            .map_err(|e| Error::UrlJoin(e, self.clone(), s.as_ref().to_owned()))
    }
}

impl Client {
    /// Create a new [Client] targeting the provided URL.
    pub fn new(url: &str) -> Result<Self, Error> {
        url.parse().map_err(|e| Error::UrlParse(e, url.to_owned()))
    }

    /// Wait until the server is healthy.
    ///
    /// Returns either `Ok(elapsed)` or the `Error` from the last healthcheck attempt.
    pub async fn healthy(&self, duration: Duration, retry: Duration) -> Result<Duration, Error> {
        let start = Instant::now();

        loop {
            match self.healthcheck().await {
                Ok(_) => return Ok(start.elapsed()),
                Err(e) if start.elapsed() > duration => return Err(e),
                _ => tokio::time::sleep(retry).await,
            }
        }
    }

    /// Perform a server healthcheck.
    pub async fn healthcheck(&self) -> Result<(), Error> {
        let response: serde_json::Value =
            process_deserialize_request(self.http_client.get(self.server_url.try_join("ok")?))
                .await?;

        if response == serde_json::json!({"ok": "true"}) {
            Ok(())
        } else {
            Err(Error::Unhealthy)
        }
    }

    /// Retrieve a list of all topics.
    pub async fn get_topics(&self) -> Result<Topics, Error> {
        process_deserialize_request(self.http_client.get(self.server_url.try_join("topics")?)).await
    }

    /// Retrieve a list of partitions for a specified topic.
    pub async fn get_partitions(&self, topic_name: impl AsRef<str>) -> Result<Partitions, Error> {
        process_deserialize_request(
            self.http_client.get(
                self.server_url
                    .try_join("topic/")?
                    .try_join(topic_name.as_ref())?,
            ),
        )
        .await
    }

    fn retrieve_request(
        &self,
        topic_name: impl AsRef<str>,
        partition_name: impl AsRef<str>,
        params: &RecordQuery,
    ) -> Result<RequestBuilder, Error> {
        let mut url = self
            .server_url
            .try_join("topic/")?
            .try_join(add_trailing_slash(topic_name))?
            .try_join("partition/")?
            .try_join(add_trailing_slash(partition_name))?
            .try_join("records")?;

        url.set_query(Some(&serde_qs::to_string(params)?));

        Ok(self.http_client.get(url))
    }

    fn iteration_request<'a>(
        &self,
        topic_name: impl AsRef<str>,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>>,
    ) -> Result<RequestBuilder, Error> {
        let mut url = self
            .server_url
            .try_join("topic/")?
            .try_join(add_trailing_slash(topic_name))?
            .try_join("records")?;
        url.set_query(Some(&serde_qs::to_string(params)?));

        let base_request = self.http_client.post(url);

        Ok(match position.into() {
            Some(position) => base_request.json(&position),
            None => base_request.json(&{}),
        })
    }

    /// Append one or more record(s) to a given topic and partition. See [InsertQuery] for more
    /// parameters, and [Insertion] for the ways in which data can be provided.
    pub async fn append_records(
        &self,
        topic_name: impl AsRef<str>,
        partition_name: impl AsRef<str>,
        query: &InsertQuery,
        records: impl Insertion,
    ) -> Result<Inserted, Error> {
        let mut url = self
            .server_url
            .try_join("topic/")?
            .try_join(add_trailing_slash(topic_name))?
            .try_join("partition/")?
            .try_join(partition_name.as_ref())?;

        url.set_query(Some(&serde_qs::to_string(query)?));
        let builder = self.http_client.post(url).query(query);
        process_deserialize_request(records.add_to_request(builder)?).await
    }
}

pub trait ArrowStream:
    TryStream<Ok = Bytes, Error = std::io::Error, Item = Result<Bytes, std::io::Error>>
    + Send
    + Sync
    + 'static
{
}
impl<T> ArrowStream for T where
    T: TryStream<Ok = Bytes, Error = std::io::Error, Item = Result<Bytes, std::io::Error>>
        + Send
        + Sync
        + 'static
{
}

pub trait Insertion {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error>;
}

impl Insertion for Insert {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        Ok(r.json(&self))
    }
}

impl Insertion for SchemaChunk<ArrowSchema> {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let bytes = self.to_bytes().map_err(Error::ArrowSerialize)?;
        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, bytes.len())
            .body(bytes))
    }
}

impl Insertion for MultiChunk {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let options = ipc::write::WriteOptions { compression: None };

        let mut writer = ipc::write::FileWriter::new(bytes, self.schema.clone(), None, options);

        writer.start().map_err(Error::ArrowSerialize)?;
        for chunk in &self.chunks {
            writer.write(chunk, None).map_err(Error::ArrowSerialize)?;
        }
        writer.finish().map_err(Error::ArrowSerialize)?;

        let bytes = writer.into_inner().into_inner();
        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, bytes.len())
            .body(bytes))
    }
}

impl Insertion for Vec<SchemaChunk<ArrowSchema>> {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let options = ipc::write::WriteOptions { compression: None };

        let schema = self.first().map(|d| &d.schema).ok_or_else(|| {
            Error::ArrowSerialize(ArrowError::InvalidArgumentError(
                "cannot send empty request".to_string(),
            ))
        })?;
        let mut writer = ipc::write::FileWriter::new(bytes, schema.clone(), None, options);

        writer.start().map_err(Error::ArrowSerialize)?;
        for data in self.iter() {
            if &data.schema != schema {
                return Err(Error::SchemaMismatch);
            }
            writer
                .write(&data.chunk, None)
                .map_err(Error::ArrowSerialize)?;
        }
        writer.finish().map_err(Error::ArrowSerialize)?;

        let bytes = writer.into_inner().into_inner();
        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, bytes.len())
            .body(bytes))
    }
}

/// An [ArrowStream] with its known size
pub struct SizedArrowStream {
    /// byte stream
    pub stream: Pin<Box<dyn ArrowStream>>,
    /// size in bytes
    pub size: u64,
}

impl Insertion for SizedArrowStream {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, self.size)
            .body(Body::wrap_stream(self.stream)))
    }
}

pub fn bytes_into_multichunk(bytes: Bytes) -> Result<MultiChunk, Error> {
    let mut cursor = Cursor::new(bytes);
    let metadata = ipc::read::read_file_metadata(&mut cursor).map_err(Error::ArrowDeserialize)?;
    let schema = metadata.schema.clone();
    let reader = ipc::read::FileReader::new(cursor, metadata, None, None);
    let chunks = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(Error::ArrowDeserialize)?;

    Ok(MultiChunk { schema, chunks })
}

pub fn bytes_into_schemachunks(bytes: Bytes) -> Result<Vec<SchemaChunk<ArrowSchema>>, Error> {
    let multi = bytes_into_multichunk(bytes)?;

    Ok(multi.to_schemachunks())
}

#[cfg(feature = "polars")]
pub fn bytes_into_polars(bytes: Bytes) -> Result<polars::frame::DataFrame, Error> {
    use polars::prelude::SerReader;

    let cursor = Cursor::new(bytes);
    let reader = polars::io::ipc::IpcReader::new(cursor);
    let df = reader.finish();
    df.map_err(Error::PolarsParse)
}

/// Trait for providing iteration through a topic's record, providing records in a specific
///  `Output` format.
#[async_trait]
pub trait Iterate<Output> {
    /// Iterate over a topic, returning records in `Output` format. See [TopicIterationQuery] for
    /// more details on the parameters.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Output, Error>;
}

#[derive(Debug, Clone)]
pub struct ArrowIterationReply {
    pub chunks: MultiChunk,
    pub status: Option<TopicIterationStatus>,
}

#[async_trait]
impl Iterate<ArrowIterationReply> for Client {
    /// Iterate over a topic, returning records in a full [`ArrowIterationReply`].
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<ArrowIterationReply, Error> {
        let response = process_request(
            self.iteration_request(topic_name, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?;

        let status = response
            .headers()
            .get(ITERATION_STATUS_HEADER)
            .map(|header| serde_json::from_slice::<TopicIterationStatus>(header.as_ref()))
            .transpose()?;

        let bytes = response.bytes().await.map_err(Error::Server)?;
        let chunks = bytes_into_multichunk(bytes)?;

        Ok(ArrowIterationReply { chunks, status })
    }
}

#[async_trait]
impl Iterate<TopicIterationReply> for Client {
    /// Iterate over a topic, returning records in [TopicIterationReply] (plaintext) format.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<TopicIterationReply, Error> {
        process_deserialize_request(
            self.iteration_request(topic_name, params, position)?
                .header("accept", CONTENT_TYPE_JSON),
        )
        .await
    }
}

#[async_trait]
impl Iterate<Pin<Box<dyn ArrowStream>>> for Client {
    /// Iterate over a topic, returning records in streaming format. The data stream should be
    /// deserializeable into a [`SchemaChunk<Schema>`] format.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Pin<Box<dyn ArrowStream>>, Error> {
        process_request_into_stream(
            self.iteration_request(topic_name, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await
    }
}

#[async_trait]
impl Iterate<Vec<SchemaChunk<ArrowSchema>>> for Client {
    /// Iterate over a topic, returning records in [`SchemaChunk<Schema>`] format.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Vec<SchemaChunk<ArrowSchema>>, Error> {
        let bytes = process_request(
            self.iteration_request(topic_name, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;

        bytes_into_schemachunks(bytes)
    }
}

#[cfg(feature = "polars")]
#[async_trait]
impl Iterate<polars::frame::DataFrame> for Client {
    /// Iterate over a topic, returning records in [`SchemaChunk<Schema>`] format.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<polars::frame::DataFrame, Error> {
        let bytes = process_request(
            self.iteration_request(topic_name, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;

        bytes_into_polars(bytes)
    }
}

#[cfg(feature = "polars")]
#[async_trait]
impl Retrieve<polars::frame::DataFrame> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// [Records] (plaintext) format.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<polars::frame::DataFrame, Error> {
        let b = process_request(
            self.retrieve_request(topic_name, partition_name, params)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;
        bytes_into_polars(b)
    }
}

/// Trait for providing retrieval of a topic and partition/s records, providing records in a
/// specific `Output` format.
#[async_trait]
pub trait Retrieve<Output> {
    /// Retrieve a set of records from a specifid topic and partition. See [RecordQuery] for
    /// more details on query parameters.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<Output, Error>;
}

#[async_trait]
impl Retrieve<Records> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// [Records] (plaintext) format.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<Records, Error> {
        process_deserialize_request(
            self.retrieve_request(topic_name, partition_name, params)?
                .header("accept", CONTENT_TYPE_JSON),
        )
        .await
    }
}

#[async_trait]
impl Retrieve<Pin<Box<dyn ArrowStream>>> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// streaming format. This stream should be deserializable into a [`SchemaChunk<Schema>`].
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<Pin<Box<dyn ArrowStream>>, Error> {
        process_request_into_stream(
            self.retrieve_request(topic_name, partition_name, params)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await
    }
}

#[async_trait]
impl Retrieve<Vec<SchemaChunk<ArrowSchema>>> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// [`SchemaChunk<Schema>`] format.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<Vec<SchemaChunk<ArrowSchema>>, Error> {
        let bytes = process_request(
            self.retrieve_request(topic_name, partition_name, params)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;

        bytes_into_schemachunks(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Cursor};

    use httptest::{
        all_of,
        matchers::{contains, eq, json_decoded, key, len, not, request, url_decoded},
        responders::json_encoded,
        Expectation, Server,
    };
    use plateau_transport::{
        arrow2::{
            array::PrimitiveArray,
            chunk::Chunk,
            datatypes::{Field, Metadata},
        },
        ArrowSchema, DataFocus, Insert, InsertQuery, Inserted, Partitions, RecordQuery,
        RecordStatus, Records, SchemaChunk, Span, Topic, TopicIterationOrder, TopicIterationQuery,
        TopicIterationReply, TopicIterationStatus, Topics, CONTENT_TYPE_ARROW,
    };
    use tokio_util::io::ReaderStream;

    use crate::{Client, Iterate, Retrieve, SizedArrowStream};

    fn example_chunk() -> SchemaChunk<ArrowSchema> {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
        let inputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let outputs = PrimitiveArray::<f32>::from_values(vec![0.6, 0.4, 0.2, 0.8, 0.8]);

        let schema = ArrowSchema {
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

    fn test_client(server: &Server) -> Client {
        server
            .url_str("/")
            .parse()
            .expect("failure to parse test server URL")
    }

    #[tokio::test]
    async fn topics() {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![request::method("GET"), request::path("/topics"),])
                .respond_with(json_encoded(Topics {
                    topics: vec!["topic-1", "topic-2"]
                        .drain(..)
                        .map(|t| Topic { name: t.to_owned() })
                        .collect(),
                })),
        );

        let client: Client = test_client(&server);

        assert_eq!(
            client
                .get_topics()
                .await
                .expect("failure getting topics")
                .topics
                .iter()
                .map(|t| t.name.as_ref())
                .collect::<Vec<&str>>(),
            vec!["topic-1", "topic-2"]
        );
    }

    #[tokio::test]
    async fn partitions() {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/topic/topic-1"),
            ])
            .respond_with(json_encoded(Partitions {
                partitions: vec!["part-1", "part-2"]
                    .drain(..)
                    .map(|p| (p.to_owned(), Span { start: 0, end: 100 }))
                    .collect(),
            })),
        );

        let client: Client = test_client(&server);

        let partitions = client
            .get_partitions("topic-1")
            .await
            .expect("failure getting partitions")
            .partitions;
        let kvs = partitions
            .iter()
            .map(|(k, v)| (k.as_ref(), v))
            .collect::<Vec<(&str, &Span)>>();

        assert_eq!(kvs.len(), 2);
        assert!(kvs
            .iter()
            .any(|p| p == &("part-1", &Span { start: 0, end: 100 })));
        assert!(kvs
            .iter()
            .any(|p| p == &("part-2", &Span { start: 0, end: 100 })));
    }

    #[tokio::test]
    async fn records() {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/topic/topic-1/partition/partition-1/records"),
                request::query(url_decoded(contains(("start", "0")))),
            ])
            .respond_with(json_encoded(Records {
                records: vec![
                    "message-1".to_owned(),
                    "message-2".to_owned(),
                    "message-3".to_owned(),
                ],
                span: Some(Span { start: 0, end: 3 }),
                status: RecordStatus::All,
            })),
        );

        let client: Client = test_client(&server);

        let records: Records = client
            .get_records(
                "topic-1",
                "partition-1",
                &RecordQuery {
                    start: 0,
                    ..Default::default()
                },
            )
            .await
            .expect("failure getting records");
        assert_eq!(
            records
                .records
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<&str>>(),
            vec!["message-1", "message-2", "message-3"]
        );
    }

    #[tokio::test]
    async fn iterate_records() {
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/topic/topic-1/records"),
                request::query(url_decoded(contains(("page_size", "4"))))
            ])
            .respond_with(json_encoded(TopicIterationReply {
                records: vec!["message-1", "message-2", "message-3", "message-4"]
                    .drain(..)
                    .map(|s| s.to_owned())
                    .collect(),
                status: TopicIterationStatus {
                    status: RecordStatus::RecordLimited,
                    next: HashMap::from([("part-1".to_owned(), 4)]),
                },
            })),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::path("/topic/topic-1/records"),
                request::query(url_decoded(contains(("page_size", "4")))),
                request::body(json_decoded(eq(serde_json::json!({"part-1": 4}))))
            ])
            .respond_with(json_encoded(TopicIterationReply {
                records: vec!["message-5", "message-6", "message-7"]
                    .drain(..)
                    .map(|s| s.to_owned())
                    .collect(),
                status: TopicIterationStatus {
                    status: RecordStatus::All,
                    next: HashMap::from([("part-1".to_owned(), 7)]),
                },
            })),
        );

        let client: Client = test_client(&server);

        let response1: TopicIterationReply = client
            .iterate_topic(
                "topic-1",
                &TopicIterationQuery {
                    page_size: Some(4),
                    start_time: None,
                    end_time: None,
                    order: Some(TopicIterationOrder::Asc),
                    data_focus: DataFocus::default(),
                    partition_filter: None,
                },
                &None,
            )
            .await
            .expect("failure iterating records");

        assert_eq!(
            response1
                .records
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<&str>>(),
            vec!["message-1", "message-2", "message-3", "message-4"]
        );
        assert_eq!(response1.status.status, RecordStatus::RecordLimited);
        assert_eq!(
            response1.status.next,
            HashMap::from([("part-1".to_owned(), 4)])
        );

        let response2: TopicIterationReply = client
            .iterate_topic(
                "topic-1",
                &TopicIterationQuery {
                    page_size: Some(4),
                    start_time: None,
                    end_time: None,
                    order: Some(TopicIterationOrder::Asc),
                    data_focus: DataFocus::default(),
                    partition_filter: None,
                },
                &Some(HashMap::from([("part-1".to_owned(), 4)])),
            )
            .await
            .expect("failure iterating records");

        assert_eq!(
            response2
                .records
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<&str>>(),
            vec!["message-5", "message-6", "message-7"]
        );
        assert_eq!(response2.status.status, RecordStatus::All);
        assert_eq!(
            response2.status.next,
            HashMap::from([("part-1".to_owned(), 7)])
        );
    }

    #[tokio::test]
    async fn append_plaintext() {
        let server = Server::run();
        let input = vec![
            "First input".to_owned(),
            "Second input".to_owned(),
            "Third input".to_owned(),
        ];

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::headers(contains(not(key("content-type")))),
                request::path("/topic/topic-1/partition/partition-1"),
                request::query(url_decoded(len(eq(0)))),
            ])
            .respond_with(json_encoded(Inserted {
                span: Span {
                    start: 0,
                    end: input.len(),
                },
            })),
        );

        let client = test_client(&server);

        let response = client
            .append_records(
                "topic-1",
                "partition-1",
                &InsertQuery { time: None },
                Insert { records: input },
            )
            .await
            .expect("failure appending to parition");

        assert_eq!(response.span.start, 0);
        assert_eq!(response.span.end, 3);
    }

    #[tokio::test]
    async fn oversize_chunk_reports_max() {
        let chunk = example_chunk();

        let server = plateau::http::TestServer::new_with_config(plateau::config::PlateauConfig {
            http: plateau::http::Config {
                max_append_bytes: 5,
                ..plateau::http::Config::default()
            },
            ..plateau::config::PlateauConfig::default()
        })
        .await
        .unwrap();

        let client = Client {
            server_url: crate::Url::parse(&server.base()).unwrap(),
            http_client: Default::default(),
        };

        let response = client
            .append_records("topic-1", "partition-1", &InsertQuery { time: None }, chunk)
            .await;

        assert_eq!(
            "RequestTooLong(MaxRequestSize(Some(5)))",
            format!("{:?}", response.err().unwrap())
        );
    }

    #[tokio::test]
    async fn append_chunk() {
        let server = Server::run();
        let chunk = example_chunk();

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::headers(contains(("content-type", CONTENT_TYPE_ARROW))),
                request::path("/topic/topic-1/partition/partition-1"),
                request::query(url_decoded(len(eq(0)))),
            ])
            .respond_with(json_encoded(Inserted {
                span: Span {
                    start: 0,
                    end: chunk.len(),
                },
            })),
        );

        let client = test_client(&server);

        let response = client
            .append_records("topic-1", "partition-1", &InsertQuery { time: None }, chunk)
            .await
            .expect("failure appending to parition");

        assert_eq!(response.span.start, 0);
        assert_eq!(response.span.end, 5);
    }

    #[tokio::test]
    async fn append_stream() {
        let server = Server::run();
        let chunk = example_chunk();
        let stream_reader = ReaderStream::new(Cursor::new(
            chunk
                .to_bytes()
                .expect("failed to convert schemachunk to bytes"),
        ));

        server.expect(
            Expectation::matching(all_of![
                request::method("POST"),
                request::headers(contains(("content-type", CONTENT_TYPE_ARROW))),
                request::headers(contains(("content-length", "27"))),
                request::path("/topic/topic-1/partition/partition-1"),
                request::query(url_decoded(len(eq(0)))),
            ])
            .respond_with(json_encoded(Inserted {
                span: Span {
                    start: 0,
                    end: chunk.len(),
                },
            })),
        );

        let client = test_client(&server);

        let response = client
            .append_records(
                "topic-1",
                "partition-1",
                &InsertQuery { time: None },
                SizedArrowStream {
                    stream: Box::pin(stream_reader),
                    size: 27,
                },
            )
            .await
            .expect("failure appending to parition");

        assert_eq!(response.span.start, 0);
        assert_eq!(response.span.end, 5);
    }
}

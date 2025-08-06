//! General-use client library for accessing plateau.
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::{pin::Pin, str::FromStr};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStream, TryStreamExt};
use plateau_transport_arrow_rs as transport;
pub use reqwest;
use reqwest::Body;
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    RequestBuilder, Response, Url,
};
use thiserror::Error;
use tracing::{trace, warn};
use transport::headers::ITERATION_STATUS_HEADER;
#[cfg(feature = "polars")]
use transport::RecordStatus;
use transport::CONTENT_TYPE_JSON;
use transport::{
    arrow_ipc,
    arrow_schema::{ArrowError, Schema, SchemaRef},
    Insert, InsertQuery, Inserted, MultiChunk, Partitions, RecordQuery, Records, SchemaChunk,
    TopicIterationQuery, TopicIterationReply, TopicIterationStatus, TopicIterator, Topics,
    CONTENT_TYPE_ARROW,
};

#[cfg(feature = "health")]
use tokio::time;

#[cfg(feature = "batch")]
pub mod batch;

pub mod replicate;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaxRequestSize(pub Option<usize>);

impl fmt::Display for MaxRequestSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(s) => format_args!("{s}").fmt(f),
            None => "Unknown".fmt(f),
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
    #[error("The request body is too long ({0}). Max request size: {1}")]
    RequestTooLong(String, MaxRequestSize),
    #[error("The request failed: {0}")]
    RequestFailed(String),
    #[error("Error from plateau server: {0}")]
    Server(reqwest::Error),
    #[error("Error deserializing plateau server response: {0}")]
    Deserialize(reqwest::Error),
    #[error("Error streaming plateau server request: {0}")]
    ArrowSerialize(ArrowError),
    #[error("Error streaming plateau server response: {0}")]
    ArrowDeserialize(ArrowError),
    #[error("Error parsing json: {0}")]
    BadJson(#[from] serde_json::Error),
    #[error("plateau server unhealthy")]
    Unhealthy,
    #[error("Empty stream from plateau server")]
    EmptyStream,
    #[error("Schemas do not match")]
    SchemaMismatch,
    #[cfg(feature = "polars")]
    #[error("Failed polars parse: {0}")]
    PolarsParse(polars::error::PolarsError),
    #[error("Cannot reshape chunks to fit within request limit (single row bytes {0} > {1})")]
    CannotReshape(String, MaxRequestSize),
}

pub const DEFAULT_MAX_BATCH_BYTES: usize = 10240000;

/// Plateau client. Creation options:
/// ```
/// // Client pointed at 'localhost:3030'.
/// let client = plateau_client_arrow_rs::Client::default();
///
/// // Client pointed at an alternate URL.
/// let client = plateau_client_arrow_rs::Client::new("plateau.my-wallaroo-cluster.dev:1234");
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    server_url: Url,
    http_client: reqwest::Client,
    max_batch_bytes: usize,
    max_rows: Option<usize>,
}

const DEFAULT_PLATEAU_PORT: u16 = 3030;

pub fn localhost() -> Url {
    format!("http://localhost:{DEFAULT_PLATEAU_PORT}")
        .parse()
        .expect("unexpected URL parse failure")
}

impl Default for Client {
    fn default() -> Self {
        Self {
            server_url: localhost(),
            http_client: reqwest::Client::new(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_rows: None,
        }
    }
}

impl From<Url> for Client {
    fn from(orig: Url) -> Self {
        Self {
            server_url: orig,
            http_client: reqwest::Client::new(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_rows: None,
        }
    }
}

impl FromStr for Client {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            server_url: s.parse()?,
            http_client: reqwest::Client::new(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_rows: None,
        })
    }
}

// send request to server and perform basic error handling
async fn process_request(r: RequestBuilder) -> Result<Response, Error> {
    let (client, result) = r.build_split();
    let request = result.map_err(Error::SendingRequest)?;
    let body_len = request
        .body()
        .and_then(|body| body.as_bytes())
        .map_or_else(|| "streaming".to_string(), |bytes| bytes.len().to_string());
    let response = client
        .execute(request)
        .await
        .map_err(Error::SendingRequest)?;

    if response.status() == 413 {
        let max = response
            .headers()
            .get(transport::headers::MAX_REQUEST_SIZE_HEADER)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| usize::from_str(s).ok());
        return Err(Error::RequestTooLong(body_len, MaxRequestSize(max)));
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
    Ok(Box::pin(
        process_request(r)
            .await?
            .bytes_stream()
            .map_err(io::Error::other),
    ))
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
    fn try_join(&self, s: impl AsRef<str>) -> Result<Self, Error> {
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
    #[cfg(feature = "health")]
    pub async fn healthy(
        &self,
        duration: time::Duration,
        retry: time::Duration,
    ) -> Result<time::Duration, Error> {
        let start = time::Instant::now();

        loop {
            match self.healthcheck().await {
                Ok(_) => return Ok(start.elapsed()),
                Err(e) if start.elapsed() > duration => return Err(e),
                _ => time::sleep(retry).await,
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
        path: impl AsRef<str>,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>>,
    ) -> Result<RequestBuilder, Error> {
        let mut url = self.server_url.try_join(path)?;
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

    /// Append many chunks of records to a topic. Attempts to dynamically resize
    /// if batch size limits are encountered. See [InsertionQueue] for the ways
    /// in which data can be provided.
    pub async fn append_queue(
        &mut self,
        topic_name: impl AsRef<str>,
        partition_name: impl AsRef<str>,
        mut queue: impl InsertionQueue,
    ) -> Result<Option<Inserted>, Error> {
        let url = self
            .server_url
            .try_join("topic/")?
            .try_join(add_trailing_slash(topic_name))?
            .try_join("partition/")?
            .try_join(partition_name.as_ref())?;

        let mut final_insertion = None;
        if let Some(max_rows) = self.max_rows {
            queue.reshape(max_rows);
        }

        while let Some(mut front) = queue.front()? {
            let builder = self.http_client.post(url.clone());
            while front.bytes.len() > self.max_batch_bytes && queue.can_reshape() {
                trace!(
                    current_front_rows = front.rows,
                    current_front_bytes = front.bytes.len()
                );

                let max_rows = front.rows.div_ceil(2);
                self.max_rows = Some(max_rows);
                queue.reshape(max_rows);

                // SAFETY: reshape preserves the total number of rows, so a
                // reshape of a non-empty queue must also be non-empty
                front = queue.front()?.unwrap();
                warn!(
                    "decreasing inferred max row count: {max_rows} ({} bytes)",
                    front.bytes.len()
                );
            }

            let builder = builder
                .header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
                .header(CONTENT_LENGTH, front.bytes.len())
                .body(front.bytes);
            let result = process_deserialize_request(builder).await;

            match result {
                Ok(r) => {
                    final_insertion = Some(r);
                    trace!(sent_rows = front.rows);
                    queue.pop_front()
                }
                Err(e) => {
                    if let Error::RequestTooLong(request_size, MaxRequestSize(Some(s))) = e {
                        if front.rows > 1 {
                            warn!("detected new max plateau batch byte limit: {s}");
                            self.max_batch_bytes = s;
                            // return to sender: because we don't pop_front here
                            // we'll start again from the top with the new limit
                        } else {
                            // we can't shrink below one row, so just die
                            return Err(Error::CannotReshape(
                                request_size,
                                MaxRequestSize(Some(s)),
                            ));
                        }
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok(final_insertion)
    }
}

pub trait ArrowStream:
    TryStream<Ok = Bytes, Error = io::Error, Item = io::Result<Bytes>> + Send + Sync + 'static
{
}
impl<T> ArrowStream for T where
    T: TryStream<Ok = Bytes, Error = io::Error, Item = io::Result<Bytes>> + Send + Sync + 'static
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

impl Insertion for SchemaChunk<Schema> {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let mut buf = Vec::new();

        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &self.schema)
            .map_err(Error::ArrowSerialize)?;

        writer.write(&self.chunk).map_err(Error::ArrowSerialize)?;
        writer.finish().map_err(Error::ArrowSerialize)?;

        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, buf.len())
            .body(buf))
    }
}

impl Insertion for SchemaChunk<SchemaRef> {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let buf = self.to_bytes().map_err(Error::ArrowSerialize)?;

        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, buf.len())
            .body(buf))
    }
}

impl Insertion for MultiChunk {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let mut buf = Vec::new();

        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, self.schema.as_ref())
            .map_err(Error::ArrowSerialize)?;

        for chunk in &self.chunks {
            writer.write(chunk).map_err(Error::ArrowSerialize)?;
        }
        writer.finish().map_err(Error::ArrowSerialize)?;

        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, buf.len())
            .body(buf))
    }
}

impl Insertion for Vec<SchemaChunk<SchemaRef>> {
    fn add_to_request(self, r: RequestBuilder) -> Result<RequestBuilder, Error> {
        let mut buf = Vec::new();
        let options = arrow_ipc::writer::IpcWriteOptions::default();

        let schema = self.first().map(|d| &d.schema).ok_or_else(|| {
            Error::ArrowSerialize(ArrowError::InvalidArgumentError(
                "cannot send empty request".to_string(),
            ))
        })?;
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new_with_options(&mut buf, schema, options)
                .map_err(Error::ArrowSerialize)?;

        for data in self.iter() {
            if &data.schema != schema {
                return Err(Error::SchemaMismatch);
            }
            writer.write(&data.chunk).map_err(Error::ArrowSerialize)?;
        }
        writer.finish().map_err(Error::ArrowSerialize)?;

        Ok(r.header(CONTENT_TYPE, CONTENT_TYPE_ARROW)
            .header(CONTENT_LENGTH, buf.len())
            .body(buf))
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

impl fmt::Debug for SizedArrowStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SizedArrowStream")
            .field("stream", &"<ArrowStream>")
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug)]
pub struct QueueChunk {
    bytes: Vec<u8>,
    rows: usize,
}

pub trait InsertionQueue {
    /// Return the byte representation of the front queue chunk.
    fn front(&self) -> Result<Option<QueueChunk>, Error>;

    /// Remove the front chunk from the queue
    fn pop_front(&mut self);

    /// Return `true` if this queue can be reshaped (otherwise,
    /// [InsertionQueue::reshape] will do nothing).
    fn can_reshape(&self) -> bool;

    /// Resize the queue so that all chunks have fewer than `max_rows` rows.
    /// This operation must preserve the overall length: the total number of
    /// rows in the resulting queue should equal the prior total number of rows.
    fn reshape(&mut self, max_rows: usize);
}

impl InsertionQueue for MultiChunk {
    fn front(&self) -> Result<Option<QueueChunk>, Error> {
        self.chunks
            .front()
            .map(|chunk| {
                let mut buf = Vec::new();
                let options = arrow_ipc::writer::IpcWriteOptions::default();

                let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
                    &mut buf,
                    self.schema.as_ref(),
                    options,
                )
                .map_err(Error::ArrowSerialize)?;

                let rows = chunk.num_rows();
                writer.write(chunk).map_err(Error::ArrowSerialize)?;
                writer.finish().map_err(Error::ArrowSerialize)?;

                Ok(QueueChunk { bytes: buf, rows })
            })
            .transpose()
    }

    fn pop_front(&mut self) {
        self.chunks.pop_front();
    }

    fn can_reshape(&self) -> bool {
        self.chunks.front().is_some_and(|c| c.num_rows() > 1)
    }

    fn reshape(&mut self, max_rows: usize) {
        self.rechunk(max_rows)
    }
}

pub fn bytes_into_multichunk(bytes: Bytes) -> Result<MultiChunk, Error> {
    let cursor = io::Cursor::new(bytes);
    let reader =
        arrow_ipc::reader::FileReader::try_new(cursor, None).map_err(Error::ArrowDeserialize)?;
    let schema = reader.schema();
    let chunks = reader
        .collect::<Result<VecDeque<_>, _>>()
        .map_err(Error::ArrowDeserialize)?;

    Ok(MultiChunk { schema, chunks })
}

pub fn bytes_into_schemachunks(bytes: Bytes) -> Result<Vec<SchemaChunk<SchemaRef>>, Error> {
    let multi = bytes_into_multichunk(bytes)?;

    let schemachunks = multi
        .chunks
        .into_iter()
        .map(|chunk| SchemaChunk {
            schema: multi.schema.clone(),
            chunk,
        })
        .collect();

    Ok(schemachunks)
}

#[cfg(feature = "polars")]
pub fn bytes_into_polars(bytes: Bytes) -> Result<polars::frame::DataFrame, Error> {
    use polars::prelude::SerReader;

    let cursor = io::Cursor::new(bytes);
    let reader = polars::io::ipc::IpcReader::new(cursor);
    let df = reader.finish();
    df.map_err(Error::PolarsParse)
}

/// Trait for providing iteration through a topic's record, providing records in a specific
///  `Output` format.
#[async_trait]
pub trait Iterate<Output> {
    /// Iterate from a given URL, returning records in `Output` format. See
    /// [TopicIterationQuery] for more details on the parameters.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Output, Error>;

    /// Iterate over a topic, returning records in `Output` format. See [TopicIterationQuery] for
    /// more details on the parameters.
    async fn iterate_topic<'a>(
        &self,
        topic_name: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Output, Error> {
        let path = format!("topic/{}/records", topic_name.as_ref());
        self.iterate_path(path, params, position).await
    }
}

#[derive(Debug, Clone)]
pub struct ArrowIterationReply {
    pub chunks: MultiChunk,
    pub status: Option<TopicIterationStatus>,
}

#[async_trait]
impl Iterate<ArrowIterationReply> for Client {
    /// Iterate over a topic, returning records in a full [`ArrowIterationReply`].
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<ArrowIterationReply, Error> {
        let response = process_request(
            self.iteration_request(path, params, position)?
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
/// Trait for iterating through a topic's records, returning all available records in the Output format.
/// This will consume lots of memory.
#[async_trait]
pub trait IterateUnlimited<Output> {
    async fn iterate_topic_unlimited(
        &self,
        topic_name: impl AsRef<str> + Send + Copy,
        params: &TopicIterationQuery,
    ) -> Result<Output, Error>;
}

#[async_trait]
impl Iterate<TopicIterationReply> for Client {
    /// Iterate over a topic, returning records in [TopicIterationReply] (plaintext) format.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<TopicIterationReply, Error> {
        process_deserialize_request(
            self.iteration_request(path, params, position)?
                .header("accept", CONTENT_TYPE_JSON),
        )
        .await
    }
}

#[async_trait]
impl Iterate<Pin<Box<dyn ArrowStream>>> for Client {
    /// Iterate over a topic, returning records in streaming format. The data stream should be
    /// deserializeable into a [`SchemaChunk<Schema>`] format.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Pin<Box<dyn ArrowStream>>, Error> {
        process_request_into_stream(
            self.iteration_request(path, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await
    }
}

#[async_trait]
impl Iterate<Vec<SchemaChunk<SchemaRef>>> for Client {
    /// Iterate over a topic, returning records in [`SchemaChunk<Schema>`] format.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<Vec<SchemaChunk<SchemaRef>>, Error> {
        let bytes = process_request(
            self.iteration_request(path, params, position)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;

        bytes_into_schemachunks(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct PandasRecordIteration {
    pub value: serde_json::Value,
}

#[async_trait]
impl Iterate<PandasRecordIteration> for Client {
    /// Iterate over a topic, returning records in [`PandasRecordIteration`] format.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<PandasRecordIteration, Error> {
        let value = process_request(
            self.iteration_request(path, params, position)?
                .header("accept", CONTENT_TYPE_JSON),
        )
        .await?
        .json()
        .await
        .map_err(Error::Server)?;

        Ok(PandasRecordIteration { value })
    }
}

#[cfg(feature = "polars")]
#[async_trait]
impl Iterate<polars::frame::DataFrame> for Client {
    /// Iterate over a topic, returning records in [`SchemaChunk<Schema>`] format.
    async fn iterate_path<'a>(
        &self,
        path: impl AsRef<str> + Send,
        params: &TopicIterationQuery,
        position: impl Into<Option<&'a TopicIterator>> + Send,
    ) -> Result<polars::frame::DataFrame, Error> {
        let bytes = process_request(
            self.iteration_request(path, params, position)?
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
impl IterateUnlimited<polars::frame::DataFrame> for Client {
    /// Iterate over a topic, returning records in [`SchemaChunk<Schema>`] format.
    async fn iterate_topic_unlimited(
        &self,
        topic_name: impl AsRef<str> + Send + Copy,
        params: &TopicIterationQuery,
    ) -> Result<polars::frame::DataFrame, Error> {
        let path = format!("topic/{}/records", topic_name.as_ref());
        let mut cursor = Some(TopicIterator::new());
        let mut ret_df: Option<polars::frame::DataFrame> = None;

        loop {
            let response = process_request(
                self.iteration_request(path.as_str(), params, &cursor)?
                    .header("accept", CONTENT_TYPE_ARROW),
            )
            .await?;

            tracing::debug!("Iterate Unlimited Response: {:?}", response);

            let headers = response.headers().get("x-iteration-status").cloned();

            let bytes = response.bytes().await.map_err(Error::Server)?;

            let temp_df = bytes_into_polars(bytes)?;

            if temp_df.width() == 0 {
                break;
            }

            if let Some(df) = &ret_df {
                tracing::debug!(df=?df);

                // Polars 0.32:
                // Prefer vstack over extend when you want to append many times before doing a query.
                // For instance when you read in multiple files and when to store them in a single DataFrame.
                // In the latter case, finish the sequence of append operations with a rechunk.
                let new_df = df.vstack(&temp_df).map_err(Error::PolarsParse)?;
                ret_df = Some(new_df);
            } else {
                ret_df = Some(temp_df);
            }

            if let Some(status) = headers {
                let s: TopicIterationStatus =
                    serde_json::from_str(status.to_str().unwrap()).unwrap();

                if s.status == RecordStatus::All {
                    break;
                } else if s.status == RecordStatus::SchemaChange {
                    tracing::warn!(
                        "A schema change was found during (probably assay) Iterate Unlimited calculations"
                    );
                    break;
                }
                cursor = Some(s.next);
            } else {
                break;
            }
        }

        ret_df.ok_or(Error::EmptyStream)
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
impl Retrieve<Vec<SchemaChunk<SchemaRef>>> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// [`SchemaChunk<Schema>`] format.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<Vec<SchemaChunk<SchemaRef>>, Error> {
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

#[async_trait]
impl Retrieve<MultiChunk> for Client {
    /// Retrieve a set of records from a specifid topic and partition, returning results in
    /// [`SchemaChunk<Schema>`] format.
    async fn get_records(
        &self,
        topic_name: impl AsRef<str> + Send,
        partition_name: impl AsRef<str> + Send,
        params: &RecordQuery,
    ) -> Result<MultiChunk, Error> {
        let bytes = process_request(
            self.retrieve_request(topic_name, partition_name, params)?
                .header("accept", CONTENT_TYPE_ARROW),
        )
        .await?
        .bytes()
        .await
        .map_err(Error::Server)?;

        bytes_into_multichunk(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::replicate::test::{inferences_large, setup_with_config};
    use httptest::{
        all_of,
        matchers::{contains, eq, json_decoded, key, len, not, request, url_decoded},
        responders::json_encoded,
        Expectation, Server,
    };
    use plateau_server::{http, Config as PlateauConfig};
    use plateau_transport_arrow_rs::{DataFocus, RecordStatus, Span, Topic, TopicIterationOrder};
    use tokio_util::io::ReaderStream;

    use super::*;

    fn example_chunk() -> SchemaChunk<SchemaRef> {
        use std::sync::Arc;
        use transport::arrow_array::RecordBatch;
        use transport::arrow_array::{Float32Array, Int64Array};
        use transport::arrow_schema::{DataType, Field, Schema};

        let time = Arc::new(Int64Array::from_iter_values(vec![0, 1, 2, 3, 4]));
        let inputs = Arc::new(Float32Array::from_iter_values(vec![
            1.0, 2.0, 3.0, 4.0, 5.0,
        ]));
        let outputs = Arc::new(Float32Array::from_iter_values(vec![
            0.6, 0.4, 0.2, 0.8, 0.8,
        ]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("inputs", DataType::Float32, false),
            Field::new("outputs", DataType::Float32, false),
        ]));

        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![time, inputs, outputs]).unwrap();

        SchemaChunk {
            schema,
            chunk: record_batch,
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
                    order: Some(TopicIterationOrder::Asc),
                    data_focus: DataFocus::default(),
                    ..Default::default()
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
                    order: Some(TopicIterationOrder::Asc),
                    data_focus: DataFocus::default(),
                    ..Default::default()
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

        let server = plateau_test::http::TestServer::new_with_config(PlateauConfig {
            http: http::Config {
                max_append_bytes: 5,
                ..http::Config::default()
            },
            ..PlateauConfig::default()
        })
        .await
        .unwrap();

        let client = Client {
            server_url: Url::parse(&server.base()).unwrap(),
            http_client: Default::default(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_rows: None,
        };

        let response = client
            .append_records("topic-1", "partition-1", &InsertQuery { time: None }, chunk)
            .await;

        assert_eq!(
            "RequestTooLong(\"1266\", MaxRequestSize(Some(5)))",
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
        let stream_reader = ReaderStream::new(io::Cursor::new(
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

    #[tokio::test]
    async fn single_large_row_fails() -> anyhow::Result<()> {
        let (_, mut client_source, _source) = setup_with_config(PlateauConfig {
            http: http::Config {
                max_append_bytes: 1000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

        let queue = MultiChunk::from(inferences_large());

        let topic = "replicate";
        let partition = "a";

        let result = client_source.append_queue(topic, partition, queue).await;

        // oh, if only assert_matches! was stable...
        let Err(Error::CannotReshape(size, request_size)) = result else {
            panic!("unexpected result {result:?}")
        };

        assert_eq!(size, "2978");
        assert_eq!(request_size, MaxRequestSize(Some(1000)));

        Ok(())
    }

    #[cfg(feature = "polars")]
    mod polars {
        use super::*;

        // TODO: This is copied from the server, refactor to share amongst other tests.

        fn inferences_schema_a() -> SchemaChunk<SchemaRef> {
            use std::sync::Arc;
            use transport::arrow_array::types::Float64Type;
            use transport::arrow_array::{Array, RecordBatch};
            use transport::arrow_array::{
                ArrayRef, Float32Array, Int64Array, ListArray, StructArray,
            };
            use transport::arrow_schema::{DataType, Field, Fields, Schema};

            let time = Arc::new(Int64Array::from_iter_values(vec![0, 1, 2, 3, 4]));
            let inputs = Arc::new(Float32Array::from_iter_values(vec![
                1.0, 2.0, 3.0, 4.0, 5.0,
            ]));
            let mul = Arc::new(Float32Array::from_iter_values(vec![
                2.0, 2.0, 2.0, 2.0, 2.0,
            ]));

            // Create a nested array for tensor
            let tensor = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(2.0), Some(2.0)]),
                Some(vec![]),
                Some(vec![Some(4.0), Some(4.0)]),
                Some(vec![Some(6.0), Some(6.0)]),
                Some(vec![Some(8.0), Some(8.0)]),
            ]));

            // Create a struct for outputs
            let struct_fields = Fields::from(vec![
                Field::new("mul", DataType::Float32, false),
                Field::new("tensor", tensor.data_type().clone(), false),
            ]);

            // Create struct array using the correct method
            // In arrow-rs, we need to provide the arrays as ArrayRef
            let mul_ref: ArrayRef = mul;
            let tensor_ref: ArrayRef = tensor.clone();
            let outputs = Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("mul", DataType::Float32, false)),
                    mul_ref,
                ),
                (
                    Arc::new(Field::new("tensor", tensor.data_type().clone(), false)),
                    tensor_ref,
                ),
            ]));

            let schema = Arc::new(Schema::new(vec![
                Field::new("time", DataType::Int64, false),
                Field::new("tensor", tensor.data_type().clone(), false),
                Field::new("inputs", DataType::Float32, false),
                Field::new("outputs", DataType::Struct(struct_fields), false),
            ]));

            let record_batch =
                RecordBatch::try_new(schema.clone(), vec![time, tensor, inputs, outputs]).unwrap();

            SchemaChunk {
                schema,
                chunk: record_batch,
            }
        }

        #[tokio::test]
        async fn iterate_unlimited_polars_simple() {
            use httptest::responders::status_code;

            let server = Server::run();

            let responder =
                status_code(200).body(Bytes::from(inferences_schema_a().to_bytes().unwrap()));

            server.expect(
                Expectation::matching(all_of![
                    request::method("POST"),
                    request::path("/topic/topic-1/records"),
                ])
                .respond_with(responder),
            );

            let client: Client = test_client(&server);

            let response1 = client
                .iterate_topic_unlimited(
                    "topic-1",
                    &TopicIterationQuery {
                        order: Some(TopicIterationOrder::Asc),
                        data_focus: DataFocus::default(),
                        ..Default::default()
                    },
                )
                .await
                .expect("failure iterating records");

            assert_eq!(response1.iter().len(), 4);
            response1
                .columns(["inputs", "outputs", "tensor", "time"])
                .expect("Could not parse columns from dataframe");
        }
    }
}

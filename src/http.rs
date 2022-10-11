use ::log::info;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future;
use parquet::data_type::ByteArray;
use rweb::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;
use tracing::Instrument;
use warp::http::StatusCode;

use crate::catalog::Catalog;
use crate::limit::{BatchStatus, RowLimit};
use crate::slog::{RecordIndex, SlogError};
use crate::topic::{Record, TopicIterator, TopicRecordResponse};

#[derive(Deserialize)]
struct Insert {
    records: Vec<String>,
}

#[derive(Schema, Deserialize)]
struct InsertQuery {
    time: Option<String>,
}

#[derive(Schema, Serialize)]
struct Span {
    start: usize,
    end: usize,
}

impl Span {
    fn from_range(r: Range<RecordIndex>) -> Self {
        Span {
            start: r.start.0,
            end: r.end.0,
        }
    }
}

#[derive(Schema, Serialize)]
struct Inserted {
    span: Span,
}

#[derive(Schema, Serialize)]
struct Partitions {
    partitions: HashMap<String, Span>,
}

// abstracted over lower-level status so as to not leak
/// Status of the record request query.
#[derive(Schema, Serialize)]
enum RecordStatus {
    /// All current records returned.
    All,
    /// Record response was limited because the next chunk of records has a different schema.
    SchemaChange,
    /// Record response was limited by record limit. Additional records exist.
    RecordLimited,
    /// Record response was limited by payload size limit. Additional records exist.
    ByteLimited,
}

impl From<BatchStatus> for RecordStatus {
    fn from(orig: BatchStatus) -> Self {
        match orig {
            BatchStatus::Open { .. } => RecordStatus::All,
            BatchStatus::SchemaChanged => RecordStatus::SchemaChange,
            BatchStatus::BytesExceeded => RecordStatus::ByteLimited,
            BatchStatus::RecordsExceeded => RecordStatus::RecordLimited,
        }
    }
}

#[derive(Schema, Serialize)]
struct Records {
    span: Option<Span>,
    status: RecordStatus,
    records: Vec<String>,
}

#[derive(Schema, Deserialize)]
struct RecordQuery {
    start: usize,
    limit: Option<usize>,
    #[serde(rename = "time.start")]
    start_time: Option<String>,
    #[serde(rename = "time.end")]
    end_time: Option<String>,
}

#[derive(Schema, Serialize)]
struct ErrorMessage {
    message: String,
    code: u16,
}

#[derive(Debug)]
enum ErrorReply {
    WriterBusy,
    InvalidQuery,
    InvalidSchema,
    NoHeartbeat,
    Unknown,
}
impl warp::reject::Reject for ErrorReply {}

pub async fn serve<I>(
    addr: I,
    catalog: Catalog,
) -> (SocketAddr, Pin<Box<dyn Future<Output = ()> + Send>>)
where
    I: Into<SocketAddr> + Send + 'static,
{
    let log = warp::log("plateau::http");

    let (spec, filter) = openapi::spec().build(move || {
        healthcheck(catalog.clone())
            .or(get_topics(catalog.clone()))
            .or(topic_append(catalog.clone()))
            .or(topic_iterate(catalog.clone()))
            .or(topic_get_partitions(catalog.clone()))
            .or(partition_get_records(catalog))
    });

    let server = rweb::serve(filter.or(openapi_docs(spec)).with(log).recover(emit_error));

    // Ideally warp would have something like a `run_ephemeral`, but here we
    // are. it's only three lines to copy from Server::run
    let (addr, fut) = server.bind_ephemeral(addr);
    let span = tracing::info_span!("Server::run", ?addr);
    tracing::info!(parent: &span, "listening on http://{}", addr);

    (addr, Box::pin(fut.instrument(span)))
}

/// A RAII wrapper around a full plateau test server.
///
/// Exposes the underlying `catalog` if checkpoints are required.
///
/// Currently, we assume that only one test server can run at a given time to
/// prevent port conflicts.
pub struct TestServer {
    addr: SocketAddr,
    end_tx: mpsc::Sender<()>,
    pub catalog: Catalog,
    pub temp: TempDir,
}

impl TestServer {
    pub async fn new() -> Result<Self> {
        let (end_tx, mut end_rx) = mpsc::channel(1);
        let temp = tempdir()?;
        let root = PathBuf::from(temp.path());
        let catalog = Catalog::attach(root).await;

        let serve_catalog = catalog.clone();
        let (addr, server) = serve(([127, 0, 0, 1], 0), serve_catalog).await;
        tokio::spawn(async move {
            future::select(Box::pin(end_rx.recv()), server).await;
        });

        Ok(TestServer {
            addr,
            end_tx,
            catalog,
            temp,
        })
    }

    pub fn base(&self) -> String {
        format!("http://{}:{}", self.addr.ip(), self.addr.port())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) -> () {
        self.end_tx.try_send(()).unwrap();
    }
}

#[get("/ok")]
#[openapi(id = "healthcheck")]
async fn healthcheck(#[data] catalog: Catalog) -> Result<Json<serde_json::Value>, Rejection> {
    let duration = SystemTime::now().duration_since(catalog.last_checkpoint().await);
    let healthy = duration
        .map(|d| d < Duration::from_secs(30))
        .unwrap_or(true);
    if healthy {
        Ok(Json::from(json!({"ok": "true"})))
    } else {
        Err(warp::reject::custom(ErrorReply::NoHeartbeat))
    }
}

#[derive(Schema, Serialize)]
struct Topics {
    topics: Vec<Topic>,
}

#[derive(Schema, Serialize)]
struct Topic {
    name: String,
}

#[get("/topics")]
#[openapi(id = "get_topics")]
async fn get_topics(#[data] catalog: Catalog) -> Result<Json<Topics>, Rejection> {
    let topics = catalog.list_topics().await;
    Ok(Json::from(Topics {
        topics: topics.into_iter().map(|name| Topic { name }).collect(),
    }))
}

#[post("/topic/{topic_name}/partition/{partition_name}")]
#[openapi(id = "topic.append")]
#[body_size(max = "10240000")]
async fn topic_append(
    topic_name: String,
    partition_name: String,
    query: Query<InsertQuery>,
    #[data] catalog: Catalog,
    #[json] request: Insert,
) -> Result<Json<Inserted>, Rejection> {
    let query = query.into_inner();
    let time = if let Some(s) = query.time {
        if let Ok(time) = DateTime::parse_from_rfc3339(&s) {
            time.with_timezone(&Utc)
        } else {
            return Err(warp::reject::custom(ErrorReply::InvalidQuery));
        }
    } else {
        Utc::now()
    };
    let rs: Vec<_> = request
        .records
        .into_iter()
        .map(|m| Record {
            time,
            message: ByteArray::from(m.as_str()),
        })
        .collect();

    let topic = catalog.get_topic(&topic_name).await;
    info!(
        "appending {} to {}/{}",
        rs.len(),
        topic_name,
        partition_name
    );
    let r = topic.extend_records(&partition_name, &rs).await;

    Ok(Json::from(Inserted {
        span: Span::from_range(r.map_err(|e| {
            warp::reject::custom(match e.downcast_ref::<SlogError>() {
                Some(SlogError::WriterThreadBusy) => ErrorReply::WriterBusy,
                None => ErrorReply::Unknown,
            })
        })?),
    }))
}

#[get("/topic/{topic_name}")]
#[openapi(id = "topic.get_partitions")]
async fn topic_get_partitions(
    topic_name: String,
    #[data] catalog: Catalog,
) -> Result<Json<Partitions>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    let indices = topic.readable_ids().await;

    Ok(Json::from(Partitions {
        partitions: indices
            .into_iter()
            .map(|(partition, range)| (partition, Span::from_range(range)))
            .collect(),
    }))
}

#[derive(Schema, Deserialize)]
struct TopicIterationQuery {
    limit: Option<usize>,
    #[serde(rename = "time.start")]
    start_time: Option<String>,
    #[serde(rename = "time.end")]
    end_time: Option<String>,
}

#[derive(Schema, Serialize)]
struct TopicIterationReply {
    records: Vec<String>,
    status: RecordStatus,
    next: TopicIterator,
}

#[post("/topic/{topic_name}/records")]
#[openapi(id = "topic.iterate")]
async fn topic_iterate(
    topic_name: String,
    query: Query<TopicIterationQuery>,
    #[json] position: Option<TopicIterator>,
    #[data] catalog: Catalog,
) -> Result<Json<TopicIterationReply>, Rejection> {
    let query = query.into_inner();
    let topic = catalog.get_topic(&topic_name).await;
    let limit = std::cmp::min(query.limit.unwrap_or(1000), 10000);
    let position = position.unwrap_or_default();

    let result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic
            .get_records_by_time(position, times, RowLimit::records(limit))
            .await
    } else {
        topic.get_records(position, RowLimit::records(limit)).await
    };

    let status = RecordStatus::from(result.batch.status);
    if let Ok(records) = result.batch.into_legacy() {
        Ok(Json::from(TopicIterationReply {
            records: serialize_records(records),
            next: result.iter,
            status,
        }))
    } else {
        Err(warp::reject::custom(ErrorReply::InvalidSchema))
    }
}

#[get("/topic/{topic_name}/partition/{partition_name}/records")]
#[openapi(id = "partition.get_records")]
async fn partition_get_records(
    topic_name: String,
    partition_name: String,
    query: Query<RecordQuery>,
    #[data] catalog: Catalog,
) -> Result<Json<Records>, Rejection> {
    let query = query.into_inner();
    let topic = catalog.get_topic(&topic_name).await;
    let start_record = RecordIndex(query.start);
    let limit = std::cmp::min(query.limit.unwrap_or(1000), 10000);
    let limit = RowLimit::records(limit);
    let resp = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic
            .get_partition(&partition_name)
            .await
            .get_records_by_time(start_record, times, limit)
            .await
    } else {
        topic
            .get_partition(&partition_name)
            .await
            .get_records(start_record, limit)
            .await
    };

    let start = resp.chunks.get(0).and_then(|i| i.start());
    let end = resp
        .chunks
        .iter()
        .next_back()
        .and_then(|i| i.end().map(|ix| ix + 1));
    let range = start.zip(end).map(|(start, end)| start..end);

    let status = RecordStatus::from(resp.status);
    if let Ok(records) = resp.into_legacy() {
        Ok(Json::from(Records {
            span: range.map(Span::from_range),
            status,
            records: serialize_records(records),
        }))
    } else {
        Err(warp::reject::custom(ErrorReply::InvalidSchema))
    }
}

async fn emit_error(err: Rejection) -> Result<impl Reply, Infallible> {
    let (code, message) = match err.find::<ErrorReply>() {
        Some(ErrorReply::InvalidQuery) => (StatusCode::BAD_REQUEST, "invalid query"),
        Some(ErrorReply::InvalidSchema) => (StatusCode::BAD_REQUEST, "invalid schema"),
        Some(ErrorReply::WriterBusy) => (StatusCode::TOO_MANY_REQUESTS, "writer busy"),
        Some(ErrorReply::NoHeartbeat) => (StatusCode::INTERNAL_SERVER_ERROR, "no heartbeat"),
        Some(ErrorReply::Unknown) | None => (StatusCode::INTERNAL_SERVER_ERROR, "unknown error"),
    };

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}

fn serialize_records<I: IntoIterator<Item = Record>>(rs: I) -> Vec<String> {
    rs.into_iter()
        .map(|r| String::from_utf8(r.message.data().to_vec()).unwrap())
        .collect()
}

fn parse_time_range(
    start: String,
    end: Option<String>,
) -> Result<RangeInclusive<DateTime<Utc>>, Rejection> {
    let end = match end {
        Some(end_time) => end_time,
        None => return Err(warp::reject::custom(ErrorReply::InvalidQuery)),
    };

    let start = DateTime::parse_from_rfc3339(&start);
    let end = DateTime::parse_from_rfc3339(&end);
    if let (Ok(start), Ok(end)) = (start, end) {
        Ok(start.with_timezone(&Utc)..=end.with_timezone(&Utc))
    } else {
        Err(warp::reject::custom(ErrorReply::InvalidQuery))
    }
}

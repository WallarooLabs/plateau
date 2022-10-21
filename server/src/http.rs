use ::log::info;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future;

use rweb::{get, openapi, openapi_docs, post, warp, Filter, Future, Json, Query, Rejection, Reply};
use serde::Serialize;
use serde_json::json;
use std::net::SocketAddr;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;
use tracing::Instrument;

use plateau_transport::{
    Inserted, Partitions, RecordQuery, RecordStatus, Records, Span, Topic, TopicIterationQuery,
    TopicIterationReply, TopicIterationStatus, TopicIterator, Topics,
};

use crate::catalog::Catalog;
use crate::chunk::{Schema, SchemaChunk};
use crate::limit::{BatchStatus, LimitedBatch, RowLimit};
use crate::slog::{RecordIndex, SlogError};
use crate::topic::Record;

mod chunk;
mod error;

use self::error::{emit_error, ErrorReply};

trait FromRange {
    fn from_range(r: Range<RecordIndex>) -> Self;
}

impl FromRange for Span {
    fn from_range(r: Range<RecordIndex>) -> Self {
        Span {
            start: r.start.0,
            end: r.end.0,
        }
    }
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
    #[data] catalog: Catalog,
    chunk: SchemaChunk<Schema>,
) -> Result<Json<Inserted>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    info!(
        "appending {} to {}/{}",
        chunk.len(),
        topic_name,
        partition_name
    );
    let r = topic.extend(&partition_name, chunk).await;

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

// issue #4 on warp's github is accept content type negotiation.
// we've been waiting since 2018...
fn negotiate<F, J>(
    content: Option<String>,
    batch: LimitedBatch,
    to_json: F,
) -> Result<Box<dyn Reply>, Rejection>
where
    F: FnOnce(Vec<Record>) -> J,
    J: Serialize + Send,
{
    match content.as_ref().map(|h| h.as_str()) {
        None | Some("*/*") | Some("application/json") => {
            if let Ok(records) = batch.into_legacy() {
                Ok(Box::new(Json::from(to_json(records)).into_response()))
            } else {
                Err(warp::reject::custom(ErrorReply::InvalidSchema))
            }
        }
        Some(chunk::ARROW_CONTENT) => {
            chunk::to_reply(batch).map_err(|e| warp::reject::custom(ErrorReply::Arrow(e)))
        }
        Some(other) => Err(warp::reject::custom(ErrorReply::CannotEmit(
            other.to_string(),
        ))),
    }
}

fn accept() -> impl Filter<Extract = (Option<String>,), Error = Rejection> + Copy {
    warp::filters::header::optional("accept")
}

#[post("/topic/{topic_name}/records")]
#[openapi(id = "topic.iterate")]
async fn topic_iterate(
    topic_name: String,
    query: Query<TopicIterationQuery>,
    #[filter = "accept"] content: Option<String>,
    #[json] position: Option<TopicIterator>,
    #[data] catalog: Catalog,
) -> Result<Box<dyn Reply>, Rejection> {
    let query = query.into_inner();
    let topic = catalog.get_topic(&topic_name).await;
    let limit = std::cmp::min(query.limit.unwrap_or(1000), 10000);
    let position = position.unwrap_or_default();

    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic
            .get_records_by_time(position, times, RowLimit::records(limit))
            .await
    } else {
        topic.get_records(position, RowLimit::records(limit)).await
    };

    let status = TopicIterationStatus {
        next: result.iter,
        status: RecordStatus::from(result.batch.status),
    };

    result.batch.schema.as_mut().map(|schema| {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
    });

    negotiate(content, result.batch, move |records| TopicIterationReply {
        records: serialize_records(records),
        status,
    })
}

#[get("/topic/{topic_name}/partition/{partition_name}/records")]
#[openapi(id = "partition.get_records")]
async fn partition_get_records(
    topic_name: String,
    partition_name: String,
    query: Query<RecordQuery>,
    #[filter = "accept"] content: Option<String>,
    #[data] catalog: Catalog,
) -> Result<Box<dyn Reply>, Rejection> {
    let query = query.into_inner();
    let topic = catalog.get_topic(&topic_name).await;
    let start_record = RecordIndex(query.start);
    let limit = std::cmp::min(query.limit.unwrap_or(1000), 10000);
    let limit = RowLimit::records(limit);
    let mut result = if let Some(start) = query.start_time {
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

    let start = result.chunks.get(0).and_then(|i| i.start());
    let end = result
        .chunks
        .iter()
        .next_back()
        .and_then(|i| i.end().map(|ix| ix + 1));
    let range = start.zip(end).map(|(start, end)| start..end);

    let status = RecordStatus::from(result.status);
    result.schema.as_mut().map(|schema| {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
    });

    negotiate(content, result, move |records| Records {
        span: range.map(Span::from_range),
        status,
        records: serialize_records(records),
    })
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

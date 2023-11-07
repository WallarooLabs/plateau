use std::net::SocketAddr;
use std::ops::{Range, RangeInclusive};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use rweb::{get, openapi, openapi_docs, post, warp, Filter, Future, Json, Rejection, Reply};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_qs;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tracing::info;
use tracing::Instrument;

use crate::config::PlateauConfig;
use plateau_transport::{
    DataFocus, Inserted, Partitions, RecordQuery, RecordStatus, Records, Span, Topic,
    TopicIterationOrder, TopicIterationQuery, TopicIterationReply, TopicIterationStatus,
    TopicIterator, Topics,
};

use crate::catalog::Catalog;
use crate::http::chunk::SchemaChunkRequest;
use crate::limit::{BatchStatus, LimitedBatch, RowLimit};
use crate::manifest::Ordering;
use crate::slog::{RecordIndex, SlogError};
use crate::topic::Record;

mod chunk;
mod error;

use self::error::{emit_error, ErrorReply};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub bind: SocketAddr,
    pub max_append_bytes: u64,
    pub max_page: RowLimit,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: SocketAddr::from(([0, 0, 0, 0], 3030)),
            max_append_bytes: 10240000,
            max_page: RowLimit::default(),
        }
    }
}

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

pub async fn serve(
    config: PlateauConfig,
    catalog: Arc<Catalog>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    Pin<Box<dyn Future<Output = ()> + Send>>,
) {
    let log = warp::log("plateau::http");
    let config = Arc::new(config);

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();

    let inner_config = config.clone();
    let (spec, filter) = openapi::spec().build(move || {
        healthcheck(catalog.clone(), inner_config.clone())
            .or(get_topics(catalog.clone()))
            .or(
                warp::body::content_length_limit(inner_config.http.max_append_bytes)
                    .and(topic_append(catalog.clone())),
            )
            .or(topic_iterate(catalog.clone(), inner_config.http.max_page))
            .or(topic_get_partitions(catalog.clone()))
            .or(partition_get_records(catalog, inner_config.http.max_page))
    });

    let inner_config = config.clone();
    let server = rweb::serve(
        filter
            .or(openapi_docs(spec))
            .with(log)
            .recover(move |e| emit_error(e, inner_config.clone())),
    );

    // Ideally warp would have something like a `run_ephemeral`, but here we
    // are. it's only three lines to copy from Server::run
    let (addr, fut) =
        server.bind_with_graceful_shutdown(config.http.bind, FutureExt::map(rx_shutdown, |_| ()));
    let span = tracing::info_span!("Server::run", ?addr);
    tracing::info!(parent: &span, "listening on http://{}", addr);

    (addr, tx_shutdown, Box::pin(fut.instrument(span)))
}

/// A RAII wrapper around a full plateau test server.
///
/// Exposes the underlying `catalog` if checkpoints are required.
///
/// Currently, we assume that only one test server can run at a given time to
/// prevent port conflicts.
pub struct TestServer {
    addr: SocketAddr,
    end_tx: oneshot::Sender<()>,
    pub catalog: Arc<Catalog>,
}

impl TestServer {
    pub async fn new() -> Result<Self> {
        let config = PlateauConfig {
            http: Config {
                bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..Config::default()
            },
            ..PlateauConfig::default()
        };

        Self::new_with_config(config).await
    }

    pub async fn new_with_config(mut config: PlateauConfig) -> Result<Self> {
        let temp = tempdir()?;
        let root = temp.into_path();
        let catalog = Arc::new(Catalog::attach(root, Default::default()).await?);

        let serve_catalog = catalog.clone();
        let replication = std::mem::take(&mut config.replication);
        let (addr, end_tx, server) = serve(config, serve_catalog).await;
        tokio::spawn(server);

        if let Some(replication) = replication {
            tokio::spawn(crate::replication::run(replication, addr));
        }

        Ok(TestServer {
            addr,
            end_tx,
            catalog,
        })
    }

    pub fn host(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    pub fn base(&self) -> String {
        format!("http://{}", self.host())
    }

    pub async fn stop(self) -> Arc<Catalog> {
        self.end_tx.send(()).unwrap();
        self.catalog
    }

    /// This simulates a "clean" plateau shutdown.
    pub async fn close(self) {
        Catalog::close(self.stop().await).await;
    }
}

#[get("/ok")]
#[openapi(id = "healthcheck")]
async fn healthcheck(
    #[data] catalog: Arc<Catalog>,
    #[data] config: Arc<PlateauConfig>,
) -> Result<Json<serde_json::Value>, Rejection> {
    let duration = SystemTime::now().duration_since(catalog.last_checkpoint().await);
    let healthy = duration
        .map(|d| d < Duration::from_millis(config.checkpoint_ms * 10))
        .unwrap_or(true);
    if healthy {
        Ok(Json::from(json!({"ok": "true"})))
    } else {
        Err(warp::reject::custom(ErrorReply::NoHeartbeat))
    }
}

#[get("/topics")]
#[openapi(id = "get_topics")]
async fn get_topics(#[data] catalog: Arc<Catalog>) -> Result<Json<Topics>, Rejection> {
    let topics = catalog.list_topics().await;
    Ok(Json::from(Topics {
        topics: topics.into_iter().map(|name| Topic { name }).collect(),
    }))
}

#[post("/topic/{topic_name}/partition/{partition_name}")]
#[openapi(id = "topic.append")]
async fn topic_append(
    topic_name: String,
    partition_name: String,
    #[data] catalog: Arc<Catalog>,
    chunk: SchemaChunkRequest,
) -> Result<Json<Inserted>, Rejection> {
    topic_append_internal(topic_name, partition_name, catalog, chunk).await
}
async fn topic_append_internal(
    topic_name: String,
    partition_name: String,
    catalog: Arc<Catalog>,
    chunk: SchemaChunkRequest,
) -> Result<Json<Inserted>, Rejection> {
    if catalog.is_readonly() {
        return Err(ErrorReply::InsufficientDiskSpace.into());
    }

    if chunk.0.contains_null_type() {
        return Err(ErrorReply::NullTypes.into());
    }

    catalog.record_write();

    let topic = catalog.get_topic(&topic_name).await;
    info!(
        "appending {} to {}/{}",
        chunk.0.len(),
        topic_name,
        partition_name
    );
    let r = topic.extend(&partition_name, chunk.0).await;

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
    #[data] catalog: Arc<Catalog>,
) -> Result<Json<Partitions>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    let indices = topic.readable_ids(None).await;

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
    focus: DataFocus,
    to_json: F,
) -> Result<Box<dyn Reply>, Rejection>
where
    F: FnOnce(Vec<Record>) -> J,
    J: Serialize + Send,
{
    match content.as_deref() {
        None | Some("*/*") | Some("application/json") => {
            if let Ok(records) = batch.into_legacy() {
                Ok(Box::new(Json::from(to_json(records)).into_response()))
            } else {
                Err(warp::reject::custom(ErrorReply::InvalidSchema))
            }
        }
        Some(accept) => chunk::to_reply(accept, batch, focus).map_err(warp::reject::custom),
    }
}

fn accept() -> impl Filter<Extract = (Option<String>,), Error = Rejection> + Copy {
    warp::filters::header::optional("accept")
}

fn nonstrict_query<T>() -> impl Filter<Extract = (T,), Error = Rejection> + Clone
where
    T: DeserializeOwned + Send + 'static,
{
    serde_qs::warp::query(serde_qs::Config::new(2, false))
}

#[post("/topic/{topic_name}/records")]
#[openapi(id = "topic.iterate")]
async fn topic_iterate(
    topic_name: String,
    #[filter = "nonstrict_query"] query: TopicIterationQuery,
    #[filter = "accept"] content: Option<String>,
    #[json] position: Option<TopicIterator>,
    #[data] catalog: Arc<Catalog>,
    #[data] max_page: RowLimit,
) -> Result<Box<dyn Reply>, Rejection> {
    topic_iterate_internal(topic_name, query, content, position, catalog, max_page).await
}

async fn topic_iterate_internal(
    topic_name: String,
    query: TopicIterationQuery,
    content: Option<String>,
    position: Option<TopicIterator>,
    catalog: Arc<Catalog>,
    max_page: RowLimit,
) -> Result<Box<dyn Reply>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    let page_size = RowLimit::records(query.page_size.unwrap_or(1000)).min(max_page);
    let position = position.unwrap_or_default();
    let partition_filter = query.partition_filter;
    let order: Ordering = query.order.unwrap_or(TopicIterationOrder::Asc).into();

    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        if order == Ordering::Reverse {
            Err(warp::reject::custom(ErrorReply::InvalidQuery))?
        }
        topic
            .get_records_by_time(position, times, page_size, partition_filter)
            .await
    } else {
        topic
            .get_records(position, page_size, &order, partition_filter)
            .await
    };

    let status = TopicIterationStatus {
        next: result.iter,
        status: RecordStatus::from(result.batch.status),
    };

    if let Some(schema) = result.batch.schema.as_mut() {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
    }

    negotiate(content, result.batch, query.data_focus, move |records| {
        TopicIterationReply {
            records: serialize_records(records),
            status,
        }
    })
}

#[get("/topic/{topic_name}/partition/{partition_name}/records")]
#[openapi(id = "partition.get_records")]
async fn partition_get_records(
    topic_name: String,
    partition_name: String,
    #[filter = "nonstrict_query"] query: RecordQuery,
    #[filter = "accept"] content: Option<String>,
    #[data] catalog: Arc<Catalog>,
    #[data] max_page: RowLimit,
) -> Result<Box<dyn Reply>, Rejection> {
    let topic = catalog.get_topic(&topic_name).await;
    let start_record = RecordIndex(query.start);
    let page_size = RowLimit::records(query.page_size.unwrap_or(1000)).min(max_page);
    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic
            .get_partition(&partition_name)
            .await
            .get_records_by_time(start_record, times, page_size)
            .await
    } else {
        topic
            .get_partition(&partition_name)
            .await
            .get_records(start_record, page_size, &Ordering::Forward)
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
    if let Some(schema) = result.schema.as_mut() {
        schema.metadata.insert(
            "status".to_string(),
            serde_json::to_string(&status).unwrap(),
        );
        schema.metadata.insert(
            "span".to_string(),
            serde_json::to_string(&range.clone().map(Span::from_range)).unwrap(),
        );
    }

    negotiate(content, result, query.data_focus, move |records| Records {
        span: range.map(Span::from_range),
        status,
        records: serialize_records(records),
    })
}

fn serialize_records<I: IntoIterator<Item = Record>>(rs: I) -> Vec<String> {
    rs.into_iter()
        .map(|r| String::from_utf8(r.message).unwrap())
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

#[cfg(test)]
mod test {
    use plateau_transport::{TopicIterationOrder, TopicIterationQuery};

    #[test]
    fn can_parse_order_query() {
        use serde_qs as qs;

        let q = qs::from_str::<TopicIterationQuery>("order=desc").unwrap();
        assert_eq!(TopicIterationOrder::Desc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=DESC").unwrap();
        assert_eq!(TopicIterationOrder::Desc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=Asc").unwrap();
        assert_eq!(TopicIterationOrder::Asc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("order=AsC").unwrap();
        assert_eq!(TopicIterationOrder::Asc, q.order.unwrap());

        let q = qs::from_str::<TopicIterationQuery>("").unwrap();
        assert!(q.order.is_none());
    }
}

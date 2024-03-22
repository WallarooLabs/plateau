use std::net::SocketAddr;
use std::ops::{Deref, Range, RangeInclusive};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, FromRef, Path, State},
    http::{header::ACCEPT, HeaderMap, Request},
    response::IntoResponse,
    routing::{get, post},
    Json, Router, Server,
};

use chrono::{DateTime, Utc};
use futures::{Future, FutureExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing::Instrument;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::PlateauConfig;
use plateau_transport::{
    DataFocus, Inserted, Partitions, RecordQuery, RecordStatus, Records, Span, Topic,
    TopicIterationOrder, TopicIterationQuery, TopicIterationReply, TopicIterationStatus,
    TopicIterator, Topics,
};

pub use crate::axum_util::{query::Query, Response};
use crate::catalog::Catalog;
use crate::http::chunk::SchemaChunkRequest;
use crate::limit::{BatchStatus, LimitedBatch, RowLimit};
use crate::manifest::Ordering;
use crate::slog::{RecordIndex, SlogError};
use crate::topic::Record;

mod chunk;
mod error;

pub use self::error::ErrorReply;

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

#[derive(Clone)]
struct AppState(Arc<Catalog>, Arc<PlateauConfig>);

impl FromRef<AppState> for PlateauConfig {
    fn from_ref(state: &AppState) -> PlateauConfig {
        state.1.deref().clone()
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
    let config = Arc::new(config);

    let (tx_shutdown, rx_shutdown) = oneshot::channel::<()>();

    let filter = Router::new()
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
        .route("/ok", get(healthcheck))
        .route("/topics", get(get_topics))
        .route(
            "/topic/:topic_name/partition/:partition_name/records",
            get(partition_get_records),
        )
        .route(
            "/topic/:topic_name/partition/:partition_name",
            post(topic_append).layer(DefaultBodyLimit::max(config.http.max_append_bytes as usize)),
        )
        .route("/topic/:topic_name/records", post(topic_iterate_route))
        .route("/topic/:topic_name", get(topic_get_partitions))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request<Body>| {
                tracing::span!(
                    target: "plateau::http",
                    tracing::Level::INFO,
                    "request",
                    method = %request.method(),
                    uri = %request.uri(),
                    version = ?request.version(),
                )
            }),
        )
        .with_state(AppState(catalog, Arc::clone(&config)));

    let server = Server::bind(&config.http.bind).serve(filter.into_make_service());
    let addr = server.local_addr();

    let fut = server.with_graceful_shutdown(FutureExt::map(rx_shutdown, |_| ()));
    let span = tracing::info_span!("Server::run", ?addr);
    tracing::info!(parent: &span, "listening on http://{}", addr);

    (
        addr,
        tx_shutdown,
        Box::pin(async move { fut.instrument(span).await.unwrap_or(()) }),
    )
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
        Catalog::close_arc(self.stop().await).await;
    }
}

#[utoipa::path(
    get,
    operation_id = "healthcheck",
    path = "/ok",
    responses(
        (status = 200, description = "Healthcheck", body = serde_json::Value),
    ),
  )]
async fn healthcheck(
    State(AppState(catalog, config)): State<AppState>,
) -> Result<Response<serde_json::Value>, ErrorReply> {
    let duration = SystemTime::now().duration_since(catalog.last_checkpoint().await);
    let healthy = duration
        .map(|d| d < config.catalog.checkpoint_interval * 10)
        .unwrap_or(true);
    if healthy {
        Ok(Response::ok(json!({"ok": "true"})))
    } else {
        Err(ErrorReply::NoHeartbeat)
    }
}

#[utoipa::path(
    get,
    operation_id = "get_topics",
    path = "/topics",
    responses(
        (status = 200, description = "List of topics", body = Topics),
    ),
  )]
async fn get_topics(
    State(AppState(catalog, _config)): State<AppState>,
) -> Result<Response<Topics>, ErrorReply> {
    let topics = catalog.list_topics().await;
    Ok(Response::ok(Topics {
        topics: topics.into_iter().map(|name| Topic { name }).collect(),
    }))
}

#[utoipa::path(
    post,
    operation_id = "topic.append",
    path = "/topic/{topic_name}/partition/{partition_name}",
    params(
        ("topic_name", Path, description = "Topic name"),
        ("partition_name", Path, description = "Partition name"),
    ),
    responses(
        (status = 200, description = "Span of inserted records", body = Inserted),
    ),
    request_body(content = SchemaChunk<plateau_transport::ArrowSchema>, content_type = "application/vnd.apache.arrow.file"),
  )]
async fn topic_append(
    State(AppState(catalog, _config)): State<AppState>,
    Path((topic_name, partition_name)): Path<(String, String)>,
    chunk: SchemaChunkRequest,
) -> Result<Response<Inserted>, ErrorReply> {
    topic_append_internal(topic_name, partition_name, catalog, chunk).await
}
async fn topic_append_internal(
    topic_name: String,
    partition_name: String,
    catalog: Arc<Catalog>,
    chunk: SchemaChunkRequest,
) -> Result<Response<Inserted>, ErrorReply> {
    if catalog.is_readonly() {
        return Err(ErrorReply::InsufficientDiskSpace);
    }

    if chunk.0.contains_null_type() {
        return Err(ErrorReply::NullTypes);
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

    Ok(Response::ok(Inserted {
        span: Span::from_range(r.map_err(|e| match e.downcast_ref::<SlogError>() {
            Some(SlogError::WriterThreadBusy) => ErrorReply::WriterBusy,
            None => ErrorReply::Unknown,
        })?),
    }))
}

#[utoipa::path(
    get,
    operation_id = "topic.get_partitions",
    path = "/topic/{topic_name}",
    params(
        ("topic_name", Path, description = "Topic name"),
    ),
    responses(
        (status = 200, description = "List of partitions for topic", body = Partitions),
    ),
  )]
async fn topic_get_partitions(
    State(AppState(catalog, _config)): State<AppState>,
    Path(topic_name): Path<String>,
) -> Result<Response<Partitions>, ErrorReply> {
    let topic = catalog.get_topic(&topic_name).await;
    let indices = topic.readable_ids(None).await;

    Ok(Response::ok(Partitions {
        partitions: indices
            .into_iter()
            .map(|(partition, range)| (partition, Span::from_range(range)))
            .collect(),
    }))
}

fn negotiate<F, J>(
    content: Option<String>,
    batch: LimitedBatch,
    focus: DataFocus,
    to_json: F,
) -> Result<axum::response::Response, ErrorReply>
where
    F: FnOnce(Vec<Record>) -> J,
    J: Serialize + Send,
{
    match content.as_deref() {
        None | Some("*/*") | Some("application/json") => {
            if let Ok(records) = batch.into_legacy() {
                Ok(Json(to_json(records)).into_response())
            } else {
                Err(ErrorReply::InvalidSchema)
            }
        }
        Some(accept) => chunk::to_reply(accept, batch, focus),
    }
}

#[utoipa::path(
    post,
    operation_id = "topic.iterate",
    path = "/topic/{topic_name}/records",
    params(
        ("topic_name", Path, description = "Topic name"),
        TopicIterationQuery,
    ),
    responses(
        (status = 200, description = "Topic's partitions with records", body = serde_json::Value),
    ),
    request_body(content = TopicIterator, content_type = "application/json"),
  )]
async fn topic_iterate_route(
    State(AppState(catalog, config)): State<AppState>,
    Path(topic_name): Path<String>,
    query: Option<Query<TopicIterationQuery>>,
    headers: HeaderMap,
    position: Option<Json<TopicIterator>>,
) -> Result<axum::response::Response, ErrorReply> {
    let max_page = config.http.max_page;
    topic_iterate(topic_name, query, headers, position, catalog, max_page).await
}

pub async fn topic_iterate(
    topic_name: String,
    query: Option<Query<TopicIterationQuery>>,
    headers: HeaderMap,
    position: Option<Json<TopicIterator>>,
    catalog: Arc<Catalog>,
    max_page: RowLimit,
) -> Result<axum::response::Response, ErrorReply> {
    let query = query.map(|Query(query)| query).unwrap_or_default();
    let content = headers
        .get(ACCEPT)
        .and_then(|header| header.to_str().ok().map(ToString::to_string));
    let position = position.map(|Json(value)| value);

    let topic = catalog.get_topic(&topic_name).await;
    let page_size = RowLimit::records(query.page_size.unwrap_or(1000)).min(max_page);
    let position = position.unwrap_or_default();
    let partition_filter = query.partition_filter;
    let order: Ordering = query.order.unwrap_or(TopicIterationOrder::Asc).into();

    let mut result = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        if order == Ordering::Reverse {
            Err(ErrorReply::InvalidQuery)?
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

#[utoipa::path(
    get,
    operation_id = "partition.get_records",
    path = "/topic/{topic_name}/partition/{partition_name}/records",
    params(
        ("topic_name", Path, description = "Topic name"),
        ("partition_name", Path, description = "Partition name"),
        RecordQuery,
    ),
    responses(
        (status = 200, description = "List of records for partition", body = serde_json::Value),
    ),
  )]
async fn partition_get_records(
    State(AppState(catalog, config)): State<AppState>,
    Path((topic_name, partition_name)): Path<(String, String)>,
    Query(query): Query<RecordQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ErrorReply> {
    let max_page = config.http.max_page;
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

    let start = result.chunks.first().and_then(|i| i.start());
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

    negotiate(
        headers
            .get(ACCEPT)
            .and_then(|header| header.to_str().ok().map(ToString::to_string)),
        result,
        query.data_focus,
        move |records| Records {
            span: range.map(Span::from_range),
            status,
            records: serialize_records(records),
        },
    )
}

fn serialize_records<I: IntoIterator<Item = Record>>(rs: I) -> Vec<String> {
    rs.into_iter()
        .map(|r| String::from_utf8(r.message).unwrap())
        .collect()
}

fn parse_time_range(
    start: String,
    end: Option<String>,
) -> Result<RangeInclusive<DateTime<Utc>>, ErrorReply> {
    let end = match end {
        Some(end_time) => end_time,
        None => return Err(ErrorReply::InvalidQuery),
    };

    let start = DateTime::parse_from_rfc3339(&start);
    let end = DateTime::parse_from_rfc3339(&end);
    if let (Ok(start), Ok(end)) = (start, end) {
        Ok(start.with_timezone(&Utc)..=end.with_timezone(&Utc))
    } else {
        Err(ErrorReply::InvalidQuery)
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        healthcheck,
        get_topics,
        topic_append,
        topic_get_partitions,
        topic_iterate_route,
        partition_get_records,
    ),
    components(
        schemas(
            DataFocus,
            Inserted,
            Partitions,
            // PartitionFilter,
            plateau_transport::ArrowSchemaChunk,
            Span,
            Topic,
            Topics,
            TopicIterationOrder,
            // TopicIterator,
        )
    ),
    tags(
        (name = "Plateau", description = "Plateau API")
    )
)]
struct ApiDoc;

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

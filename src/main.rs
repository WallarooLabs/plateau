mod catalog;
mod manifest;
mod metrics;
mod partition;
mod retention;
mod segment;
mod slog;
mod topic;

use ::log::info;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use futures::{future, stream};
use parquet::data_type::ByteArray;
use rweb::*;
use serde::{Deserialize, Serialize};
use slog::RecordIndex;
use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, SignalStream};
use warp::http::StatusCode;

use crate::catalog::Catalog;
use crate::slog::SlogError;
use crate::topic::{Record, TopicIterator};

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

#[derive(Schema, Serialize)]
struct Records {
    span: Option<Span>,
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
    Unknown,
}
impl warp::reject::Reject for ErrorReply {}

fn signal_stream(k: SignalKind) -> impl Stream<Item = ()> {
    SignalStream::new(signal(k).unwrap())
}

#[tokio::main]
async fn main() {
    let catalog = Catalog::attach(PathBuf::from("./data")).await;

    metrics::start_metrics();
    pretty_env_logger::init();
    let log = warp::log("plateau::http");

    let mut exit = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    let mut checkpoints = time::interval(Duration::from_secs(1));
    checkpoints.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let catalog_checkpoint = catalog.clone();
    let stream = IntervalStream::new(checkpoints)
        .take_until(exit.next())
        .for_each(|_| async {
            let inner = catalog_checkpoint.clone();
            inner.checkpoint().await;
            inner.retain().await;
        });

    let (spec, filter) = openapi::spec().build(move || {
        topic_append(catalog.clone())
            .or(topic_iterate(catalog.clone()))
            .or(topic_get_partitions(catalog.clone()))
            .or(partition_get_records(catalog))
    });

    future::select(
        Box::pin(stream),
        Box::pin(
            serve(filter.or(openapi_docs(spec)).with(log).recover(emit_error))
                .run(([0, 0, 0, 0], 3030)),
        ),
    )
    .await;
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
    let r = topic.append(&partition_name, &rs).await;

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

    let (next, rs) = if let Some(start) = query.start_time {
        let times = parse_time_range(start, query.end_time)?;
        topic.get_records_by_time(position, times, limit).await
    } else {
        topic.get_records(position, limit).await
    };

    Ok(Json::from(TopicIterationReply {
        records: serialize_records(rs),
        next,
    }))
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
    let rs = if let Some(start) = query.start_time {
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

    let start = rs.get(0).map(|r| r.index);
    let end = rs.iter().next_back().map(|r| r.index + 1);
    let range = start.zip(end).map(|(start, end)| start..end);

    Ok(Json::from(Records {
        span: range.map(Span::from_range),
        records: serialize_records(rs.into_iter().map(|r| r.data)),
    }))
}

async fn emit_error(err: Rejection) -> Result<impl Reply, Infallible> {
    let (code, message) = match err.find::<ErrorReply>() {
        Some(ErrorReply::InvalidQuery) => (StatusCode::BAD_REQUEST, "invalid query"),
        Some(ErrorReply::WriterBusy) => (StatusCode::TOO_MANY_REQUESTS, "writer busy"),
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

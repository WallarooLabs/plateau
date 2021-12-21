mod catalog;
mod manifest;
mod metrics;
mod partition;
mod segment;
mod slog;
mod topic;

use ::log::info;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::{future, stream};
use parquet::data_type::ByteArray;
use rweb::*;
use serde::{Deserialize, Serialize};
use slog::RecordIndex;
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, SignalStream};

use crate::catalog::Catalog;
use crate::topic::Record;

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
    partitions: HashMap<Arc<String>, Span>,
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

#[derive(Debug)]
struct InvalidQuery;
impl warp::reject::Reject for InvalidQuery {}

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
            info!("begin full catalog checkpoint");
            let start = SystemTime::now();
            catalog_checkpoint.clone().checkpoint().await;
            if let Ok(duration) = SystemTime::now().duration_since(start) {
                info!("finished full catalog checkpoint in {:?}", duration);
            } else {
                info!("finished full catalog checkpoint");
            }
        });

    let (spec, filter) = openapi::spec().build(move || {
        topic_append(catalog.clone())
            .or(topic_get_partitions(catalog.clone()))
            .or(topic_get_records(catalog))
    });

    future::select(
        Box::pin(stream),
        Box::pin(serve(filter.or(openapi_docs(spec)).with(log)).run(([0, 0, 0, 0], 3030))),
    )
    .await;
}

#[post("/topic/{topic_name}/{partition_name}")]
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
            return Err(warp::reject::custom(InvalidQuery {}));
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
        span: Span::from_range(r),
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
            .map(|(partition, range)| (Arc::new(partition), Span::from_range(range)))
            .collect(),
    }))
}

#[get("/topic/{topic_name}/{partition_name}/records")]
#[openapi(id = "topic.get_records")]
async fn topic_get_records(
    topic_name: String,
    partition_name: String,
    query: Query<RecordQuery>,
    #[data] catalog: Catalog,
) -> Result<Json<Records>, Rejection> {
    let query = query.into_inner();
    let topic = catalog.get_topic(&topic_name).await;
    let start_record = RecordIndex(query.start);
    let limit = std::cmp::min(query.limit.unwrap_or(1000), 10000);
    let (range, rs) = if let Some(start) = query.start_time {
        if let Some(end) = query.end_time {
            let start = DateTime::parse_from_rfc3339(&start);
            let end = DateTime::parse_from_rfc3339(&end);
            if let (Ok(start), Ok(end)) = (start, end) {
                let times = start.with_timezone(&Utc)..=end.with_timezone(&Utc);
                topic
                    .get_records_by_time(&partition_name, start_record, times, limit)
                    .await
            } else {
                return Err(warp::reject::custom(InvalidQuery {}));
            }
        } else {
            return Err(warp::reject::custom(InvalidQuery {}));
        }
    } else {
        topic
            .get_records(&partition_name, start_record, limit)
            .await
    };

    Ok(Json::from(Records {
        span: range.map(Span::from_range),
        records: rs
            .into_iter()
            .map(|r| String::from_utf8(r.message.data().to_vec()).unwrap())
            .collect(),
    }))
}

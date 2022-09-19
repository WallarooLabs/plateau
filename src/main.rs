use futures::stream::StreamExt;
use futures::{future, stream};
use rweb::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, SignalStream};

use plateau::catalog::Catalog;
use plateau::http;
use plateau::metrics;

fn signal_stream(k: SignalKind) -> impl Stream<Item = ()> {
    SignalStream::new(signal(k).unwrap())
}

#[tokio::main]
async fn main() {
    let catalog = Catalog::attach(PathBuf::from("./data")).await;

    metrics::start_metrics();
    pretty_env_logger::init();

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

    future::select(
        Box::pin(stream),
        http::serve(([0, 0, 0, 0], 3030), catalog.clone()).await.1,
    )
    .await;
}

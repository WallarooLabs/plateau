use futures::stream::StreamExt;
use futures::{future, stream};
use rweb::*;
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
    pretty_env_logger::init();

    let config = plateau::config::binary_config().expect("error getting configuration");
    let catalog = Catalog::attach(config.data_path.clone(), config.catalog.clone()).await;
    metrics::start_metrics(config.metrics.clone());

    let mut exit = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    let mut checkpoints = time::interval(Duration::from_millis(config.checkpoint_ms));
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
        http::serve(config, catalog.clone()).await.1,
    )
    .await;
}

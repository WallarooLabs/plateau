use ::log::info;
use futures::stream::StreamExt;
use futures::{future, stream};
use rweb::*;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, SignalStream};

use plateau::catalog::Catalog;
use plateau::metrics;
use plateau::{http, replication};

fn signal_stream(k: SignalKind) -> impl Stream<Item = ()> {
    SignalStream::new(signal(k).unwrap())
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let config = plateau::config::binary_config().expect("error getting configuration");
    let catalog = Arc::new(Catalog::attach(config.data_path.clone(), config.catalog.clone()).await);
    metrics::start_metrics(config.metrics.clone());

    let mut exit = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    let mut checkpoints = time::interval(Duration::from_millis(config.checkpoint_ms));
    checkpoints.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let checkpoint_stream = IntervalStream::new(checkpoints)
        .take_until(exit.next())
        .for_each(|_| async {
            catalog.checkpoint().await;
            catalog.retain().await;
        });

    let (addr, end_tx, server) = http::serve(config.clone(), catalog.clone()).await;
    {
        let mut tasks = vec![
            Box::pin(checkpoint_stream) as Pin<Box<dyn Future<Output = ()>>>,
            server,
        ];

        if config.catalog.storage.monitor {
            tasks.push(Box::pin(catalog.monitor_disk_storage()));
        }

        if let Some(replicate) = config.replication {
            tasks.push(Box::pin(replication::run(replicate, addr)));
        }

        future::select_all(tasks.into_iter()).await;
    }

    info!("shutting down");
    end_tx.send(()).ok();
    Catalog::close(catalog).await;
}

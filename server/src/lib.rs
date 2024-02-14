#![cfg_attr(nightly, feature(test))]

use futures::{future, stream};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_stream::wrappers::SignalStream;

mod axum_util;
pub mod catalog;
pub mod chunk;
pub mod config;
pub mod http;
mod limit;
pub mod manifest;
pub mod metrics;
mod partition;
pub mod replication;
mod segment;
mod slog;
mod storage;
mod topic;

pub use crate::config::PlateauConfig as Config;
pub use catalog::Catalog;
use plateau_transport::arrow2;

/// Future that resolves when an exit signal (SIGINT / SIGTERM / SIGQUIT) is
/// received.
pub fn exit_signal<'a>() -> future::BoxFuture<'a, ()> {
    use future::FutureExt;
    use stream::StreamExt;

    fn signal_stream(k: SignalKind) -> impl stream::Stream<Item = ()> {
        SignalStream::new(signal(k).unwrap())
    }

    let signal_stream = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    signal_stream.into_future().map(|_| ()).boxed()
}

/// Async task that runs the full plateau server stack from a user-provided
/// [PlateauConfig]
///
/// Attempts a clean shutdown when the provided `stop` signal is received (i.e.
/// [exit_signal]).
pub async fn task_from_config(
    config: config::PlateauConfig,
    stop: future::BoxFuture<'_, ()>,
) -> bool {
    let catalog = Arc::new(
        Catalog::attach(config.data_path.clone(), config.catalog.clone())
            .await
            .expect("error opening catalog"),
    );

    let (addr, end_tx, server) = http::serve(config.clone(), catalog.clone()).await;
    {
        use futures::future::FutureExt;
        let mut tasks = vec![Catalog::checkpoints(catalog.clone()).boxed(), stop, server];

        if config.catalog.storage.monitor {
            tasks.push(catalog.monitor_disk_storage().boxed());
        }

        if let Some(replicate) = config.replication {
            tasks.push(Box::pin(replication::run(replicate, addr)));
        }

        future::select_all(tasks.into_iter()).await;
    }

    tracing::info!("shutting down");
    end_tx.send(()).ok();
    Catalog::close_arc(catalog).await
}

use std::env;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use futures::{future, stream};
use rweb::*;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time;
use tokio_stream::wrappers::{IntervalStream, SignalStream};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use plateau::catalog::Catalog;
use plateau::metrics;
use plateau::{http, replication};

fn signal_stream(k: SignalKind) -> impl Stream<Item = ()> {
    SignalStream::new(signal(k).unwrap())
}

fn squawk(config: &plateau::config::PlateauConfig) {
    let version = env!("CARGO_PKG_VERSION");
    let commit = option_env!("BUILD_COMMIT").unwrap_or("unknown");
    let build_time = env!("BUILD_TIME");
    let run_time = chrono::Utc::now().to_rfc2822();
    let port = config.http.bind.port();
    let pid = std::process::id();
    let log_level = env::var("RUST_LOG").unwrap_or("unset".to_string());

    eprintln!(
        r#"
/*
** plateau v{version}
**
** commit:        {commit}
** build time:    {build_time}
** startup time:  {run_time}
** port:          {port}
** pid:           {pid}
** log level:     {log_level}
**
** https://wallaroo.ai
\*
"#
    );

    match config.to_string_pretty() {
        Ok(c) => {
            for line in c.lines() {
                info!("config toml: {}", line);
            }
        }
        Err(e) => error!("{}", e),
    }
}

#[tokio::main]
async fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "warn")
    }
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let config = plateau::config::binary_config().expect("error getting configuration");
    squawk(&config);

    let catalog = Arc::new(
        Catalog::attach(config.data_path.clone(), config.catalog.clone())
            .await
            .expect("error opening catalog"),
    );
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

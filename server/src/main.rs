use std::env;

use futures::{
    future::FutureExt,
    stream::{self, StreamExt},
};
use tokio::signal::unix::{signal, SignalKind};
use tokio_stream::wrappers::SignalStream;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use plateau::metrics;

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

    metrics::start_metrics(config.metrics.clone());

    fn signal_stream(k: SignalKind) -> impl stream::Stream<Item = ()> {
        SignalStream::new(signal(k).unwrap())
    }

    let mut signal_stream = stream::select_all(vec![
        signal_stream(SignalKind::interrupt()),
        signal_stream(SignalKind::terminate()),
        signal_stream(SignalKind::quit()),
    ]);

    let exit = signal_stream.next().map(|_| ());

    plateau::task_from_config(config, exit.boxed()).await;
}

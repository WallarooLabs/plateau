use std::env;
use tracing_subscriber::{fmt, EnvFilter};

use plateau::metrics;

fn squawk() {
    let version = env!("CARGO_PKG_VERSION");
    let commit = option_env!("BUILD_COMMIT").unwrap_or("unknown");
    let build_time = env!("BUILD_TIME");
    let run_time = chrono::Utc::now().to_rfc2822();
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
** pid:           {pid}
** log level:     {log_level}
**
** https://wallaroo.ai
\*
"#
    );
}

#[tokio::main]
async fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "warn")
    }
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let config = plateau::config::binary_config().expect("error getting configuration");
    squawk();
    config.log();

    metrics::start_metrics(config.metrics.clone());

    plateau::task_from_config(config, plateau::exit_signal()).await;
}

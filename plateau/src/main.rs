use tracing_subscriber::filter;

use plateau_server::metrics;

fn squawk(log_level: &filter::EnvFilter) {
    let version = env!("CARGO_PKG_VERSION");
    let commit = option_env!("BUILD_COMMIT").unwrap_or("unknown");
    let build_time = env!("BUILD_TIME");
    let run_time = chrono::Utc::now().to_rfc2822();
    let pid = std::process::id();

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
    let filter = filter::EnvFilter::builder()
        .with_default_directive(filter::LevelFilter::WARN.into())
        .from_env_lossy();

    squawk(&filter);

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = plateau_server::config::binary_config().expect("error getting configuration");
    config.log();

    metrics::start_metrics(config.metrics.clone());

    plateau_server::task_from_config(config, plateau_server::exit_signal()).await;
}

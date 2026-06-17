use std::time::Duration;

use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use bench::{load::basic_load_gen, run_external};

/// Load generator for an existing Plateau server.
#[derive(Parser)]
#[command(about)]
struct Args {
    /// Plateau server URL
    #[arg(long, default_value = "http://localhost:3030")]
    url: String,

    /// Path to the Arrow sample file used for data generation
    #[arg(long, default_value = "samples/list-ccfraud.arrow")]
    sample: String,

    /// Number of topics
    #[arg(long, default_value_t = 1)]
    topics: usize,

    /// Number of partitions per topic
    #[arg(long, default_value_t = 8)]
    partitions: usize,

    /// Rows per write batch
    #[arg(long, default_value_t = 50000)]
    rows: usize,

    /// Interval between writes in milliseconds
    #[arg(long, default_value_t = 8)]
    interval_ms: u64,

    /// Total load generation duration in seconds
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
}

#[tokio::main]
async fn main() {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Args::parse();

    let tasks = basic_load_gen(
        &args.sample,
        args.topics,
        args.partitions,
        args.rows,
        Duration::from_millis(args.interval_ms),
    );

    run_external(
        &args.url,
        tasks,
        Duration::from_secs(args.duration_secs),
    )
    .await;
}

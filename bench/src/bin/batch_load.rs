use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use bench::batch::{run_batch, BatchConfig};

/// Batch-oriented load generator for an existing Plateau server.
///
/// Simulates staggered batch jobs: each topic has its own schema, each
/// partition fires on a fixed schedule with an evenly-spread stagger offset.
/// A state file records the last completed batch per partition; on restart
/// the tool catches up any missed batches immediately before resuming the
/// normal schedule.
///
/// Per-topic settings (partitions, rows, batch_interval) live on each topic.
/// A [defaults] table supplies fallbacks for any topic that omits them.
///
/// Example config (batch-config.toml):
///
///   speed = 60.0          # 1h batches fire every 1 minute
///
///   [defaults]
///   partitions = 4
///   rows = 10000
///   batch_interval = "1h"
///
///   [[topics]]
///   name = "transactions"
///   sample = "samples/list-ccfraud.arrow"
///   batch_interval = "15m"   # this job runs more often
///
///   [[topics]]
///   name = "images"
///   sample = "samples/image_224x224.arrow"
///   partitions = 2
///   rows = 50
#[derive(Parser)]
#[command(about, verbatim_doc_comment)]
struct Args {
    /// Path to the TOML batch configuration file.
    #[arg(long, default_value = "batch-config.toml")]
    config: PathBuf,

    /// Plateau server URL (overrides nothing in config; config has no URL field).
    #[arg(long, default_value = "http://localhost:3030")]
    url: String,

    /// Path to the state file (overrides config.state_file if set).
    #[arg(long)]
    state: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Args::parse();

    let config = BatchConfig::from_file(&args.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config {:?}: {}", args.config, e);
        std::process::exit(1);
    });

    let state_path = args.state
        .or_else(|| config.state_file.clone())
        .unwrap_or_else(|| PathBuf::from("batch-state.json"));

    run_batch(&args.url, config, &state_path)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Fatal error: {}", e);
            std::process::exit(1);
        });
}

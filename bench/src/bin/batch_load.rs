use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::{fmt, EnvFilter};

use bench::batch::{run_batch, BatchConfig};

/// Batch-job load generator for an existing Plateau server.
///
/// Generates a pool of topics with synthetic schemas (random column counts and
/// types), then simulates staggered batch jobs over a sliding active window.
/// A state file and a directory of Arrow schema files are written on first run
/// so the tool can be safely stopped and restarted.
///
/// Example config (batch-config.toml):
///
///   speed = 60.0            # compress time: 1h intervals fire every 1 minute
///   schemas_dir = "batch-schemas"
///
///   [topics]
///   count = 200             # total topic pool
///   active = 8              # topics writing at once
///   rotation_interval = "1h"  # how often the active window advances
///   columns_min = 3         # min data columns per topic (excludes `time`)
///   columns_max = 35        # max data columns per topic (exclusive)
///   partitions_min = 1      # partitions drawn per topic from [min, max)
///   partitions_max = 8
///   rows_min = 1000         # overall rows-per-insert distribution; each topic
///   rows_max = 50000        #   draws its own sub-range, sampled per insert
///   batch_interval = "1h"  # real-world interval between batches per partition
///
/// All values drawn from a range use a clamped normal distribution.
#[derive(Parser)]
#[command(about, verbatim_doc_comment)]
struct Args {
    /// Path to the TOML batch configuration file.
    #[arg(long, default_value = "batch-config.toml")]
    config: PathBuf,

    /// Plateau server URL.
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

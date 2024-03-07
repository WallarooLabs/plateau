use std::time::Duration;
use tracing_subscriber::{fmt, EnvFilter};

use bench::{load::basic_load_gen, run};

#[tokio::main]
async fn main() {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let tasks = basic_load_gen(
        "samples/list-ccfraud.arrow",
        1,
        8,
        50000,
        Duration::from_millis(8),
    );

    run(tasks, Duration::from_secs(30)).await;
}

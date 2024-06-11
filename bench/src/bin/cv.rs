use std::time::Duration;
use tracing_subscriber::{fmt, EnvFilter};

use bench::{load::basic_load_gen, run};

#[tokio::main]
async fn main() {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let tasks = basic_load_gen(
        "samples/fixed-resnet50.arrow",
        1,
        8,
        10,
        Duration::from_millis(10),
    );

    run(tasks, Duration::from_secs(30)).await;
}

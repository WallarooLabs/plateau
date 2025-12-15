use plateau_client::replicate::{ExponentialBackoff, Replicate, ReplicateHost, ReplicationWorker};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use tracing::error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    pub replicate: Replicate,
    pub backoff: Backoff,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Backoff {
    /// Minimum (starting) backoff duration
    #[serde(with = "humantime_serde")]
    pub min: Duration,
    /// Multiplication factor for each successive retry attempt
    pub scale: f64,
    /// Random noise offset for each retry attempt
    pub jitter: f64,
    /// Maximum possible backoff duration
    #[serde(with = "humantime_serde")]
    pub max: Duration,
}

pub async fn run(mut config: Config, addr: SocketAddr) {
    let backoff = config.backoff;
    let backoff = ExponentialBackoff {
        current_interval: backoff.min,
        initial_interval: backoff.min,
        multiplier: backoff.scale,
        randomization_factor: backoff.jitter,
        max_interval: backoff.max,
        max_elapsed_time: None,
        ..Default::default()
    };

    // TODO: avoid this loopback via a trait
    config.replicate.hosts.push(ReplicateHost {
        id: "self".to_string(),
        url: format!("http://{}:{}", addr.ip(), addr.port()),
    });

    match ReplicationWorker::from_replicate(config.replicate).await {
        Ok(replication) => {
            error!(
                "unexpectedly exited loop: {:?}",
                replication.run_forever(config.period, backoff).await
            );
        }
        Err(e) => {
            error!("config error: {:?}", e)
        }
    }
}

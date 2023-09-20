use log::error;
use plateau_client::replicate::{ExponentialBackoff, Replicate, ReplicateHost, ReplicationWorker};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    pub replicate: Replicate,
}

pub async fn run(mut config: Config, addr: SocketAddr) {
    let backoff = ExponentialBackoff {
        max_interval: Duration::from_secs(3600),
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

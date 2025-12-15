use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    prometheus: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prometheus: Some("0.0.0.0:9000".to_string()),
        }
    }
}

pub fn start_metrics(config: Config) {
    if let Some(bind) = config.prometheus {
        let builder = PrometheusBuilder::new();
        let socket_addr = SocketAddr::from_str(&bind).unwrap();

        builder
            .with_http_listener(socket_addr)
            .add_global_label("system", "plateau")
            .install()
            .expect("failed to install Prometheus recorder");
    }
}

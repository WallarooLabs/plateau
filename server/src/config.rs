use anyhow::Result;
use config::{Config, File};
use log::info;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{catalog, http, metrics, replication};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PlateauConfig {
    pub data_path: PathBuf,
    pub checkpoint_ms: u64,

    pub http: http::Config,
    pub catalog: catalog::Config,
    pub metrics: metrics::Config,
    pub replication: Option<replication::Config>,
}

impl Default for PlateauConfig {
    fn default() -> Self {
        PlateauConfig {
            data_path: PathBuf::from("./data"),
            checkpoint_ms: 1000,

            http: http::Config::default(),
            catalog: catalog::Config::default(),
            metrics: metrics::Config::default(),
            replication: None,
        }
    }
}

pub fn binary_config() -> Result<PlateauConfig> {
    let config = Config::builder()
        .set_default("catalog.retain.max_bytes", "95GiB")?
        .add_source(File::with_name("/etc/plateau.yaml").required(false))
        .add_source(File::with_name("./plateau.yaml").required(false))
        .add_source(File::with_name("/etc/plateau.toml").required(false))
        .add_source(File::with_name("./plateau.toml").required(false))
        .add_source(File::with_name("/etc/replication.yaml").required(false))
        .add_source(File::with_name("./replication.yaml").required(false))
        .add_source(File::with_name("/etc/replication.toml").required(false))
        .add_source(File::with_name("./replication.toml").required(false))
        .add_source(
            config::Environment::with_prefix("PLATEAU")
                .try_parsing(true)
                .separator("__"),
        )
        .build()
        .unwrap();

    let config: PlateauConfig = config.try_deserialize()?;

    for line in toml::to_string_pretty(&config)?.lines() {
        info!("{}", line);
    }

    Ok(config)
}

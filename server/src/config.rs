use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{error, info};

use crate::{catalog, http, metrics, replication};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PlateauConfig {
    pub data_path: PathBuf,

    pub http: http::Config,
    pub catalog: catalog::Config,
    pub metrics: metrics::Config,
    pub replication: Option<replication::Config>,
}

impl PlateauConfig {
    pub fn to_string_pretty(&self) -> Result<String> {
        toml::to_string_pretty(self).map_err(|e| anyhow::anyhow!("could not format config: {}", e))
    }

    pub fn log(&self) {
        match self.to_string_pretty() {
            Ok(c) => {
                for line in c.lines() {
                    info!("config toml: {}", line);
                }
            }
            Err(e) => error!("{}", e),
        }
    }
}

impl Default for PlateauConfig {
    fn default() -> Self {
        PlateauConfig {
            data_path: PathBuf::from("./data"),

            http: http::Config::default(),
            catalog: catalog::Config::default(),
            metrics: metrics::Config::default(),
            replication: None,
        }
    }
}

pub fn env_source() -> config::Environment {
    config::Environment::with_prefix("PLATEAU")
        .try_parsing(true)
        .separator("__")
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
        .add_source(env_source())
        .build()
        .unwrap();

    let config: PlateauConfig = config.try_deserialize()?;

    Ok(config)
}

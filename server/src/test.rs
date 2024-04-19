use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tempfile::tempdir;
use tokio::sync::oneshot;

use crate::{http, Catalog, Config};

/// A RAII wrapper around a full plateau test server.
///
/// Exposes the underlying `catalog` if checkpoints are required.
///
/// Currently, we assume that only one test server can run at a given time to
/// prevent port conflicts.
pub struct TestServer {
    addr: SocketAddr,
    end_tx: oneshot::Sender<()>,
    pub catalog: Arc<Catalog>,
}

impl TestServer {
    pub async fn new() -> Result<Self> {
        Self::new_with_config(Default::default()).await
    }

    pub async fn new_with_config(config: Config) -> Result<Self> {
        let config = Config {
            http: http::Config {
                bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..config.http
            },
            ..config
        };

        Self::with_port_config(config).await
    }

    pub async fn with_port_config(mut config: Config) -> Result<Self> {
        let temp = tempdir()?;
        let root = temp.into_path();
        let catalog = Arc::new(Catalog::attach(root, config.catalog.clone()).await?);

        let serve_catalog = catalog.clone();
        let replication = std::mem::take(&mut config.replication);
        let (addr, end_tx, server) = http::serve(config, serve_catalog).await;
        tokio::spawn(server);

        if let Some(replication) = replication {
            tokio::spawn(crate::replication::run(replication, addr));
        }

        Ok(TestServer {
            addr,
            end_tx,
            catalog,
        })
    }

    pub fn host(&self) -> String {
        format!("{}:{}", self.addr.ip(), self.addr.port())
    }

    pub fn base(&self) -> String {
        format!("http://{}", self.host())
    }

    pub async fn stop(self) -> Arc<Catalog> {
        self.end_tx.send(()).unwrap();
        self.catalog
    }

    /// This simulates a "clean" plateau shutdown.
    pub async fn close(self) {
        Catalog::close_arc(self.stop().await).await;
    }

    pub fn client(&self) -> anyhow::Result<plateau_client::Client> {
        plateau_client::Client::new(&self.base()).map_err(Into::into)
    }
}

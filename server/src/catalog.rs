//! The catalog indexes all currently attached topics.
//! It is used to route reads and writes to the correct topic / partition.
use crate::manifest::Scope;
use crate::retention::Retention;
use ::log::{debug, info, trace, warn};
use metrics::gauge;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::manifest::Manifest;
use crate::topic::{PartitionConfig, Topic};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    retain: Retention,
}

#[derive(Clone)]
pub struct Catalog {
    config: Config,
    manifest: Manifest,
    root: Arc<PathBuf>,
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    last_checkpoint: Arc<RwLock<SystemTime>>,
}

impl Catalog {
    pub async fn attach(root: PathBuf) -> Self {
        Catalog {
            config: Config {
                retain: Retention {
                    max_bytes: 95 * 1024 * 1024 * 1024,
                    ..Retention::default()
                },
            },
            manifest: Manifest::attach(root.join("manifest.json")).await,
            root: Arc::new(root),
            topics: Arc::new(RwLock::new(HashMap::new())),
            last_checkpoint: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    pub async fn last_checkpoint(&self) -> SystemTime {
        *self.last_checkpoint.read().await
    }

    pub async fn checkpoint(&self) {
        trace!("begin full catalog checkpoint");
        let start = SystemTime::now();
        for (_, topic) in self.topics.read().await.iter() {
            topic.checkpoint().await
        }
        let end = SystemTime::now();
        if let Ok(duration) = end.duration_since(start) {
            if duration.as_secs() > 1 {
                warn!(
                    "full catalog checkpoint took {:?} (longer than 1s!)",
                    duration
                );
            } else {
                trace!("finished full catalog checkpoint in {:?}", duration);
            }
            gauge!(
                "catalog_checkpoint_ms",
                (duration.as_micros() as f64) / 1000.0
            );
        } else {
            warn!("finished full catalog checkpoint; time skew");
        }
        *self.last_checkpoint.write().await = end;
    }

    pub async fn retain(&self) {
        info!("begin global retention check");
        while self.over_retention_limit().await {
            // errors in here are effectively unrecoverable as the loop would otherwise spin.
            // additionally, the disk will eventually fill leading to system failure
            let oldest = self
                .manifest
                .get_oldest_segment(None)
                .await
                .expect("no partition to remove");
            let topics = self.topics.read().await;
            let topic = topics.get(oldest.topic()).expect("invalid topic");
            let partition = topic.get_partition(oldest.partition()).await;
            partition.remove_oldest().await;
        }
        info!("end global retention check");
    }

    async fn byte_size(&self) -> usize {
        self.manifest.get_size(Scope::Global).await
    }

    async fn over_retention_limit(&self) -> bool {
        let retain = &self.config.retain;

        let size = self.byte_size().await;
        debug!("catalog size: {}", size);
        gauge!("stored_size_bytes", size as f64,);
        size > retain.max_bytes
    }

    pub async fn list_topics(&self) -> Vec<String> {
        self.manifest.get_topics().await
    }

    pub async fn get_topic(&self, name: &str) -> RwLockReadGuard<'_, Topic> {
        let read = self.topics.read().await;
        let v = RwLockReadGuard::try_map(read, |m| m.get(name));
        match v {
            Ok(topic) => topic,
            Err(read) => {
                drop(read);
                let mut write = self.topics.write().await;
                info!("creating new topic: {}", name);
                let topic = Topic::attach(
                    (*self.root).clone(),
                    self.manifest.clone(),
                    String::from(name),
                    PartitionConfig::default(),
                )
                .await;
                write.insert(String::from(name), topic);
                let read = write.downgrade();
                RwLockReadGuard::map(read, |m| m.get(name).unwrap())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::partition::RecordIndex;
    use crate::segment::test::build_records;
    use crate::segment::Record;
    use anyhow::Result;
    use chrono::{TimeZone, Utc};
    use tempfile::{tempdir, TempDir};

    async fn catalog() -> (TempDir, Catalog) {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        (dir, Catalog::attach(root).await)
    }

    #[tokio::test]
    async fn test_independence() -> Result<()> {
        let (_root, catalog) = catalog().await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            catalog
                .get_topic(&name)
                .await
                .extend_records("default", &[record.clone()])
                .await?;
        }

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            let topic = catalog.get_topic(&name).await;
            assert_eq!(
                topic
                    .get_record_by_index("default", RecordIndex(ix / 3))
                    .await,
                Some(record.clone())
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_retain() -> Result<()> {
        pretty_env_logger::init();
        let (_root, mut catalog) = catalog().await;
        catalog.config.retain.max_bytes = 8000;

        let data = "x".to_string().repeat(500);

        let old_size = {
            let oldest_records = build_records((0..10).map(|_| (0, data.clone())));
            let oldest_topic = catalog.get_topic("oldest").await;
            let p = oldest_topic.get_partition("default").await;

            p.extend_records(&oldest_records).await?;
            p.compact().await;
            p.extend_records(&oldest_records).await?;
            p.compact().await;
            p.extend_records(&oldest_records).await?;
            p.byte_size().await
        };

        let records = build_records((0..10).map(|_| (100, data.clone())));
        for ix in 0..5 {
            let name = format!("topic-{}", ix);
            let topic = catalog.get_topic(&name).await;
            let partition = topic.get_partition("default").await;
            partition.extend_records(&records).await?;
            partition.compact().await;
            partition.extend_records(&records).await?;
        }

        assert!(catalog.byte_size().await > catalog.config.retain.max_bytes);
        catalog.retain().await;
        assert!(catalog.byte_size().await < catalog.config.retain.max_bytes);
        let topic = catalog.get_topic("oldest").await;
        let partition = topic.get_partition("default").await;
        assert!(partition.byte_size().await < old_size);

        Ok(())
    }
}

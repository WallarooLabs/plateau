//! The catalog indexes all currently attached topics.
//! It is used to route reads and writes to the correct topic / partition.
use ::log::info;
use metrics::gauge;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::manifest::Manifest;
use crate::topic::{PartitionConfig, Topic};

#[derive(Clone)]
pub struct Catalog {
    manifest: Manifest,
    root: Arc<PathBuf>,
    topics: Arc<RwLock<HashMap<String, Topic>>>,
}

impl Catalog {
    pub async fn attach(root: PathBuf) -> Self {
        Catalog {
            manifest: Manifest::attach(root.join("manifest.json")).await,
            root: Arc::new(root),
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn checkpoint(&self) {
        info!("begin full catalog checkpoint");
        let start = SystemTime::now();
        for (_, topic) in self.topics.read().await.iter() {
            topic.checkpoint().await
        }
        if let Ok(duration) = SystemTime::now().duration_since(start) {
            info!("finished full catalog checkpoint in {:?}", duration);
            gauge!(
                "catalog_checkpoint_ms",
                (duration.as_micros() as f64) / 1000.0
            );
        } else {
            info!("finished full catalog checkpoint");
        }
    }

    pub async fn get_topic(&self, name: &str) -> RwLockReadGuard<'_, Topic> {
        let read = self.topics.read().await;
        let v = RwLockReadGuard::try_map(read, |m| m.get(name));
        match v {
            Ok(topic) => topic,
            Err(read) => {
                drop(read);
                let mut write = self.topics.write().await;
                info!("creating new partition: {}", name);
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
    use crate::segment::Record;
    use anyhow::Result;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_independence() -> Result<()> {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let catalog = Catalog::attach(root).await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            catalog
                .get_topic(&name)
                .await
                .append("default", &vec![record.clone()])
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
}

//! A topic is a collection of partitions. It is an abstraction used for queries
//! of a given topic over _all_ partitions.
use crate::manifest::Manifest;
pub use crate::partition::Config as PartitionConfig;
use crate::partition::{Partition, PartitionId};
pub use crate::partition::{Retention, Rolling};
pub use crate::segment::Record;
use crate::slog::RecordIndex;
use chrono::{DateTime, Utc};
use futures::future::FutureExt;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use tokio::sync::{RwLock, RwLockReadGuard};

type PartitionMap = HashMap<String, Partition>;

pub struct Topic {
    root: PathBuf,
    manifest: Manifest,
    name: String,
    partitions: RwLock<PartitionMap>,
    config: PartitionConfig,
}

impl Topic {
    pub async fn attach(
        root: PathBuf,
        manifest: Manifest,
        name: String,
        config: PartitionConfig,
    ) -> Self {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }

        let partitions = HashMap::new();

        Topic {
            root,
            manifest,
            name,
            partitions: RwLock::new(partitions),
            config,
        }
    }

    fn partition_root(root: &PathBuf, name: &str) -> PathBuf {
        root.join(name)
    }

    pub async fn readable_ids(&self) -> HashMap<String, Range<RecordIndex>> {
        self.map_partitions(|partition| async move { partition.readable_ids().await })
            .await
    }

    pub async fn map_partitions<'a, Fut, F, T>(&'a self, mut f: F) -> HashMap<String, T>
    where
        F: FnMut(RwLockReadGuard<'a, Partition>) -> Fut,
        Fut: futures::Future<Output = Option<T>>,
    {
        let active: Vec<String> = { self.partitions.read().await.keys().cloned().collect() };
        let stored = self.manifest.get_partitions(&self.name).await;

        stream::iter(active.iter().chain(stored.iter()))
            .flat_map(move |name| {
                self.get_partition(name)
                    .map(move |p| (name.clone(), p))
                    .into_stream()
            })
            .flat_map(|(name, p)| {
                f(p).into_stream()
                    .flat_map(|v| stream::iter(v.into_iter()))
                    .map(move |v| (name.clone(), v))
            })
            .collect()
            .await
    }

    async fn get_partition(&self, partition_name: &str) -> RwLockReadGuard<'_, Partition> {
        let partitions = self.partitions.read().await;
        let current_partition = RwLockReadGuard::try_map(partitions, |map| map.get(partition_name));
        if let Ok(part) = current_partition {
            part
        } else {
            drop(current_partition);
            let mut partitions = self.partitions.write().await;
            let id = PartitionId::new(&self.name, partition_name);
            let part = Partition::attach(
                Topic::partition_root(&self.root, &self.name),
                self.manifest.clone(),
                id,
                self.config.clone(),
            )
            .await;
            partitions.insert(partition_name.to_string(), part);
            let partitions = partitions.downgrade();
            RwLockReadGuard::map(partitions, |map| {
                // note: this unwrap() is guaranteed to succeed, as we just inserted
                // the key and still hold the lock. ideally we'd use the
                // `OccupiedEntry` api here, but it is still unstable
                map.get(partition_name).unwrap()
            })
        }
    }

    pub async fn checkpoint(&self) {
        self.map_partitions::<_, _, ()>(|partition| async move {
            partition.checkpoint().await;
            None
        })
        .await;
    }

    pub async fn append(&self, partition_name: &str, rs: &[Record]) -> Range<RecordIndex> {
        let partition = self.get_partition(partition_name).await;
        partition.append(rs).await
    }

    pub async fn get_record_by_index(
        &self,
        partition_name: &str,
        index: RecordIndex,
    ) -> Option<Record> {
        let partition = self.get_partition(partition_name).await;
        partition.get_record_by_index(index).await
    }

    pub(crate) async fn get_records(
        &self,
        partition_name: &str,
        start: RecordIndex,
        limit: usize,
    ) -> (Option<Range<RecordIndex>>, Vec<Record>) {
        let partition = self.get_partition(partition_name).await;
        partition.get_records(start, limit).await
    }

    pub(crate) async fn get_records_by_time(
        &self,
        partition_name: &str,
        start: RecordIndex,
        times: RangeInclusive<DateTime<Utc>>,
        limit: usize,
    ) -> (Option<Range<RecordIndex>>, Vec<Record>) {
        let partition = self.get_partition(partition_name).await;
        partition.get_records_by_time(start, times, limit).await
    }

    pub async fn commit(&self) {
        for (_, part) in self.partitions.read().await.iter() {
            part.commit().await;
        }
    }
}

mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use std::collections::HashSet;
    use std::convert::TryFrom;
    use std::iter::FromIterator;
    use std::ops::Deref;
    use std::thread;
    use std::time::{Duration, Instant, SystemTime};
    use tempfile::tempdir;
    use tokio::sync::mpsc::channel;

    #[tokio::test]
    async fn test_independence() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let topic = Topic::attach(
            root.clone(),
            manifest.clone(),
            String::from("testing"),
            PartitionConfig::default(),
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic.append(&name, &vec![record.clone()]).await;
        }

        topic.commit().await;

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }

        let topic = Topic::attach(
            root,
            manifest,
            String::from("testing"),
            PartitionConfig::default(),
        )
        .await;
        assert_eq!(
            topic.readable_ids().await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );
    }

    #[tokio::test]
    async fn test_active_segments() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let topic = Topic::attach(
            root.clone(),
            manifest.clone(),
            String::from("testing"),
            PartitionConfig::default(),
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic.append(&name, &vec![record.clone()]).await;
        }

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            topic.get_records("partition-0", RecordIndex(0), 1000).await,
            (
                Some(RecordIndex(0)..RecordIndex(2)),
                vec![records[0].clone(), records[3].clone()]
            )
        );

        assert_eq!(
            topic.readable_ids().await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );
    }

    #[ignore]
    #[tokio::test]
    async fn test_bench() {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let partitions = 4;
        let sample = 40 * 1000;
        let total = sample * 15;

        let mut handles = vec![];
        let (otx, mut rx) = channel(1024);
        for part in 0..partitions {
            let tx = otx.clone();
            let data = vec!["x"; 128].join("");
            let thread_root = root.clone();
            let p = format!("part-{}", part);
            let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
            let handle = tokio::spawn(async move {
                let t = Topic::attach(
                    thread_root,
                    manifest,
                    String::from("testing"),
                    PartitionConfig {
                        retain: Retention {
                            max_bytes: 50 * 1024 * 1024,
                            ..Retention::default()
                        },
                        ..PartitionConfig::default()
                    },
                )
                .await;

                let seed = 0..sample;
                let records: Vec<_> = seed
                    .clone()
                    .into_iter()
                    .map(|message| Record {
                        time: Utc.timestamp(0, 0),
                        message: ByteArray::from(
                            format!("{{ \"data\": \"{}-{}\" }}", data, message).as_str(),
                        ),
                    })
                    .collect();

                let mut ix = 0;
                let mut prev = None;
                use itermore::IterMore;
                for rs in records.iter().cycle().take(total).chunks::<10000>() {
                    let now = Utc.timestamp(0, 0);
                    let rs: Vec<Record> = rs
                        .iter()
                        .cloned()
                        .map(|r| {
                            let mut c = r.clone();
                            c.time = now;
                            c
                        })
                        .collect();

                    t.append(&p, &rs).await;
                    if let Some(prev_ix) = prev {
                        tx.send(ix - prev_ix).await.unwrap();
                    }
                    prev = Some(ix);
                    ix += rs.len();
                }
                t.commit().await;
                tx.send(total - prev.unwrap_or(0)).await.unwrap();
            });
            handles.push(handle);
        }

        let start = Instant::now();
        let mut written = 0;
        while written < total * partitions {
            written += rx.recv().await.unwrap();
            println!(
                "{}/{} elapsed: {}ms",
                written,
                total * partitions,
                (Instant::now() - start).as_millis()
            );
        }
        let elapsed_ms = (Instant::now() - start).as_millis();
        println!(
            "written: {} / elapsed: {}ms ({:.2}kw/s)",
            written,
            elapsed_ms,
            f64::from(i32::try_from(written).unwrap())
                / f64::from(i32::try_from(elapsed_ms).unwrap())
        );

        for handle in handles {
            handle.await.unwrap();
        }
    }
}

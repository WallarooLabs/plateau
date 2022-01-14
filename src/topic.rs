//! A topic is a collection of partitions. It is an abstraction used for queries
//! of a given topic over _all_ partitions.
use crate::manifest::Manifest;
pub use crate::partition::Config as PartitionConfig;
pub use crate::partition::{IndexedRecord, Retention, Rolling};
use crate::partition::{Partition, PartitionId};
pub use crate::segment::Record;
use crate::slog::RecordIndex;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::FutureExt;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
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

pub type TopicIterator = HashMap<String, usize>;

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

    async fn partition_names(&self) -> Vec<String> {
        let active: Vec<String> = { self.partitions.read().await.keys().cloned().collect() };
        let set: HashSet<_> = active.iter().cloned().collect();
        let stored = self
            .manifest
            .get_partitions(&self.name)
            .await
            .into_iter()
            .filter(|n| !set.contains(n));

        active.into_iter().chain(stored).collect()
    }

    pub async fn map_partitions<'a, Fut, F, T>(&'a self, mut f: F) -> HashMap<String, T>
    where
        F: FnMut(RwLockReadGuard<'a, Partition>) -> Fut,
        Fut: futures::Future<Output = Option<T>>,
    {
        stream::iter(self.partition_names().await)
            .flat_map(|name| {
                async move {
                    let partition = self.get_partition(&name).await;
                    (name, partition)
                }
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

    pub async fn get_partition(&self, partition_name: &str) -> RwLockReadGuard<'_, Partition> {
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

    pub async fn append(&self, partition_name: &str, rs: &[Record]) -> Result<Range<RecordIndex>> {
        let partition = self.get_partition(partition_name).await;
        partition.append(rs).await
    }

    #[cfg(test)]
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
        starts: TopicIterator,
        limit: usize,
    ) -> (TopicIterator, Vec<Record>) {
        self.get_records_from_all(
            starts,
            limit,
            |partition, start, partition_limit| async move {
                partition.get_records(start, partition_limit).await
            },
        )
        .await
    }

    pub(crate) async fn get_records_by_time(
        &self,
        starts: TopicIterator,
        times: RangeInclusive<DateTime<Utc>>,
        limit: usize,
    ) -> (TopicIterator, Vec<Record>) {
        let times = &times;
        self.get_records_from_all(
            starts,
            limit,
            |partition, start, partition_limit| async move {
                partition
                    .get_records_by_time(start, times.clone(), partition_limit)
                    .await
            },
        )
        .await
    }

    pub(crate) async fn get_records_from_all<'a, F, Fut>(
        &'a self,
        starts: TopicIterator,
        limit: usize,
        fetch: F,
    ) -> (TopicIterator, Vec<Record>)
    where
        F: Fn(RwLockReadGuard<'a, Partition>, RecordIndex, usize) -> Fut,
        Fut: futures::Future<Output = Vec<IndexedRecord>>,
    {
        // there's a minor race condition in here that can result in too many /
        // few records getting returned. If `partition_count` is calculated and
        // then partitions are added / removed, the below `map_partitions` step
        // will use an incorrect limit. we cannot add a dummy lock to force the
        // whole function to read lock because `map_partitions` can result in a
        // lock upgrade if the partition is not currently held in memory.
        let partition_count = self.partition_names().await.len();
        // rounded-up integer division
        let partition_limit = (limit + partition_count - 1) / partition_count;
        let starts = &starts;
        let fetch = &fetch;
        let all = self
            .map_partitions(|p| async move {
                let start = RecordIndex(starts.get(p.id().partition()).cloned().unwrap_or(0));
                Some(fetch(p, start, partition_limit).await)
            })
            .await;

        // if no values are present for a given partition, we have reached the
        // end of that partition. ensure we return its final index to avoid
        // spurious restarts
        let current_ranges = self.readable_ids().await;
        let next_indices: HashMap<_, _> = all
            .iter()
            .flat_map(|(k, rs)| {
                rs.last()
                    .map(|r| (r.index.0 + 1))
                    .or_else(|| current_ranges.get(k).map(|r| r.end.0))
                    .map(|ix| (k.clone(), ix))
            })
            .collect();

        let records: Vec<_> = all
            .into_iter()
            .flat_map(|(_, rs)| rs.into_iter().map(|r| r.data))
            .collect();

        (next_indices, records)
    }

    #[cfg(test)]
    pub async fn commit(&self) -> Result<()> {
        for (_, part) in self.partitions.read().await.iter() {
            part.commit().await?
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::partition::test::deindex;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use std::collections::HashSet;
    use std::convert::TryFrom;
    use std::iter::FromIterator;
    use std::time::Instant;
    use tempfile::tempdir;
    use tokio::sync::mpsc::channel;

    fn dummy_records<I, S>(s: I) -> Vec<Record>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        timed_records(s.into_iter().enumerate().map(|(ix, is)| (ix as i64, is)))
    }

    fn timed_records<I, S>(s: I) -> Vec<Record>
    where
        I: IntoIterator<Item = (i64, S)>,
        S: Into<String>,
    {
        s.into_iter()
            .map(|(ts, is)| Record {
                time: Utc.timestamp(ts, 0),
                message: ByteArray::from(is.into().as_str()),
            })
            .collect()
    }

    async fn scratch() -> (tempfile::TempDir, Topic) {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        (
            dir,
            Topic::attach(
                root,
                manifest,
                String::from("testing"),
                PartitionConfig::default(),
            )
            .await,
        )
    }

    async fn reload(old: Topic) -> Topic {
        let root = old.root.clone();
        let manifest = old.manifest.clone();
        let name = old.name.clone();
        drop(old);
        Topic::attach(root, manifest, name, PartitionConfig::default()).await
    }

    #[tokio::test]
    async fn test_independence() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = dummy_records(vec!["abc", "def", "ghi", "jkl", "mno", "p"]);

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic.append(&name, &vec![record.clone()]).await?;
        }

        topic.commit().await?;

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }

        let topic = reload(topic).await;

        assert_eq!(
            topic.readable_ids().await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_time_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records(vec![
            (0, "abc"),
            (10, "def"),
            (12, "ghi"),
            (13, "jkl"),
            (14, "mno"),
            (15, "p"),
        ]);

        let names: Vec<_> = (0..=3)
            .into_iter()
            .map(|ix| format!("partition-{}", ix % 3))
            .collect();
        for (ix, record) in records.iter().enumerate() {
            topic.append(&names[ix % 3], &vec![record.clone()]).await?;
        }

        topic.commit().await?;

        let span = Utc.timestamp(5, 0)..=Utc.timestamp(18, 0);

        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 1, 1]).collect();
        let (it, rs) = topic
            .get_records_by_time(HashMap::new(), span.clone(), 1)
            .await;
        assert_eq!(it, expected_it);
        assert_eq!(
            rs.into_iter().collect::<HashSet<_>>(),
            records[1..4].to_vec().into_iter().collect::<HashSet<_>>()
        );

        let prior_it = it;
        let (it, rs) = topic.get_records_by_time(prior_it, span.clone(), 1).await;
        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 2, 2]).collect();
        assert_eq!(it, expected_it);
        assert_eq!(
            rs.into_iter().collect::<HashSet<_>>(),
            records[4..].to_vec().into_iter().collect::<HashSet<_>>()
        );

        let prior_it = it;
        let (it, rs) = topic.get_records_by_time(prior_it, span, 1).await;
        assert_eq!(it, expected_it);
        assert_eq!(rs, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn test_active_segments() -> Result<()> {
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
            topic.append(&name, &vec![record.clone()]).await?;
        }

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            deindex(
                topic
                    .get_partition("partition-0")
                    .await
                    .get_records(RecordIndex(0), 1000)
                    .await
            ),
            vec![records[0].clone(), records[3].clone()]
        );

        assert_eq!(
            topic.readable_ids().await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );

        Ok(())
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

                    t.append(&p, &rs).await.unwrap();
                    if let Some(prev_ix) = prev {
                        tx.send(ix - prev_ix).await.unwrap();
                    }
                    prev = Some(ix);
                    ix += rs.len();
                }
                t.commit().await.unwrap();
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

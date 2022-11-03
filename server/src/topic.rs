//! A topic is a collection of partitions. It is an abstraction used for queries
//! of a given topic over _all_ partitions.
use crate::chunk::{LegacyRecords, Schema};
use crate::limit::{BatchStatus, LimitedBatch, RowLimit};
use crate::manifest::Manifest;
pub use crate::partition::Config as PartitionConfig;
pub use crate::partition::Rolling;
use crate::partition::{Partition, PartitionId};
pub use crate::segment::Record;
use crate::slog::RecordIndex;
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::FutureExt;
use futures::stream;
use futures::stream::StreamExt;
use plateau_transport::{SchemaChunk, TopicIterator};
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

pub(crate) struct TopicRecordResponse {
    pub(crate) iter: TopicIterator,
    pub(crate) batch: LimitedBatch,
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

    fn partition_root(root: &Path, name: &str) -> PathBuf {
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

    pub async fn extend(
        &self,
        partition_name: &str,
        data: SchemaChunk<Schema>,
    ) -> Result<Range<RecordIndex>> {
        let partition = self.get_partition(partition_name).await;
        partition.extend(data).await
    }

    pub async fn extend_records(
        &self,
        partition_name: &str,
        rs: &[Record],
    ) -> Result<Range<RecordIndex>> {
        self.extend(
            partition_name,
            SchemaChunk::try_from(LegacyRecords(rs.to_vec())).unwrap(),
        )
        .await
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
        limit: RowLimit,
    ) -> TopicRecordResponse {
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
        limit: RowLimit,
    ) -> TopicRecordResponse {
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
        mut iterator: TopicIterator,
        limit: RowLimit,
        fetch: F,
    ) -> TopicRecordResponse
    where
        F: Fn(RwLockReadGuard<'a, Partition>, RecordIndex, RowLimit) -> Fut,
        Fut: futures::Future<Output = LimitedBatch>,
    {
        let mut read_starts: Vec<_> = self
            .readable_ids()
            .await
            .into_iter()
            .map(|(k, v)| {
                (
                    RecordIndex(*iterator.entry(k.clone()).or_insert(v.start.0)),
                    k,
                )
            })
            .collect();

        read_starts.sort();
        let mut batch = LimitedBatch::open(limit);
        let mut any_schema_change = false;
        for (start, name) in read_starts.into_iter() {
            let partition = self.get_partition(&name).await;
            if let BatchStatus::Open { remaining } = batch.status {
                let batch_response = fetch(partition, start, remaining).await;
                if matches!(batch_response.status, BatchStatus::SchemaChanged) {
                    any_schema_change = true;
                }
                if batch.compatible_with(&batch_response) {
                    batch.extend(batch_response);

                    if let Some(final_record) =
                        batch.chunks.last().and_then(|indexed| indexed.end())
                    {
                        iterator.insert(name, (final_record + 1).0);
                    }
                } else {
                    any_schema_change = true;
                }
            } else {
                break;
            }
        }

        // we don't want to short-circuit when one partition has a schema
        // change; we want to fetch all records with the current schema
        // from all partitions.
        if any_schema_change {
            batch.status = BatchStatus::SchemaChanged;
        }

        TopicRecordResponse {
            iter: iterator,
            batch,
        }
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
    use crate::chunk::test::{inferences_schema_a, inferences_schema_b};
    use crate::partition::test::{assert_limit_unreached, deindex};
    use crate::retention::Retention;
    use chrono::{TimeZone, Utc};
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
                message: is.into().into_bytes(),
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
            topic
                .extend(
                    &name,
                    SchemaChunk::try_from(LegacyRecords(vec![record.clone()]))?,
                )
                .await?;
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

        let mut part_records = vec![vec![], vec![], vec![]];
        let names: Vec<_> = (0..=3)
            .into_iter()
            .map(|ix| format!("partition-{}", ix % 3))
            .collect();
        for (ix, record) in records.iter().enumerate() {
            part_records[ix % 3].push(record.clone());
            topic
                .extend(
                    &names[ix % 3],
                    SchemaChunk::try_from(LegacyRecords(vec![record.clone()]))?,
                )
                .await?;
        }

        topic.commit().await?;

        let span = Utc.timestamp(5, 0)..=Utc.timestamp(18, 0);

        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 1, 0]).collect();
        // fetching two records will spill from partition-0, which has only one
        // record in the given time range, to partition-1
        let result = topic
            .get_records_by_time(HashMap::new(), span.clone(), RowLimit::records(2))
            .await;
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .into_legacy()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[0][1].clone(), part_records[1][0].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // next fetch will use partition with lowest index in iterator, partition-2
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span.clone(), RowLimit::records(1))
            .await;
        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 1, 1]).collect();
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .into_legacy()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[2][0].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // final fetch will fetch both remaining records from partition-1 and partition-2
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span.clone(), RowLimit::records(5))
            .await;
        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 2, 2]).collect();
        assert_limit_unreached(&result.batch.status);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .into_legacy()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[1][1].clone(), part_records[2][1].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // no more records left
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span, RowLimit::records(1))
            .await;
        assert_limit_unreached(&result.batch.status);
        assert_eq!(result.iter, expected_it);
        assert_eq!(result.batch.into_legacy()?, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_in_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let chunk_a = inferences_schema_a();
        let chunk_b = inferences_schema_b();

        let chunk_allocation = vec![
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_b, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_a, &chunk_b],
        ];
        let names: Vec<_> = (0..=3)
            .into_iter()
            .map(|ix| format!("partition-{}", ix))
            .collect();

        fn to_iter(names: &[String], v: [usize; 3]) -> HashMap<String, usize> {
            names.iter().cloned().zip(v).collect()
        }

        for (name, chunks) in names.iter().zip(chunk_allocation.into_iter()) {
            for chunk in chunks {
                topic.extend(name, chunk.clone()).await?;
                topic.get_partition(name).await.commit().await?;
            }
        }

        let many_rows = RowLimit::records(100);

        // first, we zip through all schema-a chunks at the start
        let result = topic.get_records(HashMap::new(), many_rows).await;
        assert_eq!(result.iter, to_iter(&names, [15, 5, 20]));
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // now, the min partition (partition-1) has a schema change to schema-b.
        // we set a row limit here so we can test final iteration over all
        // partitions with schema-b later.
        let result = topic.get_records(result.iter, RowLimit::records(5)).await;
        assert_eq!(result.iter, to_iter(&names, [15, 10, 20]));
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);

        // that same partition is still the min partition, but has
        // briefly changed back to schema-a
        let result = topic.get_records(result.iter, many_rows).await;
        assert_eq!(result.iter, to_iter(&names, [15, 15, 20]));
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // now it has changed back to schema-b, and we can resume iterating
        // through all partitions.
        let result = topic.get_records(result.iter, many_rows).await;
        assert_eq!(result.iter, to_iter(&names, [25, 25, 25]));
        assert!(result.batch.status.is_open());

        Ok(())
    }

    #[tokio::test]
    async fn test_full_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records((0..103).map(|ix| (ix, format!("record-{}", ix))));

        let parts = 7;
        assert!(records.len() % parts != 0);
        let names: Vec<_> = (0..=parts)
            .into_iter()
            .map(|ix| format!("partition-{}", ix))
            .collect();
        for (ix, record) in records.iter().enumerate() {
            topic
                .extend_records(&names[ix % parts], &[record.clone()])
                .await?;
        }

        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;
        assert!(fetch_count % parts != 0);
        assert!(records.len() % fetch_count != 0);
        for _ in 0..records.len() {
            let result = topic.get_records(it, RowLimit::records(fetch_count)).await;

            if fetched.len() + fetch_count >= records.len() {
                assert_limit_unreached(&result.batch.status);
            } else {
                assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
            }
            fetched.extend(result.batch.into_legacy().unwrap());
            it = result.iter;
        }
        assert_eq!(
            records.into_iter().collect::<HashSet<_>>(),
            fetched.into_iter().collect::<HashSet<_>>()
        );
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
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic.extend_records(&name, &[record.clone()]).await?;
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
                    .get_records(RecordIndex(0), RowLimit::records(1000))
                    .await
                    .chunks
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
                        message: format!("{{ \"data\": \"{}-{}\" }}", data, message).into_bytes(),
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

                    t.extend_records(&p, &rs).await.unwrap();
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

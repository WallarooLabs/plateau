//! A `Partition` is responsible for reading the commit stream from a `Slog` and
//! ensuring the associated finalized `SegmentData` is persisted in the
//! `Manifest`.
//!
//! There can be many `Partition`s in a `Topic`. This unlocks the write parallelism
//! advantages inherent in having many concurrent `Slog` writer threads.
//!
//! The `Partition` is also responsible for assigning sequential unique
//! identifiers to each incoming record. A `(topic, partition, record)` tuple is
//! a globally unique identifier for each durably stored record.
//!
//! To ensure durability, `.commit()` must be called after a given `.append()`.
//! Otherwise, the record will only be durably stored on the next `.roll()`.
//!
//! A `Partition` also has `Rolling` and `Retention` policies that are used to
//! determine when to roll segments and expire old data. `Rolling` policies are
//! evaluated on every insert, and `Retention` policies are enforced on every
//! `roll`.
use crate::manifest::Manifest;
pub use crate::manifest::{PartitionId, SegmentData};
pub use crate::segment::Record;
pub use crate::slog::{InternalIndex, RecordIndex};
use crate::slog::{SegmentIndex, SegmentRecordIndex, Slog};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::OptionFuture;
use futures::stream::{Stream, StreamExt};
use futures::FutureExt;
use futures::{future, stream};
use log::{info, trace};
use metrics::{counter, gauge};
use serde::{Deserialize, Serialize};
use std::fs;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rolling {
    pub max_segment_size: usize,
    pub max_segment_index: usize,
    pub max_segment_duration: Option<Duration>,
}

impl Default for Rolling {
    fn default() -> Self {
        Rolling {
            max_segment_size: 100 * 1024 * 1024,
            max_segment_index: 100000,
            max_segment_duration: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub max_segment_count: Option<usize>,
    pub max_bytes: usize,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_segment_count: Some(10000),
            max_bytes: 1 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub retain: Retention,
    pub roll: Rolling,
}

pub struct Partition {
    id: PartitionId,
    state: RwLock<State>,
    config: Config,
    manifest: Manifest,
}

pub struct State {
    last_roll: Instant,
    messages: Slog,
    commits: watch::Receiver<RecordIndex>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexedRecord {
    pub index: RecordIndex,
    pub data: Record,
}

fn merge_ranges<T: Ord>(a: Range<T>, b: Range<T>) -> Range<T> {
    std::cmp::min(a.start, b.start)..std::cmp::max(a.end, b.end)
}

impl Partition {
    pub(crate) async fn attach(
        root: PathBuf,
        manifest: Manifest,
        id: PartitionId,
        config: Config,
    ) -> Partition {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }
        let current = manifest.get_max_segment(&id).await;
        let record = OptionFuture::from(current.map(|ix| {
            manifest
                .get_segment_data(id.segment_id(ix))
                .map(|data| data.unwrap().records.end)
        }))
        .await
        .unwrap_or(RecordIndex(0));
        let segment = current.map(|s| s.next()).unwrap_or(SegmentIndex(0));
        let checkpoint = InternalIndex {
            segment,
            record: SegmentRecordIndex(0),
        };
        let (messages, mut writes) =
            Slog::attach(root.clone(), Partition::slog_name(&id), checkpoint, record);

        let (commit_writer, commits) = watch::channel(record);
        let commit_manifest = manifest.clone();
        let commit_id = id.clone();
        tokio::spawn(async move {
            while let Some(r) = writes.recv().await {
                trace!("{} checkpoint: {:?}", commit_id, &r);
                commit_manifest.update(&commit_id, &r.data).await;
                // ok if no receivers, that means nothing is awaiting a commit
                commit_writer.send(r.data.records.end).ok();
            }
        });

        let state = State {
            last_roll: Instant::now(),
            messages,
            commits,
        };

        Partition {
            manifest,
            state: RwLock::new(state),
            id,
            config,
        }
    }

    pub(crate) fn id(&self) -> &PartitionId {
        &self.id
    }

    fn slog_name(id: &PartitionId) -> String {
        format!("{}-{}", id.topic(), id.partition())
    }

    pub(crate) async fn append(&self, rs: &[Record]) -> Result<Range<RecordIndex>> {
        let mut state = self.state.write().await;
        let start = state.messages.next_record_ix().await;
        for r in rs {
            state.roll_when_needed(&self).await?;
            state.messages.append(r).await;
        }

        Ok(start..(start + rs.len()))
    }

    #[cfg(test)]
    pub(crate) async fn commit(&self) -> Result<()> {
        self.state.write().await.commit(&self).await
    }

    pub(crate) async fn checkpoint(&self) {
        self.state.write().await.messages.checkpoint().await;
    }

    async fn stream_with_active<'a>(
        &self,
        stored: impl Stream<Item = SegmentData> + 'a,
    ) -> impl Stream<Item = SegmentData> + 'a {
        let state = self.state.read().await;
        let cached = state.messages.cached_segment_data().await;
        let cached_segments: Vec<SegmentIndex> =
            cached.iter().map(|data| data.index.clone()).collect();
        // due to checkpoints, segment data for the active / pending segment may
        // already be stored in the manifest. we want to always only use
        // in-memory data as it is fresher.
        stored
            .take_while(move |data| future::ready(!cached_segments.contains(&data.index)))
            .chain(stream::iter(cached.into_iter()))
    }

    pub(crate) async fn get_records(&self, start: RecordIndex, limit: usize) -> Vec<IndexedRecord> {
        let state = self.state.read().await;
        let segments = self
            .stream_with_active(self.manifest.stream_segments(&self.id, start))
            .await;

        state
            .get_records_from_segments(limit, |r| r.index >= start, segments)
            .await
    }

    pub(crate) async fn get_records_by_time(
        &self,
        start: RecordIndex,
        times: RangeInclusive<DateTime<Utc>>,
        limit: usize,
    ) -> Vec<IndexedRecord> {
        let state = self.state.read().await;
        let segments = self
            .stream_with_active(self.manifest.stream_time_segments(&self.id, start, &times))
            .await;

        state
            .get_records_from_segments(
                limit,
                |r| r.index >= start && times.contains(&r.data.time),
                segments,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_record_by_index(&self, index: RecordIndex) -> Option<Record> {
        let records = self.get_records(index, 1).await;

        records
            .into_iter()
            .filter(|r| r.index == index)
            .next()
            .map(|r| r.data)
    }

    pub(crate) async fn readable_ids(&self) -> Option<Range<RecordIndex>> {
        let read = self.state.read().await;
        let stored = self.manifest.get_partition_range(&self.id).await;

        read.messages
            .cached_segment_data()
            .await
            .iter()
            .map(|data| data.records.clone())
            .chain(stored.into_iter())
            .reduce(|merged, range| merge_ranges(merged, range))
    }

    async fn over_retention_limit(&self) -> bool {
        let retain = &self.config.retain;

        let size = self.manifest.get_size(&self.id).await.unwrap_or(0);
        gauge!(
            "partition_size_bytes",
            size as f64,
            "topic" => String::from(self.id.topic()),
            "partition" => String::from(self.id.partition())
        );
        if size > retain.max_bytes {
            info!("over limit {}: current size is {}", self.id, size);
            return true;
        }

        // TODO validate
        if let Some(count) = retain.max_segment_count {
            let max = self
                .manifest
                .get_max_segment(&self.id)
                .await
                .unwrap_or(SegmentIndex(0));
            let min = self
                .manifest
                .get_min_segment(&self.id)
                .await
                .unwrap_or(SegmentIndex(0));

            let segments = max.0 - min.0 + 1;
            gauge!(
                "partition_size_segments",
                segments as f64,
                "topic" => String::from(self.id.topic()),
                "partition" => String::from(self.id.partition())
            );
            if segments > count {
                info!("over limit {}: {:?}..={:?}", self.id, min, max);
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    pub(crate) async fn compact(&self) {
        let mut state = self.state.write().await;
        state.retain(&self).await;
    }
}

impl State {
    async fn roll(&mut self, partition: &Partition) -> Result<()> {
        if let Some(_) = self.messages.active_segment_data().await {
            let pending = self.messages.pending_segment_data().await;
            self.messages.roll().await?;
            // without this wait, a rare race condition can occur:
            //
            // - partition.append(&rs).await;
            //   - self.messages.roll().await;
            // !!! partition.get_records(start, limit).await;
            // - commit_manifest.update(&commit_id, &r.data).await
            //
            // the call to .get_records() occurs after .roll() has replaced the
            // pending segment with the active one, but before the pending
            // metadata has been written to the manifest. wait_for_record
            // enforces the correct order:
            //
            // - partition.append()
            //   - self.messages.roll().await
            //   - commit_manifest.update(&commit_id, &r.data).await
            //   - self.wait_for_record(pending.records.end).await;
            // ✓ partition.get_records(start, limit)
            if let Some(pending) = pending {
                self.wait_for_record(pending.records.end).await;
            }
            self.last_roll = Instant::now();
            self.retain(partition).await;
        }

        Ok(())
    }

    async fn roll_when_needed(&mut self, partition: &Partition) -> Result<()> {
        if let Some(data) = self.messages.active_segment_data().await {
            let roll = &partition.config.roll;
            if let Some(d) = roll.max_segment_duration {
                let dt = Instant::now() - self.last_roll;
                if dt > d {
                    info!(
                        "rolling {}: last roll was {}s ago",
                        partition.id,
                        dt.as_secs()
                    );
                    return self.roll(partition).await;
                }
            }

            let current_len = data.records.end.0 - data.records.start.0;
            if current_len > roll.max_segment_index {
                info!("rolling {}: length is {}", partition.id, current_len);
                return self.roll(partition).await;
            }

            if data.size > roll.max_segment_size {
                info!("rolling {}: current size is {}", partition.id, data.size);
                return self.roll(partition).await;
            }
        }

        Ok(())
    }

    async fn get_records_from_segments<'a>(
        &'a self,
        limit: usize,
        filter: impl Fn(&IndexedRecord) -> bool,
        indices: impl Stream<Item = SegmentData> + 'a,
    ) -> Vec<IndexedRecord> {
        // record queries can be thought of as filtering the entire record set
        // across all segments. doing so via simply reading everything would be
        // wasteful in many cases. this is why we require a lazy stream of
        // `indices` of all segments that could possibly have said data. the
        // manifest tracks this information in a performant fashion.
        let records = indices
            .flat_map(move |segment| {
                // once we have the segments, we need to actually read the data
                // within. we also need to track record indices for subscriber
                // pagination, so compute those while we have the relevant
                // ranges easily accessible in `SegmentData`.
                self.messages
                    .iter_segment(segment.index)
                    .into_stream()
                    .flat_map(|batches| stream::iter(batches))
                    .flat_map(|rs| stream::iter(rs))
                    .enumerate()
                    .map(move |(index, data)| IndexedRecord {
                        index: RecordIndex(segment.records.start.0 + index),
                        data,
                    })
            })
            .filter_map(|indexed| {
                // now, winnow down to the data we care about. as we go, track the first
                // and final index of the records we're collating together.
                if filter(&indexed) {
                    future::ready(Some(indexed))
                } else {
                    future::ready(None)
                }
            })
            .take(limit)
            .collect()
            .await;

        records
    }

    async fn retain(&mut self, partition: &Partition) {
        while partition.over_retention_limit().await {
            if let Some(ix) = partition.manifest.get_min_segment(&partition.id).await {
                // TODO ensure we handle failure if this call
                partition
                    .manifest
                    .remove_segment(partition.id.segment_id(ix))
                    .await;
                // succeeds but this does not complete e.g. due to node failure
                self.messages.destroy(ix).expect("segment destroyed");
                info!("retain {}: destroyed {:?}", partition.id, ix);
                counter!(
                    "partition_segments_destroyed",
                    1,
                    "topic" => String::from(partition.id.topic()),
                    "partition" => String::from(partition.id.partition())
                )
            }
        }
    }

    async fn wait_for_record(&mut self, target: RecordIndex) {
        trace!("waiting for commit including {:?}", target);
        self.commits.changed().await.expect("commit watcher ended");
        while *self.commits.borrow() < target {
            self.commits.changed().await.expect("commit watcher ended");
        }
    }

    #[cfg(test)]
    pub(crate) async fn commit(&mut self, partition: &Partition) -> Result<()> {
        let target = self.messages.next_record_ix().await;
        self.roll(partition).await?;
        self.wait_for_record(target).await;
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use std::convert::TryInto;
    use tempfile::tempdir;

    pub fn deindex(rs: Vec<IndexedRecord>) -> Vec<Record> {
        rs.into_iter().map(|r| r.data).collect()
    }

    #[tokio::test]
    async fn test_append_get() -> Result<()> {
        let id = PartitionId::new("topic", "testing");
        let dir = tempdir()?;
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(root, manifest, id, Config::default()).await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(&vec![record.clone()]).await?;
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone()),
                "mismatch at {}",
                ix
            );
        }
        assert_eq!(
            t.get_record_by_index(RecordIndex(records.len())).await,
            None
        );

        Ok(())
    }

    async fn test_rolling_get(commit: bool) -> Result<()> {
        let id = PartitionId::new("topic", "testing-roll");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(
            root,
            manifest,
            id,
            Config {
                roll: Rolling {
                    max_segment_index: 2,
                    ..Rolling::default()
                },
                ..Config::default()
            },
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(&vec![record.clone()]).await?;
        }

        if commit {
            t.commit().await?;
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(RecordIndex(records.len())).await,
            None
        );

        for (start, limit) in (0..records.len()).zip(1..records.len()) {
            let range = start..std::cmp::min(start + limit, records.len());
            let slice = Vec::from(&records[range.clone()]);
            assert_eq!(
                deindex(t.get_records(RecordIndex(start), limit).await),
                slice
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_rolling_get_committed() -> Result<()> {
        test_rolling_get(true).await
    }

    #[tokio::test]
    async fn test_rolling_get_cached() -> Result<()> {
        test_rolling_get(false).await
    }

    #[tokio::test]
    async fn test_durability() -> Result<()> {
        let id = PartitionId::new("topic", "testing-roll");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();
        {
            let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
            let t = Partition::attach(
                root.clone(),
                manifest,
                id.clone(),
                Config {
                    roll: Rolling {
                        max_segment_index: 2,
                        ..Rolling::default()
                    },
                    ..Config::default()
                },
            )
            .await;

            for record in records.iter() {
                t.append(&vec![record.clone()]).await?;
            }
            t.commit().await?;
        }

        {
            let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
            let t = Partition::attach(
                root,
                manifest,
                id,
                Config {
                    roll: Rolling {
                        max_segment_index: 2,
                        ..Rolling::default()
                    },
                    ..Config::default()
                },
            )
            .await;

            for (ix, record) in records.iter().enumerate() {
                assert_eq!(
                    t.get_record_by_index(RecordIndex(ix)).await,
                    Some(record.clone()),
                    "record {}",
                    ix
                );
            }
            assert_eq!(
                t.get_record_by_index(RecordIndex(records.len())).await,
                None
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_after_compaction() -> Result<()> {
        let id = PartitionId::new("topic", "testing-roll");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(
            root,
            manifest,
            id,
            Config {
                roll: Rolling {
                    max_segment_index: 2,
                    ..Rolling::default()
                },
                retain: Retention {
                    max_segment_count: Some(1),
                    ..Retention::default()
                },
                ..Config::default()
            },
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p", "q", "r"]
            .into_iter()
            .enumerate()
            .map(|(ix, message)| Record {
                time: Utc.timestamp(ix.try_into().unwrap(), 0),
                message: ByteArray::from(message),
            })
            .collect();

        for record in records[0..6].iter() {
            t.append(&vec![record.clone()]).await?;
        }
        t.commit().await?;

        // this compaction will destroy the first segment
        t.compact().await;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.append(&vec![record.clone()]).await?;
        }

        // fetch from start fast-forwards to first valid index
        let rs = t.get_records(RecordIndex(0), 2).await;
        let start_index = rs.iter().next().unwrap().index.clone();
        assert_eq!(deindex(rs), vec![records[3].clone(), records[4].clone()]);

        // iterate through from start
        let rs = t.get_records(start_index, 2).await;
        let next_index = rs.iter().next_back().unwrap().index.clone() + 1;
        assert_eq!(deindex(rs), vec![records[3].clone(), records[4].clone()]);

        let rs = t.get_records(next_index, 2).await;
        let next_index = rs.iter().next_back().unwrap().index.clone() + 1;
        assert_eq!(deindex(rs), vec![records[5].clone(), records[6].clone()]);

        let rs = t.get_records(next_index, 2).await;
        let next_index = rs.iter().next_back().unwrap().index.clone() + 1;
        assert_eq!(deindex(rs), vec![records[7].clone()]);

        let rs = t.get_records(next_index, 2).await;
        assert_eq!(deindex(rs), vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn test_unordered_time() -> Result<()> {
        let id = PartitionId::new("topic", "testing-time");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(
            root,
            manifest,
            id,
            Config {
                roll: Rolling {
                    max_segment_index: 2,
                    ..Rolling::default()
                },
                ..Config::default()
            },
        )
        .await;

        let messages = vec!["abc", "def", "ghi", "jkl", "mno", "p", "q", "r"];
        let times = vec![5, 6, 0, 10, 4, 3, 12, 2];
        let records: Vec<_> = messages
            .iter()
            .zip(times.iter())
            .map(|(message, ix)| Record {
                time: Utc.timestamp(*ix, 0),
                message: ByteArray::from(*message),
            })
            .collect();

        for record in records[0..6].iter() {
            t.append(&vec![record.clone()]).await?;
        }
        t.commit().await?;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.append(&vec![record.clone()]).await?;
        }

        let min_time = times.iter().cloned().min().unwrap();
        let max_time = times.iter().cloned().max().unwrap();
        let time_range = min_time..=max_time;
        for (start, limit) in (0..records.len()).zip(0..records.len()) {
            let slice = Vec::from(&records[start..records.len()]);
            for query_start in time_range.clone() {
                for query_end in query_start..=max_time {
                    let query = Utc.timestamp(query_start, 0)..=Utc.timestamp(query_end, 0);
                    let values = slice
                        .iter()
                        .enumerate()
                        .filter(|(_, r)| query.contains(&r.time))
                        .take(limit);
                    let time_slice: Vec<_> = values.clone().map(|(_, r)| r.clone()).collect();
                    assert_eq!(
                        deindex(
                            t.get_records_by_time(RecordIndex(start), query, limit)
                                .await
                        ),
                        time_slice
                    );
                }
            }
        }

        Ok(())
    }
}

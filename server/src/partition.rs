//! A `Partition` is responsible for reading the commit stream from a `Slog` and
//! ensuring the associated finalized `SegmentData` is persisted in the
//! `Manifest`.
//!
//! There can be many `Partition`s in a `Topic`. This unlocks the write parallelism
//! advantages inherent in having many concurrent `Slog` writer threads.
//!
//! The `Partition` is also responsible for assigning sequential unique
//! identifiers to each incoming row of each chunk. A `(topic, partition,
//! row)` tuple is a globally unique identifier for each durably stored
//! row.
//!
//! To ensure durability, `.commit()` must be called after a given `.extend()`.
//! Otherwise, the chunk will only be durably stored on the next `.roll()`.
//!
//! A `Partition` also has `Rolling` and `Retention` policies that are used to
//! determine when to roll segments and expire old data. `Rolling` policies are
//! evaluated on every insert, and `Retention` policies are enforced on every
//! `roll`.

use std::fs;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::arrow2::array::BooleanArray;
#[cfg(test)]
use crate::arrow2::compute::comparison::eq_scalar;
use crate::arrow2::compute::comparison::gt_eq_scalar;
use crate::arrow2::scalar::PrimitiveScalar;
use crate::chunk::{parse_time, IndexedChunk, Schema};
use crate::limit::{LimitedBatch, Retention, Rolling, RowLimit};
use crate::manifest::{Manifest, Ordering};
pub use crate::manifest::{PartitionId, Scope, SegmentData};
#[cfg(test)]
pub use crate::segment::Record;
pub use crate::slog::RecordIndex;
use crate::slog::{self, SegmentIndex, Slog};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, Stream, StreamExt};
use futures::FutureExt;
use futures::{future, stream};
use metrics::{counter, gauge};
use plateau_transport::arrow2::compute::comparison::lt_scalar;
use plateau_transport::SchemaChunk;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, trace, warn};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub retain: Retention,
    pub roll: Rolling,
    pub slog: slog::Config,
}

#[must_use = "close() explicitly to flush writes"]
pub struct Partition {
    id: PartitionId,
    state: RwLock<State>,
    config: Config,
    manifest: Manifest,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition").field("id", &self.id).finish()
    }
}

pub struct State {
    last_roll: Instant,
    messages: Slog,
    commits: watch::Receiver<RecordIndex>,
    fin: oneshot::Receiver<()>,
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

        let slog_name = Self::slog_name(&id);
        let (segment, record) =
            Self::find_starting_index(&id, root.as_path(), &slog_name, &manifest).await;

        let (messages, mut writes) = Slog::attach(
            root.clone(),
            slog_name,
            segment,
            record,
            config.slog.clone(),
        );

        let (fin_tx, fin_rx) = oneshot::channel();

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
            trace!("{} exit", commit_id);
            fin_tx.send(()).ok();
        });

        let state = State {
            last_roll: Instant::now(),
            messages,
            commits,
            fin: fin_rx,
        };

        Partition {
            manifest,
            state: RwLock::new(state),
            id,
            config,
        }
    }

    async fn find_starting_index(
        id: &PartitionId,
        root: &Path,
        slog_name: &str,
        manifest: &Manifest,
    ) -> (SegmentIndex, RecordIndex) {
        let mut current = manifest.get_max_segment(id).await;
        let mut index = None;

        // rewind until we find a starting index based upon a good segment
        while index.is_none() {
            if let Some(ix) = current {
                let segment = Slog::segment_from_name(root, slog_name, ix);
                let data = manifest.get_segment_data(ix.to_id(id)).await;
                let valid = segment.validate();
                index = match data {
                    Some(data) if valid => Some((ix.next(), data.records.end)),
                    _ => {
                        error!("bad {:?} file: {} catalog: {}", ix, valid, data.is_some());
                        if let Err(e) = segment.destroy() {
                            error!("error destroying {:?}: {}", ix, e);
                        }
                        manifest.remove_segment(ix.to_id(id)).await;
                        current = ix.prev();
                        None
                    }
                }
            } else {
                break;
            }
        }

        index.unwrap_or((SegmentIndex(0), RecordIndex(0)))
    }

    #[allow(dead_code)]
    pub(crate) fn id(&self) -> &PartitionId {
        &self.id
    }

    fn slog_name(id: &PartitionId) -> String {
        format!("{}-{}", id.topic(), id.partition())
    }

    #[cfg(test)]
    pub(crate) async fn extend_records(&self, rs: &[Record]) -> Result<Range<RecordIndex>> {
        use crate::chunk::LegacyRecords;

        self.extend(SchemaChunk::try_from(LegacyRecords(rs.to_vec())).unwrap())
            .await
    }

    pub(crate) async fn extend(&self, chunk: SchemaChunk<Schema>) -> Result<Range<RecordIndex>> {
        let size = chunk.len();
        let mut state = self.state.write().await;
        let start = state.messages.next_record_ix().await;
        state.roll_when_needed(self, &chunk.schema).await?;
        state.messages.append(chunk).await;

        Ok(start..(start + size))
    }

    #[cfg(any(test, bench))]
    pub(crate) async fn commit(&self) -> Result<()> {
        self.state.write().await.commit(self).await
    }

    pub(crate) async fn checkpoint(&self) {
        self.state.write().await.messages.checkpoint().await;
    }

    async fn stream_with_active<'a>(
        &'a self,
        stored: impl Stream<Item = SegmentData> + 'a + Send,
        state: &RwLockReadGuard<'a, State>,
        order: &Ordering,
    ) -> BoxStream<'a, SegmentData> {
        let mut cached = state.messages.cached_segment_data().await;
        if order.is_reverse() {
            cached.reverse();
        }
        let cached_segments: Vec<SegmentIndex> = cached.iter().map(|data| data.index).collect();
        // due to checkpoints, segment data for the active / pending segment may
        // already be stored in the manifest. we want to always only use
        // in-memory data as it is fresher.
        match order {
            Ordering::Forward => stored
                .filter(move |data| future::ready(!cached_segments.contains(&data.index)))
                .chain(stream::iter(cached.into_iter()))
                .boxed(),
            Ordering::Reverse => stream::iter(cached.into_iter())
                .chain(
                    stored
                        .filter(move |data| future::ready(!cached_segments.contains(&data.index))),
                )
                .boxed(),
        }
    }

    pub(crate) async fn get_records(
        &self,
        start: RecordIndex,
        limit: RowLimit,
        order: &Ordering,
    ) -> LimitedBatch {
        let state = self.state.read().await;
        let segments = self
            .stream_with_active(
                self.manifest.stream_segments(&self.id, start, order),
                &state,
                order,
            )
            .await;

        let filter = |chunk: &IndexedChunk| {
            // TODO: ideally we'd be using slicing here instead; it's more
            // efficient. This is a straightforward port of the original code.
            let i = chunk.indices();
            let s = &PrimitiveScalar::from(Some(start.0 as i32));
            if order.is_reverse() {
                lt_scalar(i, s)
            } else {
                gt_eq_scalar(i, s)
            }
        };

        state
            .get_records_from_segments(limit, filter, segments, order)
            .await
    }

    pub(crate) async fn get_records_by_time(
        &self,
        start: RecordIndex,
        times: RangeInclusive<DateTime<Utc>>,
        limit: RowLimit,
    ) -> LimitedBatch {
        let state = self.state.read().await;
        let segments = self
            .stream_with_active(
                self.manifest.stream_time_segments(&self.id, start, &times),
                &state,
                &Ordering::Forward,
            )
            .await;

        let filter = |indexed: &IndexedChunk| {
            BooleanArray::from_trusted_len_values_iter(
                indexed
                    .indices()
                    .into_iter()
                    .zip(indexed.times().into_iter())
                    .map(|(index, time)| {
                        *index.unwrap() >= (start.0 as i32)
                            && times.contains(&parse_time(*time.unwrap()))
                    }),
            )
        };

        state
            .get_records_from_segments(limit, filter, segments, &Ordering::Forward)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_record_by_index(&self, index: RecordIndex) -> Option<Record> {
        let record_response = self
            .get_records(index, RowLimit::records(1), &Ordering::Forward)
            .await;

        record_response
            .chunks
            .into_iter()
            .map(|indexed| {
                indexed
                    .filter(&eq_scalar(
                        indexed.indices(),
                        &PrimitiveScalar::from(Some(index.0 as i32)),
                    ))
                    .unwrap()
            })
            .flat_map(|indexed| indexed.into_legacy(&Ordering::Forward))
            .next()
    }

    pub(crate) async fn readable_ids(&self) -> Option<Range<RecordIndex>> {
        let read = self.state.read().await;
        let stored = self.manifest.get_partition_range(&self.id).await;
        debug!("stored: {:?}", stored);

        read.messages
            .cached_segment_data()
            .await
            .iter()
            .map(|data| data.records.clone())
            .chain(stored.into_iter())
            .reduce(merge_ranges)
    }

    pub(crate) async fn byte_size(&self) -> usize {
        self.manifest.get_size(Scope::Partition(&self.id)).await
    }

    async fn over_retention_limit(&self) -> bool {
        let retain = &self.config.retain;

        let size = self.byte_size().await;
        gauge!(
            "partition_size_bytes",
            "topic" => String::from(self.id.topic()),
            "partition" => String::from(self.id.partition())
        )
        .set(size as f64);
        if size > (retain.max_bytes.as_u64() as usize) {
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
                "topic" => String::from(self.id.topic()),
                "partition" => String::from(self.id.partition())
            )
            .set(segments as f64);
            if segments > count {
                info!("over limit {}: {:?}..={:?}", self.id, min, max);
                return true;
            }
        }

        false
    }

    pub(crate) async fn remove_oldest(&self) {
        let state = self.state.read().await;
        state.remove_oldest(self).await;
    }

    pub(crate) async fn close(self) {
        self.state.into_inner().close().await;
    }

    #[cfg(test)]
    pub(crate) async fn compact(&self) {
        let mut state = self.state.write().await;
        state.commit(self).await.expect("commit failed");
        state.retain(self).await;
    }
}

impl State {
    async fn roll(&mut self, partition: &Partition) -> Result<()> {
        if self.messages.active_segment_data().await.is_some() {
            let pending = self.messages.pending_segment_data().await;
            self.messages.roll().await?;
            // without this wait, a rare race condition can occur:
            //
            // - partition.append(&rs).await;
            //   - self.messages.roll().await;
            // !!! partition.get_records(start, limit).await;
            // - commit_manifest.update(&commit_id, &r.data).await
            //
            // the core issue is that commit_manifest.update(..) happens in a
            // separate thread at some indeterminate time after this .roll() has
            // released the state write lock and returned
            //
            // as a result, the call to .get_records() occurs after .roll() has
            // replaced the pending segment with the active one, but before the
            // pending metadata has been written to the manifest.
            //
            // a deadlock ensues.
            //
            // wait_for_record waits for the manifest update to happen, and thus
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

    async fn roll_when_needed(&mut self, partition: &Partition, new_schema: &Schema) -> Result<()> {
        if !self.messages.active_schema_matches(new_schema).await {
            let partition_id = format!("{}", partition.id);
            counter!(
                "partition_schema_change",
                "partition" => partition_id,
            )
            .increment(1);

            if *self.commits.borrow() != RecordIndex(0) {
                warn!(
                    "{}: schema change after {:?}",
                    partition.id,
                    *self.commits.borrow()
                );
            }

            return self.roll(partition).await;
        }

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

            if data.size > (roll.max_segment_size.as_u64() as usize) {
                info!("rolling {}: current size is {}", partition.id, data.size);
                return self.roll(partition).await;
            }
        }

        Ok(())
    }

    async fn get_records_from_segments<'a>(
        &'a self,
        limit: RowLimit,
        filter: impl Fn(&IndexedChunk) -> BooleanArray + Send + Sync,
        indices: impl Stream<Item = SegmentData> + 'a + Send + StreamExt,
        order: &Ordering,
    ) -> LimitedBatch {
        // record queries can be thought of as filtering the entire record set
        // across all segments. doing so via simply reading everything would be
        // wasteful in many cases. this is why we require a lazy stream of
        // `indices` of all segments that could possibly have said data. the
        // manifest tracks this information in a performant fashion.
        let mut batch = LimitedBatch::open(limit);

        // Feb 2022: this box / pin is necessary due to a bug in the rust
        // compiler. it has difficulty matching lifetimes across async fns and
        // closures. aggravated by using streams. issue with linked workaround:
        //
        // - https://github.com/rust-lang/rust/issues/64552#issuecomment-669728225
        //
        // there's some noise about better support for async closures in rust; the
        // situation may resolve itself in the future
        let stream: std::pin::Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(
            indices
                .flat_map(move |segment| {
                    // once we have the segments, we need to actually read the data
                    // within. we also need to track record indices for subscriber
                    // pagination, so compute those while we have the relevant
                    // ranges easily accessible in `SegmentData`.
                    self.messages
                        .iter_segment(segment.index, order)
                        .into_stream()
                        .flat_map(stream::iter)
                        .scan(
                            if order.is_reverse() {
                                segment.records.end.0
                            } else {
                                segment.records.start.0
                            },
                            move |start: &mut usize, mut data: SchemaChunk<Schema>| {
                                let index = RecordIndex(*start);
                                if order.is_reverse() {
                                    data.reverse_inner();
                                    *start -= data.chunk.len();
                                } else {
                                    *start += data.chunk.len();
                                }
                                future::ready(Some(IndexedChunk::from_start(index, data, order)))
                            },
                        )
                })
                .map(|indexed| {
                    // now, winnow down to the data we care about.
                    indexed.filter(&filter(&indexed)).unwrap()
                })
                .scan(&mut batch, move |batch, indexed| {
                    batch.extend_one(indexed);
                    future::ready(if batch.status.is_open() {
                        Some(true)
                    } else {
                        None
                    })
                }),
        );

        // `scan` does the heavy lifting here, so we just need to drive the
        // stream to completion. this might not be necessary if it's possible
        // for `LimitedBatch` to gain `impl Sink`
        stream.count().await;

        batch
    }

    async fn retain(&self, partition: &Partition) {
        while partition.over_retention_limit().await {
            self.remove_oldest(partition).await;
        }
    }

    async fn remove_oldest(&self, partition: &Partition) {
        if let Some(ix) = partition.manifest.get_min_segment(&partition.id).await {
            // TODO ensure we handle failure if this call
            partition
                .manifest
                .remove_segment(ix.to_id(&partition.id))
                .await;
            // succeeds but this does not complete e.g. due to node failure
            self.messages.destroy(ix).expect("segment destroyed");
            info!("retain {}: destroyed {:?}", partition.id, ix);
            counter!(
                "partition_segments_destroyed",
                "topic" => String::from(partition.id.topic()),
                "partition" => String::from(partition.id.partition())
            )
            .increment(1);
        }
    }

    async fn wait_for_record(&mut self, target: RecordIndex) {
        trace!("waiting for commit including {:?}", target);
        while *self.commits.borrow() < target {
            self.commits.changed().await.expect("commit watcher ended");
        }
        trace!("notified of commit including {:?}", target);
    }

    #[cfg(any(test, bench))]
    pub(crate) async fn commit(&mut self, partition: &Partition) -> Result<()> {
        let target = self.messages.next_record_ix().await;
        self.roll(partition).await?;
        self.wait_for_record(target).await;
        Ok(())
    }

    pub(crate) async fn close(self) {
        self.messages.close().await;
        if let Err(e) = self.fin.await {
            error!("error closing: {:?}", e)
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::chunk::test::{inferences_nested, inferences_schema_a, inferences_schema_b};
    use crate::chunk::{legacy_schema, LegacyRecords};
    use crate::limit::BatchStatus;
    use crate::segment::test::build_records;
    use chrono::{TimeZone, Utc};
    use plateau_transport::SegmentChunk;
    use std::convert::TryInto;
    use tempfile::{tempdir, TempDir};

    pub(crate) fn deindex(is: Vec<IndexedChunk>) -> Vec<Record> {
        is.into_iter()
            .flat_map(|i| i.into_legacy(&Ordering::Forward))
            .collect()
    }

    pub async fn partition(config: Config) -> Result<(TempDir, Partition)> {
        let id = PartitionId::new("topic", "testing");
        let dir = tempdir()?;
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        Ok((dir, Partition::attach(root, manifest, id, config).await))
    }

    /// Insert N messages to the given partition
    pub async fn init_records(partition: &Partition, n: usize) -> Result<()> {
        let records: Vec<_> = (0..n)
            .map(|i| Record {
                time: Utc::now(),
                message: format!("m{i}").into_bytes(),
            })
            .collect();

        for record in records.iter() {
            partition.extend_records(&[record.clone()]).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_head_get() -> Result<()> {
        let (_, partition) = partition(Config::default()).await.unwrap();
        init_records(&partition, 10).await?;

        let start = RecordIndex(0);
        let result = partition
            .get_records(
                start,
                RowLimit {
                    max_records: 3,
                    max_bytes: 100_000,
                },
                &Ordering::Forward,
            )
            .await
            .chunks
            .iter()
            .map(|c| c.display_vec())
            .flatten()
            .collect::<Vec<String>>();

        assert_eq!(["0: m0", "1: m1", "2: m2"], result.as_slice());

        partition.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_tail_get() -> Result<()> {
        let (_, partition) = partition(Config::default()).await.unwrap();
        init_records(&partition, 10).await?;

        let result = partition
            .get_records(
                RecordIndex(7),
                RowLimit {
                    max_records: 3,
                    max_bytes: 100_000,
                },
                &Ordering::Forward,
            )
            .await
            .chunks
            .iter()
            .map(|c| c.display_vec())
            .flatten()
            .collect::<Vec<String>>();

        assert_eq!(["7: m7", "8: m8", "9: m9"], result.as_slice());

        partition.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_head_get_rev() -> Result<()> {
        let (_, partition) = partition(Config::default()).await.unwrap();
        init_records(&partition, 10).await?;

        let result = partition
            .get_records(
                RecordIndex(3),
                RowLimit {
                    max_records: 3,
                    max_bytes: 100_000,
                },
                &Ordering::Reverse,
            )
            .await
            .chunks
            .iter()
            .map(|c| c.display_vec())
            .flatten()
            .collect::<Vec<String>>();

        assert_eq!(["2: m2", "1: m1", "0: m0"], result.as_slice());

        partition.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_tail_get_rev() -> Result<()> {
        let (_, partition) = partition(Config::default()).await.unwrap();
        init_records(&partition, 10).await?;

        let result = partition
            .get_records(
                RecordIndex(10),
                RowLimit {
                    max_records: 3,
                    max_bytes: 100_000,
                },
                &Ordering::Reverse,
            )
            .await
            .chunks
            .iter()
            .map(|c| c.display_vec())
            .flatten()
            .collect::<Vec<String>>();

        assert_eq!(["9: m9", "8: m8", "7: m7"], result.as_slice());

        partition.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_multiple_segments_rev() -> Result<()> {
        // create first segment and shut down
        let (dir, partition) = partition(Config::default()).await.unwrap();
        init_records(&partition, 5).await?;
        partition.commit().await?;
        drop(partition);
        let id = PartitionId::new("topic", "testing");

        // re-init and create second segment
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let partition = Partition::attach(root, manifest, id, Config::default()).await;
        init_records(&partition, 5).await?;

        let result = partition
            .get_records(
                RecordIndex(10),
                RowLimit {
                    max_records: 1000,
                    max_bytes: 100_000,
                },
                &Ordering::Reverse,
            )
            .await;

        let r = result
            .chunks
            .iter()
            .map(|c| c.display_vec())
            .flatten()
            .collect::<Vec<String>>();

        assert_eq!(10, r.len());
        assert_eq!(["9: m4", "8: m3", "7: m2", "6: m1", "5: m0"], r[0..5]);
        assert_eq!(["4: m4", "3: m3", "2: m2", "1: m1", "0: m0"], r[5..10]);

        partition.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_append_get() -> Result<()> {
        let id = PartitionId::new("topic", "testing");
        let dir = tempdir()?;
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let part = Partition::attach(root, manifest, id, Config::default()).await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for record in records.iter() {
            part.extend_records(&[record.clone()]).await?;
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                part.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone()),
                "mismatch at {}",
                ix
            );
        }
        assert_eq!(
            part.get_record_by_index(RecordIndex(records.len())).await,
            None
        );

        part.close().await;

        Ok(())
    }

    fn segment_3s() -> Config {
        Config {
            roll: Rolling {
                max_segment_index: 2,
                ..Rolling::default()
            },
            ..Config::default()
        }
    }

    async fn test_rolling_get(commit: bool) -> Result<()> {
        let (_dir, part) = partition(segment_3s()).await?;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for record in records.iter() {
            part.extend_records(&[record.clone()]).await?;
        }

        if commit {
            part.commit().await?;
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                part.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            part.get_record_by_index(RecordIndex(records.len())).await,
            None
        );

        for (start, limit) in (0..records.len()).zip(1..records.len()) {
            let range = start..std::cmp::min(start + limit, records.len());
            let slice = Vec::from(&records[range.clone()]);
            assert_eq!(
                deindex(
                    part.get_records(
                        RecordIndex(start),
                        RowLimit::records(limit),
                        &Ordering::Forward
                    )
                    .await
                    .chunks
                ),
                slice
            );
        }

        part.close().await;
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
    async fn test_schema_filtering() -> Result<()> {
        let (_dir, part) = partition(Config::default()).await?;
        let chunk_a = inferences_schema_a();

        part.extend(chunk_a.clone()).await?;

        let result = part
            .get_records_by_time(
                RecordIndex(0),
                parse_time(2)..=parse_time(3),
                RowLimit::default(),
            )
            .await;

        let mut chunk_slice = IndexedChunk::from_start(RecordIndex(0), chunk_a, &Ordering::Forward);
        chunk_slice.slice(2, 2);
        assert_eq!(result.chunks, vec![chunk_slice]);

        Ok(())
    }

    type PartitionSpec = (Manifest, PartitionId, Config);
    fn to_spec(p: Partition) -> PartitionSpec {
        (p.manifest, p.id, p.config)
    }

    async fn reattach(dir: &TempDir, p: PartitionSpec) -> Partition {
        let root = PathBuf::from(dir.path());
        Partition::attach(root, p.0, p.1, p.2).await
    }

    async fn test_durability(config: Config) -> Result<()> {
        let records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: format!("record-{}", ix).into_bytes(),
            })
            .collect();

        let (dir, part) = partition(config).await?;
        let spec = {
            for record in records.iter() {
                part.extend_records(&[record.clone()]).await?;
            }
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;

            for (ix, record) in records.iter().enumerate() {
                assert_eq!(
                    part.get_record_by_index(RecordIndex(ix)).await,
                    Some(record.clone()),
                    "record {}",
                    ix
                );
            }
            assert_eq!(
                part.get_record_by_index(RecordIndex(records.len())).await,
                None
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_durability_3s() -> Result<()> {
        test_durability(segment_3s()).await
    }

    fn batch_limit(write_queue_batch_limit: usize) -> Config {
        Config {
            slog: slog::Config {
                min_full_chunk_len: write_queue_batch_limit,
                ..Default::default()
            },
            ..segment_3s()
        }
    }

    #[tokio::test]
    async fn test_durability_3s_single_batch() -> Result<()> {
        test_durability(batch_limit(1)).await
    }

    #[tokio::test]
    async fn test_durability_3s_double_batch() -> Result<()> {
        test_durability(batch_limit(2)).await
    }

    #[tokio::test]
    async fn test_arrow_durability() -> Result<()> {
        let chunk_a = inferences_schema_a();

        let (dir, part) = partition(segment_3s()).await?;
        let spec = {
            part.extend(chunk_a.clone()).await?;
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;
            let result = part
                .get_records(RecordIndex(0), RowLimit::default(), &Ordering::Forward)
                .await;

            assert_eq!(SegmentChunk::from(result.chunks[0].clone()), chunk_a.chunk);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_nested() -> Result<()> {
        let (dir, part) = partition(Config::default()).await?;
        let nested = inferences_nested();
        let spec = {
            part.extend(nested.clone()).await?;
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;

            let start = RecordIndex(0);
            let result = part
                .get_records(start, RowLimit::default(), &Ordering::Forward)
                .await;
            assert_eq!(
                format!("{:?}", SegmentChunk::from(result.chunks[0].clone())),
                format!("{:?}", nested.chunk)
            );
        }

        Ok(())
    }

    async fn _test_schema_change() -> Result<()> {
        let records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: format!("record-{}", ix).into_bytes(),
            })
            .collect();

        let chunk_a = inferences_schema_a();
        let chunk_b = inferences_schema_b();

        let (dir, part) = partition(Config::default()).await?;
        let spec = {
            part.extend_records(&records).await?;
            part.extend(chunk_a.clone()).await?;
            part.extend(chunk_a.clone()).await?;
            part.extend(chunk_b.clone()).await?;
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;
            let order = &Ordering::Forward;

            let start = RecordIndex(0);
            let result = part.get_records(start, RowLimit::default(), order).await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(
                result.chunks,
                vec![IndexedChunk::from_start(
                    start,
                    SchemaChunk::try_from(LegacyRecords(records))?,
                    order
                )]
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default(), order).await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(
                result.chunks,
                vec![IndexedChunk::from_start(
                    start,
                    SchemaChunk {
                        schema: chunk_a.schema.clone(),
                        chunk: crate::chunk::concatenate(&[
                            chunk_a.chunk.clone(),
                            chunk_a.chunk.clone()
                        ])?
                    },
                    order
                )],
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default(), order).await;
            assert!(result.status.is_open());
            assert_eq!(
                result.chunks,
                vec![IndexedChunk::from_start(start, chunk_b.clone(), order),]
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default(), order).await;
            assert!(result.status.is_open());
            assert_eq!(result.chunks, vec![]);
        }

        Ok(())
    }

    // NOTE: multiple copies of this test to expose and reliably reproduce an
    // underlying concurrency bug
    #[tokio::test]
    async fn test_schema_change1() -> Result<()> {
        _test_schema_change().await
    }

    #[tokio::test]
    async fn test_schema_change2() -> Result<()> {
        _test_schema_change().await
    }

    #[tokio::test]
    async fn test_schema_change3() -> Result<()> {
        _test_schema_change().await
    }

    #[tokio::test]
    async fn test_schema_change4() -> Result<()> {
        _test_schema_change().await
    }

    #[tokio::test]
    async fn test_chunk_indexing() -> Result<()> {
        let (_dir, part) = partition(Config::default()).await?;
        let records = (0..6)
            .into_iter()
            .map(|i| Record {
                time: Utc::now(),
                message: format!("m{i}").into_bytes(),
            })
            .collect::<Vec<_>>();

        // create two small segments
        part.extend_records(&records[0..3]).await.unwrap();
        part.commit().await.unwrap();
        part.extend_records(&records[3..6]).await.unwrap();
        part.commit().await.unwrap();

        let result = part
            .get_records(RecordIndex(0), RowLimit::default(), &Ordering::Forward)
            .await;
        let msgs = result
            .chunks
            .iter()
            .flat_map(|c| c.display_vec())
            .collect::<Vec<_>>();

        assert_eq!(
            ["0: m0", "1: m1", "2: m2", "3: m3", "4: m4", "5: m5"],
            msgs.as_slice()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_indexing_rev() -> Result<()> {
        let (_dir, part) = partition(Config::default()).await?;
        let records = (0..6)
            .into_iter()
            .map(|i| Record {
                time: Utc::now(),
                message: format!("m{i}").into_bytes(),
            })
            .collect::<Vec<_>>();

        // create two small segments
        part.extend_records(&records[0..3]).await.unwrap();
        part.commit().await.unwrap();
        part.extend_records(&records[3..6]).await.unwrap();
        part.commit().await.unwrap();

        let result = part
            .get_records(RecordIndex(6), RowLimit::default(), &Ordering::Reverse)
            .await;
        let msgs = result
            .chunks
            .iter()
            .flat_map(|c| c.display_vec())
            .collect::<Vec<_>>();

        assert_eq!(
            ["5: m5", "4: m4", "3: m3", "2: m2", "1: m1", "0: m0"],
            msgs.as_slice()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_change_rev() -> Result<()> {
        let records: Vec<_> = (0..30)
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: format!("record-{}", ix).into_bytes(),
            })
            .collect();

        let chunk_a = inferences_schema_a();
        let chunk_b = inferences_schema_b();

        let (dir, part) = partition(Config::default()).await?;
        let spec = {
            part.extend_records(&records).await?;
            part.extend(chunk_a.clone()).await?;
            part.extend(chunk_a.clone()).await?;
            part.extend(chunk_b.clone()).await?;
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;

            let part_len = part.readable_ids().await.unwrap().end.0;
            assert_eq!(45, part_len);

            // iterate schema A
            let start = RecordIndex(part_len);
            let result = part
                .get_records(start, RowLimit::default(), &Ordering::Reverse)
                .await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(result.schema.unwrap(), chunk_b.schema);
            assert_eq!(
                result.chunks.iter().map(|c| c.chunk.len()).sum::<usize>(),
                5
            );
            assert_eq!(
                result
                    .chunks
                    .first()
                    .and_then(|indexed| indexed.start())
                    .unwrap(),
                RecordIndex(44)
            );
            assert_eq!(
                result
                    .chunks
                    .last()
                    .and_then(|indexed| indexed.end())
                    .unwrap(),
                RecordIndex(40)
            );

            // iterate schema B
            let result = part
                .get_records(
                    result.chunks.last().unwrap().end().unwrap(),
                    RowLimit::default(),
                    &Ordering::Reverse,
                )
                .await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(result.schema.unwrap(), chunk_a.schema);
            assert_eq!(
                result.chunks.iter().map(|c| c.chunk.len()).sum::<usize>(),
                10
            );
            assert_eq!(
                result
                    .chunks
                    .first()
                    .and_then(|indexed| indexed.start())
                    .unwrap(),
                RecordIndex(39)
            );
            assert_eq!(
                result
                    .chunks
                    .last()
                    .and_then(|indexed| indexed.end())
                    .unwrap(),
                RecordIndex(30)
            );

            // iterate legacy records
            let result = part
                .get_records(
                    result.chunks.last().unwrap().end().unwrap(),
                    RowLimit::default(),
                    &Ordering::Reverse,
                )
                .await;
            assert!(result.status.is_open());
            assert_eq!(result.schema.unwrap(), legacy_schema());
            assert_eq!(
                result.chunks.iter().map(|c| c.chunk.len()).sum::<usize>(),
                30
            );
            assert_eq!(
                result
                    .chunks
                    .first()
                    .and_then(|indexed| indexed.start())
                    .unwrap(),
                RecordIndex(29)
            );
            assert_eq!(
                result
                    .chunks
                    .last()
                    .and_then(|indexed| indexed.end())
                    .unwrap(),
                RecordIndex(0)
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_recovery() -> Result<()> {
        let mut records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: format!("record-{}", ix).into_bytes(),
            })
            .collect();

        let (dir, part) = partition(segment_3s()).await?;
        {
            for record in records.iter() {
                part.extend_records(&[record.clone()]).await?;
            }
            part.commit().await?;
        }

        let spec = to_spec(part);

        // let's commit every imaginable sin here
        let manifest = &spec.0;
        let last = manifest.get_max_segment(&spec.1).await.unwrap();
        let slog_name = Partition::slog_name(&spec.1);
        let root = PathBuf::from(dir.path());

        // first, corrupt the last file.
        let segment = Slog::segment_from_name(root.as_path(), &slog_name, last);
        debug!("corrupting {last:?}");
        let f = std::fs::File::options().write(true).open(segment.path())?;
        f.set_len(16)?;

        // delete the next data entry from the manifest
        let ix = last.prev().unwrap();
        debug!("removing manifest entry for {ix:?}");
        manifest.remove_segment(ix.to_id(&spec.1)).await;

        // delete the next file entirely
        let segment = Slog::segment_from_name(root.as_path(), &slog_name, ix.prev().unwrap());
        debug!("deleting {:?}", ix.prev().unwrap());
        segment.destroy()?;

        records.truncate(records.len() - 3 * 3);
        let part = reattach(&dir, spec).await;

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                part.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone()),
                "record {}",
                ix
            );
        }
        assert_eq!(
            part.get_record_by_index(RecordIndex(records.len())).await,
            None
        );

        // now, test that we can write and persist still.
        for record in records.iter() {
            part.extend_records(&[record.clone()]).await?;
        }
        part.commit().await?;

        let spec = to_spec(part);
        let part = reattach(&dir, spec).await;
        let doubled: Vec<_> = records.iter().chain(records.iter()).cloned().collect();

        for (ix, record) in doubled.iter().enumerate() {
            assert_eq!(
                part.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone()),
                "record {}",
                ix
            );
        }
        assert_eq!(
            part.get_record_by_index(RecordIndex(doubled.len())).await,
            None
        );

        Ok(())
    }

    pub(crate) fn assert_limit_unreached(status: &BatchStatus) {
        assert!(status.is_open());
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
                slog: Default::default(),
            },
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p", "q", "r"]
            .into_iter()
            .enumerate()
            .map(|(ix, message)| Record {
                time: Utc.timestamp_opt(ix.try_into().unwrap(), 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for record in records[0..6].iter() {
            t.extend_records(&[record.clone()]).await?;
        }
        t.commit().await?;

        // this compaction will destroy the first segment
        t.compact().await;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.extend_records(&[record.clone()]).await?;
        }

        // fetch from start fast-forwards to first valid index
        let result = t
            .get_records(RecordIndex(0), RowLimit::records(2), &Ordering::Forward)
            .await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let start_index = result.chunks.first().unwrap().start().unwrap();
        assert_eq!(
            deindex(result.chunks),
            vec![records[3].clone(), records[4].clone()]
        );

        // iterate through from start
        let result = t
            .get_records(start_index, RowLimit::records(2), &Ordering::Forward)
            .await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(
            deindex(result.chunks),
            vec![records[3].clone(), records[4].clone()]
        );

        let result = t
            .get_records(next_index, RowLimit::records(2), &Ordering::Forward)
            .await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(
            deindex(result.chunks),
            vec![records[5].clone(), records[6].clone()]
        );

        let result = t
            .get_records(next_index, RowLimit::records(2), &Ordering::Forward)
            .await;
        assert_limit_unreached(&result.status);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(deindex(result.chunks), vec![records[7].clone()]);

        let result = t
            .get_records(next_index, RowLimit::records(2), &Ordering::Forward)
            .await;
        assert_limit_unreached(&result.status);
        assert_eq!(deindex(result.chunks), vec![]);

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
                time: Utc.timestamp_opt(*ix, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for record in records[0..6].iter() {
            t.extend_records(&[record.clone()]).await?;
        }
        t.commit().await?;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.extend_records(&[record.clone()]).await?;
        }

        let min_time = times.iter().cloned().min().unwrap();
        let max_time = times.iter().cloned().max().unwrap();
        let time_range = min_time..=max_time;
        for (start, limit) in (0..records.len()).zip(0..records.len()) {
            let slice = Vec::from(&records[start..records.len()]);
            for query_start in time_range.clone() {
                for query_end in query_start..=max_time {
                    let query = Utc.timestamp_opt(query_start, 0).unwrap()
                        ..=Utc.timestamp_opt(query_end, 0).unwrap();
                    let values = slice
                        .iter()
                        .enumerate()
                        .filter(|(_, r)| query.contains(&r.time))
                        .take(limit);
                    let time_slice: Vec<_> = values.clone().map(|(_, r)| r.clone()).collect();
                    assert_eq!(
                        deindex(
                            t.get_records_by_time(
                                RecordIndex(start),
                                query,
                                RowLimit::records(limit)
                            )
                            .await
                            .chunks
                        ),
                        time_slice
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_query_byte_limit() -> Result<()> {
        let (_dir, p) = partition(Config::default()).await?;

        let data = "x".to_string().repeat(50);

        let records = build_records((0..6).map(|_| (0, data.clone())));
        p.extend_records(&records).await?;
        let records = build_records((6..10).map(|_| (0, data.clone())));
        p.extend_records(&records).await?;
        let records = build_records((10..20).map(|_| (0, data.clone())));
        p.extend_records(&records).await?;
        let result = p
            .get_records(
                RecordIndex(0),
                RowLimit {
                    max_records: 10,
                    max_bytes: 250,
                },
                &Ordering::Forward,
            )
            .await;

        assert_eq!(result.status, BatchStatus::BytesExceeded);
        assert_eq!(
            result
                .chunks
                .iter()
                .map(|i| i.chunk.len())
                .collect::<Vec<_>>(),
            vec![6]
        );

        let result = p
            .get_records(
                RecordIndex(0),
                RowLimit {
                    max_records: 10,
                    max_bytes: 1,
                },
                &Ordering::Forward,
            )
            .await;
        assert_eq!(result.status, BatchStatus::BytesExceeded);
        assert_eq!(
            result
                .chunks
                .iter()
                .map(|i| i.chunk.len())
                .collect::<Vec<_>>(),
            vec![6]
        );

        Ok(())
    }
}

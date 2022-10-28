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
use crate::chunk::{parse_time, IndexedChunk, Schema};
use crate::limit::{LimitedBatch, RowLimit};
use crate::manifest::Manifest;
pub use crate::manifest::{PartitionId, Scope, SegmentData};
use crate::retention::Retention;
pub use crate::segment::Record;
pub use crate::slog::RecordIndex;
use crate::slog::{SegmentIndex, Slog};
use anyhow::Result;
use arrow2::array::BooleanArray;
#[cfg(test)]
use arrow2::compute::comparison::eq_scalar;
use arrow2::compute::comparison::gt_eq_scalar;
use arrow2::scalar::PrimitiveScalar;
use chrono::{DateTime, Utc};
use futures::stream::{Stream, StreamExt};
use futures::FutureExt;
use futures::{future, stream};
use log::{error, info, trace};
use metrics::{counter, gauge};
use plateau_transport::SchemaChunk;
use serde::{Deserialize, Serialize};
use std::fs;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock, RwLockReadGuard};

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

        let (messages, mut writes) = Slog::attach(root.clone(), slog_name, segment, record);

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
                let data = manifest.get_segment_data(id.segment_id(ix)).await;
                let valid = segment.validate();
                index = match data {
                    Some(data) if valid => Some((ix.next(), data.records.end)),
                    _ => {
                        error!("bad {:?} file: {} catalog: {}", ix, valid, data.is_some());
                        if let Err(e) = segment.destroy() {
                            error!("error destroying {:?}: {}", ix, e);
                        }
                        manifest.remove_segment(id.segment_id(ix)).await;
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
        state.roll_when_needed(self).await?;
        state.messages.append(chunk).await;

        Ok(start..(start + size))
    }

    #[cfg(test)]
    pub(crate) async fn commit(&self) -> Result<()> {
        self.state.write().await.commit(&self).await
    }

    pub(crate) async fn checkpoint(&self) {
        self.state.write().await.messages.checkpoint().await;
    }

    async fn stream_with_active<'a>(
        &'a self,
        stored: impl Stream<Item = SegmentData> + 'a + Send,
        state: &RwLockReadGuard<'a, State>,
    ) -> impl Stream<Item = SegmentData> + 'a + Send {
        let cached = state.messages.cached_segment_data().await;
        let cached_segments: Vec<SegmentIndex> = cached.iter().map(|data| data.index).collect();
        // due to checkpoints, segment data for the active / pending segment may
        // already be stored in the manifest. we want to always only use
        // in-memory data as it is fresher.
        stored
            .take_while(move |data| future::ready(!cached_segments.contains(&data.index)))
            .chain(stream::iter(cached.into_iter()))
    }

    pub(crate) async fn get_records(&self, start: RecordIndex, limit: RowLimit) -> LimitedBatch {
        let state = self.state.read().await;
        let segments = self
            .stream_with_active(self.manifest.stream_segments(&self.id, start), &state)
            .await;

        let filter = |chunk: &IndexedChunk| {
            // TODO: ideally we'd be using slicing here instead; it's more
            // efficient. This is a straightforward port of the original code.
            gt_eq_scalar(
                chunk.indices(),
                &PrimitiveScalar::from(Some(start.0 as i32)),
            )
        };

        state
            .get_records_from_segments(limit, filter, segments)
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
            .get_records_from_segments(limit, filter, segments)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn get_record_by_index(&self, index: RecordIndex) -> Option<Record> {
        let record_response = self.get_records(index, RowLimit::records(1)).await;

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
            .flat_map(|indexed| indexed.into_legacy())
            .next()
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

    pub(crate) async fn remove_oldest(&self) {
        let state = self.state.read().await;
        state.remove_oldest(self).await;
    }

    #[cfg(test)]
    pub(crate) async fn compact(&self) {
        let mut state = self.state.write().await;
        state.commit(&self).await.expect("commit failed");
        state.retain(&self).await;
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
        limit: RowLimit,
        filter: impl Fn(&IndexedChunk) -> BooleanArray + Send + Sync,
        indices: impl Stream<Item = SegmentData> + 'a + Send,
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
                        .iter_segment(segment.index)
                        .into_stream()
                        .flat_map(stream::iter)
                        .scan(
                            segment.records.start.0,
                            move |start: &mut usize, data: SchemaChunk<Schema>| {
                                let index = RecordIndex(*start);
                                *start += data.chunk.len();
                                future::ready(Some(IndexedChunk::from_start(index, data)))
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

    async fn wait_for_record(&mut self, target: RecordIndex) {
        trace!("waiting for commit including {:?}", target);
        while *self.commits.borrow() < target {
            self.commits.changed().await.expect("commit watcher ended");
        }
        trace!("notified of commit including {:?}", target);
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
    use crate::chunk::test::{inferences_nested, inferences_schema_a, inferences_schema_b};
    use crate::chunk::LegacyRecords;
    use crate::limit::BatchStatus;
    use crate::segment::test::build_records;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use std::convert::TryInto;
    use tempfile::{tempdir, TempDir};

    pub(crate) fn deindex(is: Vec<IndexedChunk>) -> Vec<Record> {
        is.into_iter().flat_map(|i| i.into_legacy()).collect()
    }

    pub async fn partition(config: Config) -> Result<(TempDir, Partition)> {
        let id = PartitionId::new("topic", "testing");
        let dir = tempdir()?;
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        Ok((dir, Partition::attach(root, manifest, id, config).await))
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
            t.extend_records(&vec![record.clone()]).await?;
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
        let (_dir, t) = partition(segment_3s()).await?;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.extend_records(&vec![record.clone()]).await?;
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
                deindex(
                    t.get_records(RecordIndex(start), RowLimit::records(limit))
                        .await
                        .chunks
                ),
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

        let mut chunk_slice = IndexedChunk::from_start(RecordIndex(0), chunk_a);
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

    #[tokio::test]
    async fn test_durability() -> Result<()> {
        let records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(format!("record-{}", ix).as_str()),
            })
            .collect();

        let (dir, part) = partition(segment_3s()).await?;
        let spec = {
            for record in records.iter() {
                part.extend_records(&vec![record.clone()]).await?;
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
    async fn test_nested() -> Result<()> {
        let (dir, part) = partition(Config::default()).await?;
        let spec = {
            part.extend(inferences_nested()).await?;
            part.commit().await?;
            to_spec(part)
        };

        {
            let part = reattach(&dir, spec).await;

            let start = RecordIndex(0);
            let result = part.get_records(start, RowLimit::default()).await;
            assert_eq!(result.chunks.len(), 1);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_change() -> Result<()> {
        let records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(format!("record-{}", ix).as_str()),
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

            let start = RecordIndex(0);
            let result = part.get_records(start, RowLimit::default()).await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(
                result.chunks,
                vec![IndexedChunk::from_start(
                    start,
                    SchemaChunk::try_from(LegacyRecords(records))?
                )]
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default()).await;
            assert_eq!(result.status, BatchStatus::SchemaChanged);
            assert_eq!(
                result.chunks,
                vec![
                    IndexedChunk::from_start(start, chunk_a.clone()),
                    IndexedChunk::from_start(start + chunk_a.len(), chunk_a.clone())
                ]
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default()).await;
            assert!(result.status.is_open());
            assert_eq!(
                result.chunks,
                vec![IndexedChunk::from_start(start, chunk_b.clone()),]
            );

            let start = result.chunks.last().unwrap().end().unwrap() + 1;
            let result = part.get_records(start, RowLimit::default()).await;
            assert!(result.status.is_open());
            assert_eq!(result.chunks, vec![]);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_recovery() -> Result<()> {
        let mut records: Vec<_> = (0..(3 * 10))
            .into_iter()
            .map(|ix| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(format!("record-{}", ix).as_str()),
            })
            .collect();

        let (dir, part) = partition(segment_3s()).await?;
        {
            for record in records.iter() {
                part.extend_records(&vec![record.clone()]).await?;
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
        let f = std::fs::File::options().write(true).open(segment.path())?;
        f.set_len(f.metadata().unwrap().len() - 10)?;

        // delete the next data entry from the manifest
        let ix = last.prev().unwrap();
        manifest.remove_segment(spec.1.segment_id(ix)).await;

        // delete the next file entirely
        let segment = Slog::segment_from_name(root.as_path(), &slog_name, ix.prev().unwrap());
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
            part.extend_records(&vec![record.clone()]).await?;
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
            t.extend_records(&vec![record.clone()]).await?;
        }
        t.commit().await?;

        // this compaction will destroy the first segment
        t.compact().await;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.extend_records(&vec![record.clone()]).await?;
        }

        // fetch from start fast-forwards to first valid index
        let result = t.get_records(RecordIndex(0), RowLimit::records(2)).await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let start_index = result.chunks.iter().next().unwrap().start().unwrap();
        assert_eq!(
            deindex(result.chunks),
            vec![records[3].clone(), records[4].clone()]
        );

        // iterate through from start
        let result = t.get_records(start_index, RowLimit::records(2)).await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(
            deindex(result.chunks),
            vec![records[3].clone(), records[4].clone()]
        );

        let result = t.get_records(next_index, RowLimit::records(2)).await;
        assert_eq!(result.status, BatchStatus::RecordsExceeded);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(
            deindex(result.chunks),
            vec![records[5].clone(), records[6].clone()]
        );

        let result = t.get_records(next_index, RowLimit::records(2)).await;
        assert_limit_unreached(&result.status);
        let next_index = result.chunks.iter().next_back().unwrap().end().unwrap() + 1;
        assert_eq!(deindex(result.chunks), vec![records[7].clone()]);

        let result = t.get_records(next_index, RowLimit::records(2)).await;
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
                time: Utc.timestamp(*ix, 0),
                message: ByteArray::from(*message),
            })
            .collect();

        for record in records[0..6].iter() {
            t.extend_records(&vec![record.clone()]).await?;
        }
        t.commit().await?;

        // write some more records to the active segment
        for record in records[6..].iter() {
            t.extend_records(&vec![record.clone()]).await?;
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

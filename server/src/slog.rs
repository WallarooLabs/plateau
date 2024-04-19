//! The slog (segment log) is a persistent sequence of segments with a
//! well-known storage location.
//!
//! It is additionally responsible for compaction. It buffers incoming writes
//! and periodically concatenates them together into large chunks. Large chunks
//! mitigate heap fragmentation, as well as providing faster indexing and more
//! predictable read and write performance.
//!
//! Currently, local segment files named with a simple logical index is the only
//! supported slog type.
//!
//! Every slog has a background writer thread. This achieves two goals:
//!
//! - I/O write concurrency
//! - Load shedding
//!
//! Write concurrency allows us to ingest more records while waiting for the OS
//! to flush data to disk.
//!
//! The writer thread appends data to the active backing file on each
//! `checkpoint()`. It moves on to the next sequential segment when a `roll()`
//! is requested.
//!
//! Load is shed by failing any roll or checkpoint operation while an existing
//! background checkpoint is pending. This signals the topic partition to
//! discard writes and stall rolls until the write completes.
use std::cmp::{max, min};
use std::iter::Zip;
use std::ops::{Add, AddAssign, Range, RangeBounds, RangeInclusive};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::vec;

use anyhow::Result;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use metrics::counter;
use plateau_client::estimate_size;
use plateau_transport::{SchemaChunk, SegmentChunk};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, trace};

use crate::chunk::{self, Schema, TimeRange};
use crate::limit::Rolling;
use crate::manifest::{Ordering, PartitionId, SegmentData, SegmentId, SEGMENT_FORMAT_VERSION};
use crate::segment::{Config as SegmentConfig, Segment, SegmentIterator, Writer};

#[cfg(test)]
use crate::chunk::Record;

#[derive(Error, Debug)]
pub(crate) enum SlogError {
    #[error("writer thread busy")]
    WriterThreadBusy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub write_queue_timeout: Duration,
    pub write_queue_size: usize,
    pub chunk_limits: Rolling,
    pub segment: SegmentConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            write_queue_timeout: Duration::from_millis(100),
            write_queue_size: 1,
            chunk_limits: Rolling {
                max_rows: 1000,
                max_bytes: ByteSize::mib(1),
                max_duration: None,
            },
            segment: SegmentConfig::default(),
        }
    }
}

/// Each segment in the slog has a unique increasing index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct SegmentIndex(pub usize);

impl SegmentIndex {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn prev(&self) -> Option<Self> {
        if self.0 > 0 {
            Some(Self(self.0 - 1))
        } else {
            None
        }
    }

    pub fn to_id(self, partition_id: &PartitionId) -> SegmentId<&PartitionId> {
        SegmentId {
            partition_id,
            segment: self,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub(crate) segment: SegmentIndex,
    pub(crate) record: RecordIndex,
}

/// Each record also has a global unique sequential index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RecordIndex(pub usize);

impl Add<usize> for RecordIndex {
    type Output = Self;

    fn add(self, span: usize) -> Self {
        Self(self.0 + span)
    }
}

impl AddAssign<usize> for RecordIndex {
    fn add_assign(&mut self, span: usize) {
        self.0 += span;
    }
}

pub(crate) type SlogWrites = mpsc::Receiver<WriteResult>;

/// A slog (segment log) is a named and ordered series of segments.
#[must_use = "close() explicitly to flush writes"]
pub(crate) struct Slog {
    root: PathBuf,
    name: String,
    state: RwLock<State>,
}

#[derive(Clone, Debug, Default)]
struct SizedChunks {
    chunks: Vec<SegmentChunk>,
    bytes: Vec<usize>,
    total_rows: usize,
    total_bytes: usize,
}

impl SizedChunks {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    fn len(&self) -> usize {
        self.chunks.len()
    }

    fn drain<R>(&mut self, range: R) -> impl Iterator<Item = (SegmentChunk, usize)> + '_
    where
        R: RangeBounds<usize> + Clone,
    {
        self.chunks
            .drain(range.clone())
            .zip(self.bytes.drain(range))
    }

    fn push(&mut self, chunk: SegmentChunk, bytes: usize) {
        self.total_rows += chunk.len();
        self.total_bytes += bytes;
        self.chunks.push(chunk);
        self.bytes.push(bytes);
    }

    fn pop(&mut self) -> Option<(SegmentChunk, usize)> {
        let chunk = self.chunks.pop()?;
        self.total_rows -= chunk.len();
        let bytes = self.bytes.pop()?;
        self.total_bytes -= bytes;
        Some((chunk, bytes))
    }

    fn concat(mut self) -> anyhow::Result<(SegmentChunk, usize)> {
        if self.len() == 1 {
            return Ok((self.chunks.pop().unwrap(), self.bytes.pop().unwrap()));
        }

        let chunk = chunk::concatenate(&self.chunks)?;

        Ok((chunk, self.total_bytes))
    }
}

impl Extend<(SegmentChunk, usize)> for SizedChunks {
    fn extend<T: IntoIterator<Item = (SegmentChunk, usize)>>(&mut self, iter: T) {
        for (chunk, bytes) in iter.into_iter() {
            self.push(chunk, bytes);
        }
    }
}

impl IntoIterator for SizedChunks {
    type Item = (SegmentChunk, usize);

    type IntoIter = Zip<vec::IntoIter<SegmentChunk>, vec::IntoIter<usize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter().zip(self.bytes)
    }
}

#[derive(Clone, Debug)]
struct MemorySegment {
    metadata: SegmentData,
    schema: Schema,
    saved_chunks: Vec<SegmentChunk>,
    saved_rows: usize,
    active_chunks: SizedChunks,
}

impl MemorySegment {
    fn record_count(&self) -> usize {
        self.saved_chunks
            .iter()
            .chain(self.active_chunks.chunks.iter())
            .map(|c| c.len())
            .sum()
    }

    /// Compact the chunks that have accumulated in this memory segment into
    /// "full" chunks that each exceed configurable row or byte limits.
    ///
    /// Any existing chunks that already meet the size limits will be left
    /// unmodified.
    ///
    /// The final chunk will often be a "partial" chunk that does not meet the
    /// size limit. Future calls to `compact` will merge any new chunks into
    /// this partial chunk until it meets the limit, at which point the cycle
    /// begins anew.
    ///
    /// Returns the total number of rows in the active chunk.
    fn compact(&mut self, config: &Config) -> usize {
        if self.active_chunks.is_empty() {
            return 0;
        }

        let mut start_ix = 0;
        let mut partial = SizedChunks::default();
        let mut compact = SizedChunks::default();
        for (ix, (chunk, bytes)) in self.active_chunks.drain(..).enumerate() {
            partial.push(chunk, bytes);
            if partial.total_rows >= config.chunk_limits.max_rows
                || partial.total_bytes >= config.chunk_limits.max_bytes.0 as usize
            {
                let prior_start = start_ix;
                start_ix = ix + 1;
                trace!(
                    "full chunk {:?} {}",
                    prior_start..start_ix,
                    partial.total_rows
                );
                match std::mem::take(&mut partial).concat() {
                    Ok((chunk, bytes)) => compact.push(chunk, bytes),
                    Err(e) => error!("error building full chunk: {:?}", e),
                }
            }
        }

        if !partial.is_empty() {
            trace!(
                "compact active segment {:?} {}",
                start_ix..(start_ix + partial.len()),
                partial.total_rows
            );
            match partial.concat() {
                Ok((chunk, bytes)) => compact.push(chunk, bytes),
                Err(e) => error!("error building final partial chunk: {:?}", e),
            };
        }

        let total_rows = compact.total_rows;
        self.active_chunks = compact;

        total_rows
    }
}

struct State {
    active: Option<MemorySegment>,
    active_checkpoint: Checkpoint,
    active_first_record_ix: RecordIndex,
    thread: SlogThread,
    pending: Option<MemorySegment>,
    config: Config,
}

#[derive(Debug)]
enum WriterMessage {
    Append(AppendRequest),
    Fin,
}

#[derive(Debug)]
struct AppendRequest {
    seal: bool,
    segment: SegmentIndex,
    records: Range<RecordIndex>,
    time: RangeInclusive<DateTime<Utc>>,
    schema: Schema,
    full_chunks: Vec<SegmentChunk>,
    active_chunk: SegmentChunk,
}

#[derive(Debug)]
pub(crate) struct WriteResult {
    pub(crate) data: SegmentData,
}

impl Slog {
    /// Because a slog is stateless, whatever attaches it is responsible for processing
    /// commit events. The channel is bounded to a size of one; if it is not consumed,
    /// the writer thread will immediately stall.
    pub(crate) fn attach(
        root: PathBuf,
        name: String,
        active_segment: SegmentIndex,
        active_first_record_ix: RecordIndex,
        config: Config,
    ) -> (Self, SlogWrites) {
        let (thread, rx) = spawn_slog_thread(
            root.clone(),
            name.clone(),
            config.write_queue_size,
            config.segment.clone(),
        );
        let active_checkpoint = Checkpoint {
            segment: active_segment,
            record: active_first_record_ix,
        };
        let state = State {
            active: None,
            pending: None,
            active_checkpoint,
            active_first_record_ix,
            thread,
            config,
        };

        let slog = Self {
            root,
            name,
            state: RwLock::new(state),
        };

        (slog, rx)
    }

    fn segment_path(root: &Path, name: &str, segment_ix: SegmentIndex) -> PathBuf {
        let file = PathBuf::from(format!("{}-{}", name, segment_ix.0));
        [root, file.as_path()].into_iter().collect()
    }

    pub(crate) fn segment_from_name(root: &Path, name: &str, segment_ix: SegmentIndex) -> Segment {
        Segment::at(Self::segment_path(root, name, segment_ix))
    }

    pub(crate) fn get_segment(&self, segment_ix: SegmentIndex) -> Segment {
        Self::segment_from_name(&self.root, &self.name, segment_ix)
    }

    #[cfg(test)]
    pub(crate) async fn get_record(
        &self,
        segment: SegmentIndex,
        relative: usize,
    ) -> Option<Record> {
        use crate::chunk::LegacyRecords;

        self.iter_segment(segment, &Ordering::Forward)
            .await
            .flat_map(|chunk| LegacyRecords::try_from(chunk).unwrap().0)
            .nth(relative)
    }

    pub(crate) async fn iter_segment<'a>(
        &'a self,
        ix: SegmentIndex,
        order: &'a Ordering,
    ) -> Box<dyn DoubleEndedIterator<Item = SchemaChunk<Schema>> + Send + 'a> {
        let state = self.state.read().await;
        if ix > state.active_checkpoint.segment {
            return Box::new(std::iter::empty());
        }

        if let Some(in_mem) = state.get_segment(ix).await {
            let schema = in_mem.schema.clone();
            // NOTE: the backing [Buffer] behind arrow2 arrays is actually
            // arc-ed, so the clones here _should_ be relatively cheap.
            let mut chunks: Vec<_> = in_mem
                .saved_chunks
                .iter()
                .chain(in_mem.active_chunks.chunks.iter())
                .cloned()
                .collect();

            if order.is_reverse() {
                chunks.reverse();
            }

            return Box::new(chunks.into_iter().map(move |chunk| SchemaChunk {
                schema: schema.clone(),
                chunk,
            }));
        }

        let segment = self.get_segment(ix);
        if !Path::new(segment.path()).exists() {
            return Box::new(std::iter::empty());
        }

        let reader = segment.iter().unwrap();
        let schema = reader.schema().clone();
        let filter = move |c: Result<SegmentChunk>| {
            c.map_or(None, |chunk| {
                Some(SchemaChunk {
                    schema: schema.clone(),
                    chunk,
                })
            })
        };
        if order.is_reverse() {
            Box::new(reader.rev().filter_map(filter))
        } else {
            Box::new(reader.filter_map(filter))
        }
    }

    pub(crate) async fn append(&self, data: SchemaChunk<Schema>) -> Checkpoint {
        self.state.write().await.append(data).await
    }

    pub(crate) fn destroy(&self, segment_ix: SegmentIndex) -> Result<()> {
        self.get_segment(segment_ix).destroy()
    }

    pub(crate) async fn cached_segment_data(&self) -> Vec<SegmentData> {
        self.state.read().await.cached_segment_data()
    }

    pub(crate) async fn next_record_ix(&self) -> RecordIndex {
        let state = self.state.read().await;
        let active_size = state
            .active
            .as_ref()
            .map_or(0, |d| d.metadata.records.end.0 - d.metadata.records.start.0);
        state.active_first_record_ix + active_size
    }

    pub(crate) async fn active_schema_matches(&self, other: &Schema) -> bool {
        let active = &self.state.read().await.active;

        let matches = active.as_ref().map_or(true, |s| &s.schema == other);
        if !matches {
            trace!(
                "schema mismatch: {:?} != {:?}",
                active.as_ref().map(|s| &s.schema),
                other
            );
        }

        matches
    }

    pub(crate) async fn active_segment_data(&self) -> Option<SegmentData> {
        self.state
            .read()
            .await
            .active
            .as_ref()
            .map(|d| d.metadata.clone())
    }

    pub(crate) async fn pending_segment_data(&self) -> Option<SegmentData> {
        self.state
            .read()
            .await
            .pending
            .as_ref()
            .map(|segment| segment.metadata.clone())
    }

    pub(crate) async fn roll(&self) -> Result<()> {
        counter!("slog_roll", "name" => self.name.clone()).increment(1);
        self.state.write().await.roll().await
    }

    pub(crate) async fn checkpoint(&self) -> bool {
        self.state.write().await.checkpoint(false).await
    }

    pub(crate) async fn close(self) {
        self.state.into_inner().close().await;
    }
}

impl State {
    pub(crate) async fn append(&mut self, d: SchemaChunk<Schema>) -> Checkpoint {
        let first = self.active_first_record_ix;
        let index = self.active_checkpoint.segment;
        let data_time_range = d.time_range().unwrap();

        let len = d.chunk.len();
        let bytes = match estimate_size(&d.chunk) {
            Ok(size) => size,
            Err(e) => {
                error!("error estimating chunk size: {e:?}");
                len
            }
        };

        let active = self.active.get_or_insert_with(|| MemorySegment {
            schema: d.schema,
            metadata: SegmentData {
                index,
                size: 0,
                records: first..first,
                time: data_time_range.clone(),
                version: SEGMENT_FORMAT_VERSION,
            },
            saved_chunks: vec![],
            saved_rows: 0,
            active_chunks: Default::default(),
        });

        active.metadata.size += bytes;
        active.metadata.records.end += len;
        active.metadata.time = min(*active.metadata.time.start(), *data_time_range.start())
            ..=max(*active.metadata.time.end(), *data_time_range.end());
        active.active_chunks.push(d.chunk, bytes);

        Checkpoint {
            segment: index,
            record: active.metadata.records.end,
        }
    }

    async fn get_segment(&self, ix: SegmentIndex) -> Option<&MemorySegment> {
        if ix == self.active_checkpoint.segment {
            self.active.as_ref()
        } else {
            match &self.pending {
                Some(pending) if ix.next() == self.active_checkpoint.segment => Some(pending),
                _ => None,
            }
        }
    }

    pub(crate) fn cached_segment_data(&self) -> Vec<SegmentData> {
        self.pending
            .iter()
            .map(|p| p.metadata.clone())
            .chain(self.active.iter().map(|d| d.metadata.clone()))
            .collect()
    }

    /// Checkpoints make an `AppendRequest` to the writer thread for for all
    /// records in memory that were not stored in the last checkpoint.
    ///
    /// If `seal` is set, the request additionally indicates that the current
    /// segment should be finalized via `close()`, which closes the underlying
    /// segment and begins a new one.
    ///
    /// If the checkpoint is successful, all active chunks except for the last
    /// (partial) chunk are considered "saved". They are "full", will not be
    /// modified, and do not need to be resent to the writer. The final partial
    /// chunk has been persisted as well, but may receive future updates before
    /// it is "saved".
    ///
    /// We keep this final partial chunk "open" for efficiency reasons. See the
    /// implementation of the active chunk cache in [crate::segment] for details.
    ///
    /// Returns `true` if the checkpoint was successful, and `false` if it timed
    /// out.
    async fn checkpoint(&mut self, seal: bool) -> bool {
        if let Some(segment) = &mut self.active {
            let active_rows = segment.compact(&self.config);

            let active_start = segment.metadata.records.start;
            let records = active_start..(active_start + segment.saved_rows + active_rows);

            if !segment.active_chunks.is_empty() && records.end > self.active_checkpoint.record {
                let mut full_chunks = std::mem::take(&mut segment.active_chunks);
                let Some((active_chunk, active_bytes)) = full_chunks.pop() else {
                    return true;
                };

                let request = AppendRequest {
                    seal,
                    segment: self.active_checkpoint.segment,
                    records: records.clone(),
                    time: segment.metadata.time.clone(),
                    schema: segment.schema.clone(),
                    full_chunks: full_chunks.chunks.clone(),
                    active_chunk: active_chunk.clone(),
                };

                trace!(
                    "{:?}: {:?} (seal: {}, full chunks: {}, active rows: {})",
                    segment.metadata.index,
                    records,
                    seal,
                    full_chunks.len(),
                    active_chunk.len()
                );

                if timeout(
                    self.config.write_queue_timeout,
                    self.thread.tx.send(WriterMessage::Append(request)),
                )
                .await
                .is_ok_and(|r| r.is_ok())
                {
                    segment.saved_rows += full_chunks.chunks.iter().map(|c| c.len()).sum::<usize>();
                    segment.saved_chunks.extend(full_chunks.chunks);
                    segment.active_chunks = SizedChunks::default();
                    segment.active_chunks.push(active_chunk, active_bytes);
                    self.active_checkpoint.record = records.end;
                } else {
                    full_chunks.push(active_chunk, active_bytes);
                    segment.active_chunks = full_chunks;
                    return false;
                }
            }

            true
        } else {
            true
        }
    }

    pub(crate) async fn roll(&mut self) -> Result<()> {
        if let Some(records) = self.active.as_ref().map(|s| s.record_count()) {
            if self.checkpoint(true).await {
                let segment = self.active_checkpoint.segment;
                trace!("rolling {:?}", segment);
                self.active_checkpoint.segment = segment.next();
                self.active_first_record_ix += records;
                self.pending = self.active.take();
                Ok(())
            } else {
                Err(anyhow::Error::new(SlogError::WriterThreadBusy))
            }
        } else {
            Ok(())
        }
    }

    pub(crate) async fn close(self) {
        let now = Instant::now();
        self.thread.tx_fin.send(()).ok();
        self.thread.tx.send(WriterMessage::Fin).await.ok();
        if let Err(e) = self.thread.handle.join() {
            error!("error joining writer thread: {:?}", e);
        }
        info!("writer thread shutdown in {:?}", now.elapsed());
    }
}

struct SlogThread {
    tx: mpsc::Sender<WriterMessage>,
    tx_fin: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

fn spawn_slog_thread(
    root: PathBuf,
    name: String,
    write_queue_size: usize,
    config: SegmentConfig,
) -> (SlogThread, SlogWrites) {
    let (tx, mut rx_records) = mpsc::channel(write_queue_size);
    let (tx_fin, mut rx_fin) = oneshot::channel();
    let (tx_commits, rx_commits) = mpsc::channel(1);

    let handle = std::thread::spawn(move || {
        let mut active = true;
        let mut current: Option<(Schema, Writer, SegmentIndex)> = None;
        let mut prior_size = 0;
        while active {
            match rx_records.blocking_recv() {
                Some(WriterMessage::Append(AppendRequest {
                    seal,
                    segment,
                    records,
                    time,
                    schema,
                    full_chunks,
                    active_chunk,
                })) => {
                    trace!("{}: received request for {:?}", name, records);
                    let new_segment = Slog::segment_from_name(&root, &name, segment);
                    current = current.and_then(|(schema, writer, id)| {
                        if id != segment {
                            trace!("{}: segment change {:?} {:?}", name, id, segment);
                            writer.close().expect("sealed segment");
                            None
                        } else {
                            Some((schema, writer, id))
                        }
                    });
                    let (schema, ref mut writer, _) = current.get_or_insert_with(|| {
                        trace!("{}: opening segment {:?}", name, segment);
                        (
                            schema.clone(),
                            new_segment
                                .create(schema, config.clone())
                                .expect("segment creation"),
                            segment,
                        )
                    });

                    let now = Instant::now();
                    writer
                        .log_arrows(schema, full_chunks, Some(active_chunk))
                        .expect("extend full chunks");

                    let size = writer.size_estimate().expect("segment size estimate");
                    let record_len = records.end.0 - records.start.0;
                    debug!(
                        "{}: wrote to {:?} (end {:?}, len {}, size {}) in {:?}",
                        name,
                        segment,
                        records.end,
                        record_len,
                        size - prior_size,
                        now.elapsed()
                    );
                    prior_size = size;
                    counter!(
                        "slog_thread_records_written",
                        "name" => name.clone()
                    )
                    .increment(u64::try_from(record_len).unwrap());

                    if seal {
                        prior_size = 0;
                        current
                            .take()
                            .map(|(_, w, _)| w.close().expect("segment close"));
                        trace!("{}: sealed {:?} {:?}", name, segment, records);
                    }

                    let response = WriteResult {
                        data: SegmentData {
                            index: segment,
                            records: records.clone(),
                            time,
                            size,
                            version: SEGMENT_FORMAT_VERSION,
                        },
                    };
                    trace!("{}: commit {:?}/{:?} send", name, segment, records.end);
                    tx_commits.blocking_send(response).expect("channel closed");
                    trace!("{}: commit sent", name);
                }
                Some(WriterMessage::Fin) | None => {
                    trace!("channel closed; shutting down");
                    active = false;
                }
            }

            if rx_fin.try_recv().is_ok() {
                info!("received shutdown signal");
                active = false;
            }
        }

        current.take().map(|(_, writer, _)| writer.close());
        info!("writer for \"{}\" closed", name);
    });

    (SlogThread { tx, tx_fin, handle }, rx_commits)
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::chunk::LegacyRecords;

    use chrono::TimeZone;
    use tempfile::tempdir;

    #[tokio::test]
    async fn reverse_chunks() {
        // create a simple set of records
        let foo = Record {
            time: Default::default(),
            message: "foo".as_bytes().to_vec(),
        };
        let bar = Record {
            time: Default::default(),
            message: "bar".as_bytes().to_vec(),
        };
        let baz = Record {
            time: Default::default(),
            message: "baz".as_bytes().to_vec(),
        };
        let set1 = LegacyRecords(vec![foo.clone(), bar.clone(), baz.clone()]);
        let set2 = LegacyRecords(vec![foo, bar, baz]);

        // extract the underlying chunk and reverse
        let mut chunk: SchemaChunk<Schema> = set2.try_into().unwrap();
        chunk.reverse_inner();
        let set2 = LegacyRecords::try_from(chunk).unwrap();

        assert_eq!(set1.0[0], set2.0[2]);
        assert_eq!(set1.0[1], set2.0[1]);
        assert_eq!(set1.0[2], set2.0[0]);
    }

    #[tokio::test]
    async fn basic_sequencing() -> Result<()> {
        let root = tempdir().unwrap();
        let (slog, mut commits) = Slog::attach(
            PathBuf::from(root.path()),
            String::from("testing"),
            SegmentIndex(0),
            RecordIndex(0),
            Config {
                segment: crate::segment::Config::default(),
                ..Default::default()
            },
        );
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        let chunk = SchemaChunk::try_from(LegacyRecords(records.clone()))?;

        let first = slog.append(chunk).await;
        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                slog.get_record(first.segment, ix).await,
                Some(record.clone())
            );
        }
        slog.roll().await?;
        assert_eq!(
            commits
                .recv()
                .await
                .map(|r| (r.data.records, r.data.size > 0)),
            Some((RecordIndex(0)..RecordIndex(3), true))
        );

        let chunk = SchemaChunk::try_from(LegacyRecords(records.clone()))?;
        let second = slog.append(chunk).await;
        assert!(slog.roll().await.is_ok());
        assert_eq!(
            commits
                .recv()
                .await
                .map(|r| (r.data.records, r.data.size > 0)),
            Some((RecordIndex(3)..RecordIndex(6), true))
        );

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                slog.get_record(second.segment, ix).await,
                Some(record.clone())
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_timeouts() -> Result<()> {
        let root = tempdir().unwrap();

        let segment = SegmentIndex(0);
        let (slog, mut commits) = Slog::attach(
            PathBuf::from(root.path()),
            String::from("testing"),
            segment,
            RecordIndex(0),
            Config {
                write_queue_timeout: Duration::from_millis(1),
                ..Default::default()
            },
        );

        // kill the writer thread to ensure that checkpoints time out
        {
            let (bogus_tx, _) = oneshot::channel();
            std::mem::replace(&mut slog.state.write().await.thread.tx_fin, bogus_tx)
                .send(())
                .unwrap();
            // now to receive some records so we can process the fin signal...
        }

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        let chunk = SchemaChunk::try_from(LegacyRecords(records[0..1].to_vec()))?;

        let first = slog.append(chunk).await;
        assert!(slog.state.write().await.checkpoint(false).await);

        assert_eq!(
            commits
                .recv()
                .await
                .map(|r| (r.data.records, r.data.size > 0)),
            Some((RecordIndex(0)..RecordIndex(1), true))
        );
        while commits.recv().await.is_some() {}

        for (ix, record) in records[0..1].iter().enumerate() {
            assert_eq!(
                slog.get_record(first.segment, ix).await,
                Some(record.clone())
            );
        }

        for record in records[1..].iter() {
            let chunk = SchemaChunk::try_from(LegacyRecords(vec![record.clone()]))?;
            slog.append(chunk).await;
        }
        assert!(!slog.state.write().await.checkpoint(true).await);

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(slog.get_record(segment, ix).await, Some(record.clone()));
        }

        // verify post first compaction
        assert!(!slog.state.write().await.checkpoint(true).await);
        for (ix, record) in records.iter().enumerate() {
            assert_eq!(slog.get_record(segment, ix).await, Some(record.clone()));
        }

        // verify that compaction runs properly again and everything still can
        // be fetched
        assert!(!slog.state.write().await.checkpoint(true).await);
        for (ix, record) in records.iter().enumerate() {
            assert_eq!(slog.get_record(segment, ix).await, Some(record.clone()));
        }

        Ok(())
    }
}

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
use crate::chunk::{self, Schema, TimeRange};
use crate::manifest::{Ordering, PartitionId, SegmentData, SegmentId};
#[cfg(test)]
use crate::segment::Record;
use crate::segment::{Config as SegmentConfig, Segment, SegmentWriter2};
use anyhow::Result;
use chrono::{DateTime, Utc};
use metrics::counter;
use plateau_transport::{SchemaChunk, SegmentChunk};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::convert::TryFrom;
use std::ops::{Add, AddAssign, Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, info, trace};

#[derive(Error, Debug)]
pub enum SlogError {
    #[error("writer thread busy")]
    WriterThreadBusy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub write_queue_timeout: Duration,
    pub write_queue_size: usize,
    pub min_full_chunk_len: usize,
    pub segment: SegmentConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            write_queue_timeout: Duration::from_millis(100),
            write_queue_size: 1,
            min_full_chunk_len: 1000,
            segment: SegmentConfig::default(),
        }
    }
}

/// Each segment in the slog has a unique increasing index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct SegmentIndex(pub usize);

impl SegmentIndex {
    pub fn next(&self) -> Self {
        SegmentIndex(self.0 + 1)
    }

    pub fn prev(&self) -> Option<Self> {
        if self.0 > 0 {
            Some(SegmentIndex(self.0 - 1))
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
    type Output = RecordIndex;

    fn add(self, span: usize) -> Self {
        RecordIndex(self.0 + span)
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

#[derive(Clone, Debug)]
struct MemorySegment {
    metadata: SegmentData,
    schema: Schema,
    full_chunks: Vec<SegmentChunk>,
    full_chunk_len: usize,
    active_chunk: Vec<SegmentChunk>,
}

impl MemorySegment {
    fn record_count(&self) -> usize {
        self.full_chunks
            .iter()
            .chain(self.active_chunk.iter())
            .map(|c| c.len())
            .sum()
    }

    /// Compact the chunks that have accumulated in this memory segment into
    /// "full" chunks that have a minimum size limit.
    ///
    /// Any existing chunks that meet the size limit will be left unmodified.
    ///
    /// The final chunk will often be a "partial" chunk that does not meet the
    /// size limit. Future calls to `compact` will merge any new chunks into
    /// this partial chunk until it meets the limit, at which point the cycle
    /// begins anew.
    ///
    /// Returns the total number of rows in this memory segment.
    fn compact(&mut self, config: &Config) -> usize {
        if self.active_chunk.is_empty() {
            return 0;
        }

        let mut last_ix = 0;
        let mut indices = vec![];
        let mut cur_rows = 0;
        let mut all_rows = 0;
        for (ix, chunk) in self.active_chunk.iter().enumerate() {
            all_rows += chunk.len();
            cur_rows += chunk.len();
            if cur_rows >= config.min_full_chunk_len {
                let next = ix + 1;
                trace!("full chunk {:?} {}", last_ix..next, cur_rows);
                indices.push(last_ix..next);
                last_ix = next;
                cur_rows = 0;
            }
        }

        fn log_concat(slice: &[SegmentChunk]) -> Option<SegmentChunk> {
            if slice.len() > 1 {
                match chunk::concatenate(slice) {
                    Ok(full_chunk) => Some(full_chunk),
                    Err(e) => {
                        error!("error building full chunk: {:?}", e);
                        None
                    }
                }
            } else {
                slice.first().cloned()
            }
        }

        let active = std::mem::take(&mut self.active_chunk);
        let mut compact = vec![];
        for slice in indices {
            compact.extend(log_concat(&active[slice]));
        }

        trace!("compact active segment {:?}", last_ix..active.len());
        compact.extend(log_concat(&active[last_ix..]));
        self.active_chunk = compact;

        all_rows
    }
}

pub struct State {
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
    pub fn attach(
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

        let slog = Slog {
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
        Segment::at(Slog::segment_path(root, name, segment_ix))
    }

    pub(crate) fn get_segment(&self, segment_ix: SegmentIndex) -> Segment {
        Slog::segment_from_name(&self.root, &self.name, segment_ix)
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
                .full_chunks
                .iter()
                .chain(in_mem.active_chunk.iter())
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

        let (schema, reader) = segment.iter().unwrap();
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
            .map(|d| d.metadata.records.end.0 - d.metadata.records.start.0)
            .unwrap_or(0);
        state.active_first_record_ix + active_size
    }

    pub(crate) async fn active_schema_matches(&self, other: &Schema) -> bool {
        let active = &self.state.read().await.active;

        let matches = active.as_ref().map(|s| &s.schema == other).unwrap_or(true);
        if !matches {
            trace!("{:?} != {:?}", active.as_ref().map(|s| &s.schema), other);
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
        counter!("slog_roll", 1, "name" => self.name.clone());
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

        let size = d.chunk.len();
        let active = self.active.get_or_insert_with(|| MemorySegment {
            schema: d.schema,
            metadata: SegmentData {
                index,
                size: 0,
                records: first..first,
                time: data_time_range.clone(),
            },
            full_chunks: vec![],
            full_chunk_len: 0,
            active_chunk: vec![],
        });

        active.metadata.size += size;
        active.metadata.records.end += size;
        active.metadata.time = min(*active.metadata.time.start(), *data_time_range.start())
            ..=max(*active.metadata.time.end(), *data_time_range.end());
        active.active_chunk.push(d.chunk);

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
    /// records in memory that were not stored in the last checkpoint. If `seal`
    /// is set, the request additionally indicates that the current segment
    /// should be finalized via `close()`, which writes the parquet footer and
    /// syncs the file.
    async fn checkpoint(&mut self, seal: bool) -> bool {
        if let Some(segment) = &mut self.active {
            let active_rows = segment.compact(&self.config);

            let active_start = segment.metadata.records.start;
            let records = active_start..(active_start + segment.full_chunk_len + active_rows);
            if !segment.active_chunk.is_empty() && records.end > self.active_checkpoint.record {
                let mut full_chunks = std::mem::take(&mut segment.active_chunk);
                let active_chunk = full_chunks
                    .split_off(full_chunks.len() - 1)
                    .into_iter()
                    .next()
                    .unwrap(); // SAFETY: !segment.active_chunk.is_empty(), so full_chunks.len() > 0

                let request = AppendRequest {
                    seal,
                    segment: self.active_checkpoint.segment,
                    records: records.clone(),
                    time: segment.metadata.time.clone(),
                    schema: segment.schema.clone(),
                    full_chunks: full_chunks.clone(),
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
                    segment.full_chunk_len += full_chunks.iter().map(|c| c.len()).sum::<usize>();
                    segment.full_chunks.extend(full_chunks);
                    segment.active_chunk = vec![active_chunk];
                    self.active_checkpoint.record = records.end;
                } else {
                    full_chunks.push(active_chunk);
                    segment.active_chunk = full_chunks;
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
        let mut current: Option<(Schema, SegmentWriter2, SegmentIndex)> = None;
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
                                .create2(schema, config.clone())
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
                        u64::try_from(record_len).unwrap(),
                        "name" => name.clone()
                    );

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
    use crate::chunk::LegacyRecords;

    use super::*;
    use chrono::{TimeZone, Utc};
    use std::path::PathBuf;
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
        use tracing_subscriber::{fmt, EnvFilter};
        fmt().with_env_filter(EnvFilter::from_default_env()).init();
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

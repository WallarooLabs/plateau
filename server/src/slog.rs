//! The slog (segment log) is a sequence of segments with a well-known storage
//! location.
//!
//! Currently, local files named with a simple logical index is the only
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
use crate::chunk::{Schema, TimeRange};
use crate::manifest::SegmentData;
#[cfg(test)]
use crate::segment::Record;
use crate::segment::{CloseArrow, Segment, SegmentWriter2};
use anyhow::Result;
use chrono::{DateTime, Utc};
use log::{debug, trace};
use metrics::counter;
use plateau_transport::{SchemaChunk, SegmentChunk};
use std::cmp::{max, min};
use std::convert::TryFrom;
use std::ops::{Add, AddAssign, Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;

#[derive(Error, Debug)]
pub enum SlogError {
    #[error("writer thread busy")]
    WriterThreadBusy,
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
}

/// Each chunk inside a segment has its own segment-scoped unique index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub(crate) struct SegmentChunkIndex(pub(crate) usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub(crate) segment: SegmentIndex,
    pub(crate) chunk: SegmentChunkIndex,
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
pub(crate) struct Slog {
    root: PathBuf,
    name: String,
    state: RwLock<State>,
}

#[derive(Clone, Debug)]
struct MemorySegment {
    metadata: SegmentData,
    schema: Schema,
    chunks: Vec<SegmentChunk>,
}

impl MemorySegment {
    fn record_count(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }
}

pub struct State {
    active: Option<MemorySegment>,
    active_checkpoint: Checkpoint,
    active_first_record_ix: RecordIndex,
    writer: mpsc::Sender<AppendRequest>,
    handle: Option<JoinHandle<()>>,
    pending: Option<MemorySegment>,
}

struct AppendRequest {
    seal: bool,
    segment: SegmentIndex,
    records: Range<RecordIndex>,
    time: RangeInclusive<DateTime<Utc>>,
    schema: Schema,
    chunks: Vec<SegmentChunk>,
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
    ) -> (Self, SlogWrites) {
        let (writer, rx, handle) = spawn_slog_thread(root.clone(), name.clone());
        let active_checkpoint = Checkpoint {
            segment: active_segment,
            chunk: SegmentChunkIndex(0),
        };
        let state = State {
            active: None,
            pending: None,
            active_checkpoint,
            active_first_record_ix,
            writer,
            handle: Some(handle),
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

        self.iter_segment(segment)
            .await
            .map(|chunk| LegacyRecords::try_from(chunk).unwrap().0)
            .flatten()
            .skip(relative)
            .next()
    }

    pub(crate) async fn iter_segment<'a>(
        &'a self,
        ix: SegmentIndex,
    ) -> Box<dyn Iterator<Item = SchemaChunk<Schema>> + Send + 'a> {
        let state = self.state.read().await;
        if ix > state.active_checkpoint.segment {
            return Box::new(std::iter::empty());
        }

        if let Some(in_mem) = state.get_segment(ix).await {
            let schema = in_mem.schema.clone();
            // NOTE: the backing [Buffer] behind arrow2 arrays is actually
            // arc-ed, so this clone _should_ be relatively cheap.
            return Box::new(
                in_mem
                    .chunks
                    .clone()
                    .into_iter()
                    .map(move |chunk| SchemaChunk {
                        schema: schema.clone(),
                        chunk,
                    }),
            );
        }

        let segment = self.get_segment(ix);

        if !Path::new(segment.path()).exists() {
            return Box::new(std::iter::empty());
        }

        let (schema, iter) = segment.read2().unwrap().into_chunk_iter();
        Box::new(iter.map(move |c| SchemaChunk {
            schema: schema.clone(),
            chunk: c.unwrap(),
        }))
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
}

impl State {
    pub(crate) async fn append(&mut self, d: SchemaChunk<Schema>) -> Checkpoint {
        if let Some(segment) = self.active.as_ref() {
            if segment.schema != d.schema {
                debug!("schema change after {:?}", segment.metadata.index);
                // TODO: bubble this failure up
                self.roll().await.unwrap();
            }
        }
        let chunk = SegmentChunkIndex(
            self.active
                .as_ref()
                .map(|segment| segment.chunks.len())
                .unwrap_or(0),
        );
        let first = self.active_first_record_ix;
        let index = self.active_checkpoint.segment;
        let data_time_range = d.time_range().unwrap();

        let size = d.chunk.len();
        if let Some(active) = &mut self.active {
            active.metadata.size += size;
            active.metadata.records.end += size;
            active.metadata.time = min(*active.metadata.time.start(), *data_time_range.start())
                ..=max(*active.metadata.time.end(), *data_time_range.end());
            active.chunks.push(d.chunk);
        } else {
            self.active = Some(MemorySegment {
                schema: d.schema,
                metadata: SegmentData {
                    index,
                    size,
                    records: first..(first + size),
                    time: data_time_range,
                },
                chunks: vec![d.chunk],
            });
        }

        Checkpoint {
            segment: index,
            chunk,
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
        if let Some(segment) = &self.active {
            let chunk_size = segment.chunks.len();
            let append_indices = self.active_checkpoint.chunk.0..chunk_size;
            let record_size = segment.chunks[append_indices.clone()]
                .iter()
                .map(|c| c.len())
                .sum();
            let start = self.active_first_record_ix;
            let records = start..(start + record_size);
            if append_indices.end - append_indices.start == 0 {
                return true;
            }
            // TODO make timeout configurable
            if timeout(
                Duration::from_millis(100),
                self.writer.send(AppendRequest {
                    seal,
                    segment: self.active_checkpoint.segment,
                    records,
                    time: segment.metadata.time.clone(),
                    schema: segment.schema.clone(),
                    chunks: segment.chunks[append_indices.clone()].to_vec(),
                }),
            )
            .await
            .is_ok()
            {
                self.active_checkpoint.chunk = SegmentChunkIndex(append_indices.end);
                true
            } else {
                false
            }
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
                self.active_checkpoint.chunk = SegmentChunkIndex(0);
                self.active_first_record_ix += records;
                self.pending = std::mem::replace(&mut self.active, None);
                Ok(())
            } else {
                Err(anyhow::Error::new(SlogError::WriterThreadBusy))
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let (tx, _) = mpsc::channel(1);
        let messages = std::mem::replace(&mut self.writer, tx);
        drop(messages);
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
    }
}

fn spawn_slog_thread(
    root: PathBuf,
    name: String,
) -> (mpsc::Sender<AppendRequest>, SlogWrites, JoinHandle<()>) {
    let (tx, mut rx_records) = mpsc::channel(1);
    let (tx_done, rx) = mpsc::channel(1);

    let handle = std::thread::spawn(move || {
        let mut active = true;
        let mut current: Option<(Schema, SegmentWriter2, SegmentIndex)> = None;
        while active {
            match rx_records.blocking_recv() {
                Some(AppendRequest {
                    seal,
                    segment,
                    records,
                    time,
                    schema,
                    chunks,
                }) => {
                    let new_segment = Slog::segment_from_name(&root, &name, segment);
                    current = current.and_then(|(schema, writer, id)| {
                        if id != segment {
                            trace!("segment change {:?} {:?}", id, segment);
                            writer.close().expect("sealed segment");
                            None
                        } else {
                            Some((schema, writer, id))
                        }
                    });
                    let (schema, ref mut writer, _) = current.get_or_insert_with(|| {
                        trace!("opening segment {:?}", segment);
                        (
                            schema.clone(),
                            new_segment.create2(schema).expect("segment creation"),
                            segment,
                        )
                    });

                    for chunk in chunks.into_iter() {
                        let count = chunk.len();
                        writer
                            .log_arrow(SchemaChunk {
                                schema: schema.clone(),
                                chunk,
                            })
                            .expect("added records");
                        counter!("slog_thread_records_written", u64::try_from(count).unwrap(), "name" => name.clone());
                    }
                    let size = writer.size_estimate().expect("segment size estimate");

                    if seal {
                        current
                            .take()
                            .map(|(_, w, _)| w.close().expect("segment close"));
                        trace!("sealed {:?} {:?}", segment, records);
                    }

                    let response = WriteResult {
                        data: SegmentData {
                            index: segment,
                            records,
                            time,
                            size,
                        },
                    };
                    tx_done.blocking_send(response).expect("channel closed");
                }
                None => {
                    current.take().map(|(_, writer, _)| writer.close());
                    active = false
                }
            }
        }
    });

    (tx, rx, handle)
}

#[cfg(test)]
mod test {
    use crate::chunk::LegacyRecords;

    use super::*;
    use chrono::{TimeZone, Utc};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[tokio::test]
    async fn basic_sequencing() -> Result<()> {
        let root = tempdir().unwrap();
        let (slog, mut commits) = Slog::attach(
            PathBuf::from(root.path()),
            String::from("testing"),
            SegmentIndex(0),
            RecordIndex(0),
        );
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: message.bytes().collect(),
            })
            .collect();

        let chunk = SchemaChunk::try_from(LegacyRecords(records.clone()))?;

        let first = slog.append(chunk).await;
        for ix in 0..3 {
            assert_eq!(
                slog.get_record(first.segment, ix).await,
                Some(records[ix].clone())
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

        for ix in 0..3 {
            assert_eq!(
                slog.get_record(second.segment, ix).await,
                Some(records[ix].clone())
            );
        }

        Ok(())
    }
}

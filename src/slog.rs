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
use crate::manifest::SegmentData;
use crate::segment::{Record, Segment};
use chrono::{DateTime, Utc};
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::{future, stream, Stream};
use metrics::counter;
use std::cmp::{max, min};
use std::convert::TryFrom;
use std::fs;
use std::ops::{Add, AddAssign, Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;

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

/// Each record inside a segment has its own segment-scoped unique index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct SegmentRecordIndex(pub(crate) usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalIndex {
    pub segment: SegmentIndex,
    pub record: SegmentRecordIndex,
}

/// Each record also has a global unique sequential index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RecordIndex(pub usize);

impl RecordIndex {
    pub fn rewind(&self, count: usize) -> Option<Self> {
        if count >= self.0 {
            Some(RecordIndex(self.0 - count))
        } else {
            None
        }
    }
}

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

pub struct State {
    active: Vec<Record>,
    active_checkpoint: InternalIndex,
    active_first_record_ix: RecordIndex,
    active_size: usize,
    writer: mpsc::Sender<AppendRequest>,
    handle: Option<JoinHandle<()>>,
    pending: Option<Vec<Record>>,
    time_range: Option<RangeInclusive<DateTime<Utc>>>,
}

struct AppendRequest {
    seal: bool,
    segment: SegmentIndex,
    index: Range<RecordIndex>,
    time: RangeInclusive<DateTime<Utc>>,
    append_records: Vec<Record>,
}

#[derive(Debug)]
pub(crate) struct WriteResult {
    pub(crate) segment: SegmentIndex,
    pub(crate) data: SegmentData,
}

impl Slog {
    //! Because a slog is stateless, whatever attaches it is responsible for processing
    //! commit events. The channel is bounded to a size of one; if it is not consumed,
    //! the writer thread will immediately stall.
    pub fn attach(
        root: PathBuf,
        name: String,
        active_checkpoint: InternalIndex,
        active_first_record_ix: RecordIndex,
    ) -> (Self, SlogWrites) {
        let (writer, rx, handle) = spawn_slog_thread(root.clone(), name.clone());
        let state = State {
            active_checkpoint,
            active_first_record_ix,
            active: vec![],
            active_size: 0,
            pending: None,
            time_range: None,
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

    fn segment_path(root: &PathBuf, name: &str, segment_ix: SegmentIndex) -> PathBuf {
        let file = PathBuf::from(format!("{}-{}", name, segment_ix.0));
        root.join(file)
    }

    pub(crate) fn segment_from_name(
        root: &PathBuf,
        name: &str,
        segment_ix: SegmentIndex,
    ) -> Segment {
        Segment::at(Slog::segment_path(root, name, segment_ix))
    }

    pub(crate) fn get_segment(&self, segment_ix: SegmentIndex) -> Segment {
        Slog::segment_from_name(&self.root, &self.name, segment_ix)
    }

    pub(crate) async fn get_record(&self, ix: InternalIndex) -> Option<Record> {
        self.get_records_for_segment(ix.segment)
            .await
            .get(ix.record.0)
            .cloned()
    }

    pub(crate) fn segment_stream<'a>(
        &'a self,
        indices: impl Stream<Item = SegmentIndex> + 'a,
    ) -> impl Stream<Item = Vec<Record>> + 'a {
        indices
            .flat_map(move |index| self.get_records_for_segment(index).into_stream())
            .take_while(|rs| future::ready(rs.len() > 0))
    }

    pub(crate) async fn get_records_for_segment(&self, ix: SegmentIndex) -> Vec<Record> {
        let state = self.state.read().await;
        if ix > state.active_checkpoint.segment {
            return vec![];
        }
        state.get_segment(ix).await.cloned().unwrap_or_else(|| {
            let segment = self.get_segment(ix);
            if Path::new(segment.path()).exists() {
                segment.read().read_all()
            } else {
                vec![]
            }
        })
    }

    pub(crate) async fn append(&self, r: &Record) -> InternalIndex {
        self.state.write().await.append(r).await
    }

    pub(crate) fn destroy(&self, segment_ix: SegmentIndex) {
        fs::remove_file(Slog::segment_path(&self.root, &self.name, segment_ix)).unwrap()
    }

    pub(crate) async fn current_segment_ix(&self) -> SegmentIndex {
        self.state.read().await.active_checkpoint.segment
    }

    pub(crate) async fn current_len(&self) -> usize {
        self.state.read().await.active.len()
    }

    pub(crate) async fn current_size(&self) -> usize {
        self.state.read().await.active_size
    }

    pub(crate) async fn current_time_range(&self) -> Option<RangeInclusive<DateTime<Utc>>> {
        self.state.read().await.time_range.clone()
    }

    pub(crate) async fn roll(&self) -> bool {
        counter!("slog_roll", 1, "name" => self.name.clone());
        self.state.write().await.roll().await
    }

    pub(crate) async fn checkpoint(&self) -> bool {
        self.state.write().await.checkpoint(false).await
    }
}

impl State {
    pub(crate) async fn append(&mut self, r: &Record) -> InternalIndex {
        let record = SegmentRecordIndex(self.active.len());
        self.active_size += r.message.len();
        self.active.push(r.clone());
        self.time_range = self
            .time_range
            .clone()
            .map(|range| (min(*range.start(), r.time)..=max(*range.end(), r.time)))
            .or(Some(r.time..=r.time));
        InternalIndex {
            segment: self.active_checkpoint.segment,
            record,
        }
    }

    async fn get_segment(&self, ix: SegmentIndex) -> Option<&Vec<Record>> {
        if ix == self.active_checkpoint.segment {
            Some(&self.active)
        } else {
            match &self.pending {
                Some(pending) if ix.next() == self.active_checkpoint.segment => {
                    self.pending.as_ref()
                }
                _ => None,
            }
        }
    }

    pub(crate) async fn get_record(&self, ix: InternalIndex) -> Option<Record> {
        self.get_segment(ix.segment)
            .await
            .and_then(|rs| rs.get(ix.record.0).cloned())
    }

    /// Checkpoints make an `AppendRequest` to the writer thread for for all
    /// records in memory that were not stored in the last checkpoint. If `seal`
    /// is set, the request additionally indicates that the current segment
    /// should be finalized via `close()`, which writes the parquet footer and
    /// syncs the file.
    async fn checkpoint(&mut self, seal: bool) -> bool {
        let append_indices = self.active_checkpoint.record.0..self.active.len();
        let start = self.active_first_record_ix;
        let index = start..(start + self.active.len());
        if append_indices.end - append_indices.start == 0 {
            return true;
        }
        if let Some(time_range) = &self.time_range {
            // TODO make timeout configurable
            if timeout(
                Duration::from_millis(100),
                self.writer.send(AppendRequest {
                    seal,
                    segment: self.active_checkpoint.segment,
                    index,
                    time: time_range.clone(),
                    append_records: self.active[append_indices.clone()]
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                }),
            )
            .await
            .is_ok()
            {
                self.active_checkpoint.record = SegmentRecordIndex(append_indices.end);
                true
            } else {
                false
            }
        } else {
            true
        }
    }

    pub(crate) async fn roll(&mut self) -> bool {
        if self.checkpoint(true).await {
            self.active_checkpoint.segment = self.active_checkpoint.segment.next();
            self.active_checkpoint.record = SegmentRecordIndex(0);
            self.active_first_record_ix += self.active.len();
            self.active_size = 0;
            self.pending = Some(std::mem::replace(&mut self.active, vec![]));
            self.time_range = None;
            true
        } else {
            panic!("log overrun")
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let (tx, _) = mpsc::channel(1);
        let messages = std::mem::replace(&mut self.writer, tx);
        drop(messages);
        self.handle.take().map(|h| h.join().unwrap());
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
        let mut current: Option<(crate::segment::SegmentWriter, SegmentIndex)> = None;
        while active {
            match rx_records.blocking_recv() {
                Some(AppendRequest {
                    seal,
                    segment,
                    index,
                    time,
                    append_records,
                }) => {
                    let new_segment = Slog::segment_from_name(&root, &name, segment);
                    current = current.and_then(|(writer, id)| {
                        if id != segment {
                            writer.close();
                            None
                        } else {
                            Some((writer, id))
                        }
                    });
                    let (ref mut writer, _) =
                        current.get_or_insert_with(|| (new_segment.create(), segment));
                    let count = append_records.len();
                    writer.log(append_records);
                    let size = writer.size_estimate();
                    counter!("slog_thread_records_written", u64::try_from(count).unwrap(), "name" => name.clone());

                    if seal {
                        current.take().map(|(w, _)| w.close());
                    }

                    let response = WriteResult {
                        segment,
                        data: SegmentData { index, time, size },
                    };
                    tx_done.blocking_send(response).expect("channel closed");
                }
                None => {
                    current.take().map(|(writer, _)| writer.close());
                    active = false
                }
            }
        }
    });

    (tx, rx, handle)
}

mod test {
    use super::*;
    use chrono::{TimeZone, Utc};
    use parquet::data_type::ByteArray;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[tokio::test]
    async fn basic_sequencing() {
        let root = tempdir().unwrap();
        let (slog, mut commits) = Slog::attach(
            PathBuf::from(root.path()),
            String::from("testing"),
            InternalIndex {
                segment: SegmentIndex(0),
                record: SegmentRecordIndex(0),
            },
            RecordIndex(0),
        );
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp(0, 0),
                message: ByteArray::from(message),
            })
            .collect();

        let abc = slog.append(&records[0]).await;
        assert_eq!(slog.get_record(abc.clone()).await, Some(records[0].clone()));
        slog.roll().await;
        assert_eq!(
            commits
                .recv()
                .await
                .map(|r| (r.data.index, r.data.size > 0)),
            Some((RecordIndex(0)..RecordIndex(1), true))
        );
        let def = slog.append(&records[1]).await;
        let ghi = slog.append(&records[2]).await;
        assert!(slog.roll().await);
        assert_eq!(
            commits
                .recv()
                .await
                .map(|r| (r.data.index, r.data.size > 0)),
            Some((RecordIndex(1)..RecordIndex(3), true))
        );

        assert_eq!(slog.get_record(abc).await, Some(records[0].clone()));
        assert_eq!(slog.get_record(def).await, Some(records[1].clone()));
        assert_eq!(slog.get_record(ghi).await, Some(records[2].clone()));
    }
}

//! Backend agnostic keyed batch [Transmission] utility. Supports batch collation
//! and splitting alongside element count and byte size limits.
//!
//! Requires downstream consumers to implement:
//! - [Batch] for data to transmit.
//! - [BatchSender] for the mechanism used to transmit a [Batch] of data.
use async_trait::async_trait;
use core::ops::Range;
use log::{error, trace};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::timeout;

use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum TransmissionError {
    #[error("transmission queue full")]
    QueueFull,
}

pub type Result<T> = std::result::Result<T, TransmissionError>;

// DEFAULT_MAX_BATCH_BYTES maximum number of bytes in a batch
const DEFAULT_MAX_BATCH_BYTES: usize = 100 * 1024;
// DEFAULT_MAX_BATCH_SIZE how many events to collect in a batch
const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
// DEFAULT_BATCH_TIMEOUT how frequently to send unfilled batches
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_millis(1000);
// DEFAULT_PENDING_WORK_CAPACITY how many events to queue up for busy batches
const DEFAULT_PENDING_WORK_CAPACITY: usize = 10_000;

/// Options includes various options to tweak the behavior of the sender.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Options {
    /// byte cutoff for a batch
    pub max_batch_bytes: usize,

    /// how many events to collect into a batch before sending. Overrides
    /// DEFAULT_MAX_BATCH_SIZE.
    pub max_batch_size: usize,

    /// how often to send off batches. Overrides DEFAULT_BATCH_TIMEOUT.
    #[serde(with = "humantime_serde")]
    pub batch_timeout: Duration,

    /// how many events to allow to pile up. Overrides DEFAULT_PENDING_WORK_CAPACITY
    pub pending_work_capacity: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            pending_work_capacity: DEFAULT_PENDING_WORK_CAPACITY,
        }
    }
}

#[async_trait]
pub trait BatchSender: Send + Sync + 'static {
    type Batch;

    /// Send a batch of data. Returns false if the send failed and a retry
    /// should be attempted.
    async fn send_batch(&self, key: &str, batch: Self::Batch, clock: Instant) -> bool;
}

pub trait Batch: Sized {
    /// Keys are used to group batches into separate queues for transmission.
    fn key(&self) -> &str;

    /// The number of events in this batch.
    fn len(&self) -> usize;

    /// The byte size of this batch.
    fn bytes(&self) -> usize;

    /// The maximum size of a single element of this batch.
    fn max_element_bytes(&self) -> usize;

    /// Extend this batch by adding all elements of `other` to the end.
    fn extend(&mut self, other: Self);

    /// Create a subset of this batch with the given range indices.
    fn slice(&self, ixs: Range<usize>) -> Self;

    /// Attempt to extend this batch with another. Return the original batch
    /// if that extension would result in a batch that violates the configured
    /// size limits.
    ///
    /// By default, both event and byte limits are checked against a simple sum
    /// of the respective sizes of the two smaller batches.
    fn check_extend(&mut self, other: Self, config: &Options) -> Option<Self> {
        let ok_len = self.len() + other.len() <= config.max_batch_size;
        let ok_bytes = self.bytes() + other.bytes() <= config.max_batch_bytes;
        if ok_len && ok_bytes {
            self.extend(other);
            None
        } else {
            trace!(
                "extend rejected. {}",
                if !ok_bytes {
                    format!(
                        "self bytes: {}, other bytes: {}",
                        self.bytes(),
                        other.bytes()
                    )
                } else {
                    format!("self len: {}, other len: {}", self.len(), other.len())
                }
            );
            Some(other)
        }
    }

    /// Split this batch into smaller batches that respect the configured byte limits.
    ///
    /// By default, this uses [Batch::max_element_bytes] to calculate the
    /// largest possible event size. This is used to derive to a byte-limited
    /// event count by simple division with the max bytes per batch.
    ///
    /// It then splits this batch into equal-length smaller batches. The smaller
    /// of the byte-limited event count and the configured max number of events
    /// is used for the split batch size. The final batch may be smaller
    /// depending on the alignment of the smaller batch size and the larger
    /// batch.
    fn split(self, config: &Options) -> Vec<Self> {
        let byte_limited_event_count = config.max_batch_bytes / self.max_element_bytes();
        let stride = std::cmp::min(byte_limited_event_count, config.max_batch_size);
        if stride == 0 {
            error!(
                "dropping batch: max element bytes for batch is {}",
                self.max_element_bytes(),
            );
            vec![]
        } else {
            let end = self.len();
            if stride >= end {
                vec![self]
            } else {
                let starts = (0..end).step_by(stride);
                starts
                    .map(|start| self.slice(start..std::cmp::min(end, start + stride)))
                    .collect()
            }
        }
    }
}

/// [Transmission] is a queue reshaper that enables differential input and
/// output batch rates and sizes.
///
/// A [Batch] of events can be queued via [Transmission::send]. [Transmission]
/// is then responsible for restructuring and queueing those batches as defined
/// in its [Options]. It will finally send the reshaped batches out via a
/// configured [BatchSender].
///
/// It handles several important concerns:
/// - collation of many small batches into a larger batch for efficiency.
/// - splitting of large batches into smaller batches as required by configured
///   size limits.
/// - retries of failed batches on errors.
/// - queue flushing at regular and configurable intervals.
#[derive(Debug, Clone)]
pub struct Transmission<E: Clone> {
    work_sender: Sender<E>,
}

impl<B: Clone + Batch + Send + 'static> Transmission<B> {
    pub fn send(&self, event: B) -> Result<()> {
        self.work_sender
            .try_send(event)
            .map_err(|_| TransmissionError::QueueFull)
    }
}

impl<B: Clone + Batch + Send + 'static> Transmission<B> {
    pub fn start(runtime: Handle, sender: impl BatchSender<Batch = B>, options: Options) -> Self {
        let (work_sender, work_receiver) = channel(options.pending_work_capacity);

        runtime
            .spawn(async move { Self::process_work(work_receiver, sender, options.clone()).await });

        Self { work_sender }
    }

    async fn process_work(
        mut work_receiver: Receiver<B>,
        sender: impl BatchSender<Batch = B>,
        options: Options,
    ) {
        let mut batches: HashMap<String, VecDeque<B>> = HashMap::new();
        let mut send = true;

        while send {
            let mut expired = false;
            match timeout(options.batch_timeout, work_receiver.recv()).await {
                Ok(Some(batch)) => {
                    let key = String::from(batch.key());
                    let queue = batches.entry(key).or_insert(VecDeque::new());

                    for part in batch.split(&options) {
                        // try and add onto the last batch in the queue.
                        // if the batches cannot be combined, push the new batch
                        // onto the end of the queue instead.
                        match queue.back_mut() {
                            Some(end) => end
                                .check_extend(part, &options)
                                .map(|reject| queue.push_back(reject)),
                            // if there is no last batch in queue, push onto the
                            // queue to make one.
                            None => Some(queue.push_back(part)),
                        };
                    }
                }
                Ok(None) => {
                    send = false;
                }
                Err(_) => {
                    expired = true;
                }
            };

            for (key, queue) in batches.iter_mut() {
                if queue.len() > 1 {
                    trace!("queue {} has at least one ready batch", key);
                    Self::send_queue(&sender, key, queue, 1).await;
                }
                if expired && queue.len() > 0 {
                    trace!(
                        "queue {} timeout {:?} exceeded, sending all batches",
                        key,
                        options.batch_timeout
                    );
                    Self::send_queue(&sender, key, queue, 0).await;
                }
            }
        }
    }

    async fn send_queue(
        sender: &impl BatchSender<Batch = B>,
        key: &str,
        queue: &mut VecDeque<B>,
        depth: usize,
    ) {
        while queue.len() > depth {
            if let Some(batch) = queue.pop_front() {
                let batch_clone = batch.clone();
                if sender.send_batch(key, batch, Instant::now()).await {
                    trace!("sent batch for {} - queue size {}", key, queue.len());
                } else {
                    queue.push_front(batch_clone);
                    trace!(
                        "failed to send batch for {} - queue size {}",
                        key,
                        queue.len()
                    );
                    break;
                }
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use parking_lot::Mutex;

    impl Batch for Vec<String> {
        fn key(&self) -> &str {
            "single-batch"
        }

        fn len(&self) -> usize {
            self.len()
        }

        fn bytes(&self) -> usize {
            self.iter().map(|s| s.len()).sum()
        }

        fn max_element_bytes(&self) -> usize {
            self.iter().map(|s| s.len()).max().unwrap_or(1)
        }

        fn extend(&mut self, other: Self) {
            Extend::extend(self, other);
        }

        fn slice(&self, ixs: Range<usize>) -> Self {
            self[ixs].to_vec()
        }
    }

    #[derive(Clone, Debug)]
    struct RecordReader {
        sent: Arc<Mutex<Vec<Vec<String>>>>,
        expected: usize,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for RecordReader {
        type Output = Vec<Vec<String>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.sent.lock().len() >= self.expected {
                Poll::Ready(self.sent.lock().clone())
            } else {
                drop(self.waker.lock().insert(cx.waker().clone()));
                Poll::Pending
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RecordWriter {
        sent: Arc<Mutex<Vec<Vec<String>>>>,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    #[async_trait]
    impl BatchSender for RecordWriter {
        type Batch = Vec<String>;

        async fn send_batch(&self, _key: &str, batch: Vec<String>, _time: Instant) -> bool {
            assert!(batch.len() > 0);
            let mut writer = self.sent.lock();
            writer.push(batch);
            if let Some(waker) = &mut *self.waker.lock() {
                waker.wake_by_ref();
            }
            true
        }
    }

    fn recorder(expected: usize) -> (RecordWriter, RecordReader) {
        let sent = Arc::new(Mutex::new(vec![]));
        let waker = Arc::new(Mutex::new(None));
        let reader = RecordReader {
            expected,
            sent: sent.clone(),
            waker: waker.clone(),
        };

        let writer = RecordWriter { sent, waker };

        (writer, reader)
    }

    fn stringify(v: Vec<Vec<&str>>) -> Vec<Vec<String>> {
        v.into_iter()
            .map(|i| i.into_iter().map(|s| String::from(s)).collect())
            .collect()
    }

    #[tokio::test]
    async fn test_single_batch() {
        let (writer, reader) = recorder(1);
        let transmission = Transmission::start(
            Handle::current(),
            writer,
            Options {
                max_batch_size: 5,
                ..Options::default()
            },
        );

        for i in 0..5 {
            transmission.send(vec![format!("event {}", i)]).unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            stringify(vec![vec![
                "event 0", "event 1", "event 2", "event 3", "event 4"
            ]])
        );
    }

    #[tokio::test]
    async fn test_multi_batch() {
        let (writer, reader) = recorder(2);
        let transmission = Transmission::start(
            Handle::current(),
            writer,
            Options {
                max_batch_size: 2,
                ..Options::default()
            },
        );

        for i in 0..5 {
            transmission.send(vec![format!("event {}", i)]).unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            stringify(vec![vec!["event 0", "event 1"], vec!["event 2", "event 3"]])
        );
    }

    #[tokio::test]
    async fn test_batch_split() {
        let (writer, reader) = recorder(2);
        let transmission = Transmission::start(
            Handle::current(),
            writer,
            Options {
                max_batch_size: 2,
                ..Options::default()
            },
        );

        transmission
            .send((0..5).map(|i| format!("event {}", i)).collect())
            .unwrap();

        let batches = reader.await;
        assert_eq!(
            batches,
            stringify(vec![vec!["event 0", "event 1"], vec!["event 2", "event 3"]])
        );
    }

    #[tokio::test]
    async fn test_byte_limit() {
        let (writer, reader) = recorder(2);
        let transmission = Transmission::start(
            Handle::current(),
            writer,
            Options {
                max_batch_bytes: 15,
                ..Options::default()
            },
        );

        for i in 0..5 {
            transmission.send(vec![format!("event {}", i)]).unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            stringify(vec![vec!["event 0", "event 1"], vec!["event 2", "event 3"]])
        );
    }
}

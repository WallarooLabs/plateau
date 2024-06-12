//! Backend agnostic keyed batch [Transmission] utility. Supports batch collation
//! and splitting alongside element count and byte size limits.
//!
//! Requires downstream consumers to implement:
//! - [Batch] for data to transmit.
//! - [BatchSender] for the mechanism used to transmit a [Batch] of data.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{error, warn};

use plateau_transport::InsertQuery;

use crate::{Client, Error as ClientError, Insertion, MaxRequestSize};

static DEFAULT_MAX_BATCH_BYTES: AtomicUsize = AtomicUsize::new(100 * 1024);

pub fn get_default_max_batch_bytes() -> usize {
    DEFAULT_MAX_BATCH_BYTES.load(Ordering::Relaxed)
}

pub fn update_default_max_batch_bytes(max: usize) {
    DEFAULT_MAX_BATCH_BYTES.store(max, Ordering::Relaxed);
}

#[derive(Clone, Debug, Error)]
pub enum TransmissionError {
    #[error("transmission queue full")]
    QueueFull,
}

pub type TransmissionResult<T> = Result<T, TransmissionError>;
pub type BatchResult = Result<(), BatchSendError>;

#[derive(Debug)]
pub enum BatchSendError {
    /// The batch failed to send, and should be retried as-is
    Retriable,
    /// The batch failed to send, and should be resized before retrying
    Resize,
    /// The batch failed with a permanent error
    Fail,
}

#[derive(Clone, Debug)]
pub struct BatchClient {
    client: Client,
    pub max_batch_bytes: usize,
}

impl BatchClient {
    pub fn new(base_url: &str) -> Result<Self, ClientError> {
        Ok(Self {
            client: Client::new(base_url)?,
            max_batch_bytes: get_default_max_batch_bytes(),
        })
    }

    /// Attempt to send a single batch to plateau
    pub async fn post_batch<B: Batch + Clone + Insertion>(
        &mut self,
        batch: &B,
        partition: &str,
    ) -> Result<(), BatchSendError> {
        self.client
            .append_records(
                batch.key(),
                partition,
                &InsertQuery { time: None },
                batch.clone(),
            )
            .await
            .map_err(|e| match e {
                ClientError::RequestTooLong(_, MaxRequestSize(Some(s))) => {
                    warn!("detected new max plateau row size: {s}");
                    update_default_max_batch_bytes(s);
                    self.max_batch_bytes = s;
                    BatchSendError::Resize
                }
                ClientError::RequestTooLong(_, MaxRequestSize(None)) => {
                    error!("batch send failed with 413, and no new size limit could be detected");
                    BatchSendError::Fail
                }
                _ => BatchSendError::Retriable,
            })
            .map(|_| ())
    }
}

#[async_trait]
pub trait BatchSender: Send + Sync + Clone + 'static {
    type Batch;

    fn pending_work_capacity(&self) -> usize;

    /// Send a batch, reshaping as necessary and sending in multiple requests. If [retain] is true,
    /// the data from the last (or only) request is retained and returned as a new batch. Any failed
    /// requests are passed to [handler].
    async fn send_batch(&mut self, retain: bool, batch: Self::Batch) -> Option<Self::Batch>;
}

pub trait Batch: Sized {
    /// Keys are used to group batches into separate queues for transmission.
    fn key(&self) -> &str;

    /// The number of events in this batch.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() > 0
    }

    /// The byte size of this batch.
    fn to_bytes(&self) -> Vec<u8>;

    /// Checks whether this batch is able to be combined with another
    fn compatible_with(&self, other: &Self) -> bool;

    /// Extend this batch by adding all elements of `other` to the end.
    /// Return the original batch if the batches cannot be combined
    /// for any reason.
    fn extend(&mut self, other: Self) -> Option<Self>;

    /// Create a subset of this batch with the given range indices.
    fn slice(&self, ixs: Range<usize>) -> Self;

    /// Split this batch evenly into [n] separate batches.
    fn split_into(self, n: usize) -> Vec<Self>;

    /// Attempt to shink a batch by dropping some fragment of data from every element in the batch.
    /// If successful, the pruned data/field is returned, otherwise None. Typically only used when
    /// a batch has already been subdivided until it contains a single row.
    fn prune(&mut self) -> Option<String>;

    /// Reshape a batch into one or more smaller batches if necessary, either by subdividing the
    /// batch or by pruning data from the remaining element if a batch cannot be subdivided. If
    /// a batch cannot be further subdivided or pruned and is still larger than [max_bytes], the
    /// entire batch is dropped and this method returns an empty set.
    fn reshape_batch(mut self, max_bytes: usize) -> Vec<Self> {
        let bytes = self.to_bytes().len();

        if bytes > max_bytes {
            if self.len() > 1 {
                self.split_into(bytes / max_bytes + 1)
            } else if self.prune().is_none() {
                error!("inference log batch is too large to send ({bytes} bytes), and cannot be reduced");
                vec![]
            } else {
                vec![self]
            }
        } else {
            vec![self]
        }
    }
}

struct WorkerHandle<T> {
    sender: mpsc::Sender<T>,
    task: task::JoinHandle<()>,
}

struct WorkerPool<T, C> {
    pool: HashMap<String, WorkerHandle<T>>,
    sender: C,
}
impl<T: Clone + Batch + Send + 'static, C: BatchSender<Batch = T>> WorkerPool<T, C> {
    fn new(client: C) -> Self {
        Self {
            pool: HashMap::new(),
            sender: client,
        }
    }

    /// Queue a batch with a send worker
    async fn send_work(&mut self, batch: T) {
        // get or start a worker
        let key = batch.key().to_owned();
        let worker = if let Some(w) = self.pool.get_mut(&key) {
            w
        } else {
            let (s, r) = mpsc::channel(self.sender.pending_work_capacity());
            let worker = WorkerHandle {
                sender: s,
                task: Self::spawn_worker(r, self.sender.clone()),
            };
            self.pool.insert(key.clone(), worker);
            self.pool.get_mut(&key).unwrap()
        };

        worker
            .sender
            .send(batch)
            .await
            .unwrap_or_else(|e| error!("queueing batch to worker failed: {e}"));
    }

    /// Flush all pending work and shut down workers
    async fn drain(&mut self) {
        // flush & close workers
        for (_, worker) in self.pool.drain() {
            drop(worker.sender);
            worker
                .task
                .await
                .expect("awaiting sender completion failed")
        }
    }

    fn spawn_worker(
        mut receiver: mpsc::Receiver<T>,
        mut sender: impl BatchSender<Batch = T>,
    ) -> task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut last = None;

            loop {
                // queue all from receiver, sending as needed
                while let Ok(batch) = receiver.try_recv() {
                    last = match (last, batch) {
                        (None, batch) => Some(batch), // start a new queue
                        (Some(mut last), batch) if last.compatible_with(&batch) => {
                            // push onto existing queue and send if necessary
                            assert!(last.extend(batch).is_none());
                            sender.send_batch(true, last).await
                        }
                        (Some(last), batch) => {
                            // send existing queue and start a new one
                            assert!(sender.send_batch(false, last).await.is_none());
                            Some(batch)
                        }
                    };
                }

                // send all remaining in queue
                if let Some(batch) = last {
                    assert!(sender.send_batch(false, batch).await.is_none());
                }

                // wait for more events or sender shutdown
                last = match receiver.recv().await {
                    Some(batch) => Some(batch),
                    None => break,
                }
            }
        })
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
#[derive(Debug)]
pub struct Transmission<E> {
    work_sender: mpsc::Sender<E>,
    dispatch_task: task::JoinHandle<()>,
}

impl<B: Clone + Batch + Send + 'static> Transmission<B> {
    pub fn start(
        runtime: Handle,
        sender: impl BatchSender<Batch = B>,
        pending_capacity: usize,
    ) -> Self {
        let (work_sender, work_receiver) = mpsc::channel(pending_capacity);
        let dispatch_task = runtime.spawn(Self::dispatch_work(work_receiver, sender));

        Self {
            work_sender,
            dispatch_task,
        }
    }

    pub async fn end(self) {
        drop(self.work_sender);
        self.dispatch_task.await.unwrap();
    }

    pub fn send(&self, event: B) -> TransmissionResult<()> {
        self.work_sender
            .try_send(event)
            .map_err(|_| TransmissionError::QueueFull)
    }

    async fn dispatch_work(
        mut work_receiver: mpsc::Receiver<B>,
        sender: impl BatchSender<Batch = B>,
    ) {
        let mut workers = WorkerPool::new(sender.clone());

        loop {
            while let Ok(batch) = work_receiver.try_recv() {
                workers.send_work(batch).await;
            }

            workers.drain().await;

            match work_receiver.recv().await {
                Some(batch) => workers.send_work(batch).await,
                _ => break,
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
    use tokio::time;

    // test when bibs and bobs cannot be combined
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WidgetType {
        Bib,
        #[allow(dead_code)]
        Bob,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct WidgetBatch {
        widget_type: WidgetType,
        widgets: Vec<String>,
    }

    impl Batch for WidgetBatch {
        fn key(&self) -> &str {
            "single-batch"
        }

        fn len(&self) -> usize {
            self.widgets.len()
        }

        fn to_bytes(&self) -> Vec<u8> {
            self.widgets
                .iter()
                .map(|w| w.as_bytes().to_vec())
                .collect::<Vec<_>>()
                .concat()
        }

        fn compatible_with(&self, other: &Self) -> bool {
            self.widget_type == other.widget_type
        }

        fn extend(&mut self, other: Self) -> Option<Self> {
            if self.widget_type == other.widget_type {
                Extend::extend(&mut self.widgets, other.widgets);
                None
            } else {
                Some(other)
            }
        }

        fn slice(&self, ixs: Range<usize>) -> Self {
            Self {
                widget_type: self.widget_type,
                widgets: self.widgets[ixs].to_vec(),
            }
        }

        fn split_into(self, n: usize) -> Vec<Self> {
            self.widgets
                .chunks(f32::ceil(self.widgets.len() as f32 / n as f32) as usize)
                .map(|c| Self {
                    widget_type: self.widget_type,
                    widgets: c.to_vec(),
                })
                .collect()
        }

        fn prune(&mut self) -> Option<String> {
            todo!()
        }
    }

    impl WidgetBatch {
        fn from_iter<S, I>(widget_type: WidgetType, into_iter: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: AsRef<str>,
        {
            Self {
                widget_type,
                widgets: into_iter
                    .into_iter()
                    .map(|s| String::from(s.as_ref()))
                    .collect(),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RecordReader {
        sent: Arc<Mutex<Vec<WidgetBatch>>>,
        expected: usize,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for RecordReader {
        type Output = Vec<WidgetBatch>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.sent.lock().len() >= self.expected {
                Poll::Ready(self.sent.lock().clone())
            } else {
                *self.waker.lock() = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RecordWriter {
        delay: Option<time::Duration>,
        max_batch_len: usize,
        sent: Arc<Mutex<Vec<WidgetBatch>>>,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    #[async_trait]
    impl BatchSender for RecordWriter {
        type Batch = WidgetBatch;

        fn pending_work_capacity(&self) -> usize {
            10
        }

        async fn send_batch(&mut self, retain: bool, batch: Self::Batch) -> Option<Self::Batch> {
            if let Some(delay) = self.delay {
                // A tiny delay here helps simulate a "real" sender and prevent races between a
                // test sending events and a latency-free sender processing them
                time::sleep(delay).await;
            }

            let batch_len = batch.len();
            assert!(batch_len > 0);

            let batches =
                batch.split_into(f32::ceil(batch_len as f32 / self.max_batch_len as f32) as usize);
            let (last, rest) = batches.split_last().unwrap();

            let mut writer = self.sent.lock();
            for batch in rest {
                writer.push(batch.clone());
            }

            let result = if retain {
                Some(last.clone())
            } else {
                writer.push(last.clone());
                None
            };
            drop(writer);

            if let Some(waker) = &mut *self.waker.lock() {
                waker.wake_by_ref();
            }

            result
        }
    }

    fn recorder(
        expected: usize,
        max_batch_len: usize,
        delay: Option<time::Duration>,
    ) -> (RecordWriter, RecordReader) {
        let sent = Arc::new(Mutex::new(vec![]));
        let waker = Arc::new(Mutex::new(None));
        let reader = RecordReader {
            expected,
            sent: sent.clone(),
            waker: waker.clone(),
        };

        let writer = RecordWriter {
            delay,
            max_batch_len,
            sent,
            waker,
        };

        (writer, reader)
    }

    fn widgetify<S: AsRef<str>>(v: Vec<Vec<S>>) -> Vec<WidgetBatch> {
        v.into_iter()
            .map(|vs| WidgetBatch::from_iter(WidgetType::Bib, vs))
            .collect()
    }

    #[tokio::test]
    async fn test_dispatch_start_stop() {
        let (writer, _) = recorder(1, 1000, None);
        let transmission = Transmission::start(Handle::current(), writer, 10000);

        // send a lot of events
        for i in 0..10000 {
            transmission
                .send(WidgetBatch::from_iter(
                    WidgetType::Bib,
                    vec![format!("event {}", i)],
                ))
                .unwrap();
        }

        // transmission end awaits the dispatch task, which awaits all worker threads
        // a more in depth version of this test will require significant refactoring for DI
        assert!(
            time::timeout(time::Duration::from_secs(1), transmission.end())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_single_batch() {
        let (writer, reader) = recorder(1, 10, Some(time::Duration::from_millis(1)));
        let transmission = Transmission::start(Handle::current(), writer, 10);

        for i in 0..5 {
            transmission
                .send(WidgetBatch::from_iter(
                    WidgetType::Bib,
                    vec![format!("event {}", i)],
                ))
                .unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            widgetify(vec![vec![
                "event 0", "event 1", "event 2", "event 3", "event 4"
            ]])
        );
    }

    #[tokio::test]
    async fn test_multi_batch() {
        let (writer, reader) = recorder(3, 2, Some(time::Duration::from_millis(1)));
        let transmission = Transmission::start(Handle::current(), writer, 10);

        for i in 0..5 {
            transmission
                .send(WidgetBatch::from_iter(
                    WidgetType::Bib,
                    vec![format!("event {}", i)],
                ))
                .unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            widgetify(vec![
                vec!["event 0", "event 1"],
                vec!["event 2", "event 3"],
                vec!["event 4"]
            ])
        );
    }
}

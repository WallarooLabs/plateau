//! Backend agnostic batch transmission utility.
//!
//! Takes arbitrarily sized inputs and bundles or slices them as necessary.
use async_trait::async_trait;
use log::trace;
use serde::Deserialize;
use std::collections::HashMap;
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

/// Options includes various options to tweak the behavious of the sender.
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
    type Event;
    async fn send_batch(&self, key: &str, events: Vec<Self::Event>, clock: Instant) -> bool;
}

pub trait Keyed {
    fn key(&self) -> &str;
    fn size(&self) -> usize;
}

/// `Transmission` handles collecting individual events into batches for bulk upload.
#[derive(Debug, Clone)]
pub struct Transmission<E: Clone> {
    work_sender: Sender<E>,
}

impl<E: Clone + Keyed + Send + 'static> Transmission<E> {
    pub fn send(&self, event: E) -> Result<()> {
        self.work_sender
            .try_send(event)
            .map_err(|_| TransmissionError::QueueFull)
    }
}

impl<E: Clone + Keyed + Send + 'static> Transmission<E> {
    pub fn start(runtime: Handle, sender: impl BatchSender<Event = E>, options: Options) -> Self {
        let (work_sender, work_receiver) = channel(options.pending_work_capacity);

        runtime
            .spawn(async move { Self::process_work(work_receiver, sender, options.clone()).await });

        Self { work_sender }
    }

    async fn process_work(
        mut work_receiver: Receiver<E>,
        sender: impl BatchSender<Event = E>,
        options: Options,
    ) {
        let mut batches: HashMap<String, (usize, Vec<E>)> = HashMap::new();
        let mut send = true;

        while send {
            let mut expired = false;
            match timeout(options.batch_timeout, work_receiver.recv()).await {
                Ok(Some(event)) => {
                    let event_bytes = event.size();
                    let key = String::from(event.key());
                    let current = batches
                        .entry(key)
                        .or_insert((0, Vec::with_capacity(options.max_batch_size)));

                    current.0 += event_bytes;
                    current.1.push(event);
                }
                Ok(None) => {
                    send = false;
                }
                Err(_) => {
                    expired = true;
                }
            };

            let mut batches_sent = Vec::new();
            for (batch_name, (batch_bytes, batch)) in batches.iter_mut() {
                if batch.is_empty() {
                    break;
                }
                let options = options.clone();

                let over_count = batch.len() >= options.max_batch_size;
                let over_bytes = *batch_bytes >= options.max_batch_bytes;
                if !batch.is_empty() && (over_count || over_bytes || expired) {
                    if expired {
                        trace!("{:?} batch timeout exceeded", options.batch_timeout);
                    } else if over_bytes {
                        trace!("sending over byte limit");
                    } else if over_count {
                        trace!("sending full batch");
                    }
                    let batch_copy = batch.clone();

                    if sender
                        .send_batch(batch_name, batch_copy, Instant::now())
                        .await
                    {
                        trace!("sent {}", batch_name);
                        batches_sent.push(batch_name.to_string());
                    } else {
                        trace!("failed to send batch");
                    }
                }
            }
            // clear all sent batches
            batches_sent.iter_mut().for_each(|name| {
                batches.remove(name);
            });
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

    impl Keyed for String {
        fn key(&self) -> &str {
            "single-batch"
        }

        fn size(&self) -> usize {
            self.len()
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
        type Event = String;

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
            transmission.send(format!("event {}", i)).unwrap();
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
            transmission.send(format!("event {}", i)).unwrap();
        }

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
                max_batch_bytes: 10,
                ..Options::default()
            },
        );

        for i in 0..5 {
            transmission.send(format!("event {}", i)).unwrap();
        }

        let batches = reader.await;
        assert_eq!(
            batches,
            stringify(vec![vec!["event 0", "event 1"], vec!["event 2", "event 3"]])
        );
    }
}

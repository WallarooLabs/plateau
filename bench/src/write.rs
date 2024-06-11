use std::ops::Range;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use plateau_client::{Error, InsertQuery, MultiChunk};
use reqwest::StatusCode;
use sample_std::{arbitrary, Random, Sample};
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, trace, trace_span, warn};

use crate::{load::build_sampler, BoxedTask, Config, Task, TaskBuilder, WorkerConfig};

#[derive(Deserialize, Debug, Clone)]
pub struct WriterJob {
    pub topic: String,
    pub partition: String,
    pub sample_path: PathBuf,
    pub rows: Range<usize>,
    pub workers: usize,
    #[serde(flatten)]
    pub worker_config: WorkerConfig,
}

impl TaskBuilder for WriterJob {
    fn to_jobs(&self, r: &mut Random) -> Result<Vec<BoxedTask>> {
        let workers = (0..self.workers)
            .map(|id| {
                let seed = arbitrary::<u64>().generate(r);

                let (tx, rx) = mpsc::channel(8);

                let generator = Generator {
                    path: self.sample_path.clone(),
                    seed,
                    rows: self.rows.clone(),
                    tx,
                };
                thread::spawn(move || generator.run());

                Box::new(Writer {
                    topic: self.topic.clone(),
                    partition: self.partition.clone(),
                    id,
                    sample_rx: rx,
                    count: 0,
                }) as BoxedTask
            })
            .collect();

        Ok(workers)
    }

    fn config(&self) -> &WorkerConfig {
        &self.worker_config
    }
}

struct Writer {
    topic: String,
    partition: String,
    id: usize,
    sample_rx: mpsc::Receiver<MultiChunk>,
    count: usize,
}

struct Generator {
    path: PathBuf,
    seed: u64,
    rows: Range<usize>,
    tx: mpsc::Sender<MultiChunk>,
}

impl Generator {
    fn run(self) {
        let mut sampler = build_sampler(&self.path).unwrap();
        let mut random = Random::from_seed(self.seed);

        loop {
            let len = random.gen_range(self.rows.clone());
            sampler.set_len(len);

            let multi = trace_span!("generate").in_scope(|| sampler.generate(&mut random));

            if self.tx.blocking_send(multi).is_err() {
                break;
            }
        }

        debug!("sample generator exiting");
    }
}

#[async_trait]
impl Task for Writer {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let start = Instant::now();
        let multi = self.sample_rx.recv().await.unwrap();
        trace!("{:?} {:?}", start.elapsed(), multi);

        let r = config
            .client
            .append_records(&self.topic, &self.partition, &InsertQuery::default(), multi)
            .await;

        let rate_limited = match &r {
            Err(Error::Server(e)) => e.status() == Some(StatusCode::TOO_MANY_REQUESTS),
            _ => false,
        };

        let (batch_size, span) = if !rate_limited {
            let ok = r.unwrap();
            let rows = ok.span.end - ok.span.start;
            self.count += rows;
            (rows, (ok.span.start..ok.span.end))
        } else {
            warn!("{}/{} rate limited", self.topic, self.partition);
            (0, 0..0)
        };

        (
            batch_size,
            format!(
                "[write] {}/{}#{} {} ({:?})",
                self.topic, self.partition, self.id, self.count, span
            ),
        )
    }
}

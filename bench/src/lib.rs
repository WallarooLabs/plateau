use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::FutureExt;
use hdrhistogram::Histogram;
use plateau_client::Client;
use plateau_server::config::PlateauConfig;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use sample_std::Random;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, trace};

pub mod load;
pub mod read;
pub mod status;
pub mod write;

pub use read::IteratorJob;
pub use status::{HealthCheckJob, SummarizerJob};
pub use write::WriterJob;

#[derive(Clone)]
pub struct Config {
    client: Client,
}

#[async_trait]
pub trait Task {
    async fn run(&mut self, config: &Config) -> (usize, String);
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    stats_group: String,
    #[serde(with = "humantime_serde")]
    interval: Duration,
}

type BoxedTask = Box<dyn Task + Send>;

struct Worker {
    task: BoxedTask,
    config: Config,
    delay: Duration,
    stats: mpsc::Sender<(usize, Duration)>,
    status: watch::Sender<String>,
    fin: oneshot::Receiver<()>,
}

impl Worker {
    async fn run(mut self, seed: u64) -> usize {
        let mut total = 0;
        let mut rng = StdRng::seed_from_u64(seed);
        while self.fin.try_recv() == Err(oneshot::error::TryRecvError::Empty) {
            let start = Instant::now();
            let (dn, status) = self.task.run(&self.config).await;
            let dt = start.elapsed();
            total += dn;
            trace!("{}", status);
            self.status.send(status).unwrap();
            self.stats.send((dn, dt)).await.unwrap();
            if self.delay > dt {
                tokio::time::sleep(self.delay - dt).await;
            }
            tokio::time::sleep(Duration::from_millis(rng.gen_range(0..=5))).await;
        }
        total
    }
}

pub trait TaskBuilder {
    fn to_jobs(&self, r: &mut Random) -> Result<Vec<BoxedTask>>;
    fn config(&self) -> &WorkerConfig;
}

pub type WorkerTask = Box<dyn TaskBuilder>;

pub async fn run(mut tasks: Vec<Box<dyn TaskBuilder>>, test_duration: Duration) {
    let path = Path::new("./data");
    if !path.exists() {
        fs::create_dir(path).unwrap();
    }

    let (tx_exit, rx_exit) = tokio::sync::oneshot::channel();
    let exit = rx_exit.map(|_| ()).boxed();
    let config = PlateauConfig::default();
    let plateau_server = tokio::spawn(plateau_server::task_from_config(config, exit));

    let config = Config {
        client: Client::new("http://localhost:3030").unwrap(),
    };

    config
        .client
        .healthy(Duration::from_secs(10), Duration::from_millis(10))
        .await
        .unwrap();

    let mut rng = rand::thread_rng();
    let seed = rng.next_u64();
    debug!("seed: {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);
    tasks.shuffle(&mut rng);

    let mut counters = HashMap::new();
    let mut stat_groups = HashMap::new();
    let mut handles = BTreeMap::new();
    let mut updates = vec![];
    let mut fins = vec![];
    let mut r = Random::new();
    for builder in tasks {
        let worker_config = builder.config();
        let group = worker_config.stats_group.clone();
        let (stats, _) = stat_groups
            .entry(group.clone())
            .or_insert_with(|| mpsc::channel(32));

        for task in builder.to_jobs(&mut r).unwrap() {
            let (status, rx) = watch::channel(String::from("no update"));
            updates.push(rx);
            let (tx_fin, fin) = oneshot::channel();
            let worker = Worker {
                task,
                delay: worker_config.interval,
                status,
                stats: stats.clone(),
                config: config.clone(),
                fin,
            };
            fins.push(tx_fin);

            let seed = rng.next_u64();
            let ix: &mut usize = counters.entry(group.clone()).or_default();
            let name = format!("{}-{}", group, *ix);
            *ix += 1;
            handles.insert(name, tokio::spawn(worker.run(seed)));
        }

        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..=5))).await;
    }

    let mut stat_updates = vec![];
    for (name, (_, mut rx)) in stat_groups.into_iter() {
        let (tx, update) = watch::channel(String::from(""));
        tokio::spawn(async move {
            let mut total = 0;
            let start = Instant::now();
            let mut h = Histogram::<u64>::new_with_bounds(1, 1000 * 1000, 2).unwrap();
            while let Some((dn, d)) = rx.recv().await {
                let elapsed = start.elapsed().as_secs();
                total += dn;
                let nps = if elapsed > 0 {
                    ((total as u64) / elapsed).to_string()
                } else {
                    String::from("-")
                };
                h.record(d.as_millis() as u64).unwrap();
                let p50 = Duration::from_millis(h.value_at_percentile(50.));
                let p90 = Duration::from_millis(h.value_at_percentile(90.));
                let p99 = Duration::from_millis(h.value_at_percentile(99.));
                tx.send(format!(
                    "{:8} {:>8}/s | p50: {:>6?} p90: {:>6?} p99: {:>6?}",
                    format!("({})", name),
                    nps,
                    p50,
                    p90,
                    p99
                ))
                .unwrap();
            }
        });
        stat_updates.push(update);
    }

    let start = Instant::now();
    let mut strings = vec![];
    while start.elapsed() < test_duration {
        debug!("{:->80}", "");
        let mut update_strings: Vec<_> = updates.iter_mut().map(|up| up.borrow().clone()).collect();
        update_strings.sort();
        for up in update_strings {
            debug!("{}", up);
        }

        strings = stat_updates
            .iter_mut()
            .map(|up| up.borrow().clone())
            .collect();
        strings.sort();
        for up in &strings {
            debug!("{}", up);
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    info!("shutting down load generation");
    for fin in fins {
        fin.send(()).unwrap();
    }

    for (name, handle) in handles {
        let value = handle.await.unwrap();
        debug!("{}: {}", name, value);
    }

    let start = Instant::now();
    info!("shutting down plateau");
    tx_exit.send(()).unwrap();
    assert!(
        plateau_server.await.unwrap(),
        "plateau failed to shutdown cleanly"
    );
    info!("plateau shut down in {:?}", start.elapsed());

    for up in strings {
        info!("{}", up);
    }
}

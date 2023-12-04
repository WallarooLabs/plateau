use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use futures::future::FutureExt;
use hdrhistogram::Histogram;
use plateau::config::PlateauConfig;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use reqwest::{Client, StatusCode};
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, trace};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Clone)]
struct Config {
    client: Client,
    base: String,
}

impl Config {
    fn relative(&self, rel: &str) -> String {
        format!("{}/{}", self.base, rel)
    }
}

#[async_trait]
trait Task {
    async fn run(&mut self, config: &Config) -> (usize, String);
}

struct Worker {
    task: Box<dyn Task + Send>,
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
            let start = SystemTime::now();
            let (dn, status) = self.task.run(&self.config).await;
            let dt = SystemTime::now().duration_since(start).unwrap();
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

struct Writer {
    url: String,
    count: usize,
    batch_size: usize,
    msg: serde_json::Value,
}

impl Writer {
    fn create(topic: &str, partition: &str, record: &str, batch_size: usize) -> Self {
        let url = format!("topic/{}/partition/{}", topic, partition);
        let records: Vec<_> = vec![record.to_string()]
            .iter()
            .cycle()
            .take(batch_size)
            .cloned()
            .collect();
        let msg = json!({ "records": records });
        Writer {
            url,
            count: 0,
            batch_size,
            msg,
        }
    }
}

#[async_trait]
impl Task for Writer {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let url = config.relative(&self.url);
        let r = config
            .client
            .post(&url)
            .json(&self.msg)
            .send()
            .await
            .unwrap();

        let batch_size = if r.status() != StatusCode::TOO_MANY_REQUESTS {
            r.error_for_status().unwrap();
            self.count += self.batch_size;
            self.batch_size
        } else {
            trace!("{url} rate limited");
            0
        };

        (
            batch_size,
            format!("[write] {} - sent {}", self.url, self.count),
        )
    }
}

struct Iterator {
    url: String,
    limit: usize,
    count: usize,
    state: serde_json::Value,
}

impl Iterator {
    fn create(topic: &str, limit: usize) -> Self {
        let url = format!("topic/{}/records", topic);
        let state = json!({});
        Iterator {
            url,
            limit,
            count: 0,
            state,
        }
    }
}

#[async_trait]
impl Task for Iterator {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let url = config.relative(&self.url);
        let r = config
            .client
            .post(&url)
            .json(&self.state)
            .query(&[("limit", self.limit)])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        let result: serde_json::Value = r.json().await.unwrap();
        let read_count = result.get("records").unwrap().as_array().unwrap().len();
        self.count += read_count;
        self.state = result.get("next").unwrap().clone();
        (
            read_count,
            format!("[iter]  {} - read {}", self.url, self.count),
        )
    }
}

struct Summarizer {
    url: String,
    count: usize,
}

impl Summarizer {
    fn create(topic: &str) -> Self {
        let url = format!("topic/{}", topic);
        Summarizer { url, count: 0 }
    }
}

#[async_trait]
impl Task for Summarizer {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        self.count += 1;
        let url = config.relative(&self.url);
        let r = config
            .client
            .get(&url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let result: serde_json::Value = r.json().await.unwrap();
        let count: u64 = result
            .get("partitions")
            .unwrap()
            .as_object()
            .unwrap()
            .values()
            .map(|v| {
                v.get("end").unwrap().as_u64().unwrap() - v.get("start").unwrap().as_u64().unwrap()
            })
            .sum();

        (1, format!("[sum]   {} - {}", self.url, count))
    }
}

struct HealthCheck {
    count: usize,
    errors: usize,
    last: Option<serde_json::Value>,
}

impl HealthCheck {
    fn new() -> Self {
        HealthCheck {
            count: 0,
            errors: 0,
            last: None,
        }
    }
}

#[async_trait]
impl Task for HealthCheck {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let url = config.relative("ok");
        let r = config.client.get(&url).send().await.unwrap();
        let err = r.error_for_status_ref().is_err();
        self.last = Some(r.json().await.unwrap());

        if !err {
            self.count += 1;
        } else {
            self.errors += 1;
        }

        let last = self
            .last
            .as_ref()
            .map(|v| format!("last: {}", v))
            .unwrap_or_default();

        if self.errors == 0 {
            (1, format!("[ok]    ✓ {}", last))
        } else {
            (0, format!("[ok]    ✗ {} {}", self.errors, last))
        }
    }
}

#[tokio::main]
async fn main() {
    fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let path = Path::new("./data");
    if !path.exists() {
        fs::create_dir(path).unwrap();
    }

    let (tx_exit, rx_exit) = tokio::sync::oneshot::channel();
    let exit = rx_exit.map(|_| ()).boxed();
    let plateau_server = tokio::spawn(plateau::task_from_config(PlateauConfig::default(), exit));

    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = Config {
        base: String::from("http://localhost:3030"),
        client: Client::new(),
    };
    let small: String = (0..128).map(|_| "x").collect();

    let mut tasks = vec![];
    for topic in 0..1 {
        let name = format!("pipeline-{}", topic);
        let topic_tasks: Vec<(&str, u64, Box<dyn Task + Send>)> = vec![
            ("check", 1_000_000, Box::new(HealthCheck::new())),
            ("sum", 100_000, Box::new(Summarizer::create(&name))),
            ("read", 10, Box::new(Iterator::create(&name, 2000))),
        ];

        for _ in 0..4 {
            let writers: [(&str, u64, Box<dyn Task + Send>); 6] = [
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-0", &small, 10)),
                ),
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-1", &small, 10)),
                ),
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-2", &small, 10)),
                ),
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-3", &small, 10)),
                ),
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-4", &small, 10)),
                ),
                (
                    "write",
                    0,
                    Box::new(Writer::create(&name, "engine-5", &small, 10)),
                ),
            ];
            tasks.extend(writers);
        }
        tasks.extend(topic_tasks);
    }

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
    for (group, cadence_us, task) in tasks {
        let (status, rx) = watch::channel(String::from(""));
        updates.push(rx);

        let (stats, _) = stat_groups
            .entry(group)
            .or_insert_with(|| mpsc::channel(32));

        let (tx_fin, fin) = oneshot::channel();

        let worker = Worker {
            task,
            delay: Duration::from_micros(cadence_us),
            status,
            stats: stats.clone(),
            config: config.clone(),
            fin,
        };
        fins.push(tx_fin);

        let seed = rng.next_u64();
        let ix: &mut usize = counters.entry(group).or_default();
        let name = format!("{}-{}", group, *ix);
        *ix += 1;
        handles.insert(name, tokio::spawn(worker.run(seed)));

        tokio::time::sleep(Duration::from_millis(rng.gen_range(0..=5))).await;
    }

    let mut stat_updates = vec![];
    for (name, (_, mut rx)) in stat_groups.into_iter() {
        let name = String::from(name);
        let (tx, update) = watch::channel(String::from(""));
        tokio::spawn(async move {
            let mut total = 0;
            let start = SystemTime::now();
            let mut h = Histogram::<u64>::new_with_bounds(1, 1000 * 1000, 2).unwrap();
            while let Some((dn, d)) = rx.recv().await {
                let elapsed = SystemTime::now().duration_since(start).unwrap().as_secs();
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
    while start.elapsed() < Duration::from_secs(30) {
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

    info!("shutting down plateau");
    tx_exit.send(()).unwrap();
    assert!(
        plateau_server.await.unwrap(),
        "plateau failed to shutdown cleanly"
    );

    for up in strings {
        info!("{}", up);
    }
}

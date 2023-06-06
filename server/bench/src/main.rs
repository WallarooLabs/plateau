use async_trait::async_trait;
use hdrhistogram::Histogram;
use log::{debug, info, trace};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, watch};

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
}

impl Worker {
    async fn run(&mut self, seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        loop {
            let start = SystemTime::now();
            let (dn, status) = self.task.run(&self.config).await;
            let dt = SystemTime::now().duration_since(start).unwrap();
            trace!("{}", status);
            self.status.send(status).unwrap();
            self.stats.send((dn, dt)).await.unwrap();
            if self.delay > dt {
                tokio::time::sleep(self.delay - dt).await;
            }
            tokio::time::sleep(Duration::from_millis(rng.gen_range(0..=5))).await;
        }
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
        config
            .client
            .post(&url)
            .json(&self.msg)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        self.count += self.batch_size;

        (
            self.batch_size,
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
    pretty_env_logger::init();
    let config = Config {
        base: String::from("http://localhost:3030"),
        client: Client::new(),
    };
    let small: String = (0..128).map(|_| "x").collect();

    let mut tasks = vec![];
    for topic in 0..4 {
        let name = format!("pipeline-{}", topic);
        let topic_tasks: Vec<(&str, u64, Box<dyn Task + Send>)> = vec![
            ("sum", 100, Box::new(HealthCheck::new())),
            (
                "write",
                100,
                Box::new(Writer::create(&name, "engine-0", &small, 300)),
            ),
            (
                "write",
                100,
                Box::new(Writer::create(&name, "engine-1", &small, 300)),
            ),
            (
                "write",
                100,
                Box::new(Writer::create(&name, "engine-2", &small, 300)),
            ),
            (
                "write",
                100,
                Box::new(Writer::create(&name, "engine-3", &small, 300)),
            ),
            ("sum", 100, Box::new(Summarizer::create(&name))),
            ("read", 10, Box::new(Iterator::create(&name, 2000))),
        ];
        tasks.extend(topic_tasks);
    }

    let mut rng = rand::thread_rng();
    let seed = rng.next_u64();
    info!("seed: {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);
    tasks.shuffle(&mut rng);

    let mut stat_groups = HashMap::new();
    let mut handles = vec![];
    let mut updates = vec![];
    for (group, cadence_ms, task) in tasks {
        let (status, rx) = watch::channel(String::from(""));
        updates.push(rx);

        let (stats, _) = stat_groups
            .entry(group)
            .or_insert_with(|| mpsc::channel(32));

        let mut w = Worker {
            task,
            delay: Duration::from_millis(cadence_ms),
            status,
            stats: stats.clone(),
            config: config.clone(),
        };

        let seed = rng.next_u64();
        handles.push(tokio::spawn(async move {
            w.run(seed).await;
        }));

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
            loop {
                let (dn, d) = rx.recv().await.unwrap();
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

    loop {
        info!("{:->80}", "");
        let mut strings: Vec<_> = updates.iter_mut().map(|up| up.borrow().clone()).collect();
        strings.sort();
        for up in strings {
            debug!("{}", up);
        }

        let mut strings: Vec<_> = stat_updates
            .iter_mut()
            .map(|up| up.borrow().clone())
            .collect();
        strings.sort();
        for up in strings {
            info!("{}", up);
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

use std::collections::HashMap;
use std::hash::{Hash, Hasher as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use plateau_client::{Client, Error, InsertQuery, MultiChunk};
use reqwest::StatusCode;
use sample_std::Random;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::load::build_sampler;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub sample: PathBuf,
    /// Override global partitions count for this topic.
    pub partitions: Option<usize>,
    /// Override global rows per batch for this topic.
    pub rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct BatchConfig {
    /// Number of partitions per topic (unless overridden per-topic).
    #[serde(default = "default_partitions")]
    pub partitions: usize,
    /// Rows per batch (unless overridden per-topic).
    #[serde(default = "default_rows")]
    pub rows: usize,
    /// Real-world interval between successive batches for each partition.
    #[serde(with = "humantime_serde")]
    pub batch_interval: Duration,
    /// How much faster than real time to run (1.0 = real time, 60.0 = 1h batches every 1 min).
    #[serde(default = "default_speed")]
    pub speed: f64,
    /// Path to the state file (default: batch-state.json).
    pub state_file: Option<PathBuf>,
    pub topics: Vec<TopicConfig>,
}

fn default_partitions() -> usize { 4 }
fn default_rows() -> usize { 10_000 }
fn default_speed() -> f64 { 1.0 }

impl BatchConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    pub fn batch_period(&self) -> Duration {
        self.batch_interval.div_f64(self.speed)
    }
}

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchState {
    /// RFC 3339 timestamp of first-ever run start; drives schedule alignment.
    pub origin: Option<String>,
    /// Last completed batch index per "topic/partition-N" key.
    pub last_batch: HashMap<String, u64>,
}

impl BatchState {
    pub fn load(path: &Path) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    fn last_batch_for(&self, topic: &str, partition: &str) -> u64 {
        *self.last_batch.get(&format!("{topic}/{partition}")).unwrap_or(&0)
    }

    fn set_last_batch(&mut self, topic: &str, partition: &str, batch: u64) {
        self.last_batch.insert(format!("{topic}/{partition}"), batch);
    }
}

// ── Deterministic seed ────────────────────────────────────────────────────────

fn batch_seed(topic: &str, partition: &str, batch_idx: u64) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut h);
    partition.hash(&mut h);
    batch_idx.hash(&mut h);
    h.finish()
}

// ── Sampler thread ────────────────────────────────────────────────────────────
//
// The sampler (Box<dyn ChunkLen>) is !Send, so it must live in its own
// std::thread. The async worker communicates via channels.

struct SamplerRequest {
    seed: u64,
    rows: usize,
}

struct SamplerThread {
    sample_path: PathBuf,
    req_rx: std::sync::mpsc::Receiver<SamplerRequest>,
    result_tx: mpsc::Sender<MultiChunk>,
}

impl SamplerThread {
    fn run(self) {
        let mut sampler = build_sampler(&self.sample_path).expect("failed to build sampler");
        for req in self.req_rx {
            let mut random = Random::from_seed(req.seed);
            sampler.set_len(req.rows);
            let multi = sampler.generate(&mut random);
            if self.result_tx.blocking_send(multi).is_err() {
                break;
            }
        }
    }
}

// ── Per-partition async worker ────────────────────────────────────────────────

struct PartitionWorker {
    client: Client,
    topic: String,
    partition: String,
    sample_path: PathBuf,
    rows: usize,
    batch_period: Duration,
    stagger_offset: Duration,
    state: Arc<Mutex<BatchState>>,
    state_path: PathBuf,
}

impl PartitionWorker {
    async fn run(self) {
        // Spawn the sampler in its own thread.
        let (req_tx, req_rx) = std::sync::mpsc::channel::<SamplerRequest>();
        let (result_tx, mut result_rx) = mpsc::channel::<MultiChunk>(2);
        let st = SamplerThread {
            sample_path: self.sample_path.clone(),
            req_rx,
            result_tx,
        };
        thread::spawn(move || st.run());

        // Load resume position from state.
        let resume_from = {
            let s = self.state.lock().await;
            s.last_batch_for(&self.topic, &self.partition)
        };

        // Determine the origin timestamp for schedule alignment.
        let origin: SystemTime = {
            let s = self.state.lock().await;
            if let Some(ref ts) = s.origin {
                humantime::parse_rfc3339(ts).unwrap_or(SystemTime::now())
            } else {
                SystemTime::now()
            }
        };

        // Compute how many batches should have fired by now (for catchup).
        let elapsed = origin.elapsed().unwrap_or_default();
        let catchup_to = if self.batch_period.is_zero() {
            0
        } else {
            let adjusted = elapsed.saturating_sub(self.stagger_offset);
            (adjusted.as_nanos() / self.batch_period.as_nanos()) as u64
        };

        let mut batch_idx = resume_from;

        if catchup_to > batch_idx {
            info!(
                "{}/{}: catching up {} missed batches",
                self.topic, self.partition,
                catchup_to - batch_idx
            );
        }

        loop {
            // Determine wall-clock time when this batch should fire.
            let fire_at = origin
                + self.stagger_offset
                + self.batch_period * batch_idx as u32;

            // Only sleep if we're past catchup.
            if batch_idx >= catchup_to {
                let now = SystemTime::now();
                if fire_at > now {
                    let wait = fire_at.duration_since(now).unwrap_or_default();
                    tokio::time::sleep(wait).await;
                }
            }

            // Request the sampler thread to generate this batch.
            let seed = batch_seed(&self.topic, &self.partition, batch_idx);
            if req_tx.send(SamplerRequest { seed, rows: self.rows }).is_err() {
                break;
            }
            let multi = match result_rx.recv().await {
                Some(m) => m,
                None => break,
            };

            let start = Instant::now();
            let r = self.client
                .append_records(&self.topic, &self.partition, &InsertQuery::default(), multi)
                .await;

            match r {
                Ok(ok) => {
                    let rows = ok.span.end - ok.span.start;
                    tracing::debug!(
                        "{}/{} batch {} → {} rows in {:?}",
                        self.topic, self.partition, batch_idx, rows, start.elapsed()
                    );
                }
                Err(Error::Server(ref e)) if e.status() == Some(StatusCode::TOO_MANY_REQUESTS) => {
                    warn!("{}/{} rate limited on batch {}", self.topic, self.partition, batch_idx);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue; // retry same batch
                }
                Err(e) => {
                    warn!("{}/{} batch {} failed: {}", self.topic, self.partition, batch_idx, e);
                }
            }

            // Persist state after each batch.
            {
                let mut s = self.state.lock().await;
                s.set_last_batch(&self.topic, &self.partition, batch_idx);
                let _ = s.save(&self.state_path);
            }

            batch_idx += 1;
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run_batch(url: &str, config: BatchConfig, state_path: &Path) -> Result<()> {
    let client = Client::new(url)?;
    client
        .healthy(Duration::from_secs(10), Duration::from_millis(100))
        .await?;

    let mut state = BatchState::load(state_path);

    // Set origin on first run so all workers share the same schedule anchor.
    if state.origin.is_none() {
        state.origin = Some(humantime::format_rfc3339(SystemTime::now()).to_string());
        state.save(state_path)?;
    }

    let state = Arc::new(Mutex::new(state));

    let batch_period = config.batch_period();
    let total_partitions: usize = config.topics.iter()
        .map(|t| t.partitions.unwrap_or(config.partitions))
        .sum();
    let stagger = if total_partitions > 1 {
        batch_period / total_partitions as u32
    } else {
        Duration::ZERO
    };

    info!(
        "batch period: {:?}, stagger: {:?}, {} topics, {} total partitions",
        batch_period, stagger, config.topics.len(), total_partitions
    );

    let mut handles = vec![];
    let mut global_partition_idx: usize = 0;

    for topic in config.topics {
        let n_partitions = topic.partitions.unwrap_or(config.partitions);
        let rows = topic.rows.unwrap_or(config.rows);

        for p in 0..n_partitions {
            let partition_name = format!("partition-{p}");
            let stagger_offset = stagger * global_partition_idx as u32;
            global_partition_idx += 1;

            let worker = PartitionWorker {
                client: client.clone(),
                topic: topic.name.clone(),
                partition: partition_name,
                sample_path: topic.sample.clone(),
                rows,
                batch_period,
                stagger_offset,
                state: state.clone(),
                state_path: state_path.to_path_buf(),
            };

            handles.push(tokio::spawn(worker.run()));
        }
    }

    // Print a progress summary every 30 seconds.
    let state_for_stats = state.clone();
    tokio::spawn(async move {
        let mut last: HashMap<String, u64> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let s = state_for_stats.lock().await;
            let mut lines: Vec<String> = s.last_batch.iter()
                .map(|(k, &v)| {
                    let prev = last.get(k).copied().unwrap_or(0);
                    let delta = v.saturating_sub(prev);
                    last.insert(k.clone(), v);
                    format!("  {k}: batch {v} (+{delta} in 30s)")
                })
                .collect();
            lines.sort();
            if !lines.is_empty() {
                info!("batch progress:\n{}", lines.join("\n"));
            }
        }
    });

    // Wait for all workers (they run indefinitely until the process is killed).
    for h in handles {
        let _ = h.await;
    }

    Ok(())
}

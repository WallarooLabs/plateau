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
    /// Number of partitions for this topic (falls back to the top-level default).
    pub partitions: Option<usize>,
    /// Rows per batch for this topic (falls back to the top-level default).
    pub rows: Option<usize>,
    /// Real-world interval between successive batches for this topic
    /// (falls back to the top-level default).
    #[serde(default, with = "humantime_serde::option")]
    pub batch_interval: Option<Duration>,
}

/// Optional top-level defaults applied to any topic that omits a value.
#[derive(Debug, Deserialize, Default)]
pub struct Defaults {
    pub partitions: Option<usize>,
    pub rows: Option<usize>,
    #[serde(default, with = "humantime_serde::option")]
    pub batch_interval: Option<Duration>,
}

#[derive(Debug, Deserialize)]
pub struct BatchConfig {
    /// Defaults applied to topics that omit partitions / rows / batch_interval.
    #[serde(default)]
    pub defaults: Defaults,
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

/// A topic with all settings resolved to concrete values.
pub struct ResolvedTopic {
    pub name: String,
    pub sample: PathBuf,
    pub partitions: usize,
    pub rows: usize,
    pub batch_interval: Duration,
}

impl BatchConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    /// Resolve each topic against the top-level defaults. Errors if a topic
    /// has no batch_interval and no default is provided.
    pub fn resolve_topics(&self) -> Result<Vec<ResolvedTopic>> {
        self.topics
            .iter()
            .map(|t| {
                let batch_interval = t
                    .batch_interval
                    .or(self.defaults.batch_interval)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "topic '{}' has no batch_interval and no default is set",
                            t.name
                        )
                    })?;
                Ok(ResolvedTopic {
                    name: t.name.clone(),
                    sample: t.sample.clone(),
                    partitions: t
                        .partitions
                        .or(self.defaults.partitions)
                        .unwrap_or_else(default_partitions),
                    rows: t.rows.or(self.defaults.rows).unwrap_or_else(default_rows),
                    batch_interval,
                })
            })
            .collect()
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

    let topics = config.resolve_topics()?;

    info!(
        "speed {}x, {} topics",
        config.speed,
        topics.len()
    );

    let mut handles = vec![];

    for topic in topics {
        // Each topic runs on its own schedule; its partitions are evenly
        // staggered across that topic's own (sped-up) batch period.
        let batch_period = topic.batch_interval.div_f64(config.speed);
        let stagger = if topic.partitions > 1 {
            batch_period / topic.partitions as u32
        } else {
            Duration::ZERO
        };

        info!(
            "  {}: {} partitions, {} rows, interval {:?} → period {:?}, stagger {:?}",
            topic.name, topic.partitions, topic.rows, topic.batch_interval, batch_period, stagger
        );

        for p in 0..topic.partitions {
            let partition_name = format!("partition-{p}");
            let stagger_offset = stagger * p as u32;

            let worker = PartitionWorker {
                client: client.clone(),
                topic: topic.name.clone(),
                partition: partition_name,
                sample_path: topic.sample.clone(),
                rows: topic.rows,
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

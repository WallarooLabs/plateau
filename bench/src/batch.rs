use std::collections::HashMap;
use std::hash::{Hash, Hasher as _};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use plateau_client::{Client, Error, InsertQuery, MultiChunk};
use reqwest::StatusCode;
use sample_arrow_rs::array::FromDataType;
use sample_arrow_rs::datatypes::sample_flat;
use sample_arrow_rs::primitive::primitive_len_sampler;
use sample_arrow_rs::{AlwaysValid, SetLen};
use sample_std::{Random, Sample};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::load::Now;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TopicsConfig {
    /// Total number of topics in the simulated pool.
    pub count: usize,
    /// How many topics are active (writing) at once.
    pub active: usize,
    /// Real-world duration after which the active window advances.
    #[serde(with = "humantime_serde")]
    pub rotation_interval: Duration,
    /// Min number of data columns per topic (not counting the `time` column).
    pub columns_min: usize,
    /// Max number of data columns per topic (exclusive).
    pub columns_max: usize,
    /// Real-world batch interval per topic.
    #[serde(with = "humantime_serde")]
    pub batch_interval: Duration,
    /// Partitions per topic.
    #[serde(default = "default_partitions")]
    pub partitions: usize,
    /// Rows per batch.
    #[serde(default = "default_rows")]
    pub rows: usize,
}

fn default_partitions() -> usize { 4 }
fn default_rows() -> usize { 10_000 }

#[derive(Debug, Deserialize)]
pub struct BatchConfig {
    /// Speed multiplier: 60.0 means 1h intervals fire every 1 minute.
    #[serde(default = "default_speed")]
    pub speed: f64,
    /// Path to the state file.
    pub state_file: Option<PathBuf>,
    /// Directory where generated topic schema files are stored.
    pub schemas_dir: Option<PathBuf>,
    pub topics: TopicsConfig,
}

fn default_speed() -> f64 { 1.0 }

impl BatchConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchState {
    /// RFC 3339 timestamp of first-ever run start; schedule anchor.
    pub origin: Option<String>,
    /// Seed used to generate topic schemas (for regeneration if files are lost).
    pub schemas_seed: Option<u64>,
    /// Current rotation window start index (topic index, not partition).
    pub window_start: usize,
    /// When the current window started (RFC 3339).
    pub window_since: Option<String>,
    /// Last completed batch index per "topic-NNN/partition-N" key.
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

// ── Schema generation ─────────────────────────────────────────────────────────

fn topic_name(idx: usize) -> String {
    format!("topic-{idx:04}")
}

fn schema_path(schemas_dir: &Path, idx: usize) -> PathBuf {
    schemas_dir.join(format!("{}.arrow", topic_name(idx)))
}

/// Generate a schema with a random number of flat columns in [col_range) and
/// write a tiny seed batch to `path` so `build_sampler` can read it back.
fn generate_schema_file(
    path: &Path,
    col_range: Range<usize>,
    rng: &mut Random,
) -> Result<()> {
    let n_cols = rng.gen_range(col_range);
    let mut flat = sample_flat();

    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("time", DataType::Int64, false))];
    for i in 0..n_cols {
        let dt = flat.generate(rng);
        fields.push(Arc::new(Field::new(format!("col_{i}"), dt, false)));
    }

    let schema: SchemaRef = Arc::new(Schema::new(fields.clone()));

    // Build one-row seed batch so build_sampler has an example array per column.
    let converter = FromDataType {
        validity: AlwaysValid,
        branch: 1_i32..2_i32,
    };

    let mut time_sampler = primitive_len_sampler::<_, _, arrow_array::types::Int64Type>(Now, AlwaysValid);
    time_sampler.set_len(1);
    let time_col = time_sampler.generate(rng);

    let mut arrays: Vec<arrow_array::ArrayRef> = vec![time_col];
    for field in fields.iter().skip(1) {
        let mut s = converter.from_data_type(field.data_type());
        s.set_len(1);
        arrays.push(s.generate(rng));
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let file = std::fs::File::create(path)?;
    let mut writer = FileWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}

/// Ensure all N topic schema files exist in `schemas_dir`, generating any
/// that are missing. Returns the seed used (stored in state for provenance).
pub fn ensure_schemas(
    schemas_dir: &Path,
    count: usize,
    col_min: usize,
    col_max: usize,
    seed: u64,
) -> Result<()> {
    std::fs::create_dir_all(schemas_dir)?;

    let col_range = col_min..col_max;
    let mut rng = Random::from_seed(seed);

    // Advance the RNG past any already-generated schemas so adding more topics
    // later doesn't change existing schemas.
    let mut missing = vec![];
    for idx in 0..count {
        let path = schema_path(schemas_dir, idx);
        if path.exists() {
            // Burn the same number of RNG calls as generation would to keep
            // future topics consistent.
            let _ = rng.gen_range(col_range.clone());
            // Burn one call per possible column for datatypes — approximate.
        } else {
            missing.push(idx);
        }
    }

    if !missing.is_empty() {
        info!("generating {} topic schema files in {:?}", missing.len(), schemas_dir);
        // Re-seed cleanly and generate all from scratch for simplicity.
        // (All files are written atomically, so existing ones are not touched.)
        let mut rng = Random::from_seed(seed);
        for idx in 0..count {
            let path = schema_path(schemas_dir, idx);
            if !path.exists() {
                generate_schema_file(&path, col_range.clone(), &mut rng)?;
            } else {
                // Burn the RNG state as if we had generated this one.
                let _ = rng.gen_range(col_range.clone());
            }
        }
    }

    Ok(())
}

// ── Deterministic batch seed ──────────────────────────────────────────────────

fn batch_seed(topic: &str, partition: &str, batch_idx: u64) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut h);
    partition.hash(&mut h);
    batch_idx.hash(&mut h);
    h.finish()
}

// ── Sampler thread ────────────────────────────────────────────────────────────
//
// Box<dyn ChunkLen> is !Send, so the sampler lives in a std::thread and
// communicates with the async worker via channels.

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
        let mut sampler = crate::load::build_sampler(&self.sample_path)
            .expect("failed to build sampler");
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
        let (req_tx, req_rx) = std::sync::mpsc::channel::<SamplerRequest>();
        let (result_tx, mut result_rx) = mpsc::channel::<MultiChunk>(2);
        let st = SamplerThread {
            sample_path: self.sample_path.clone(),
            req_rx,
            result_tx,
        };
        thread::spawn(move || st.run());

        let resume_from = {
            let s = self.state.lock().await;
            s.last_batch_for(&self.topic, &self.partition)
        };

        let origin: SystemTime = {
            let s = self.state.lock().await;
            if let Some(ref ts) = s.origin {
                humantime::parse_rfc3339(ts).unwrap_or(SystemTime::now())
            } else {
                SystemTime::now()
            }
        };

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
            let fire_at = origin
                + self.stagger_offset
                + self.batch_period * batch_idx as u32;

            if batch_idx >= catchup_to {
                let now = SystemTime::now();
                if fire_at > now {
                    let wait = fire_at.duration_since(now).unwrap_or_default();
                    tokio::time::sleep(wait).await;
                }
            }

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
                    continue;
                }
                Err(e) => {
                    warn!("{}/{} batch {} failed: {}", self.topic, self.partition, batch_idx, e);
                }
            }

            {
                let mut s = self.state.lock().await;
                s.set_last_batch(&self.topic, &self.partition, batch_idx);
                let _ = s.save(&self.state_path);
            }

            batch_idx += 1;
        }
    }
}

// ── Window management ─────────────────────────────────────────────────────────

fn spawn_window(
    client: &Client,
    topics: &TopicsConfig,
    schemas_dir: &Path,
    state: &Arc<Mutex<BatchState>>,
    state_path: &Path,
    window_start: usize,
    batch_period: Duration,
    speed: f64,
) -> Vec<JoinHandle<()>> {
    let window_end = (window_start + topics.active).min(topics.count);
    let total_partitions = (window_end - window_start) * topics.partitions;
    let stagger = if total_partitions > 1 {
        batch_period / total_partitions as u32
    } else {
        Duration::ZERO
    };

    let mut handles = vec![];
    let mut slot = 0usize;

    for topic_idx in window_start..window_end {
        let name = topic_name(topic_idx);
        let sample_path = schema_path(schemas_dir, topic_idx);

        for p in 0..topics.partitions {
            let worker = PartitionWorker {
                client: client.clone(),
                topic: name.clone(),
                partition: format!("partition-{p}"),
                sample_path: sample_path.clone(),
                rows: topics.rows,
                batch_period,
                stagger_offset: stagger * slot as u32,
                state: state.clone(),
                state_path: state_path.to_path_buf(),
            };
            handles.push(tokio::spawn(worker.run()));
            slot += 1;
        }
    }

    info!(
        "window [{window_start}, {window_end}): {} topics, {} partitions, period {:?} ({}x speed), stagger {:?}",
        window_end - window_start,
        total_partitions,
        batch_period,
        speed,
        stagger,
    );

    handles
}

// ── Public entry point ────────────────────────────────────────────────────────

pub async fn run_batch(url: &str, config: BatchConfig, state_path: &Path) -> Result<()> {
    let client = Client::new(url)?;
    client
        .healthy(Duration::from_secs(10), Duration::from_millis(100))
        .await?;

    let schemas_dir = config
        .schemas_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("batch-schemas"));

    let mut state = BatchState::load(state_path);

    // Assign a stable schema seed on first run.
    if state.schemas_seed.is_none() {
        state.schemas_seed = Some(rand::random());
    }
    if state.origin.is_none() {
        state.origin = Some(humantime::format_rfc3339(SystemTime::now()).to_string());
    }
    if state.window_since.is_none() {
        state.window_since = Some(humantime::format_rfc3339(SystemTime::now()).to_string());
    }
    state.save(state_path)?;

    // Generate any missing schema files.
    ensure_schemas(
        &schemas_dir,
        config.topics.count,
        config.topics.columns_min,
        config.topics.columns_max,
        state.schemas_seed.unwrap(),
    )?;

    let state = Arc::new(Mutex::new(state));

    let batch_period = config.topics.batch_interval.div_f64(config.speed);
    let rotation_period = config.topics.rotation_interval.div_f64(config.speed);

    // Compute how many rotations have elapsed since origin to restore window.
    {
        let mut s = state.lock().await;
        let origin = humantime::parse_rfc3339(s.origin.as_ref().unwrap())
            .unwrap_or(SystemTime::now());
        let elapsed = origin.elapsed().unwrap_or_default();
        let rotations = if rotation_period.is_zero() {
            0
        } else {
            (elapsed.as_nanos() / rotation_period.as_nanos()) as usize
        };
        let computed_window = (rotations * config.topics.active) % config.topics.count;
        if computed_window != s.window_start {
            info!(
                "restoring window to [{computed_window}) based on elapsed time (was {})",
                s.window_start
            );
            s.window_start = computed_window;
            s.window_since =
                Some(humantime::format_rfc3339(SystemTime::now()).to_string());
            s.save(state_path)?;
        }
    }

    let mut window_start = state.lock().await.window_start;
    let mut handles = spawn_window(
        &client,
        &config.topics,
        &schemas_dir,
        &state,
        state_path,
        window_start,
        batch_period,
        config.speed,
    );

    // Progress reporter.
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

    // Rotation loop.
    loop {
        tokio::time::sleep(rotation_period).await;

        // Stop current window.
        for h in handles.drain(..) {
            h.abort();
        }

        // Advance window.
        window_start = (window_start + config.topics.active) % config.topics.count;
        {
            let mut s = state.lock().await;
            s.window_start = window_start;
            s.window_since =
                Some(humantime::format_rfc3339(SystemTime::now()).to_string());
            s.save(state_path)?;
        }

        handles = spawn_window(
            &client,
            &config.topics,
            &schemas_dir,
            &state,
            state_path,
            window_start,
            batch_period,
            config.speed,
        );
    }
}

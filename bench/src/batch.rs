use std::collections::HashMap;
use std::hash::{Hash, Hasher as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use plateau_client::{Client, Error, MultiChunk};
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
    /// Min partitions per topic.
    pub partitions_min: usize,
    /// Max partitions per topic (exclusive).
    pub partitions_max: usize,
    /// Min of the overall rows-per-insert distribution. Each topic draws its
    /// own [min, max] sub-range from this distribution; every insert then
    /// samples a row count from that per-topic range.
    pub rows_min: usize,
    /// Max of the overall rows-per-insert distribution (exclusive).
    pub rows_max: usize,
    /// Real-world batch interval per topic.
    #[serde(with = "humantime_serde")]
    pub batch_interval: Duration,
}

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

/// Per-topic parameters derived deterministically from the schema seed.
#[derive(Debug, Clone)]
pub struct TopicParams {
    pub columns: usize,
    pub partitions: usize,
    /// Inclusive-min / exclusive-max row count for each insert into this topic.
    pub rows_min: usize,
    pub rows_max: usize,
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

// ── Range sampling (normal distribution) ──────────────────────────────────────

/// Draw an integer in [min, max) from a normal distribution centered on the
/// midpoint, with σ = range/4 (so ~95% of mass lands in range), clamped to
/// [min, max). Uses the Box-Muller transform.
fn normal_range(rng: &mut Random, min: usize, max: usize) -> usize {
    if max <= min + 1 {
        return min;
    }
    let lo = min as f64;
    let hi = (max - 1) as f64;
    let mean = (lo + hi) / 2.0;
    let std = (hi - lo) / 4.0;

    // u1 in (0, 1] to keep ln() finite.
    let u1: f64 = 1.0 - rng.gen_range(0.0..1.0);
    let u2: f64 = rng.gen_range(0.0..1.0);
    let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();

    (mean + z * std).round().clamp(lo, hi) as usize
}

// ── Per-topic parameter derivation ────────────────────────────────────────────

/// Stable per-topic RNG seed derived from the schema seed and topic index.
fn topic_seed(seed: u64, idx: usize) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut h);
    idx.hash(&mut h);
    h.finish()
}

/// Deterministically derive a topic's parameters. The same `rng` is then used
/// to generate the schema, so params and schema stay consistent.
fn draw_params(rng: &mut Random, t: &TopicsConfig) -> TopicParams {
    let columns = normal_range(rng, t.columns_min, t.columns_max);
    let partitions = normal_range(rng, t.partitions_min, t.partitions_max).max(1);
    // Each topic draws two values from the global rows distribution to form its
    // own [min, max] sub-range; per-insert counts are sampled from that range.
    let a = normal_range(rng, t.rows_min, t.rows_max);
    let b = normal_range(rng, t.rows_min, t.rows_max);
    let (rmin, rmax) = if a <= b { (a, b) } else { (b, a) };
    TopicParams {
        columns,
        partitions,
        rows_min: rmin,
        rows_max: rmax + 1,
    }
}

/// Recompute a topic's parameters without touching the filesystem.
fn topic_params(seed: u64, idx: usize, t: &TopicsConfig) -> TopicParams {
    let mut rng = Random::from_seed(topic_seed(seed, idx));
    draw_params(&mut rng, t)
}

// ── Schema generation ─────────────────────────────────────────────────────────

fn topic_name(idx: usize) -> String {
    format!("topic-{idx:04}")
}

fn schema_path(schemas_dir: &Path, idx: usize) -> PathBuf {
    schemas_dir.join(format!("{}.arrow", topic_name(idx)))
}

/// Generate a schema with `n_cols` flat columns and write a one-row seed batch
/// to `path` so `build_sampler` can read it back. `rng` must already have had
/// `draw_params` applied (so its state follows the params draw).
fn generate_schema_file(path: &Path, n_cols: usize, rng: &mut Random) -> Result<()> {
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

    let mut time_sampler =
        primitive_len_sampler::<_, _, arrow_array::types::Int64Type>(Now, AlwaysValid);
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

/// Ensure every topic's schema file exists, generating any that are missing.
/// Each topic is generated from its own stable seed, so files are reproducible
/// and independent — regenerating one never disturbs the others.
pub fn ensure_schemas(schemas_dir: &Path, t: &TopicsConfig, seed: u64) -> Result<()> {
    std::fs::create_dir_all(schemas_dir)?;

    let mut generated = 0;
    for idx in 0..t.count {
        let path = schema_path(schemas_dir, idx);
        if path.exists() {
            continue;
        }
        let mut rng = Random::from_seed(topic_seed(seed, idx));
        let params = draw_params(&mut rng, t);
        generate_schema_file(&path, params.columns, &mut rng)?;
        generated += 1;
    }

    if generated > 0 {
        info!("generated {generated} topic schema files in {schemas_dir:?}");
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
    rows_min: usize,
    rows_max: usize,
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
            // Sample this insert's row count from the topic's range (normal).
            let rows = normal_range(&mut random, req.rows_min, req.rows_max);
            sampler.set_len(rows);
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
    rows_min: usize,
    rows_max: usize,
    batch_period: Duration,
    stagger_offset: Duration,
    state: Arc<Mutex<BatchState>>,
    state_path: PathBuf,
}

impl PartitionWorker {
    async fn run(mut self) {
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
            let req = SamplerRequest {
                seed,
                rows_min: self.rows_min,
                rows_max: self.rows_max,
            };
            if req_tx.send(req).is_err() {
                break;
            }
            let multi = match result_rx.recv().await {
                Some(m) => m,
                None => break,
            };

            let start = Instant::now();
            let r = self.client
                .append_queue(&self.topic, &self.partition, multi)
                .await;

            match r {
                Ok(Some(ok)) => {
                    let rows = ok.span.end - ok.span.start;
                    tracing::debug!(
                        "{}/{} batch {} → {} rows in {:?}",
                        self.topic, self.partition, batch_idx, rows, start.elapsed()
                    );
                }
                Ok(None) => {}
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

#[allow(clippy::too_many_arguments)]
fn spawn_window(
    client: &Client,
    topics: &TopicsConfig,
    schemas_dir: &Path,
    state: &Arc<Mutex<BatchState>>,
    state_path: &Path,
    window_start: usize,
    batch_period: Duration,
    speed: f64,
    schemas_seed: u64,
) -> Vec<JoinHandle<()>> {
    let window_end = (window_start + topics.active).min(topics.count);

    // Resolve each topic's parameters; partitions vary per topic.
    let params: Vec<(usize, TopicParams)> = (window_start..window_end)
        .map(|idx| (idx, topic_params(schemas_seed, idx, topics)))
        .collect();

    let total_partitions: usize = params.iter().map(|(_, p)| p.partitions).sum();
    let stagger = if total_partitions > 1 {
        batch_period / total_partitions as u32
    } else {
        Duration::ZERO
    };

    let mut handles = vec![];
    let mut slot = 0usize;

    for (topic_idx, tp) in &params {
        let name = topic_name(*topic_idx);
        let sample_path = schema_path(schemas_dir, *topic_idx);

        for p in 0..tp.partitions {
            let worker = PartitionWorker {
                client: client.clone(),
                topic: name.clone(),
                partition: format!("partition-{p}"),
                sample_path: sample_path.clone(),
                rows_min: tp.rows_min,
                rows_max: tp.rows_max,
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

    let schemas_seed = state.schemas_seed.unwrap();

    // Generate any missing schema files.
    ensure_schemas(&schemas_dir, &config.topics, schemas_seed)?;

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
        schemas_seed,
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
            schemas_seed,
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn cfg() -> TopicsConfig {
        TopicsConfig {
            count: 16,
            active: 4,
            rotation_interval: Duration::from_secs(60),
            columns_min: 3,
            columns_max: 35,
            partitions_min: 1,
            partitions_max: 8,
            rows_min: 1000,
            rows_max: 50000,
            batch_interval: Duration::from_secs(60),
        }
    }

    #[test]
    fn normal_range_stays_in_bounds() {
        let mut rng = Random::from_seed(7);
        for _ in 0..10_000 {
            let v = normal_range(&mut rng, 3, 35);
            assert!((3..35).contains(&v), "out of range: {v}");
        }
        // Degenerate ranges.
        assert_eq!(normal_range(&mut rng, 5, 5), 5);
        assert_eq!(normal_range(&mut rng, 5, 6), 5);
    }

    #[test]
    fn topic_params_are_deterministic() {
        let t = cfg();
        for idx in 0..t.count {
            let a = topic_params(42, idx, &t);
            let b = topic_params(42, idx, &t);
            assert_eq!(a.columns, b.columns);
            assert_eq!(a.partitions, b.partitions);
            assert_eq!(a.rows_min, b.rows_min);
            assert_eq!(a.rows_max, b.rows_max);
            assert!(a.partitions >= 1);
            assert!((t.columns_min..t.columns_max).contains(&a.columns));
            assert!(a.rows_min < a.rows_max);
        }
        // Different seeds should generally differ.
        let p1 = topic_params(1, 0, &t);
        let p2 = topic_params(2, 0, &t);
        assert!(p1.columns != p2.columns || p1.partitions != p2.partitions || p1.rows_min != p2.rows_min);
    }

    #[test]
    fn generated_schemas_are_loadable_and_stable() {
        let t = cfg();
        let dir = std::env::temp_dir().join(format!("batch-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        ensure_schemas(&dir, &t, 99).unwrap();

        for idx in 0..t.count {
            let path = schema_path(&dir, idx);
            assert!(path.exists(), "missing schema {idx}");
            // build_sampler must read it back and produce rows.
            let mut sampler = crate::load::build_sampler(&path).unwrap();
            sampler.set_len(10);
            let mut rng = Random::from_seed(idx as u64);
            let multi = sampler.generate(&mut rng);
            let cols = multi.schema.fields().len();
            let expected = topic_params(99, idx, &t).columns + 1; // + time column
            assert_eq!(cols, expected, "topic {idx} column count mismatch");
        }

        // Re-running must not regenerate (files already present).
        let before: Vec<_> = (0..t.count)
            .map(|i| std::fs::metadata(schema_path(&dir, i)).unwrap().modified().unwrap())
            .collect();
        ensure_schemas(&dir, &t, 99).unwrap();
        let after: Vec<_> = (0..t.count)
            .map(|i| std::fs::metadata(schema_path(&dir, i)).unwrap().modified().unwrap())
            .collect();
        assert_eq!(before, after, "schemas were unexpectedly regenerated");

        let _ = std::fs::remove_dir_all(&dir);
    }
}

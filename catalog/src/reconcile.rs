//! Reconciliation job for verifying consistency between the manifest and files on disk.
//!
//! This module provides functionality to:
//! - Verify all files on disk are tracked in the manifest
//! - Verify file sizes on disk match sizes in the manifest
//! - Detect files that don't belong to any segment in their directory

use std::collections::{BTreeSet, HashSet};
use std::iter;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::slog::SegmentIndex;

use tokio::fs;

use crate::data::segment::Segment;
use anyhow::Result;
use bytesize::ByteSize;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::data::RecordIndex;
use crate::manifest::{PartitionId, SegmentData};
use crate::partition::Partition;
use crate::slog::Slog;
use crate::topic::Topic;

/// Whether a segment is sealed relative to a partition's `sealed_ix` watermark.
///
/// A segment is sealed only when the partition has a watermark (`Some`) and the
/// segment index is at or below it. When the watermark is `None` (no segment has
/// sealed durably yet) every segment is treated as active.
fn is_sealed(sealed_ix: Option<SegmentIndex>, index: SegmentIndex) -> bool {
    sealed_ix.is_some_and(|watermark| index <= watermark)
}

/// Configuration for the reconciliation job
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReconcileConfig {
    /// Maximum units of work to process in a single run
    pub limit: Option<usize>,
    /// Ratio for controlling how much idle time the reconciler takes
    /// If zero, we are never idle, if one, we idle for as long as we work, 10 we idle 10x the work
    /// time, etc.
    pub idle_ratio: f64,
    /// Whether to track individual file paths or just count them
    pub track_files: bool,
    /// Set of fixes to apply during reconciliation
    #[serde(default)]
    pub fixes: BTreeSet<ReconcileFix>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReconcileFix {
    /// Workaround for inability to pass an empty collection via [config]
    Noop,
    UpdateManifestSizes,
    // TODO: RemoveOrphans,
    // TODO: RemoveUntrackedSegments
}

/// A reconciliation job that incrementally validates consistency between
/// the manifest and files on disk.
#[derive(Debug)]
pub struct ReconcileJob {
    /// Catalog to reconcile
    catalog: Arc<Catalog>,
    /// Current position in the reconciliation process
    state: ReconcileState,
    /// Configuration for the reconciliation job
    config: ReconcileConfig,
}

/// The current state of a reconciliation job
#[derive(Clone, Debug, Default)]
struct ReconcileState {
    /// Current topic being processed
    current_topic_index: usize,
    /// Current partition being processed within the current topic
    current_partition_index: usize,
    /// All topics in the catalog
    topics: Option<Vec<String>>,
    /// Accumulator for all segments in the current topic
    topic_segments: BTreeSet<PathBuf>,
    /// Report accumulated during the reconciliation (sealed + active buckets)
    report: ReconcileReport,
}

impl ReconcileState {
    pub fn new(track_files: bool) -> Self {
        Self {
            report: if track_files {
                ReconcileReport::with_path_tracking()
            } else {
                ReconcileReport::default()
            },
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub enum PathStats {
    Paths(Vec<PathBuf>),
    Counter(usize),
}

impl Default for PathStats {
    fn default() -> Self {
        Self::Counter(0)
    }
}

impl PathStats {
    pub fn empty_paths() -> Self {
        Self::Paths(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Paths(paths) => paths.len(),
            Self::Counter(count) => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Per-entry bookkeeping for a recorded diff (a missing file, an orphan, or a
/// size mismatch). Carries an optional segment locator so the low-water filter
/// can later drop entries that retention legitimately retired mid-scan. Kept
/// in lock-step with the bucket's `paths`/counter so filtering can remove the
/// matching path, byte count, and diff entry together.
#[derive(Debug, Clone)]
struct DiffEntry {
    /// Segment this diff is about, used for filtering. `None` for entries with
    /// no segment identity (e.g. a genuinely foreign orphan file), which the
    /// low-water filter always keeps.
    locator: Option<(PartitionId, SegmentIndex)>,
    /// Bytes this entry contributed to `total_bytes`, retained so the filter
    /// can subtract it back out when the entry is dropped.
    bytes: usize,
}

// File tracking statistics to track untracked, checked, and missing files
#[derive(Debug, Clone, Default)]
pub struct FileStats {
    pub paths: PathStats,
    pub total_bytes: usize,
    /// Diff locators, kept aligned with `paths` for buckets that record diffs
    /// (missing/untracked/size-mismatch). Empty for `files_checked`, which is
    /// never filtered.
    diffs: Vec<DiffEntry>,
}

impl FileStats {
    pub fn display_bytes(&self) -> ByteSize {
        ByteSize(self.total_bytes as u64)
    }

    pub fn empty_paths() -> Self {
        Self {
            paths: PathStats::Paths(Vec::new()),
            total_bytes: 0,
            diffs: Vec::new(),
        }
    }

    pub fn add_path(&mut self, path: PathBuf, bytes: usize) {
        match &mut self.paths {
            PathStats::Paths(paths) => paths.push(path),
            PathStats::Counter(count) => *count += 1,
        }
        self.total_bytes += bytes;
    }

    pub fn add_paths(&mut self, paths: Vec<PathBuf>, bytes: usize) {
        match &mut self.paths {
            PathStats::Paths(existing_paths) => existing_paths.extend(paths),
            PathStats::Counter(count) => *count += paths.len(),
        }
        self.total_bytes += bytes;
    }

    /// Record a diff entry (missing/untracked/size-mismatch) along with the
    /// optional segment locator used by the low-water filter. Unlike
    /// [add_path], this always appends a parallel [DiffEntry] so the filter can
    /// operate structurally instead of reparsing paths. Used regardless of
    /// `track_files`: the locator is needed to filter even when paths are not
    /// retained.
    pub fn add_diff(
        &mut self,
        path: PathBuf,
        bytes: usize,
        locator: Option<(PartitionId, SegmentIndex)>,
    ) {
        match &mut self.paths {
            PathStats::Paths(paths) => paths.push(path),
            PathStats::Counter(count) => *count += 1,
        }
        self.total_bytes += bytes;
        self.diffs.push(DiffEntry { locator, bytes });
    }

    /// Drop diff entries for `partition` whose segment index is strictly below
    /// `low_water`, returning the number removed. Entries with no locator (a
    /// foreign orphan) and entries from other partitions are always kept.
    fn drop_below_low_water(&mut self, partition: &PartitionId, low_water: SegmentIndex) -> usize {
        if self.diffs.is_empty() {
            return 0;
        }

        let keep: Vec<bool> = self
            .diffs
            .iter()
            .map(|d| match &d.locator {
                Some((p, ix)) => !(p == partition && *ix < low_water),
                None => true,
            })
            .collect();
        let removed = keep.iter().filter(|k| !**k).count();
        if removed == 0 {
            return 0;
        }

        match &mut self.paths {
            PathStats::Paths(paths) => {
                let mut keep_it = keep.iter();
                paths.retain(|_| *keep_it.next().unwrap());
            }
            PathStats::Counter(count) => {
                *count -= removed;
            }
        }
        let mut keep_it = keep.iter();
        self.diffs.retain(|_| *keep_it.next().unwrap());
        // total_bytes for a diff bucket is exactly the sum of its entries'
        // bytes, so recompute it from what survived.
        self.total_bytes = self.diffs.iter().map(|d| d.bytes).sum();

        removed
    }

    pub fn len(&self) -> usize {
        match &self.paths {
            PathStats::Paths(paths) => paths.len(),
            PathStats::Counter(count) => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Counts of sealed-bucket diff entries dropped by the low-water filter because
/// the underlying segment was retired by retention while the pass was scanning.
/// These distinguish a quiet system from one where retention is racing
/// reconcile and the report is being smoothed.
#[derive(Debug, Clone, Default)]
pub struct RetentionRemoved {
    /// `missing_files` entries dropped (segment now below low-water).
    pub missing_files: usize,
    /// `untracked_files` (orphan) entries dropped.
    pub untracked_files: usize,
    /// `size_mismatches` entries dropped. Rare, since the CAS fix path already
    /// tolerates concurrent removal, but recorded for symmetry and visibility.
    pub size_mismatches: usize,
}

impl RetentionRemoved {
    /// Total number of diff entries dropped across all three buckets.
    pub fn rm_total(&self) -> usize {
        self.missing_files + self.untracked_files + self.size_mismatches
    }
}

/// Parse the partition name and segment index out of a segment file name of
/// the form `{topic}-{partition}-{index}` (see [Slog::segment_path] and
/// [Partition::slog_name]). The index is the final dash-delimited, purely
/// numeric token, so partition names containing dashes parse correctly. Returns
/// `None` for files that are not segment main files (foreign orphans, part
/// files like `...-{index}.part`), which the low-water filter then always keeps.
fn parse_segment_locator(topic: &str, file_name: &str) -> Option<(String, SegmentIndex)> {
    let rest = file_name.strip_prefix(topic)?.strip_prefix('-')?;
    let (partition, index) = rest.rsplit_once('-')?;
    if partition.is_empty() {
        return None;
    }
    let index: usize = index.parse().ok()?;
    Some((partition.to_string(), SegmentIndex(index)))
}

/// Statistics collected during reconciliation
#[derive(Debug, Clone, Default)]
pub struct ReconcileStats {
    /// Number of files checked
    pub files_checked: FileStats,
    /// Number of untracked files found
    pub untracked_files: FileStats,
    /// Number of size mismatches found
    pub size_mismatches: FileStats,
    /// Number of missing files found
    pub missing_files: FileStats,
    /// Total expected byte count
    pub expected_size: ByteSize,
    /// Total actual byte count
    pub actual_size: ByteSize,
}

impl ReconcileStats {
    pub fn with_path_tracking() -> Self {
        Self {
            files_checked: FileStats::empty_paths(),
            untracked_files: FileStats::empty_paths(),
            size_mismatches: FileStats::empty_paths(),
            missing_files: FileStats::empty_paths(),
            ..Default::default()
        }
    }
}

/// An informational report for the currently *active* (writeable) tail segment
/// of a single partition.
///
/// The active segment is deliberately excluded from the strict sealed-diff
/// pipeline. Writes accumulate in it before the manifest is flushed, so its
/// on-disk size routinely runs ahead of the size recorded in the manifest.
/// Treating that as a "size mismatch" would flag the active segment of every
/// partition on every pass. This bucket exists to surface that drift without
/// ever acting on it — reconciliation fixes never touch the active segment.
///
/// How to read the signals:
/// - `delta != 0` is *expected*: disk is ahead of the manifest between flushes.
/// - `delta < 0` is alert-worthy: the manifest claims more bytes than exist on
///   disk, which points at corruption or an accounting bug.
/// - `delta` growing unboundedly across consecutive scans suggests manifest
///   updates are stuck or falling behind.
#[derive(Debug, Clone)]
pub struct ActiveSegmentReport {
    /// Topic the active segment belongs to.
    pub topic: String,
    /// Partition the active segment belongs to.
    pub partition: String,
    /// Size the manifest currently records for the active segment.
    pub manifest_size: usize,
    /// Size observed on disk (main segment file plus any parts).
    pub disk_size: usize,
    /// `disk_size as i64 - manifest_size as i64`. Signed; negative is an alert
    /// signal (manifest claims more bytes than disk has).
    pub delta: i64,
}

/// The full output of a reconciliation pass, split into two buckets.
///
/// `sealed` is the strict pipeline: every segment at or below a partition's
/// `sealed_ix` watermark is diffed against the manifest and, when configured,
/// fixed. This is the historical [ReconcileStats] behaviour, unchanged.
///
/// `active` is informational only: one entry per partition whose active tail
/// segment was observed during the pass. Reconciliation fixes never touch the
/// active bucket. See [ActiveSegmentReport] for how to interpret its entries.
#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    /// Strict sealed-segment diffs (the historical [ReconcileStats] shape).
    pub sealed: ReconcileStats,
    /// Informational per-partition active-segment reports.
    pub active: Vec<ActiveSegmentReport>,
    /// How many sealed-bucket diffs the low-water filter dropped as retention
    /// churn. See [RetentionRemoved].
    pub retention_rm: RetentionRemoved,
}

impl ReconcileReport {
    fn with_path_tracking() -> Self {
        Self {
            sealed: ReconcileStats::with_path_tracking(),
            active: Vec::new(),
            retention_rm: RetentionRemoved::default(),
        }
    }
}

impl ReconcileJob {
    /// Create a new reconciliation job for the given catalog with default configuration
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self::with_config(catalog, ReconcileConfig::default())
    }

    /// Create a new reconciliation job for the given catalog with custom configuration
    pub fn with_config(catalog: Arc<Catalog>, config: ReconcileConfig) -> Self {
        Self {
            catalog,
            state: ReconcileState::new(config.track_files),
            config,
        }
    }

    /// Run a limited reconciliation pass, returning true when complete
    ///
    /// This method will process up to `limit` units of work and return
    /// true when the entire reconciliation is complete, false otherwise.
    /// If limit is None, run until completion.
    pub async fn run(&mut self, limit: Option<usize>) -> Result<bool> {
        self.run_pass(limit, true).await
    }

    /// Run a pass, optionally applying the low-water retention filter on
    /// completion. The filter is only ever skipped by tests that need to
    /// interpose a retention removal between the scan and the filter; all
    /// production callers go through [run] with the filter enabled.
    async fn run_pass(&mut self, limit: Option<usize>, apply_filter: bool) -> Result<bool> {
        // Use the limit from the parameter if provided, otherwise use config
        let effective_limit = limit.or(self.config.limit);

        let mut work_done = 0;
        let max_work = effective_limit.unwrap_or(usize::MAX);

        while work_done < max_work {
            let start_time = Instant::now();
            let done = self.process_next_unit().await?;
            let work_time = start_time.elapsed();

            work_done += 1;

            // If we're done, return true
            if done {
                if apply_filter {
                    self.apply_low_water_filter().await;
                }
                let report = self.report();
                info!("Reconciliation complete: {:?}", report);
                return Ok(true);
            }

            // Sleep for ratio * work_time to control how much idle time the reconciler takes
            if self.config.idle_ratio > 0.0 {
                let sleep_duration = Duration::from_micros(
                    (work_time.as_micros() as f64 * self.config.idle_ratio) as u64,
                );
                tokio::time::sleep(sleep_duration).await;
            }
        }

        Ok(false)
    }

    /// Process the next unit of work in the reconciliation
    async fn process_next_unit(&mut self) -> Result<bool> {
        // Load topics if we haven't already
        let topics = {
            if let Some(topics) = &self.state.topics {
                topics
            } else {
                self.state.topics = Some(self.catalog.manifest().get_topics().await);
                self.state.topics.as_ref().unwrap()
            }
        };

        // Get current positions
        let (current_topic_index, current_partition_index, topics_len) = {
            (
                self.state.current_topic_index,
                self.state.current_partition_index,
                topics.len(),
            )
        };

        // If we've processed all topics, we're done
        if current_topic_index >= topics_len {
            return Ok(true);
        }

        // Get the current topic name
        let topic_name = topics[current_topic_index].clone();

        debug!("Reconciling topic: {}", topic_name);

        // Get all partitions for this topic
        let partitions = self.catalog.manifest().get_partitions(&topic_name).await;

        // If we've processed all partitions in this topic, move to the next topic
        if current_partition_index >= partitions.len() {
            let topics_len = topics.len();
            let topic_segments = mem::take(&mut self.state.topic_segments);
            self.identify_untracked_files(&topic_name, topic_segments)
                .await?;
            self.state.current_partition_index = 0;
            self.state.current_topic_index += 1;

            // If we've processed all topics, we're done
            if self.state.current_topic_index >= topics_len {
                return Ok(true);
            }

            // Not done yet, but we've completed this unit of work
            return Ok(false);
        }

        // Process the current partition
        let partition_name = partitions[current_partition_index].clone();
        debug!("Reconciling partition: {}/{}", topic_name, partition_name);

        // Process this partition
        let partition_segments = self
            .process_partition_phase(&topic_name, &partition_name)
            .await?;

        self.state.topic_segments.extend(partition_segments);
        self.state.current_partition_index += 1;

        // We've processed one unit of work
        Ok(false)
    }

    async fn identify_untracked_files(
        &mut self,
        topic_name: &str,
        tracked_files: BTreeSet<PathBuf>,
    ) -> Result<()> {
        let root = self.catalog.topic_root();
        let topic_path = Topic::partition_root(root, topic_name);

        // Get all files in the partition directory
        let partition_files = self.list_segment_files(&topic_path).await?;
        let mut partition_bytes = 0;
        for path in &partition_files {
            if let Ok(metadata) = fs::metadata(path).await {
                partition_bytes += metadata.len() as usize;
            }
        }

        // Add the partition files to our stats
        self.state
            .report
            .sealed
            .files_checked
            .add_paths(partition_files.clone(), partition_bytes);
        debug!(
            "Found {} files in partition directory",
            partition_files.len()
        );

        for file_path in partition_files {
            debug!("Checking file: {:?}", file_path);
            if !tracked_files.contains(&file_path) {
                warn!("Untracked file in topic {:?}: {:?}", topic_name, file_path);
                // Add the untracked path to our stats. If the file name parses
                // as a segment main file we tag it with a locator so the
                // low-water filter can drop it when retention removed both the
                // row and (eventually) the file mid-scan. Foreign orphans that
                // do not parse get no locator and are never filtered.
                let file_size = fs::metadata(&file_path)
                    .await
                    .map(|m| m.len() as usize)
                    .unwrap_or(0);
                let locator = file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|name| parse_segment_locator(topic_name, name))
                    .map(|(partition, index)| (PartitionId::new(topic_name, partition), index));
                self.state.report.sealed.untracked_files.add_diff(
                    file_path.clone(),
                    file_size,
                    locator,
                );
            } else {
                debug!("Found tracked path {:?}", file_path);
            }
        }

        Ok(())
    }

    /// Process the current partition, returning true when complete
    async fn process_partition_phase(
        &mut self,
        topic_name: &str,
        partition_name: &str,
    ) -> Result<BTreeSet<PathBuf>> {
        // Process the entire partition in one go since we don't need incremental resume
        let root = self.catalog.topic_root();
        let partition_id = PartitionId::new(topic_name, partition_name);
        let topic_path = Topic::partition_root(root, topic_name);

        debug!(
            "Processing partition {}/{} with path {:?}",
            topic_name, partition_name, topic_path
        );

        // Read the sealed watermark once for this partition pass. Segments at or
        // below it are durable and run through the strict sealed-diff pipeline;
        // anything above it (or every segment, if the partition has not sealed
        // one yet) is the currently active tail and goes to the informational
        // active bucket instead. The watermark never moves backward, so this
        // single read is a stable basis for the whole pass.
        let sealed_ix = {
            let topic = self.catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            partition.sealed_ix()
        };

        // Create sets to track files
        let mut tracked_files = BTreeSet::new();

        // Fetch all segments for this partition
        let segments_stream = self.catalog.manifest().stream_segments(
            &partition_id,
            RecordIndex(0),
            crate::data::index::Ordering::Forward,
        );

        let segments: Vec<SegmentData> = segments_stream.collect().await;
        if let Some((start, end)) = segments.first().zip(segments.last()) {
            debug!(
                "Fetched {} segments: {} ..= {} (sealed_ix={:?})",
                segments.len(),
                start.index.0,
                end.index.0,
                sealed_ix,
            );
        } else {
            debug!("Found no segments")
        }

        // Validate each segment
        for segment in segments {
            debug!("Validating segment: {:?}", segment.index);

            let slog_name = Partition::slog_name(&partition_id);
            let segment_file_name = format!("{}-{}", slog_name, segment.index.0);
            let segment_path = Slog::segment_path(&topic_path, &slog_name, segment.index);

            debug!("Checking segment file: {} at {:?}", slog_name, segment_path);

            // Mark this file as tracked regardless of bucket so the orphan
            // detection phase does not false-positive on active segment files.
            tracked_files.insert(segment_path.clone());

            // A segment is "sealed" only when the partition has a watermark and
            // this segment's index is at or below it. Everything else is the
            // active tail (this includes the all-segments-active case when the
            // partition has never sealed a segment, i.e. sealed_ix is None).
            if is_sealed(sealed_ix, segment.index) {
                self.process_sealed_segment(
                    &partition_id,
                    &segment,
                    &segment_file_name,
                    segment_path,
                    &mut tracked_files,
                )
                .await;
            } else {
                self.process_active_segment(
                    topic_name,
                    partition_name,
                    &segment,
                    &segment_file_name,
                    segment_path,
                    &mut tracked_files,
                )
                .await;
            }
        }

        // We've completed processing this partition
        Ok(tracked_files)
    }

    /// Drop sealed-bucket diff entries that retention legitimately retired
    /// while this pass was scanning, and record how many were dropped in
    /// [ReconcileReport::retention_rm].
    ///
    /// SOUNDNESS: retention in plateau is monotonic-from-the-bottom — it only
    /// ever removes the *oldest* segment of a partition (see
    /// `Partition::remove_oldest`), and partition deletion does not exist:
    /// partitions only age out one segment at a time via retention. Therefore
    /// the manifest's current minimum segment index ("low water", from
    /// [Manifest::get_min_segment]) is a hard floor — any segment below it has
    /// been retired and is gone for good. A diff recorded against such a segment
    /// is retention churn, not corruption: the segment's row was in our snapshot
    /// (a spurious "missing file" once the file was deleted), or its file was on
    /// disk during the directory walk (a spurious orphan once the row was
    /// deleted), and it has since been retired. Re-reading the low water after
    /// the scan and dropping anything below it is sound and never hides a real
    /// problem on a live segment.
    ///
    /// The active bucket is unaffected: active segments sit above `sealed_ix`,
    /// which is at or above the low water, so they are never matched here.
    async fn apply_low_water_filter(&mut self) {
        // Collect the partitions that contributed at least one sealed diff.
        // PartitionId is Hash + Eq (but not Ord), so use a HashSet.
        let mut partitions: HashSet<PartitionId> = HashSet::new();
        for bucket in [
            &self.state.report.sealed.missing_files,
            &self.state.report.sealed.untracked_files,
            &self.state.report.sealed.size_mismatches,
        ] {
            for entry in &bucket.diffs {
                if let Some((partition, _)) = &entry.locator {
                    partitions.insert(partition.clone());
                }
            }
        }

        for partition in partitions {
            // `None` means the partition currently has no segments at all (e.g.
            // fully drained). There is no floor to compare against, so treat the
            // filter as a no-op rather than guessing — and never panic.
            let Some(low_water) = self.catalog.manifest().get_min_segment(&partition).await else {
                continue;
            };

            let report = &mut self.state.report;
            let removed_missing = report
                .sealed
                .missing_files
                .drop_below_low_water(&partition, low_water);
            let removed_untracked = report
                .sealed
                .untracked_files
                .drop_below_low_water(&partition, low_water);
            let removed_mismatch = report
                .sealed
                .size_mismatches
                .drop_below_low_water(&partition, low_water);

            report.retention_rm.missing_files += removed_missing;
            report.retention_rm.untracked_files += removed_untracked;
            report.retention_rm.size_mismatches += removed_mismatch;

            if removed_missing + removed_untracked + removed_mismatch > 0 {
                debug!(
                    "Low-water filter dropped {} missing, {} orphan, {} mismatch diffs below {:?} for {}",
                    removed_missing, removed_untracked, removed_mismatch, low_water, partition
                );
            }
        }
    }

    /// Strict diff (and optional CAS-fix) for a sealed segment. This preserves
    /// the historical reconciliation behaviour and only ever touches the sealed
    /// bucket of the report.
    async fn process_sealed_segment(
        &mut self,
        partition_id: &PartitionId,
        segment: &SegmentData,
        segment_file_name: &str,
        segment_path: PathBuf,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) {
        // Check if the file exists
        if !segment_path.exists() {
            warn!("Missing file {:?}", segment_path);
            // Add the missing path to our stats, tagged with its segment
            // locator so the low-water filter can drop it if retention retired
            // this segment mid-scan.
            self.state.report.sealed.missing_files.add_diff(
                segment_path.clone(),
                segment.size,
                Some((partition_id.clone(), segment.index)),
            );
            return;
        }

        let total_actual_size = self
            .segment_disk_size(&segment_path, segment_file_name, tracked_files)
            .await;

        let expected_size = ByteSize(segment.size as u64);
        let actual_size = ByteSize(total_actual_size as u64);
        self.state.report.sealed.expected_size =
            ByteSize(self.state.report.sealed.expected_size.as_u64() + expected_size.as_u64());
        self.state.report.sealed.actual_size =
            ByteSize(self.state.report.sealed.actual_size.as_u64() + actual_size.as_u64());

        // Compare total size with expected size
        debug!(
            "Comparing sizes - total_actual_size={}, segment.size={}, diff={}",
            total_actual_size,
            segment.size,
            total_actual_size.abs_diff(segment.size)
        );
        if total_actual_size.abs_diff(segment.size) > 0 {
            warn!(
                "Size mismatch for segment {}. Expected {}, actual {}",
                segment_file_name, expected_size, actual_size
            );
            // Add the mismatched path to our stats, tagged with its segment
            // locator for the low-water filter.
            self.state.report.sealed.size_mismatches.add_diff(
                segment_path.clone(),
                // NOTE: this is probably not ideal as it can "overcount" the total difference
                total_actual_size.abs_diff(segment.size),
                Some((partition_id.clone(), segment.index)),
            );

            if self
                .config
                .fixes
                .contains(&ReconcileFix::UpdateManifestSizes)
            {
                // Apply the fix conditionally on the size we observed. If
                // retention removed this segment (or another writer changed
                // it) since we snapshotted the manifest, the update is a
                // no-op rather than re-creating a stale row. This keeps the
                // fix safe to run while writes and retention are in flight.
                let applied = self
                    .catalog
                    .manifest()
                    .update_size_if_unchanged(
                        partition_id,
                        segment.index,
                        segment.size,
                        total_actual_size,
                    )
                    .await;
                if applied {
                    info!("Fixed size mismatch for segment {}", segment_file_name);
                } else {
                    info!(
                        "Skipped stale size fix for segment {} (concurrently removed or changed)",
                        segment_file_name
                    );
                }
            }
        } else {
            debug!(
                "Segment {} size ok. Expected: {}, actual: {}",
                segment_file_name, segment.size, total_actual_size
            );
        }
    }

    /// Record an informational active-segment report. This never contributes a
    /// size mismatch to the sealed bucket and never applies a fix — the active
    /// tail segment is, by definition, not sealed, so its on-disk size legally
    /// runs ahead of the manifest between flushes.
    async fn process_active_segment(
        &mut self,
        topic_name: &str,
        partition_name: &str,
        segment: &SegmentData,
        segment_file_name: &str,
        segment_path: PathBuf,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) {
        let disk_size = if segment_path.exists() {
            self.segment_disk_size(&segment_path, segment_file_name, tracked_files)
                .await
        } else {
            // The active segment may not have been flushed to disk yet. A zero
            // disk size against a non-zero manifest size yields a negative delta,
            // which is exactly the alert signal we want to surface.
            debug!("Active segment file {:?} not present on disk", segment_path);
            0
        };

        let manifest_size = segment.size;
        let delta = disk_size as i64 - manifest_size as i64;
        debug!(
            "Active segment {} for {}/{}: manifest_size={}, disk_size={}, delta={}",
            segment_file_name, topic_name, partition_name, manifest_size, disk_size, delta
        );

        self.state.report.active.push(ActiveSegmentReport {
            topic: topic_name.to_string(),
            partition: partition_name.to_string(),
            manifest_size,
            disk_size,
            delta,
        });
    }

    /// Compute the on-disk size of a segment: the main file plus any parts.
    /// Every part path encountered is inserted into `tracked_files` so the
    /// orphan-detection phase does not flag it. The caller is responsible for
    /// confirming the main segment file exists before calling.
    async fn segment_disk_size(
        &self,
        segment_path: &Path,
        segment_file_name: &str,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) -> usize {
        let mut total_actual_size = 0;

        // Check main segment file
        match fs::metadata(segment_path).await {
            Ok(metadata) => {
                total_actual_size += metadata.len() as usize;
                debug!(
                    "Segment {} file size: {}",
                    segment_file_name,
                    metadata.len()
                );
            }
            Err(e) => {
                warn!(
                    "Error getting metadata for segment {}: {:?}",
                    segment_file_name, e
                );
            }
        }

        // Check for associated parts and add their size
        let segment_file = Segment::at(segment_path.to_path_buf());
        for part_path in segment_file
            .parts()
            .chain(iter::once(segment_file.cache_path()))
        {
            if part_path.exists() {
                tracked_files.insert(part_path.clone());
                if part_path != segment_file.cache_path() {
                    match fs::metadata(&part_path).await {
                        Ok(metadata) => {
                            total_actual_size += metadata.len() as usize;
                            debug!("Part {:?} size: {}", part_path, metadata.len());
                        }
                        Err(e) => {
                            warn!("Error getting metadata for part {:?}: {:?}", part_path, e);
                        }
                    }
                }
            } else {
                debug!("Part {:?} does not exist", part_path);
            }
        }

        total_actual_size
    }

    /// List all segment-related files in a partition directory
    async fn list_segment_files(&self, partition_path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if partition_path.exists() {
            let mut entries = fs::read_dir(partition_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_file() {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    /// Get the current reconciliation report (sealed + active buckets)
    pub fn report(&self) -> &ReconcileReport {
        &self.state.report
    }

    /// Run a full pass but *without* applying the low-water retention filter.
    /// Test-only seam: paired with [run_low_water_filter] it lets a test
    /// deterministically interpose a retention removal (manifest row delete)
    /// between the scan that records a diff and the filter that drops it,
    /// reproducing the reconcile/retention race without spawning or sleeping.
    #[cfg(test)]
    pub(crate) async fn run_without_low_water_filter(
        &mut self,
        limit: Option<usize>,
    ) -> Result<bool> {
        self.run_pass(limit, false).await
    }

    /// Apply the low-water retention filter to the already-scanned report.
    /// Test-only seam; see [run_without_low_water_filter].
    #[cfg(test)]
    pub(crate) async fn run_low_water_filter(&mut self) {
        self.apply_low_water_filter().await;
    }

    /// Reset the reconciliation job to start from the beginning
    pub async fn reset(&mut self) {
        self.state.current_topic_index = 0;
        self.state.current_partition_index = 0;
        self.state.topics = None;
        self.state.report = if self.config.track_files {
            ReconcileReport::with_path_tracking()
        } else {
            ReconcileReport::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Config;
    use crate::data::records::Record;
    use chrono::Utc;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;
    use tracing::trace;

    async fn create_test_catalog() -> (TempDir, Arc<Catalog>) {
        let dir = TempDir::new().unwrap();
        let root = PathBuf::from(dir.path());
        let config = Config::default();
        let catalog = Catalog::attach(root, config).await.unwrap();
        (dir, Arc::new(catalog))
    }

    /// Create a catalog whose partitions roll after `max_rows` rows, so tests
    /// can deterministically force a segment to seal.
    async fn create_test_catalog_rolling(max_rows: usize) -> (TempDir, Arc<Catalog>) {
        let dir = TempDir::new().unwrap();
        let root = PathBuf::from(dir.path());
        let mut config = Config::default();
        config.partition.roll.max_rows = max_rows;
        let catalog = Catalog::attach(root, config).await.unwrap();
        (dir, Arc::new(catalog))
    }

    fn test_records(messages: &[&str]) -> Vec<Record> {
        messages
            .iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect()
    }

    /// Look up the size the manifest currently records for a single segment.
    async fn manifest_segment_size(
        catalog: &Catalog,
        partition_id: &PartitionId,
        index: SegmentIndex,
    ) -> Option<usize> {
        catalog
            .manifest()
            .get_segment_data(index.to_id(partition_id))
            .await
            .map(|data| data.size)
    }

    // run reconcile with tracking for all of these tests and verify the associated
    // path(s) end up in the path stats.

    #[test_log::test(tokio::test)]
    async fn test_reconcile_empty_catalog() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        // Create reconciler with summary check disabled for predictable test behavior
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog, config);

        // Should complete immediately on an empty catalog
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        let stats = &reconciler.report().sealed;
        assert_eq!(stats.files_checked.len(), 0);
        assert_eq!(stats.untracked_files.len(), 0);
        assert_eq!(stats.size_mismatches.len(), 0);
        assert_eq!(stats.missing_files.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_with_data() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to the catalog
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.extend_records("other", &records).await?;
        topic.commit().await?;

        // Force a checkpoint to ensure files are written to disk
        drop(topic);
        catalog.checkpoint().await;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // Should have validated some segments
        let stats = &reconciler.report().sealed;
        assert!(!stats.files_checked.is_empty());
        assert_eq!(stats.untracked_files.paths.len(), 0);
        assert_eq!(stats.missing_files.paths.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_incremental_reconciliation() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to multiple topics
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        for i in 0..3 {
            let topic_name = format!("topic-{}", i);
            let topic = catalog.get_topic(&topic_name).await;
            topic.extend_records("default", &records).await?;
            topic.commit().await?;
        }

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation with a small limit
        let done1 = reconciler.run(Some(1)).await?;
        assert!(!done1); // Should not be done after just 1 unit of work

        // Run again with another small limit
        let done2 = reconciler.run(Some(1)).await?;
        assert!(!done2); // Still not done

        // Run with enough limit to finish
        let done3 = reconciler.run(Some(100)).await?;
        assert!(done3); // Should be done now

        // Stats should show work was done
        let stats = &reconciler.report().sealed;
        assert_eq!(stats.files_checked.len(), 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_unlimited_reconciliation() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data
        let records: Vec<_> = vec!["abc", "def"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.commit().await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation with no limit
        let done = reconciler.run(None).await?;
        assert!(done); // Should be done

        // Stats should show work was done
        let stats = &reconciler.report().sealed;
        assert_eq!(stats.files_checked.len(), 1);
        assert_eq!(stats.missing_files.len(), 0);
        assert_eq!(stats.size_mismatches.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_orphan_files() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to the catalog to create a partition directory
        let records: Vec<_> = vec!["abc", "def"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.commit().await?;
        drop(topic);

        // Force a checkpoint to ensure files are written to disk
        catalog.checkpoint().await;

        // Debug: Check what directories exist
        let topic_root = catalog.topic_root().join("test-topic");
        // Orphan files are created in the topic directory, not partition subdirectory
        let partition_path = topic_root.clone();
        trace!("Topic root: {:?}", topic_root);
        trace!("Partition path (topic directory): {:?}", partition_path);
        trace!("Partition path exists: {}", partition_path.exists());

        if partition_path.exists() {
            let mut entries = fs::read_dir(&partition_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                trace!("Existing file: {:?}", entry.file_name());
            }
        } else {
            // Create the directory if it doesn't exist
            fs::create_dir_all(&partition_path).await?;
        }

        // Manually create an orphan file in the topic directory (where partition files are stored)
        let orphan_file_path = partition_path.join("orphan-file-123");
        fs::write(&orphan_file_path, "orphan content").await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // Should have found one untracked file
        let stats = &reconciler.report().sealed;
        assert_eq!(stats.untracked_files.len(), 1);
        assert_eq!(stats.files_checked.len(), 2);

        // Verify the orphan file path is recorded when tracking is enabled
        match &stats.untracked_files.paths {
            PathStats::Paths(paths) => {
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], orphan_file_path);
            }
            PathStats::Counter(_) => panic!("Expected Paths variant when track_files is enabled"),
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_corrupted_segment_size() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to create segments
        let records: Vec<_> = vec!["record1", "record2", "record3"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic_name = "corruption-test";
        let partition_name = "partition1";
        let topic = catalog.get_topic(topic_name).await;
        topic.extend_records(partition_name, &records).await?;
        topic.commit().await?;
        drop(topic);

        // Force a checkpoint to ensure files are written to disk
        catalog.checkpoint().await;

        // Partition files are stored in the topic directory, not a separate partition subdirectory
        let topic_root = Topic::partition_root(catalog.topic_root(), topic_name);
        let partition_path = topic_root.clone(); // Use topic root, not partition subdirectory

        if !partition_path.exists() {
            fs::create_dir_all(&partition_path).await?;
        }

        // Get the manifest and partition information
        let manifest = catalog.manifest();
        let partition_id = PartitionId::new(topic_name, partition_name);

        // Get the first segment for this partition
        let segments_stream = manifest.stream_segments(
            &partition_id,
            RecordIndex(0),
            crate::data::index::Ordering::Forward,
        );

        let segments: Vec<_> = segments_stream.collect().await;
        assert!(!segments.is_empty(), "Should have at least one segment");

        let segment_to_corrupt = segments[0].clone();
        let _original_size = segment_to_corrupt.size; // Keep for documentation, not used in test

        // Create the actual segment file manually to test size validation
        // This simulates a scenario where the file exists but has a different size than expected
        let slog_name = Partition::slog_name(&partition_id);
        let segment_file_name = format!("{}-{}", slog_name, segment_to_corrupt.index.0);
        let segment_file_path = partition_path.join(&segment_file_name);

        // Create a file with a different size than what's in the manifest
        // Make the difference much larger than the tolerance (200 bytes) to ensure it's detected
        let corrupted_size = segment_to_corrupt.size + 1000; // This will definitely be different from the real size
        let dummy_content = vec![0u8; corrupted_size];
        fs::write(&segment_file_path, &dummy_content).await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config.clone());

        // Now run reconciliation - it should detect the size mismatch
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // We should check that there's at least one segment validated
        let stats = &reconciler.report().sealed;
        assert_eq!(stats.files_checked.len(), 1);

        // We should have found exactly one size mismatch
        assert_eq!(stats.size_mismatches.len(), 1);

        // Verify the corrupted file path is recorded when tracking is enabled
        match &stats.size_mismatches.paths {
            PathStats::Paths(paths) => {
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], segment_file_path);
            }
            PathStats::Counter(_) => panic!("Expected Paths variant when track_files is enabled"),
        }

        // Now run a fix reconciliation
        let fix_config = ReconcileConfig {
            track_files: true,
            fixes: BTreeSet::from([ReconcileFix::UpdateManifestSizes]),
            ..Default::default()
        };
        let mut fix_reconciler = ReconcileJob::with_config(catalog.clone(), fix_config);

        info!("running a reconciliation fix job");
        // Run reconciliation with fix - it should fix the size mismatch
        let done = fix_reconciler.run(Some(100)).await?;
        assert!(done);

        // After fixing, we should still have validated segments but no size mismatches in stats
        // NOTE: The stats tracking the mismatches that were already found won't be cleared,
        // but the actual size comparison should now match
        let fix_stats = &fix_reconciler.report().sealed;
        assert_eq!(fix_stats.files_checked.len(), 1);

        // Now run another reconciliation to verify there are no errors
        let mut verify_reconciler = ReconcileJob::with_config(catalog.clone(), config.clone());
        let done = verify_reconciler.run(Some(100)).await?;
        assert!(done);

        // Verify no size mismatches are found after the fix
        let verify_stats = &verify_reconciler.report().sealed;
        assert_eq!(verify_stats.size_mismatches.len(), 0,
                   "Should detect no size mismatches after fix. Got: files_checked.len()={}, size_mismatches.len()={}, missing_files.len()={}",
                   verify_stats.files_checked.len(), verify_stats.size_mismatches.len(), verify_stats.missing_files.len());

        Ok(())
    }

    /// Collect the active-bucket entries for a single partition.
    fn active_entries<'a>(
        report: &'a ReconcileReport,
        topic: &str,
        partition: &str,
    ) -> Vec<&'a ActiveSegmentReport> {
        report
            .active
            .iter()
            .filter(|a| a.topic == topic && a.partition == partition)
            .collect()
    }

    /// A partition that has data but has never rolled (sealed_ix == None) keeps
    /// its only segment in the active bucket; it must never appear as a sealed
    /// size mismatch.
    #[test_log::test(tokio::test)]
    async fn test_active_segment_not_sealed_mismatch() -> Result<()> {
        // A high roll threshold keeps a single batch in the active segment, so
        // no roll happens and the watermark stays None.
        let (_tmpdir, catalog) = create_test_catalog_rolling(1000).await;

        let topic_name = "active-only";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        drop(topic);

        // Make the active segment durable in the manifest without sealing it.
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(3))
            .await?;

        // Sanity: the partition has not sealed a segment yet.
        {
            let topic = catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            assert_eq!(partition.sealed_ix(), None);
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        // The active segment must not surface as a sealed-bucket problem.
        assert_eq!(report.sealed.size_mismatches.len(), 0);
        assert_eq!(report.sealed.missing_files.len(), 0);

        // It must show up exactly once in the active bucket, with real sizes.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);
        assert!(active[0].manifest_size > 0);
        assert!(active[0].disk_size > 0);

        Ok(())
    }

    /// After a roll, the just-sealed segment runs through the strict sealed
    /// pipeline while the new active segment lands in the active bucket. A
    /// consistent system reports no problems in either bucket.
    #[test_log::test(tokio::test)]
    async fn test_just_rolled_segment_in_sealed_bucket() -> Result<()> {
        // Roll after 2 rows so the second batch seals segment 0.
        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "rolled";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        // First batch fills the active segment past the roll threshold.
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        // Second batch rolls (and seals) segment 0, then lands in segment 1.
        // The roll waits for segment 0's seal to be durable.
        topic
            .extend_records(partition_name, &test_records(&["d", "e", "f"]))
            .await?;
        drop(topic);

        // Make the active segment (1) durable so it appears in the manifest,
        // and finalize segment 0's file on disk.
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(6))
            .await?;
        catalog.checkpoint().await;

        // Segment 0 sealed; segment 1 is the active tail.
        {
            let topic = catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            assert_eq!(partition.sealed_ix(), Some(SegmentIndex(0)));
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        // The rolled segment was checked in the sealed bucket.
        assert!(!report.sealed.files_checked.is_empty());
        // Healthy system: no problems in either bucket.
        assert_eq!(report.sealed.size_mismatches.len(), 0);
        assert_eq!(report.sealed.missing_files.len(), 0);

        // The still-active segment shows up in the active bucket.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);

        Ok(())
    }

    /// The UpdateManifestSizes fix must never apply to an active segment, even
    /// when its on-disk size is corrupted. The drift is reported (non-zero
    /// delta) but the manifest is left untouched.
    #[test_log::test(tokio::test)]
    async fn test_fix_never_targets_active_segment() -> Result<()> {
        use tokio::io::AsyncWriteExt;

        // High roll threshold keeps the single segment active (sealed_ix None).
        let (_tmpdir, catalog) = create_test_catalog_rolling(1000).await;
        let topic_name = "active-fix";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        drop(topic);
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(3))
            .await?;

        let partition_id = PartitionId::new(topic_name, partition_name);
        // Record the manifest's stored size for the (active) segment 0.
        let original_size = manifest_segment_size(&catalog, &partition_id, SegmentIndex(0))
            .await
            .expect("segment 0 should be in the manifest");

        // Corrupt the on-disk size by appending extra bytes to the active
        // segment file before any roll seals it.
        let topic_path = Topic::partition_root(catalog.topic_root(), topic_name);
        let slog_name = Partition::slog_name(&partition_id);
        let segment_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(0));
        {
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .await?;
            f.write_all(&vec![0u8; 1000]).await?;
            f.flush().await?;
        }

        let fix_config = ReconcileConfig {
            track_files: true,
            fixes: BTreeSet::from([ReconcileFix::UpdateManifestSizes]),
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), fix_config);
        assert!(reconciler.run(Some(100)).await?);

        // The fix must not have touched the active segment's manifest size.
        let after_size = manifest_segment_size(&catalog, &partition_id, SegmentIndex(0))
            .await
            .expect("segment 0 should still be in the manifest");
        assert_eq!(
            after_size, original_size,
            "active-segment manifest size must be unchanged by the fix"
        );

        let report = reconciler.report();
        // The corruption is never a sealed-bucket mismatch for an active segment.
        assert_eq!(report.sealed.size_mismatches.len(), 0);

        // The active bucket reports the (positive) drift we introduced.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);
        assert!(
            active[0].delta > 0,
            "expected positive delta, got {}",
            active[0].delta
        );

        Ok(())
    }

    /// Drive a rolling (`max_rows == 2`) partition until segments 0 and 1 are
    /// sealed (`sealed_ix == Some(1)`) with segment 2 as the active tail, and
    /// the sealed segments' files and rows are durable. Each sealed segment has
    /// exactly its main file on disk (no parts/cache), and the low water starts
    /// at segment 0.
    async fn build_two_sealed_segments(
        catalog: &Arc<Catalog>,
        topic_name: &str,
        partition_name: &str,
    ) -> Result<()> {
        let topic = catalog.get_topic(topic_name).await;
        // Each batch overflows the 2-row roll threshold, so the next batch
        // seals the previous segment: batch 1 -> seg0, batch 2 seals seg0 into
        // seg1, batch 3 seals seg1 into seg2.
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        topic
            .extend_records(partition_name, &test_records(&["d", "e", "f"]))
            .await?;
        topic
            .extend_records(partition_name, &test_records(&["g", "h", "i"]))
            .await?;
        drop(topic);

        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(9))
            .await?;
        catalog.checkpoint().await;

        // Sanity: segments 0 and 1 sealed, segment 2 active, low water at 0.
        {
            let topic = catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            assert_eq!(partition.sealed_ix(), Some(SegmentIndex(1)));
        }
        let partition_id = PartitionId::new(topic_name, partition_name);
        assert_eq!(
            catalog.manifest().get_min_segment(&partition_id).await,
            Some(SegmentIndex(0))
        );

        Ok(())
    }

    /// The reconcile/retention race for a *missing file*: reconcile snapshots a
    /// sealed segment's manifest row, observes the file is gone (retention
    /// already deleted it), and records a "missing file". Retention then removes
    /// the row, advancing the low water past the segment. The low-water filter
    /// must drop the spurious entry and count it.
    #[test_log::test(tokio::test)]
    async fn test_retention_removed_missing_file_filtered() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "ret-missing";
        let partition_name = "p0";
        build_two_sealed_segments(&catalog, topic_name, partition_name).await?;
        let partition_id = PartitionId::new(topic_name, partition_name);

        // Simulate retention's file delete for the oldest sealed segment (0),
        // while leaving its manifest row so reconcile's snapshot still sees it.
        let topic_path = Topic::partition_root(catalog.topic_root(), topic_name);
        let slog_name = Partition::slog_name(&partition_id);
        let seg0_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(0));
        assert!(seg0_path.exists(), "segment 0 file should exist pre-delete");
        fs::remove_file(&seg0_path).await?;

        // Scan without the filter: segment 0 is recorded missing (row present,
        // file gone).
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run_without_low_water_filter(Some(100)).await?);
        assert_eq!(
            reconciler.report().sealed.missing_files.len(),
            1,
            "segment 0 should be recorded missing before filtering"
        );

        // Retention completes: drop the row, advancing the low water to 1.
        catalog
            .manifest()
            .remove_segment(SegmentIndex(0).to_id(&partition_id))
            .await;
        assert_eq!(
            catalog.manifest().get_min_segment(&partition_id).await,
            Some(SegmentIndex(1))
        );

        // The filter drops the spurious entry and records it.
        reconciler.run_low_water_filter().await;
        let report = reconciler.report();
        assert_eq!(report.sealed.missing_files.len(), 0);
        assert!(report.retention_rm.missing_files >= 1);
        assert_eq!(report.retention_rm.rm_total(), report.retention_rm.missing_files);

        Ok(())
    }

    /// The reconcile/retention race for an *orphan*: retention removed a sealed
    /// segment's manifest row before reconcile's scan (so the scan does not
    /// track it), but its file is still on disk during the directory walk. The
    /// file is recorded as untracked, then dropped by the low-water filter
    /// because the segment is below the (now advanced) low water.
    #[test_log::test(tokio::test)]
    async fn test_retention_removed_orphan_filtered() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "ret-orphan";
        let partition_name = "p0";
        build_two_sealed_segments(&catalog, topic_name, partition_name).await?;
        let partition_id = PartitionId::new(topic_name, partition_name);

        // Retention removed the row for segment 0 before the scan; its single
        // main file lingers on disk. The low water is now segment 1.
        catalog
            .manifest()
            .remove_segment(SegmentIndex(0).to_id(&partition_id))
            .await;
        assert_eq!(
            catalog.manifest().get_min_segment(&partition_id).await,
            Some(SegmentIndex(1))
        );
        let topic_path = Topic::partition_root(catalog.topic_root(), topic_name);
        let slog_name = Partition::slog_name(&partition_id);
        let seg0_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(0));
        assert!(seg0_path.exists(), "segment 0 file should still be on disk");

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        // The orphan was filtered out and counted; it must not appear.
        assert_eq!(report.sealed.untracked_files.len(), 0);
        assert!(report.retention_rm.untracked_files >= 1);

        Ok(())
    }

    /// A real size mismatch on a live sealed segment must survive filtering,
    /// while a retention-collapsed mismatch on an older sealed segment in the
    /// same partition is dropped. Exercises the size-mismatch filter path.
    #[test_log::test(tokio::test)]
    async fn test_real_mismatch_preserved_retention_dropped() -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "ret-mixed";
        let partition_name = "p0";
        build_two_sealed_segments(&catalog, topic_name, partition_name).await?;
        let partition_id = PartitionId::new(topic_name, partition_name);

        let topic_path = Topic::partition_root(catalog.topic_root(), topic_name);
        let slog_name = Partition::slog_name(&partition_id);
        let seg0_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(0));
        let seg1_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(1));

        // Corrupt the on-disk size of both sealed segments so each produces a
        // size mismatch while its row is still present in the snapshot.
        for path in [&seg0_path, &seg1_path] {
            let mut f = fs::OpenOptions::new().append(true).open(path).await?;
            f.write_all(&vec![0u8; 1000]).await?;
            f.flush().await?;
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run_without_low_water_filter(Some(100)).await?);
        assert_eq!(
            reconciler.report().sealed.size_mismatches.len(),
            2,
            "both sealed segments should be recorded as mismatches pre-filter"
        );

        // Retention retires only the older segment (0), advancing the low water
        // to 1. Segment 1's mismatch is a real, live problem.
        catalog
            .manifest()
            .remove_segment(SegmentIndex(0).to_id(&partition_id))
            .await;
        assert_eq!(
            catalog.manifest().get_min_segment(&partition_id).await,
            Some(SegmentIndex(1))
        );

        reconciler.run_low_water_filter().await;
        let report = reconciler.report();

        // The live mismatch survives; the retention-collapsed one is dropped.
        assert_eq!(report.sealed.size_mismatches.len(), 1);
        assert_eq!(report.retention_rm.size_mismatches, 1);
        match &report.sealed.size_mismatches.paths {
            PathStats::Paths(paths) => {
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], seg1_path, "the surviving mismatch is segment 1");
            }
            PathStats::Counter(_) => panic!("expected Paths variant with track_files"),
        }

        Ok(())
    }

    /// The filter is a no-op on a catalog with no diffs and no segments: it must
    /// not panic (e.g. on `get_min_segment` returning `None`) and must record
    /// nothing.
    #[test_log::test(tokio::test)]
    async fn test_filter_noop_on_empty_catalog() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        assert_eq!(report.sealed.missing_files.len(), 0);
        assert_eq!(report.sealed.untracked_files.len(), 0);
        assert_eq!(report.sealed.size_mismatches.len(), 0);
        assert_eq!(report.retention_rm.rm_total(), 0);

        Ok(())
    }

    /// Filter stats roll up across partitions: two partitions each contribute a
    /// single retention-collapsed orphan, and the total reflects both.
    #[test_log::test(tokio::test)]
    async fn test_filter_stats_roll_up_across_partitions() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "ret-rollup";

        for partition_name in ["p0", "p1"] {
            build_two_sealed_segments(&catalog, topic_name, partition_name).await?;
            let partition_id = PartitionId::new(topic_name, partition_name);
            // Drop segment 0's row, leaving its file as a soon-to-be-filtered
            // orphan; low water advances to 1 for each partition.
            catalog
                .manifest()
                .remove_segment(SegmentIndex(0).to_id(&partition_id))
                .await;
            assert_eq!(
                catalog.manifest().get_min_segment(&partition_id).await,
                Some(SegmentIndex(1))
            );
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        assert_eq!(report.sealed.untracked_files.len(), 0);
        assert_eq!(report.retention_rm.untracked_files, 2);
        assert_eq!(report.retention_rm.rm_total(), 2);

        Ok(())
    }
}


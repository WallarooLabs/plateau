//! Utilities for managing and monitoring local log storage.
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(not(test))]
use systemstat::{Platform, System};
use tokio::task::spawn_blocking;
use tokio::time::sleep;

const MAX_WRITES: usize = 10_000;
const MAX_DURATION: Duration = Duration::from_secs(10);
const MIN_AVAILABLE: ByteSize = ByteSize::gb(1);

/// Periodically monitors the disk space available for logs
/// and switches its state between read-only and writable
/// based on configured thresholds.
#[derive(Clone, Debug)]
pub struct DiskMonitor {
    readonly: Arc<AtomicBool>,
    write_count: Arc<AtomicUsize>,
    last_write: Arc<AtomicU64>,
    epoch: Instant,
}

impl Default for DiskMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskMonitor {
    /// Returns a newly initialized instance.
    pub fn new() -> Self {
        let readonly = AtomicBool::new(false);
        let write_count = AtomicUsize::new(0);
        let last_write = AtomicU64::new(0);

        Self {
            readonly: Arc::new(readonly),
            write_count: Arc::new(write_count),
            last_write: Arc::new(last_write),
            epoch: Instant::now(),
        }
    }

    /// Returns true if the disk status is currently read-only.
    pub fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::SeqCst)
    }

    /// Records a single log write operation.
    pub fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::SeqCst);
        let now = self.epoch.elapsed().as_secs();
        self.last_write.store(now, Ordering::SeqCst);
    }

    /// Loops indefinitely while checking for available disk space on the configured path.
    pub async fn run(&self, root: impl AsRef<Path>, config: &Config) -> anyhow::Result<()> {
        loop {
            let deadline = self
                .epoch
                .elapsed()
                .saturating_sub(config.max_duration)
                .as_secs();
            if self.write_count.load(Ordering::SeqCst) >= config.max_writes
                || self.last_write.load(Ordering::SeqCst) < deadline
            {
                let stat = System::new();
                let path = root.as_ref().to_path_buf();
                let avail = spawn_blocking(move || {
                    let fs = stat.mount_at(path)?;
                    Ok::<_, anyhow::Error>(fs.avail)
                })
                .await??;

                self.readonly
                    .store(avail < config.min_available, Ordering::SeqCst);
                self.write_count.store(0, Ordering::SeqCst);
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Stores configuration properties used for storage monitoring.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    /// Maximum number of log writes before checking for available storage.
    #[serde(default = "Config::default_max_writes")]
    max_writes: usize,

    /// Maximum duration to wait between storage availability checks.
    #[serde(default = "Config::default_max_duration")]
    max_duration: Duration,

    /// Minimum bytes available for storage before turning read-only.
    #[serde(default = "Config::default_min_available")]
    min_available: ByteSize,
}

impl Config {
    fn default_max_writes() -> usize {
        MAX_WRITES
    }

    fn default_max_duration() -> Duration {
        MAX_DURATION
    }

    fn default_min_available() -> ByteSize {
        MIN_AVAILABLE
    }
}

#[cfg(test)]
struct System;

#[cfg(test)]
impl System {
    fn new() -> Self {
        Self
    }

    fn mount_at(&self, path: impl AsRef<Path>) -> std::io::Result<systemstat::Filesystem> {
        let mut fs = systemstat::Filesystem {
            files: Default::default(),
            files_total: Default::default(),
            files_avail: Default::default(),
            free: Default::default(),
            avail: Default::default(),
            total: Default::default(),
            name_max: Default::default(),
            fs_type: Default::default(),
            fs_mounted_from: Default::default(),
            fs_mounted_on: Default::default(),
        };

        let suffix = path.as_ref().file_name().unwrap_or_default();
        if let Some(avail) = suffix.to_str().and_then(|path| path.parse::<u64>().ok()) {
            fs.avail = ByteSize::b(avail);
        }

        Ok(fs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn monitor_available_disk_space() {
        let monitor = DiskMonitor::default();
        let fixture = monitor.clone();
        let config = Config {
            max_writes: 1,
            max_duration: Duration::from_secs(20),
            min_available: ByteSize::b(1024),
        };

        tokio::spawn(async move { monitor.run("/test/1024", &config).await });

        sleep(Duration::from_secs(1)).await;
        assert!(!fixture.is_readonly());

        fixture.record_write();

        sleep(Duration::from_secs(1)).await;
        assert!(!fixture.is_readonly());
    }

    #[tokio::test]
    async fn monitor_insufficient_disk_space() {
        let monitor = DiskMonitor::default();
        let fixture = monitor.clone();
        let config = Config {
            max_writes: 1,
            max_duration: Duration::ZERO,
            min_available: ByteSize::b(1024),
        };

        let _ = timeout(Duration::from_secs(2), monitor.run("/test/1023", &config)).await;
        assert!(fixture.is_readonly());
    }
}

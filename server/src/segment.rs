//! A segment contains a bundle of time and logically indexed rows.
//!
//! Additionally, a segment keeps an "active chunk" cache to avoid chunk
//! fragmentation in low write frequency workloads. This cache persists the
//! "current" non-full chunk in the segment. New rows may be appended to this
//! cache via [SegmentWriter2::update_cache] until a full row group is written
//! via [SegmentWriter2::log_arrow].
//!
//! At that point, the cache is discarded and a new empty active chunk cache is
//! opened for the next chunk in the file.
//!
//! `{segment}.arrows` is the file that records the active chunk cache. See
//! [cache] for more information about the contents of this file.
//!
//! For caching and crash recovery, each segment file may have a variety of
//! other associated files. See [arrow] and [parquet] for details on these
//! additional files.

use std::io::Read;
use std::{fs, path::Path, path::PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{error, trace, warn};

use crate::arrow2::datatypes::Schema;
use plateau_transport::DataFocus;
use plateau_transport::SegmentChunk;

#[cfg(test)]
use crate::chunk::Record;

#[allow(dead_code)]
mod arrow;
mod cache;
mod parquet;

const PLATEAU_HEADER: &str = "plateau1";

fn validate_header(mut reader: impl Read) -> Result<()> {
    let mut buffer = [0u8; 8];
    reader.read_exact(&mut buffer)?;
    if std::str::from_utf8(&buffer)? != PLATEAU_HEADER {
        anyhow::bail!("invalid checkpoint header");
    }

    Ok(())
}

// these are incomplete; they are currently only used in testing
#[cfg(test)]
#[allow(clippy::derived_hash_with_manual_eq)]
impl std::hash::Hash for Record {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.time.hash(state);
        let data_string = String::from_utf8(self.message.clone()).unwrap();
        data_string.hash(state);
    }
}

/// This is currently a placeholder for future segment storage settings (e.g.
/// compression)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    durable_checkpoints: bool,
    arrow: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durable_checkpoints: true,
            arrow: true,
        }
    }
}

pub trait SegmentIterator: DoubleEndedIterator<Item = Result<SegmentChunk>> {
    fn schema(&self) -> &Schema;
}

#[derive(Clone, Debug)]
pub(crate) struct Segment {
    path: PathBuf,
    focus: Option<DataFocus>,
}

impl Segment {
    pub(crate) fn at(path: impl AsRef<Path>, focus: impl Into<Option<DataFocus>>) -> Self {
        let path = path.as_ref().to_path_buf();
        let focus = focus.into();
        Self { path, focus }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    fn file(&self) -> Result<fs::File> {
        if self.path.exists() {
            warn!("truncating extant segment file at {}", self.path.display());
        }

        Ok(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?)
    }

    pub(crate) fn create(&self, schema: Schema, config: Config) -> Result<Writer> {
        let file = self.file()?;
        let writer = if config.arrow {
            WriteFormat::Arrow(arrow::Writer::create(file, &schema)?)
        } else {
            WriteFormat::Parquet(parquet::Writer::create(
                self.path.clone(),
                file,
                &schema,
                config.clone(),
            )?)
        };

        let cache = cache::ActiveChunk::new(self.cache_path());
        Ok(Writer {
            segment: self.clone(),
            writer,
            schema,
            chunk_ix: 0,
            cache,
        })
    }

    pub(crate) fn parts(&self) -> impl Iterator<Item = PathBuf> {
        let parquet_parts = parquet::Segment::new(self.path.clone())
            .map(|s| s.parts())
            .inspect_err(|e| error!("error enumerating parquet parts for {:?}, {e:?}", self.path))
            .ok();

        let arrow_parts = arrow::Segment::new(self.path.clone())
            .map(|s| s.parts())
            .inspect_err(|e| error!("error enumerating arrow parts for {:?}, {e:?}", self.path))
            .ok();

        parquet_parts
            .into_iter()
            .flatten()
            .chain(arrow_parts.into_iter().flatten())
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        } else {
            warn!("main segment file at {:?} missing", self.path);
        }

        for part in self.parts().filter(|p| p.exists()) {
            fs::remove_file(&part)
                .inspect_err(|e| error!("error removing part {part:?}: {e:?}"))
                .ok();
        }

        if self.cache_path().exists() {
            fs::remove_file(self.cache_path())?;
        }

        Ok(())
    }

    pub(crate) fn validate(&self) -> bool {
        match self.iter() {
            Ok(_) => true,
            Err(err) => {
                warn!("error validating segment: {err:?}");
                false
            }
        }
    }

    pub(crate) fn iter(&self) -> Result<impl SegmentIterator> {
        let focus = self.focus.clone().unwrap_or_default();
        let cache = cache::read(self.cache_path(), &focus)
            .inspect_err(|err| error!("error reading cache at {:?}: {err:?}", self.cache_path()))
            .unwrap_or_default();

        if self.path.exists() {
            trace!(
                "found segment file {:?}, cache: {}",
                self.path,
                cache.is_some()
            );
            let mut file = fs::File::open(&self.path)?;

            // Check for a header
            let parquet = parquet::check_file(&mut file);
            let arrow = arrow::check_file(&mut file);
            if let (Ok(parquet), Ok(arrow)) = (parquet, arrow) {
                return if parquet {
                    trace!("{:?} in parquet format", self.path);
                    let segment = parquet::Segment::new(self.path.clone())?;
                    Ok(ReadFormat::Parquet(segment.read(cache)?))
                } else if arrow {
                    trace!("{:?} in arrow format", self.path);
                    let segment = arrow::Segment::new(self.path.clone())?.focus(focus);
                    Ok(ReadFormat::Arrow(segment.read(cache)?))
                } else {
                    anyhow::bail!("unable to detect file format for segment {:?}", self.path)
                };
            }

            trace!("empty segment file {:?}", self.path);
        }

        if let Some(data) = cache {
            trace!("only cache file present");
            anyhow::ensure!(
                data.chunk_ix == 0,
                "cache file requires segment {:?} that is not present",
                self.path
            );
            Ok(ReadFormat::OnlyCache(
                data.rows.schema,
                std::iter::once(Ok(data.rows.chunk)),
            ))
        } else {
            anyhow::bail!("no segment file or cache data for {:?}", self.path)
        }
    }

    fn cache_path(&self) -> PathBuf {
        let mut path: PathBuf = self.path.clone();
        assert!(path.set_extension("arrows"));
        path
    }

    /// Return an estimate of the on-disk size of the corresponding file(s),
    /// excluding the active chunk cache.
    pub fn size_estimate(&self) -> Result<usize> {
        let main_size = fs::metadata(&self.path).map(|p| p.len()).unwrap_or(0);
        let part_size: u64 = self
            .parts()
            .map(|part| fs::metadata(part).map(|p| p.len()).unwrap_or(0))
            .sum();
        Ok(usize::try_from(main_size + part_size)?)
    }
}

enum ReadFormat {
    Arrow(arrow::Reader),
    Parquet(parquet::Reader),
    OnlyCache(Schema, std::iter::Once<Result<SegmentChunk>>),
}

impl Iterator for ReadFormat {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Arrow(a) => a.next(),
            Self::Parquet(p) => p.next(),
            Self::OnlyCache(_, c) => c.next(),
        }
    }
}

impl DoubleEndedIterator for ReadFormat {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Arrow(a) => a.next_back(),
            Self::Parquet(p) => p.next_back(),
            Self::OnlyCache(_, c) => c.next_back(),
        }
    }
}

impl SegmentIterator for ReadFormat {
    fn schema(&self) -> &Schema {
        match self {
            Self::Arrow(a) => a.schema(),
            Self::Parquet(p) => p.schema(),
            Self::OnlyCache(schema, _) => schema,
        }
    }
}

enum WriteFormat {
    Arrow(arrow::Writer),
    Parquet(parquet::Writer),
}

pub struct Writer {
    segment: Segment,
    writer: WriteFormat,
    schema: Schema,
    chunk_ix: u32,

    cache: cache::ActiveChunk,
}

impl Writer {
    pub fn check_schema(&self, schema: &Schema) -> bool {
        &self.schema == schema
    }

    fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        match &mut self.writer {
            WriteFormat::Parquet(p) => p.write_chunk(&self.schema, chunk),
            WriteFormat::Arrow(a) => a.write_chunk(chunk),
        }
    }

    /// Log a combination of full and active chunks to this segment.
    ///
    /// This operation is append-only. All full chunks are appended as-is onto
    /// the underlying segment file.
    ///
    /// The "active" chunk is also considered append-only. If no full chunks
    /// are present, all rows in the active chunk after the end of cache are
    /// appended onto the cache.
    ///
    /// All rows currently in the cache are assumed to be equivalent to their
    /// same-index counterparts in the "new" active chunk.
    ///
    /// When full chunks are present, the cache is reset, as the active chunk
    /// is always considered to be the last chunk in the file.
    pub fn log_arrows(
        &mut self,
        schema: &Schema,
        full: Vec<SegmentChunk>,
        active: Option<SegmentChunk>,
    ) -> Result<()> {
        anyhow::ensure!(
            self.check_schema(schema),
            "cannot use different schemas within the same segment"
        );

        let chunk_count = full.len();
        for chunk in full {
            self.write_chunk(chunk)?;
        }

        if chunk_count > 0 {
            self.chunk_ix += chunk_count as u32;
            self.cache.clear();
        }

        if let Some(active) = active {
            self.cache.update(self.chunk_ix, schema, active)?;
        }

        self.write_checkpoint()?;

        Ok(())
    }

    fn write_checkpoint(&self) -> Result<()> {
        // First, sync the segment file itself. The cache will not be valid if the
        // chunks that precede it are missing.
        match &self.writer {
            WriteFormat::Parquet(p) => p.checkpoint()?,
            WriteFormat::Arrow(a) => a.checkpoint()?,
        }

        // Then, we can sync the active chunk cache
        self.cache.sync()?;

        Ok(())
    }

    fn get_path(&self) -> &Path {
        &self.segment.path
    }

    pub fn end(mut self) -> Result<()> {
        if let Some(rows) = self.cache.take() {
            self.write_chunk(rows.chunk)?;
        }

        // NOTE: it is critical that the writer syncs the file as part of the
        // end operation, otherwise the data in cache may be lost in recovery
        // scenarios.
        match self.writer {
            WriteFormat::Parquet(mut p) => p.end()?,
            WriteFormat::Arrow(a) => a.end()?,
        }

        self.cache.destroy()?;

        Ok(())
    }

    /// Return an estimate of the on-disk size of the corresponding file(s).
    pub fn size_estimate(&self) -> Result<usize> {
        let segment_size = self.segment.size_estimate()?;
        let cache_size = self.cache.size() as usize;
        Ok(segment_size + cache_size)
    }

    pub fn close(self) -> Result<usize> {
        let mut parent = self.get_path().to_path_buf();
        let size = self.size_estimate()?;
        self.end()?;

        // NOTE: the file data is now synchronized, but the file itself may not appear in the
        // parent directory on crash unless we fsync that too.
        parent.pop();
        let directory = fs::File::open(&parent)?;
        directory.sync_all()?;

        Ok(size)
    }
}

#[cfg(test)]
pub mod test;

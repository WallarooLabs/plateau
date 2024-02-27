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

use std::convert::TryFrom;
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::{fs, path::Path, path::PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::arrow2::datatypes::Schema;
pub use crate::chunk::Record;
use plateau_transport::SegmentChunk;

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
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durable_checkpoints: true,
        }
    }
}

pub trait SegmentIterator: DoubleEndedIterator<Item = Result<SegmentChunk>> {
    fn schema(&self) -> &Schema;
}

pub struct Segment {
    path: PathBuf,
}

impl Segment {
    pub(crate) fn at(path: PathBuf) -> Segment {
        Segment { path }
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

    pub(crate) fn create2(&self, schema: Schema, config: Config) -> Result<SegmentWriter2> {
        let file = self.file()?;
        let writer = parquet::Writer::create(self.path.clone(), file, &schema, config.clone())?;

        Ok(SegmentWriter2 {
            path: self.path.clone(),
            writer,
            schema,
            chunk_ix: 0,
            cache: cache::ActiveChunk::new(self.cache_path()),
        })
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        parquet::Segment::new(self.path.clone())?.destroy()?;

        if Path::exists(&self.cache_path()) {
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
        let cache = cache::read(self.cache_path()).unwrap_or_else(|err| {
            error!("error reading cache at {:?}: {err:?}", self.cache_path());
            None
        });
        parquet::Segment::new(self.path.clone())?.read(cache)
    }

    fn cache_path(&self) -> PathBuf {
        let mut path: PathBuf = self.path.clone();
        assert!(path.set_extension("arrows"));
        path
    }
}

pub struct SegmentWriter2 {
    path: PathBuf,
    writer: parquet::Writer,
    schema: Schema,
    chunk_ix: u32,

    cache: cache::ActiveChunk,
}

impl SegmentWriter2 {
    pub fn check_schema(&self, schema: &Schema) -> bool {
        &self.schema == schema
    }

    fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        self.writer.write_chunk(&self.schema, chunk)
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
        self.writer.checkpoint()?;

        // Then, we can sync the active chunk cache
        self.cache.sync()?;

        Ok(())
    }

    fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn end(&mut self) -> Result<()> {
        if let Some(rows) = self.cache.take() {
            self.write_chunk(rows.chunk)?;
        }

        self.writer.end()?;

        self.cache.destroy()?;

        Ok(())
    }

    /// Return an estimate of the on-disk size of the corresponding file(s).
    /// Note that this will _not_ include the checkpoint, in-flight checkpoint,
    /// or final footer (unless this segment has been sealed).
    pub fn size_estimate(&self) -> Result<usize> {
        let main_size = fs::metadata(self.get_path()).map(|p| p.len()).unwrap_or(0);
        let cache_size = self.cache.size();
        Ok(usize::try_from(main_size + cache_size)?)
    }

    pub fn close(mut self) -> Result<usize> {
        self.end()?;

        // NOTE: the file data is now synchronized, but the file itself may not appear in the
        // parent directory on crash unless we fsync that too.
        let mut parent = self.get_path().to_path_buf();
        parent.pop();
        let directory = fs::File::open(&parent)?;
        directory.sync_all()?;

        self.size_estimate()
    }
}

#[cfg(test)]
pub mod test {
    use std::borrow::Borrow;

    use super::*;
    use crate::chunk::{iter_legacy, test::inferences_schema_a};
    use chrono::{TimeZone, Utc};
    use plateau_transport::SchemaChunk;
    use sample_arrow2::{
        array::ArbitraryArray,
        chunk::ArbitraryChunk,
        datatypes::{sample_flat, ArbitraryDataType},
    };
    use sample_std::{Chance, Regex};
    use tempfile::tempdir;

    impl Config {
        pub fn nocommit() -> Self {
            Config {
                durable_checkpoints: false,
            }
        }
    }

    impl SegmentWriter2 {
        pub fn log_arrow<S: Borrow<Schema> + Clone + PartialEq>(
            &mut self,
            data: SchemaChunk<S>,
            active: Option<SegmentChunk>,
        ) -> Result<()> {
            self.log_arrows(data.schema.borrow(), vec![data.chunk], active)
        }

        pub fn update_cache(&mut self, active: SegmentChunk) -> Result<()> {
            self.cache.update(self.chunk_ix, &self.schema, active)
        }
    }

    pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
        it.map(|(ix, message)| Record {
            time: Utc.timestamp_opt(ix, 0).unwrap(),
            message: message.into_bytes(),
        })
        .collect()
    }

    pub fn collect_records(
        schema: Schema,
        iter: impl Iterator<Item = Result<SegmentChunk, anyhow::Error>>,
    ) -> Vec<Record> {
        iter_legacy(schema, iter).flat_map(Result::unwrap).collect()
    }

    pub fn deep_chunk(depth: usize, len: usize) -> ArbitraryChunk<Regex, Chance> {
        let names = Regex::new("[a-z]{4,8}");
        let data_type = ArbitraryDataType {
            struct_branch: 1..3,
            names: names.clone(),
            // this appears to break arrow2's parquet support
            // nullable: Chance(0.5),
            nullable: Chance(0.0),
            flat: sample_flat,
        }
        .sample_depth(depth);

        let array = ArbitraryArray {
            names,
            branch: 0..10,
            len: len..(len + 1),
            null: Chance(0.1),
            // this appears to break arrow2's parquet support
            // is_nullable: true,
            is_nullable: false,
        };

        ArbitraryChunk {
            chunk_len: 10..1000,
            array_count: 1..2,
            data_type,
            array,
        }
    }

    #[test]
    fn test_partial_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        drop(w);

        let f = std::fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_cache_updates() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();

        let all_counts = vec![1, 3, 4, 2, 1];
        for ix in 1..all_counts.len() {
            let mut chunk = a.chunk.clone();
            let mut w = s.create2(a.schema.clone(), Config::default())?;

            for count in &all_counts[0..ix] {
                let new_parts: Vec<_> = std::iter::once(chunk.clone())
                    .chain(std::iter::repeat(a.chunk.clone()).take(*count))
                    .collect();
                chunk = crate::chunk::concatenate(&new_parts)?;
                w.update_cache(chunk.clone())?;
            }

            drop(w);

            let mut r = s.iter()?;
            assert_eq!(r.next().map(|v| v.unwrap()), Some(chunk));
        }

        Ok(())
    }
}

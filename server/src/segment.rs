//! A segment contains a bundle of time and logically indexed rows.
//!
//! Additionally, a segment keeps an "active chunk" cache to avoid chunk
//! fragmentation in low write frequency workloads. This cache persists the
//! "current" non-full chunk in the segment. New rows may be appended to this
//! cache via [SegmentWriter2::update_cache] until a full row group is written
//! via [SegmentWriter2::log_arrow].
//!
//! At that point, the cache is discarded and a new empty active chunk cache is
//! opened for the next segment in the file.
//!
//! Currently, the only supported segment format is local Parquet files.
//!
//! For caching and crash recovery, each segment may be accompanied by up to
//! three other files:
//!
//! - `{segment}.arrows`: Active chunk cache. On disk, the file is prefixed with
//!   a fixed version identifier and the active chunk index. After this index,
//!   arrow chunks are appended in *streaming* IPC format, not the typical v2
//!   feather format. The additional footer required by the feather format would
//!   add overhead and create headaches during crash recovery.
//! - `{segment}.header`: Recovery "header". Contains a fixed version identifier
//!   prefix, current offset, and parquet footer.  See `write_checkpoint` for
//!   internal details of this file, and `recover` for the recovery process.
//! - `{segment}.header.tmp`: A temporary file that the above data is written
//!   into before moving to the above file, to avoid issues occurring if we
//!   crash while writing the recovery header.
use crate::arrow2::{
    datatypes::Schema,
    io::ipc,
    io::parquet::read::FileReader as FileReader2,
    io::parquet::read::{infer_schema, read_metadata},
    io::parquet::write::{
        add_arrow_schema, transverse, CompressionOptions, Encoding, RowGroupIterator, Version,
        WriteOptions,
    },
};
use anyhow::Result;
use log::{debug, error, warn};
use parquet2::metadata::{FileMetaData, KeyValue};
use plateau_client::ipc::read::StreamState;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::ffi::OsStr;
#[cfg(test)]
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter;
use std::{fs, path::Path, path::PathBuf};

pub use crate::chunk::Record;
use plateau_transport::{arrow2, SchemaChunk, SegmentChunk};

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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Config {}

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
        Ok(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?)
    }

    pub(crate) fn create2(&self, schema: Schema, config: Config) -> Result<SegmentWriter2> {
        let checkpoint = self.checkpoint_path();
        let mut tmp: PathBuf = self.path.clone();
        assert!(tmp.extension() != Some(OsStr::new("header")));
        assert!(tmp.extension() != Some(OsStr::new("header.tmp")));
        assert!(tmp.set_extension("header.tmp"));

        // TODO: ideally we'd use compression, but this currently causes nasty dependency issues
        // between parquet2 and parquet
        let options = WriteOptions {
            data_pagesize_limit: None,
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
        };

        let parquet_schema = arrow2::io::parquet::write::to_parquet_schema(&schema)?;

        let created_by = Some("plateau v0.1.0 segment v1".to_string());

        let file = self.file()?;
        let writer = parquet2::write::FileWriter::new(
            file.try_clone()?,
            parquet_schema,
            parquet2::write::WriteOptions {
                version: options.version,
                write_statistics: options.write_statistics,
            },
            created_by,
        );

        let key_value_metadata = add_arrow_schema(&schema, None);

        Ok(SegmentWriter2 {
            path: self.path.clone(),
            checkpoint,
            tmp,
            file,
            writer,
            schema,
            key_value_metadata,
            options,
            durable_checkpoints: false,
            chunk_ix: 0,
            cache: RowGroupCache::empty(self.cache_path(), 0),
            _config: config,
        })
    }

    pub(crate) fn destroy(&self) -> Result<()> {
        if Path::exists(&self.path) {
            fs::remove_file(&self.path)?;
        }

        if Path::exists(&self.cache_path()) {
            fs::remove_file(self.cache_path())?;
        }

        Ok(())
    }

    pub(crate) fn validate(&self) -> bool {
        self.read_double_ended().is_ok()
    }

    pub(crate) fn read_double_ended(&self) -> Result<DoubleEndedChunkReader> {
        DoubleEndedChunkReader::open(
            self.path.as_path(),
            self.cache_path(),
            self.checkpoint_path(),
        )
    }

    fn checkpoint_path(&self) -> PathBuf {
        let mut path: PathBuf = self.path.clone();
        assert!(path.set_extension("header"));
        path
    }

    fn cache_path(&self) -> PathBuf {
        let mut path: PathBuf = self.path.clone();
        assert!(path.set_extension("arrows"));
        path
    }
}

struct RowGroupCache {
    chunk_ix: u32,
    len: usize,
    partial_chunk: Option<SegmentChunk>,
    path: PathBuf,
    writer: Option<ipc::write::StreamWriter<fs::File>>,
}

impl RowGroupCache {
    fn empty(path: PathBuf, chunk_ix: u32) -> Self {
        Self {
            chunk_ix,
            len: 0,
            partial_chunk: None,
            path,
            writer: None,
        }
    }

    fn read(path: PathBuf) -> Result<(Option<Schema>, Self)> {
        if Path::exists(&path) {
            let mut reader = fs::File::open(&path)?;
            validate_header(&mut reader)?;

            let mut buffer = [0; 4];
            reader.read_exact(&mut buffer)?;
            let chunk_ix = u32::from_le_bytes(buffer);

            let metadata = ipc::read::read_stream_metadata(&mut reader)?;
            let schema = metadata.schema.clone();
            let reader = ipc::read::StreamReader::new(reader, metadata, None);

            let chunks = reader
                .take_while(|state| matches!(state, Ok(StreamState::Some(..)) | Err(..)))
                .filter_map(|result| match result {
                    Ok(StreamState::Some(chunk)) => Some(chunk),
                    Ok(StreamState::Waiting) => {
                        warn!("halting on unexpected waiting state");
                        None
                    }
                    Err(e) => {
                        error!("error reading cache segment: {:?}", e);
                        None
                    }
                })
                .collect::<Vec<_>>();

            let len = chunks.iter().map(|chunk| chunk.len()).sum();
            let partial_chunk = if chunks.is_empty() {
                None
            } else {
                Some(crate::chunk::concatenate(&chunks)?)
            };

            Ok((
                Some(schema),
                Self {
                    chunk_ix,
                    len,
                    partial_chunk,
                    path,
                    writer: None,
                },
            ))
        } else {
            Ok((None, Self::empty(path, 0)))
        }
    }

    fn update(&mut self, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        if self.len == chunk.len() {
            return Ok(());
        }

        // sigh. can't compose get_or_insert_with and a closure returning a Result
        if self.writer.is_none() {
            let mut file = fs::File::create(&self.path)?;
            file.write_all(PLATEAU_HEADER.as_bytes())?;

            let buffer = self.chunk_ix.to_le_bytes();
            file.write_all(&buffer)?;
            let options = ipc::write::WriteOptions { compression: None };
            let mut writer = ipc::write::StreamWriter::new(file, options);
            writer.start(schema, None)?;

            self.writer = Some(writer)
        };

        // SAFETY: if `self.writer` was None, that was fixed above
        let writer = self.writer.as_mut().unwrap();
        let new_len = chunk.len();
        let additional = crate::chunk::slice(chunk.clone(), self.len, new_len - self.len);
        writer.write(&additional, None)?;

        self.len = new_len;
        self.partial_chunk = Some(chunk);

        Ok(())
    }
}

pub struct SegmentWriter2 {
    path: PathBuf,
    checkpoint: PathBuf,
    tmp: PathBuf,
    file: fs::File,
    writer: parquet2::write::FileWriter<fs::File>,
    schema: Schema,
    key_value_metadata: Option<Vec<KeyValue>>,
    options: WriteOptions,
    durable_checkpoints: bool,
    chunk_ix: u32,
    cache: RowGroupCache,
    _config: Config,
}

impl SegmentWriter2 {
    pub fn check_schema(&self, schema: &Schema) -> bool {
        &self.schema == schema
    }

    fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        let encodings: Vec<_> = self
            .schema
            .fields
            .iter()
            .map(|field| transverse(field.data_type(), |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(
            iter::once(Ok(chunk)),
            &self.schema,
            self.options,
            encodings,
        )?;

        for group in row_groups {
            self.writer.write(group?)?;
        }

        Ok(())
    }

    pub fn log_arrow<S: Borrow<Schema> + Clone + PartialEq>(
        &mut self,
        data: SchemaChunk<S>,
    ) -> Result<()> {
        if !self.check_schema(data.schema.borrow()) {
            anyhow::bail!("cannot use different schemas within the same segment");
        }

        self.write_chunk(data.chunk)?;
        self.write_checkpoint()?;

        self.chunk_ix += 1;
        self.cache = RowGroupCache::empty(self.cache.path.clone(), self.chunk_ix);

        Ok(())
    }

    /// Update the active chunk cache with a superset of the active rows.
    ///
    /// This operation is append-only. All rows currently in the cache
    /// are assumed to be equivalent to their same-index counterparts in the
    /// "new" active chunk.
    pub fn update_cache(&mut self, active: SegmentChunk) -> Result<()> {
        self.cache.update(&self.schema, active)
    }

    fn write_checkpoint(&self) -> Result<()> {
        use parquet_format_safe::thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
        let metadata = self
            .writer
            .compute_metadata(self.key_value_metadata.clone())?;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.tmp)?;

        // Write header
        file.write_all(PLATEAU_HEADER.as_bytes())?;

        // Write offset
        let offset_bytes = self.writer.offset().to_le_bytes();
        file.write_all(&offset_bytes)?;

        // Then, write the header
        let mut protocol = TCompactOutputProtocol::new(&mut file);
        let metadata_len = metadata.write_to_out_protocol(&mut protocol)? as i32;
        protocol.flush()?;
        drop(protocol);
        file.write_all(&metadata_len.to_le_bytes())?;

        // Now, sync all the things
        if self.durable_checkpoints {
            // first the header file
            file.sync_data()?;
            drop(file);
            // then the actual file, to ensure the underlying data is on disk
            self.file.sync_data()?;
        }

        std::fs::rename(&self.tmp, &self.checkpoint)?;

        // Finally sync the directory metadata, which will capture the rename.
        if self.durable_checkpoints {
            let mut parent = self.path.clone();
            parent.pop();
            let directory = fs::File::open(&parent)?;
            directory.sync_all()?;
        }

        Ok(())
    }

    fn get_path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn end(&mut self) -> Result<()> {
        if let Some(chunk) = std::mem::take(&mut self.cache.partial_chunk) {
            self.write_chunk(chunk)?;
        }

        self.writer.end(self.key_value_metadata.clone())?;
        self.file.sync_data()?;
        if Path::exists(&self.checkpoint) {
            std::fs::remove_file(&self.checkpoint)?;
        }
        if Path::exists(&self.cache.path) {
            std::fs::remove_file(&self.cache.path)?;
        }
        Ok(())
    }

    /// Return an estimate of the on-disk size of the corresponding file(s).
    /// Note that this will _not_ include the checkpoint, in-flight checkpoint,
    /// or final footer (unless this segment has been sealed).
    pub fn size_estimate(&self) -> Result<usize> {
        let main_size = fs::metadata(self.get_path()).map(|p| p.len()).unwrap_or(0);
        let cache_size = fs::metadata(&self.cache.path).map(|p| p.len()).unwrap_or(0);
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

fn recover(path: impl AsRef<Path>, checkpoint_path: impl AsRef<Path>) -> Result<FileMetaData> {
    let path = path.as_ref();
    let checkpoint_path = checkpoint_path.as_ref();
    warn!("attempting to recover checkpoint {:?}", checkpoint_path);

    {
        let mut checkpoint = fs::File::open(checkpoint_path)?;
        let mut segment = fs::File::options().write(true).open(path)?;

        validate_header(&mut checkpoint)?;
        let mut buffer = [0u8; 8];
        checkpoint.read_exact(&mut buffer)?;
        let offset = u64::from_le_bytes(buffer);
        debug!(
            "found valid v1 checkpoint at offset {} (file length {})",
            offset,
            segment.metadata()?.len()
        );

        let rest: Vec<u8> = checkpoint.bytes().collect::<Result<_, _>>()?;

        segment.set_len(offset)?;
        segment.seek(SeekFrom::Start(offset))?;
        segment.write_all(&rest)?;
        segment.write_all("PAR1".as_bytes())?;
        warn!("successfully recovered checkpoint {:?}", checkpoint_path);
    }

    let mut f = fs::File::open(path)?;
    read_metadata(&mut f).map_err(anyhow::Error::from)
}

pub(crate) struct DoubleEndedChunkReader {
    pub path: PathBuf,
    parquet: Option<arrow2::io::parquet::read::FileMetaData>,
    pub schema: Schema,
    cache: RowGroupCache,
}

impl DoubleEndedChunkReader {
    pub fn open(
        path: impl AsRef<Path>,
        cache: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let (schema, parquet) = if path.as_ref().exists() {
            let mut f = fs::File::open(path.as_ref())?;
            let metadata = read_metadata(&mut f)
                .map_err(anyhow::Error::from)
                .or_else(|_| {
                    drop(f);
                    recover(path.as_ref(), checkpoint_path)
                });

            if let Ok(metadata) = metadata {
                let schema = infer_schema(&metadata)?;
                (Some(schema), Some(metadata))
            } else {
                // error!
                (None, None)
            }
        } else {
            (None, None)
        };

        let (cache_schema, cache) = RowGroupCache::read(cache.as_ref().to_owned())?;

        if let Some(schema) = schema.or(cache_schema) {
            Ok(Self {
                path: path.as_ref().to_path_buf(),
                schema,
                parquet,
                cache,
            })
        } else {
            Err(anyhow::anyhow!("both cache and segment are missing"))
        }
    }

    pub fn iter(self) -> impl DoubleEndedIterator<Item = Result<SegmentChunk>> {
        let file = fs::File::open(self.path.clone()).unwrap();

        let (rev_position, fwd_reader) = if let Some(metadata) = self.parquet {
            let len = metadata.row_groups.len();
            let fwd_rdr =
                FileReader2::new(file, metadata.row_groups, self.schema, None, None, None);
            (len, Some(fwd_rdr))
        } else {
            (0, None)
        };

        DoubleEndedChunkIterator {
            fwd_reader,
            fwd_position: 0,
            rev_position,
            chunks: vec![],
            cache: self.cache,
            full_read: false,
        }
    }
}

/// Reads chunks in forward or reverse order, with lazy caching of chunk data
pub(crate) struct DoubleEndedChunkIterator {
    fwd_reader: Option<FileReader2<fs::File>>,
    fwd_position: usize,
    rev_position: usize,
    chunks: Vec<SegmentChunk>,
    cache: RowGroupCache,
    full_read: bool,
}
impl Iterator for DoubleEndedChunkIterator {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fwd_position >= self.rev_position && self.full_read {
            None
        } else {
            let index = self.fwd_position;
            self.fwd_position += 1;
            if self.chunks.len() < self.fwd_position && !self.full_read {
                match self.fwd_reader.as_mut().and_then(|r| r.next()) {
                    Some(Ok(chunk)) => self.chunks.push(chunk),
                    Some(Err(e)) => {
                        error!("failed to read chunk from segment: {e}");
                        return Some(Err(e.into()));
                    }
                    None => {
                        // we reached the end of the segment
                        self.full_read = true;
                        // check that the cache is valid
                        if self.cache.partial_chunk.is_some()
                            && (self.chunks.len() as u32) == self.cache.chunk_ix
                        {
                            // dump the cache onto the end
                            self.chunks
                                .extend(std::mem::take(&mut self.cache.partial_chunk));
                            self.rev_position += self.cache.len;
                        }
                    }
                }
            }

            if index >= self.chunks.len() {
                return None;
            }

            Some(Ok(self.chunks[index].clone()))
        }
    }
}

impl DoubleEndedIterator for DoubleEndedChunkIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.rev_position <= self.fwd_position {
            None
        } else {
            if !self.full_read {
                let fwd = self.fwd_position;
                if let Some(e) = self.into_iter().find_map(|r| r.err()) {
                    return Some(Err(e));
                }
                self.fwd_position = fwd;
            }
            self.rev_position -= 1;
            Some(Ok(self.chunks[self.rev_position].clone()))
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::arrow2::datatypes::{Field, Metadata};
    use crate::chunk::test::{inferences_nested, inferences_schema_a, inferences_schema_b};
    use crate::chunk::{iter_legacy, legacy_schema, LegacyRecords};
    use chrono::{TimeZone, Utc};
    use sample_arrow2::{
        array::ArbitraryArray,
        chunk::{ArbitraryChunk, ChainedChunk, ChainedMultiChunk},
        datatypes::{sample_flat, ArbitraryDataType},
    };
    use sample_std::{Chance, Random, Regex, Sample};
    use sample_test::sample_test;
    use tempfile::tempdir;

    pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
        it.map(|(ix, message)| Record {
            time: Utc.timestamp_opt(ix, 0).unwrap(),
            message: message.into_bytes(),
        })
        .collect()
    }

    fn collect_records(
        schema: Schema,
        iter: impl Iterator<Item = Result<SegmentChunk, anyhow::Error>>,
    ) -> Vec<Record> {
        iter_legacy(schema, iter).flat_map(Result::unwrap).collect()
    }

    #[test]
    fn can_iter_forward_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create2(legacy_schema(), Config::default())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(SchemaChunk::try_from(LegacyRecords([record].to_vec()))?)?;
        }
        let _ = w.close()?;

        let reader = s.read_double_ended()?;

        assert_eq!(collect_records(schema, reader.iter()), records);
        Ok(())
    }

    #[test]
    fn can_iter_reverse_double_ended() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path.clone());
        let mut records: Vec<_> = build_records((0..10).into_iter().map(|i| (i, format!("m{i}"))));

        let mut w = s.create2(legacy_schema(), Config::default())?;
        let schema = w.schema.clone();
        for record in records.clone() {
            w.log_arrow(SchemaChunk::try_from(LegacyRecords([record].to_vec()))?)?;
        }
        let _ = w.close()?;

        records.reverse();

        let reader = s.read_double_ended()?;
        //let result: Vec<_> = r.map(|i| i.unwrap()).collect();
        //let (schema, iter) = r.into_chunk_iter();
        assert_eq!(collect_records(schema, reader.iter().rev()), records);
        Ok(())
    }

    #[test]
    fn round_trip1_2() -> Result<()> {
        let path = PathBuf::from("tests/data/v1.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let r = s.read_double_ended()?;
        assert_eq!(collect_records(r.schema.clone(), r.iter()), records);
        Ok(())
    }

    #[test]
    fn round_trip2() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}", ix))),
        );

        let schema = legacy_schema();
        let mut w = s.create2(schema.clone(), Config::default())?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[0..10].to_vec(),
        ))?)?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[10..].to_vec(),
        ))?)?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read_double_ended()?;
        assert_eq!(collect_records(schema, r.iter()), records);
        Ok(())
    }

    #[test]
    fn schema_change() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a)?;
        let b = inferences_schema_b();
        assert!(!w.check_schema(&b.schema));
        assert!(w.log_arrow(b).is_err());
        Ok(())
    }

    #[test]
    fn schema_file_metadata() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let mut a = inferences_schema_a();
        a.schema
            .metadata
            .insert("pipeline.name".to_string(), "pied-piper".to_string());
        a.schema
            .metadata
            .insert("pipeline.version".to_string(), "3.1".to_string());
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a)?;
        w.close()?;

        let r = s.read_double_ended()?;
        assert_eq!(
            r.schema.metadata.get("pipeline.name").unwrap(),
            "pied-piper"
        );
        assert_eq!(r.schema.metadata.get("pipeline.version").unwrap(), "3.1");

        Ok(())
    }

    #[test]
    fn nested() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let a = inferences_nested();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a)?;
        Ok(())
    }

    #[test]
    fn large_records() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);
        let large: String = (0..100 * 1024).map(|_| "x").collect();
        let records: Vec<_> = build_records(
            (0..20)
                .into_iter()
                .map(|ix| (ix, format!("message-{}-{}", ix, large))),
        );

        let mut w = s.create2(legacy_schema(), Config::default())?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[0..10].to_vec(),
        ))?)?;
        w.log_arrow(SchemaChunk::try_from(LegacyRecords(
            records[10..].to_vec(),
        ))?)?;
        let size = w.close()?;
        assert!(size > 0);

        let r = s.read_double_ended()?;
        assert_eq!(collect_records(legacy_schema(), r.iter()), records);

        Ok(())
    }

    #[test]
    fn test_open_drop_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("open-drop.parquet");
        let s = Segment::at(path);

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone())?;
        drop(w);

        let r = s.read_double_ended()?;
        assert_eq!(r.iter().next().map(|v| v.unwrap()), Some(a.chunk));

        Ok(())
    }

    #[test]
    fn test_partial_write_recovery() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone())?;
        w.update_cache(a.chunk.clone())?;
        let len = w.writer.offset();
        drop(w);

        std::fs::File::options()
            .append(true)
            .open(path)?
            .set_len(len - 4)?;

        let r = s.read_double_ended()?;
        let mut iter = r.iter();
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(iter.next().map(|v| v.unwrap()), Some(a.chunk));
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn test_partial_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create2(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone())?;
        w.update_cache(a.chunk.clone())?;
        drop(w);

        let f = std::fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let r = s.read_double_ended()?;
        assert_eq!(r.iter().next().map(|v| v.unwrap()), Some(a.chunk));

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

            let r = s.read_double_ended()?;
            assert_eq!(r.iter().next().map(|v| v.unwrap()), Some(chunk));
        }

        Ok(())
    }

    fn deep_chunk(depth: usize, len: usize) -> ArbitraryChunk<Regex, Chance> {
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

    #[sample_test]
    fn arbitrary_chunk(#[sample(deep_chunk(3, 100).sample_one())] chunk: ChainedChunk) {
        let chunk = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        use sample_std::Sample;
        let name = sample_std::Regex::new("[a-z]{4, 8}");
        let mut g = sample_std::Random::new();

        let schema = Schema {
            fields: chunk
                .iter()
                .map(|arr| {
                    Field::new(
                        name.generate(&mut g),
                        arr.data_type().clone(),
                        arr.validity().is_some(),
                    )
                })
                .collect(),
            metadata: Metadata::default(),
        };
        let mut w = s.create2(schema.clone(), Config::default()).unwrap();
        let expected = SchemaChunk {
            schema,
            chunk: chunk.clone(),
        };
        w.log_arrow(expected).unwrap();
        w.close().unwrap();

        let r = s.read_double_ended().unwrap();
        let chunks = r.iter().collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(chunks, vec![chunk]);
    }

    #[sample_test]
    fn arbitrary_many_chunk(
        #[sample(deep_chunk(5, 100).sample_many(2..10))] chunk: ChainedMultiChunk,
    ) {
        let chunks = chunk.value;
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(path);

        let name = Regex::new("[a-z]{4, 8}");
        let mut g = Random::new();

        let schema = Schema {
            fields: chunks
                .first()
                .unwrap()
                .iter()
                .map(|arr| {
                    Field::new(
                        name.generate(&mut g),
                        arr.data_type().clone(),
                        arr.validity().is_some(),
                    )
                })
                .collect(),
            metadata: Metadata::default(),
        };
        let mut w = s.create2(schema.clone(), Config::default()).unwrap();

        for chunk in &chunks {
            let expected = SchemaChunk {
                schema: schema.clone(),
                chunk: chunk.clone(),
            };
            w.log_arrow(expected).unwrap();
        }
        w.close().unwrap();

        let r = s.read_double_ended().unwrap();
        let actual_chunks = r.iter().collect::<anyhow::Result<Vec<_>>>().unwrap();
        assert_eq!(actual_chunks, chunks);
    }
}

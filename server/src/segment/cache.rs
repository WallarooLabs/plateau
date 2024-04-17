//! Cache file format for the "active" chunk.
//!
//! On disk, the file is prefixed with a fixed version identifier and the active
//! chunk index. After this index, arrow chunks are appended in *streaming* IPC
//! format, not the typical v2 feather format. The additional footer required by
//! the feather format would add overhead and create headaches during crash
//! recovery.
use std::{
    fs,
    io::{Read, Write},
    mem,
    path::{Path, PathBuf},
    time::Instant,
};

use crate::arrow2::{datatypes::Schema, io::ipc};
use anyhow::Result;
use plateau_client::ipc::read::StreamState;
use plateau_transport::{SchemaChunk, SegmentChunk};
use tracing::{debug, error, trace, warn};

use super::{validate_header, PLATEAU_HEADER};

pub struct ActiveChunk {
    path: PathBuf,
    writer: Option<Writer>,
}

impl ActiveChunk {
    pub fn new(path: PathBuf) -> Self {
        Self { path, writer: None }
    }

    pub fn clear(&mut self) {
        self.writer = None;
    }

    pub fn take(&mut self) -> Option<SchemaChunk<Schema>> {
        mem::take(&mut self.writer).map(|w| w.into_inner())
    }

    pub fn update(&mut self, chunk_ix: u32, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        match &mut self.writer {
            Some(cache) => cache.update(schema, chunk)?,
            None => {
                self.writer = Some(Writer::new(
                    self.path.clone(),
                    SchemaChunk {
                        schema: schema.clone(),
                        chunk,
                    },
                    chunk_ix,
                )?);
            }
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(writer) = &self.writer {
            writer.sync()?;
        }

        Ok(())
    }

    pub fn destroy(&mut self) -> Result<()> {
        if Path::exists(&self.path) {
            std::fs::remove_file(&self.path)?;
        }
        self.writer = None;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.writer.as_ref().map_or(0, |w| w.size())
    }
}

pub struct Writer {
    path: PathBuf,
    file: fs::File,

    writer: ipc::write::StreamWriter<fs::File>,
    partial: SchemaChunk<Schema>,
}

impl Writer {
    pub fn new(path: PathBuf, initial: SchemaChunk<Schema>, chunk_ix: u32) -> Result<Self> {
        trace!("start {:?} with {}", path, initial.chunk.len());
        let mut file = fs::File::create(&path)?;
        file.write_all(PLATEAU_HEADER.as_bytes())?;

        let buffer = chunk_ix.to_le_bytes();
        file.write_all(&buffer)?;

        let options = ipc::write::WriteOptions { compression: None };
        let mut writer = ipc::write::StreamWriter::new(file.try_clone()?, options);
        writer.start(&initial.schema, None)?;
        writer.write(&initial.chunk, None)?;

        Ok(Self {
            path,
            file,
            writer,
            partial: initial,
        })
    }

    fn len(&self) -> usize {
        self.partial.chunk.len()
    }

    pub(super) fn update(&mut self, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        if self.len() == chunk.len() {
            return Ok(());
        }

        anyhow::ensure!(schema == &self.partial.schema);

        let new_len = chunk.len();
        let additional = crate::chunk::slice(chunk.clone(), self.len(), new_len - self.len());
        trace!("{} => {}", self.len(), chunk.len());
        self.writer.write(&additional, None)?;
        self.partial.chunk = chunk;

        Ok(())
    }

    pub(super) fn sync(&self) -> Result<()> {
        let now = Instant::now();
        self.file.sync_data()?;
        debug!("cache sync elapsed: {:?}", now.elapsed());

        Ok(())
    }

    pub(super) fn into_inner(self) -> SchemaChunk<Schema> {
        self.partial
    }

    pub(super) fn size(&self) -> u64 {
        fs::metadata(&self.path).map(|p| p.len()).unwrap_or(0)
    }
}

pub struct Data {
    pub chunk_ix: u32,
    pub rows: SchemaChunk<Schema>,
}

pub fn read(path: PathBuf) -> Result<Option<Data>> {
    if path.exists() {
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

        let concat = if chunks.is_empty() {
            // This can really only happen if plateau / the system crashes while
            // writing the first chunk to the stream. That seems unlikely enough
            // to warn about.
            warn!("could not read any chunks from cache at {:?}", path);
            return Ok(None);
        } else {
            let chunk = crate::chunk::concatenate(&chunks)?;
            trace!(
                "read {} rows ({} chunks) from cache",
                chunk.len(),
                chunks.len()
            );
            chunk
        };

        Ok(Some(Data {
            chunk_ix,
            rows: SchemaChunk {
                schema,
                chunk: concat,
            },
        }))
    } else {
        Ok(None)
    }
}

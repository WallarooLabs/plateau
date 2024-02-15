use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use crate::arrow2::{datatypes::Schema, io::ipc};
use anyhow::Result;
use plateau_client::ipc::read::StreamState;
use plateau_transport::SegmentChunk;
use tracing::{error, warn};

use super::{validate_header, PLATEAU_HEADER};

pub(super) struct RowGroupCache {
    chunk_ix: u32,
    len: usize,
    pub(super) partial_chunk: Option<SegmentChunk>,
    pub(super) path: PathBuf,
    writer: Option<ipc::write::StreamWriter<fs::File>>,
    file: Option<fs::File>,
}

impl RowGroupCache {
    pub(super) fn empty(path: PathBuf, chunk_ix: u32) -> Self {
        Self {
            chunk_ix,
            len: 0,
            partial_chunk: None,
            path,
            writer: None,
            file: None,
        }
    }

    pub(super) fn read(path: PathBuf) -> Result<(Option<Schema>, Self)> {
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
                    file: None,
                },
            ))
        } else {
            Ok((None, Self::empty(path, 0)))
        }
    }

    pub(super) fn update(&mut self, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        if self.len == chunk.len() {
            return Ok(());
        }

        // sigh. can't compose get_or_insert_with and a closure returning a Result
        if self.writer.is_none() {
            let mut file = fs::File::create(&self.path)?;
            file.write_all(PLATEAU_HEADER.as_bytes())?;

            let buffer = self.chunk_ix.to_le_bytes();
            file.write_all(&buffer)?;
            self.file = Some(file.try_clone()?);

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

    pub(super) fn sync(&self) -> Result<()> {
        if let Some(f) = self.file.as_ref() {
            f.sync_data()?;
        }

        Ok(())
    }
}

impl IntoIterator for RowGroupCache {
    type Item = SegmentChunk;
    type IntoIter = std::option::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.partial_chunk.into_iter()
    }
}

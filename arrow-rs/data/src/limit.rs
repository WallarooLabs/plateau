use crate::{
    chunk::{IndexedChunk, RecordBatchExt, Schema},
    compatible::Compatible,
    transport::estimate_size,
};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Retention {
    pub max_segment_count: Option<usize>,
    pub max_bytes: ByteSize,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_segment_count: Some(10000),
            max_bytes: ByteSize::gib(1),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Rolling {
    pub max_bytes: ByteSize,
    pub max_rows: usize,
    pub max_duration: Option<Duration>,
}

impl Default for Rolling {
    fn default() -> Self {
        Self {
            max_bytes: ByteSize::mib(100),
            max_rows: 100000,
            max_duration: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct RowLimit {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for RowLimit {
    fn default() -> Self {
        Self {
            max_records: 10000,
            max_bytes: crate::DEFAULT_BYTE_LIMIT,
        }
    }
}

impl RowLimit {
    pub fn records(max_records: usize) -> Self {
        Self {
            max_records,
            ..Self::default()
        }
    }

    pub fn after(&self, indexed: &mut IndexedChunk) -> BatchStatus {
        let count: usize = indexed.chunk.len();
        let bytes: usize = estimate_size(&indexed.chunk).unwrap_or(0);
        if count >= self.max_records {
            indexed.slice(0, self.max_records);
            BatchStatus::RecordsExceeded
        } else if bytes >= self.max_bytes {
            // NOTE: byte-size slicing is much more complex in arrow, so we're
            // punting for now.
            BatchStatus::BytesExceeded
        } else {
            BatchStatus::Open {
                remaining: Self {
                    max_records: self.max_records - count,
                    max_bytes: self.max_bytes - bytes,
                },
            }
        }
    }

    pub fn min(self, other: Self) -> Self {
        Self {
            max_records: std::cmp::min(self.max_records, other.max_records),
            max_bytes: std::cmp::min(self.max_bytes, other.max_bytes),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BatchStatus {
    Open { remaining: RowLimit },
    SchemaChanged,
    BytesExceeded,
    RecordsExceeded,
}

impl BatchStatus {
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open { .. })
    }

    pub fn is_schema_changed(&self) -> bool {
        matches!(self, Self::SchemaChanged)
    }
}

#[derive(Debug)]
pub struct LimitedBatch {
    pub schema: Option<Schema>,
    pub chunks: Vec<IndexedChunk>,
    pub status: BatchStatus,
}

impl LimitedBatch {
    pub fn open(limit: RowLimit) -> Self {
        Self {
            schema: None,
            chunks: vec![],
            status: BatchStatus::Open { remaining: limit },
        }
    }

    pub fn compatible_with(&self, other: &Self) -> bool {
        self.schema
            .as_ref()
            .zip(other.schema.as_ref())
            .is_none_or(|(a, b)| a.compatible(b))
    }

    pub fn extend_one(&mut self, mut indexed: IndexedChunk) {
        if indexed.chunk.is_empty() {
            return;
        }

        if let BatchStatus::Open { remaining } = self.status {
            let schema = self
                .schema
                .get_or_insert_with(|| indexed.inner_schema.clone());
            self.status = if !schema.compatible(&indexed.inner_schema) {
                BatchStatus::SchemaChanged
            } else {
                let status = remaining.after(&mut indexed);
                self.chunks.push(indexed);
                status
            };
        }
    }
}

impl Extend<IndexedChunk> for LimitedBatch {
    fn extend<I>(&mut self, i: I)
    where
        I: IntoIterator<Item = IndexedChunk>,
    {
        for chunk in i {
            self.extend_one(chunk);
            if self.status.is_schema_changed() {
                tracing::debug!("SchemaChanged status detected");
            }
            if !self.status.is_open() {
                return;
            }
        }
    }
}

impl IntoIterator for LimitedBatch {
    type Item = IndexedChunk;
    type IntoIter = std::vec::IntoIter<IndexedChunk>;

    fn into_iter(self) -> Self::IntoIter {
        self.chunks.into_iter()
    }
}

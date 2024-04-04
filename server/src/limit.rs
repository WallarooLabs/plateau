use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use plateau_transport::estimate_size;

use crate::chunk::{IndexedChunk, Schema};
use crate::compatible::Compatible;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Retention {
    pub max_segment_count: Option<usize>,
    pub max_bytes: ByteSize,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_segment_count: Some(10000),
            max_bytes: ByteSize::gib(1),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Rolling {
    pub max_segment_size: ByteSize,
    pub max_segment_index: usize,
    pub max_segment_duration: Option<Duration>,
}

impl Default for Rolling {
    fn default() -> Self {
        Rolling {
            max_segment_size: ByteSize::mib(100),
            max_segment_index: 100000,
            max_segment_duration: None,
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
        RowLimit {
            max_records: 10000,
            max_bytes: 100 * 1024,
        }
    }
}

impl RowLimit {
    pub fn records(max_records: usize) -> Self {
        RowLimit {
            max_records,
            ..RowLimit::default()
        }
    }

    pub(crate) fn after(&self, indexed: &mut IndexedChunk) -> BatchStatus {
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
                remaining: RowLimit {
                    max_records: self.max_records - count,
                    max_bytes: self.max_bytes - bytes,
                },
            }
        }
    }

    pub fn min(self, other: RowLimit) -> Self {
        RowLimit {
            max_records: std::cmp::min(self.max_records, other.max_records),
            max_bytes: std::cmp::min(self.max_bytes, other.max_bytes),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum BatchStatus {
    Open { remaining: RowLimit },
    SchemaChanged,
    BytesExceeded,
    RecordsExceeded,
}

impl BatchStatus {
    pub(crate) fn is_open(&self) -> bool {
        matches!(self, BatchStatus::Open { .. })
    }

    pub(crate) fn is_schema_changed(&self) -> bool {
        matches!(self, Self::SchemaChanged)
    }
}

#[derive(Debug)]
pub(crate) struct LimitedBatch {
    pub(crate) schema: Option<Schema>,
    pub(crate) chunks: Vec<IndexedChunk>,
    pub(crate) status: BatchStatus,
}

impl LimitedBatch {
    pub fn open(limit: RowLimit) -> Self {
        LimitedBatch {
            schema: None,
            chunks: vec![],
            status: BatchStatus::Open { remaining: limit },
        }
    }

    pub fn compatible_with(&self, other: &LimitedBatch) -> bool {
        self.schema
            .as_ref()
            .zip(other.schema.as_ref())
            .map(|(a, b)| a.compatible(b))
            .unwrap_or(true)
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

#[cfg(test)]
mod test {
    use plateau_transport::SegmentChunk;

    use crate::{chunk::iter_legacy, topic::Record};

    use super::*;

    impl LimitedBatch {
        pub fn into_legacy(self) -> anyhow::Result<Vec<Record>> {
            if self.chunks.is_empty() {
                return Ok(vec![]);
            }

            if let Some(schema) = self.schema {
                use itertools::Itertools;
                iter_legacy(
                    schema,
                    self.chunks
                        .into_iter()
                        .map(|chunk| Ok(SegmentChunk::from(chunk))),
                )
                .flatten_ok()
                .collect()
            } else {
                Ok(vec![])
            }
        }
    }
}

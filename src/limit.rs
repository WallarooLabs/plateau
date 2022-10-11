use crate::chunk::{estimate_size, IndexedChunk, Record, Schema, SchemaChunk, SegmentChunk};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq)]
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
        match self {
            BatchStatus::Open { .. } => true,
            _ => false,
        }
    }
}

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

    pub fn schema_matches(&self, other: &LimitedBatch) -> bool {
        self.schema.is_none() || other.schema.as_ref() == self.schema.as_ref()
    }

    pub fn extend_one(&mut self, mut indexed: IndexedChunk) {
        match self.status {
            BatchStatus::Open { remaining } => {
                let schema = self
                    .schema
                    .get_or_insert_with(|| indexed.inner_schema.clone());
                self.status = if schema != &indexed.inner_schema {
                    BatchStatus::SchemaChanged
                } else {
                    let status = remaining.after(&mut indexed);
                    self.chunks.push(indexed);
                    status
                };
            }
            _ => {}
        }
    }

    pub fn into_legacy(self) -> anyhow::Result<Vec<Record>> {
        if self.chunks.len() == 0 {
            return Ok(vec![]);
        }

        if let Some(schema) = self.schema {
            use itertools::Itertools;
            SchemaChunk::iter_legacy(
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

impl Extend<IndexedChunk> for LimitedBatch {
    fn extend<I>(&mut self, i: I)
    where
        I: IntoIterator<Item = IndexedChunk>,
    {
        for chunk in i.into_iter() {
            self.extend_one(chunk);
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

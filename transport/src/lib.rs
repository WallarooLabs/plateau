use std::{borrow::Borrow, collections::HashMap, io::Cursor};

use arrow2::{array::Array, chunk::Chunk, io::ipc::write};
use rweb::Schema;
use serde::{Deserialize, Serialize};
#[cfg(feature = "structopt-cli")]
use structopt::StructOpt;

pub use arrow2::{self, datatypes::Schema as ArrowSchema, error::Error as ArrowError};

#[derive(Debug, Schema, Serialize, Deserialize)]
#[cfg_attr(feature = "structopt-cli", derive(StructOpt))]
pub struct InsertQuery {
    /// RFC3339 timestamp associated with inserted records
    #[cfg_attr(feature = "structopt-cli", structopt(short, long))]
    pub time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Insert {
    pub records: Vec<String>,
}

#[derive(Debug, Schema, Deserialize, Serialize)]
pub struct Inserted {
    pub span: Span,
}

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct Partitions {
    pub partitions: HashMap<String, Span>,
}

#[derive(Debug, Schema, Serialize, Deserialize, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct Records {
    pub span: Option<Span>,
    pub status: RecordStatus,
    pub records: Vec<String>,
}

#[derive(Schema, Debug, Default, Deserialize, Serialize)]
#[cfg_attr(feature = "structopt-cli", derive(StructOpt))]
pub struct RecordQuery {
    /// Start position
    pub start: usize,
    /// Number of records to return (defaults to 1000, maximum of 10000)
    #[cfg_attr(feature = "structopt-cli", structopt(short, long))]
    pub limit: Option<usize>,
    /// RFC3339 start time for records (defaults to earliest record)
    #[serde(rename = "time.start")]
    pub start_time: Option<String>,
    /// RFC3339 end time for records (required if start time exists)
    #[serde(rename = "time.end")]
    pub end_time: Option<String>,
}

/// Status of the record request query.
#[derive(Debug, Schema, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecordStatus {
    /// All current records returned.
    All,
    /// Record response was limited because the next chunk of records has a different schema.
    SchemaChange,
    /// Record response was limited by record limit. Additional records exist.
    RecordLimited,
    /// Record response was limited by payload size limit. Additional records exist.
    ByteLimited,
}

#[derive(Schema, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "structopt-cli", derive(StructOpt))]
pub struct TopicIterationQuery {
    /// Number of records to return (defaults to 1000, maximum of 10000)
    #[cfg_attr(feature = "structopt-cli", structopt(short, long))]
    pub limit: Option<usize>,
    /// RFC3339 start time for records (defaults to earliest record)
    #[serde(rename = "time.start")]
    pub start_time: Option<String>,
    /// RFC3339 end time for records (required if start time exists)
    #[serde(rename = "time.end")]
    pub end_time: Option<String>,
}

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct TopicIterationReply {
    pub records: Vec<String>,
    #[serde(flatten)]
    pub status: TopicIterationStatus,
}

pub type TopicIterator = HashMap<String, usize>;

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct TopicIterationStatus {
    pub status: RecordStatus,
    pub next: TopicIterator,
}

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct Topics {
    pub topics: Vec<Topic>,
}

#[derive(Debug, Schema, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
}

#[derive(Debug, Schema, Deserialize, Serialize)]
pub struct ErrorMessage {
    pub message: String,
    pub code: u16,
}

pub const CONTENT_TYPE_ARROW: &str = "application/vnd.apache.arrow.file";

pub type SegmentChunk = Chunk<Box<dyn Array>>;

/// A [SegmentChunk] packaged with its associated [ArrowSchema].
#[derive(Debug, Clone)]
pub struct SchemaChunk<S: Borrow<ArrowSchema> + Clone> {
    pub schema: S,
    pub chunk: SegmentChunk,
}

impl<S: Borrow<ArrowSchema> + Clone> SchemaChunk<S> {
    pub fn len(&self) -> usize {
        self.chunk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunk.is_empty()
    }
}

impl SchemaChunk<ArrowSchema> {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ArrowError> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let options = write::WriteOptions { compression: None };
        let mut writer = write::StreamWriter::new(bytes, options);

        writer.start(&self.schema, None)?;
        writer.write(&self.chunk, None)?;
        writer.finish()?;

        Ok(writer.into_inner().into_inner())
    }
}

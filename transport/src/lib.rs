use std::collections::HashMap;

use rweb::Schema;
use serde::{Deserialize, Serialize};

#[derive(Schema, Serialize, Deserialize)]
pub struct InsertQuery {
    pub time: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Insert {
    pub records: Vec<String>,
}

#[derive(Schema, Deserialize, Serialize)]
pub struct Inserted {
    pub span: Span,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct Partitions {
    pub partitions: HashMap<String, Span>,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct Records {
    pub span: Option<Span>,
    pub status: RecordStatus,
    pub records: Vec<String>,
}

#[derive(Schema, Default, Deserialize, Serialize)]
pub struct RecordQuery {
    pub start: usize,
    pub limit: Option<usize>,
    #[serde(rename = "time.start")]
    pub start_time: Option<String>,
    #[serde(rename = "time.end")]
    pub end_time: Option<String>,
}

/// Status of the record request query.
#[derive(Schema, Serialize, Deserialize)]
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

#[derive(Schema, Deserialize, Serialize)]
pub struct TopicIterationQuery {
    pub limit: Option<usize>,
    #[serde(rename = "time.start")]
    pub start_time: Option<String>,
    #[serde(rename = "time.end")]
    pub end_time: Option<String>,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct TopicIterationReply {
    pub records: Vec<String>,
    #[serde(flatten)]
    pub status: TopicIterationStatus,
}

pub type TopicIterator = HashMap<String, usize>;

#[derive(Schema, Serialize, Deserialize)]
pub struct TopicIterationStatus {
    pub status: RecordStatus,
    pub next: TopicIterator,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct Topics {
    pub topics: Vec<Topic>,
}

#[derive(Schema, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
}

#[derive(Schema, Deserialize, Serialize)]
pub struct ErrorMessage {
    pub message: String,
    pub code: u16,
}

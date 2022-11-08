use std::{borrow::Borrow, collections::HashMap, io::Cursor};

use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    datatypes::Field,
    io::ipc::write,
};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathError {
    Empty,
    KeyMissing(Vec<String>),
    NotStruct(Vec<String>),
}

impl SchemaChunk<ArrowSchema> {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ArrowError> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let options = write::WriteOptions { compression: None };
        let mut writer = write::FileWriter::new(bytes, self.schema.clone(), None, options);

        writer.start()?;
        writer.write(&self.chunk, None)?;
        writer.finish()?;

        Ok(writer.into_inner().into_inner())
    }

    pub fn get_array<'a>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Result<Box<dyn Array>, PathError> {
        let mut it = path.into_iter();
        let start = it.next().ok_or(PathError::Empty)?;
        let mut current_path = vec![start];
        let collect_path =
            |path: Vec<&'a str>| path.iter().map(|k| String::from(*k)).collect::<Vec<_>>();
        let key_missing = |path| PathError::KeyMissing(collect_path(path));
        let mut current = index_into(current_path[0], &self.schema.fields, self.chunk.arrays())
            .ok_or_else(|| key_missing(std::mem::take(&mut current_path)))?;

        for key in it {
            match current.as_any().downcast_ref::<StructArray>() {
                Some(arr) => {
                    current_path.push(key);
                    current = index_into(key, arr.fields(), arr.values())
                        .ok_or_else(|| key_missing(std::mem::take(&mut current_path)))?
                }
                None => return Err(PathError::NotStruct(collect_path(current_path))),
            }
        }

        Ok(current)
    }
}

fn index_into(key: &str, fields: &[Field], arrays: &[Box<dyn Array>]) -> Option<Box<dyn Array>> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(ix, f)| if f.name == key { Some(ix) } else { None })
        .next()
        .and_then(|ix| arrays.get(ix).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::{DataType, Field, Metadata, Schema};

    #[test]
    fn test_get_array() {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
        let index = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);

        let grandchild_struct = StructArray::new(
            DataType::Struct(vec![Field::new("index", index.data_type().clone(), false)]),
            vec![index.clone().boxed()],
            None,
        );

        let child_struct = StructArray::new(
            DataType::Struct(vec![Field::new(
                "grandchild",
                grandchild_struct.data_type().clone(),
                false,
            )]),
            vec![grandchild_struct.clone().boxed()],
            None,
        );

        let schema = Schema {
            fields: vec![
                Field::new("time", time.data_type().clone(), false),
                Field::new("child", child_struct.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        let test = SchemaChunk {
            schema,
            chunk: Chunk::try_new(vec![time.clone().boxed(), child_struct.clone().boxed()])
                .unwrap(),
        };

        assert_eq!(test.get_array(["time"]), Ok(time.boxed()));
        assert_eq!(test.get_array(["child"]), Ok(child_struct.boxed()));
        assert_eq!(
            test.get_array(["child", "grandchild"]),
            Ok(grandchild_struct.boxed())
        );
        assert_eq!(
            test.get_array(["child", "grandchild", "index"]),
            Ok(index.boxed())
        );

        assert_eq!(test.get_array([]), Err(PathError::Empty));
        assert_eq!(
            test.get_array(["child", "missing", "wrong"]),
            Err(PathError::KeyMissing(vec![
                "child".to_string(),
                "missing".to_string()
            ]))
        );

        assert_eq!(
            test.get_array(["time", "incorrect", "wrong"]),
            Err(PathError::NotStruct(vec!["time".to_string()]))
        );
    }
}

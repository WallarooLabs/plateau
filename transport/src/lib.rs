use std::{borrow::Borrow, collections::HashMap, io::Cursor};

use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    datatypes::Field,
    io::ipc::{read, write},
};
use rweb::Schema;
use serde::{Deserialize, Serialize};
#[cfg(feature = "structopt-cli")]
use structopt::StructOpt;

use arrow2::datatypes::{DataType, Metadata};
pub use arrow2::{self, datatypes::Schema as ArrowSchema, error::Error as ArrowError};
use thiserror::Error;

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
pub struct DataFocus {
    /// Data sets to return for this query.
    #[serde(default)]
    pub dataset: Vec<String>,
    /// When specified, flattens the output data sets into a single table. Uses
    /// the given separator to join nested keys.
    #[serde(default, rename = "dataset.separator")]
    pub dataset_separator: Option<String>,
}

impl DataFocus {
    pub fn is_some(&self) -> bool {
        !self.dataset.is_empty() || self.dataset_separator.is_some()
    }
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
    #[serde(flatten)]
    #[cfg_attr(feature = "structopt-cli", structopt(flatten))]
    pub data_focus: DataFocus,
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
    #[serde(flatten)]
    #[cfg_attr(feature = "structopt-cli", structopt(flatten))]
    pub data_focus: DataFocus,
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
pub const CONTENT_TYPE_JSON: &str = "application/json";

pub type SegmentChunk = Chunk<Box<dyn Array>>;

/// A [SegmentChunk] packaged with its associated [ArrowSchema].
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaChunk<S: Borrow<ArrowSchema> + Clone + PartialEq> {
    pub schema: S,
    pub chunk: SegmentChunk,
}

impl<S: Borrow<ArrowSchema> + Clone + PartialEq> SchemaChunk<S> {
    pub fn len(&self) -> usize {
        self.chunk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunk.is_empty()
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("provided path was empty")]
    Empty,
    #[error("key not found: {0:?}")]
    KeyMissing(Vec<String>),
    #[error("not a struct array: {0:?}")]
    NotStruct(Vec<String>),
}

impl SchemaChunk<ArrowSchema> {
    /// Serialize this [SchemaChunk] into Arrow IPC bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ArrowError> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
        let options = write::WriteOptions { compression: None };
        let mut writer = write::FileWriter::new(bytes, self.schema.clone(), None, options);

        writer.start()?;
        writer.write(&self.chunk, None)?;
        writer.finish()?;

        Ok(writer.into_inner().into_inner())
    }

    /// Deserialize a [SchemaChunk] from Arrow IPC bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ArrowError> {
        let mut cursor = Cursor::new(bytes);
        let metadata = read::read_file_metadata(&mut cursor)?;
        let schema = metadata.schema.clone();
        let mut reader = read::FileReader::new(cursor, metadata, None, None);

        Ok(SchemaChunk {
            chunk: reader.next().unwrap()?,
            schema,
        })
    }

    /// Convert a [StructArray] into a [SchemaChunk]
    pub fn from_struct(s: &StructArray) -> SchemaChunk<ArrowSchema> {
        SchemaChunk {
            chunk: Chunk::new(s.values().to_vec()),
            schema: ArrowSchema {
                fields: s.fields().to_vec(),
                metadata: Metadata::default(),
            },
        }
    }

    /// Convert a [StructArray] into a [SchemaChunk]
    pub fn to_struct(&self) -> StructArray {
        // TODO: nullable
        StructArray::new(
            DataType::Struct(self.schema.fields.clone()),
            self.chunk.arrays().to_vec(),
            None,
        )
    }

    /// Focus a new [SchemaChunk] on particular `dataset` keys, and flatten it
    /// into a single [StructArray] if a `separator` is given.
    ///
    /// If a single dataset is specified, return the array(s) directly without
    /// any nesting.
    pub fn focus(&self, focus: &DataFocus) -> Result<Self, PathError> {
        let mut fields = vec![];
        let mut arrays = vec![];
        for path in focus.dataset.iter() {
            let split = focus
                .dataset_separator
                .as_ref()
                .map(|s| path.split(s.as_str()).collect::<Vec<_>>())
                .unwrap_or_else(|| vec![path.as_str()]);
            let arr = self.get_array(split)?;

            if let Some(s) = focus.dataset_separator.as_ref() {
                gather_flat_arrays(&mut fields, &mut arrays, path, arr, s);
            } else {
                // TODO: nullable
                fields.push(Field::new(path, arr.data_type().clone(), false));
                arrays.push(arr);
            }
        }

        if arrays.len() == 1 {
            if let Some(s) = arrays[0].as_any().downcast_ref::<StructArray>() {
                return Ok(Self::from_struct(s));
            }
        }

        Ok(SchemaChunk {
            chunk: Chunk::new(arrays),
            schema: ArrowSchema {
                fields,
                metadata: Metadata::default(),
            },
        })
    }

    /// List keys for all nested struct arrays within this [SchemaChunk].
    pub fn tables(&self) -> Vec<Vec<String>> {
        let mut keys = vec![];
        for field in &self.schema.fields {
            collect_keys(&mut keys, field, true);
        }
        keys
    }

    /// Get a nested struct array within this [SchemaChunk].
    pub fn get_table<'a>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Result<SchemaChunk<ArrowSchema>, PathError> {
        let (key, arr) = self.get_key_array(path)?;

        match arr.as_any().downcast_ref::<StructArray>() {
            Some(arr) => Ok(SchemaChunk {
                chunk: Chunk::new(arr.values().to_vec()),
                schema: ArrowSchema {
                    fields: arr.fields().to_vec(),
                    metadata: Metadata::default(),
                },
            }),
            None => Err(PathError::NotStruct(
                key.into_iter().map(|s| s.to_string()).collect(),
            )),
        }
    }

    /// List keys for all nested arrow non-struct arrays within this [SchemaChunk].
    pub fn arrays(&self) -> Vec<Vec<String>> {
        let mut keys = vec![];
        for field in &self.schema.fields {
            collect_keys(&mut keys, field, false);
        }
        keys
    }

    /// Get a nested non-struct array within this [SchemaChunk].
    pub fn get_array<'a>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Result<Box<dyn Array>, PathError> {
        Ok(self.get_key_array(path)?.1)
    }

    fn get_key_array<'a>(
        &self,
        path: impl IntoIterator<Item = &'a str>,
    ) -> Result<(Vec<&'a str>, Box<dyn Array>), PathError> {
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

        Ok((current_path, current))
    }
}

fn gather_flat_arrays(
    fields: &mut Vec<Field>,
    arrays: &mut Vec<Box<dyn Array>>,
    key: &str,
    arr: Box<dyn Array>,
    separator: &str,
) {
    let mut path = vec![key.to_string()];
    let mut stack = match arr.as_any().downcast_ref::<StructArray>() {
        Some(s) => {
            vec![s.fields().iter().zip(s.values().iter())]
        }
        None => {
            // TODO nullable
            fields.push(Field::new(key.to_string(), arr.data_type().clone(), false));
            arrays.push(arr.clone());
            vec![]
        }
    };

    while let Some(top) = stack.last_mut() {
        match top.next() {
            Some((field, arr)) => match arr.as_any().downcast_ref::<StructArray>() {
                Some(s) => {
                    path.push(field.name.clone());
                    stack.push(s.fields().iter().zip(s.values().iter()));
                }
                _ => {
                    let mut name = path.as_slice().join(separator);
                    name.push_str(separator);
                    name.push_str(&field.name);

                    fields.push(Field::new(
                        name,
                        field.data_type().clone(),
                        field.is_nullable,
                    ));
                    arrays.push(arr.clone());
                }
            },
            None => {
                stack.pop();
                path.pop();
            }
        }
    }
}

fn collect_keys(keys: &mut Vec<Vec<String>>, field: &Field, tables: bool) {
    let mut path = vec![field.name.clone()];
    let mut stack = match field.data_type() {
        DataType::Struct(fields) => {
            if tables {
                keys.push(path.clone());
            }
            vec![fields.iter()]
        }
        _ => {
            if !tables {
                keys.push(path.clone());
            }
            vec![]
        }
    };

    while let Some(top) = stack.last_mut() {
        match top.next() {
            Some(field) => match field.data_type() {
                DataType::Struct(fields) => {
                    path.push(field.name.clone());
                    stack.push(fields.iter());
                    if tables {
                        keys.push(path.clone());
                    }
                }
                _ => {
                    if !tables {
                        keys.push(
                            path.iter()
                                .chain(std::iter::once(&field.name))
                                .cloned()
                                .collect(),
                        );
                    }
                }
            },
            None => {
                stack.pop();
                path.pop();
            }
        }
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

    fn nested_chunk() -> (
        SchemaChunk<ArrowSchema>,
        (
            Box<dyn Array>,
            Box<dyn Array>,
            Box<dyn Array>,
            Box<dyn Array>,
        ),
    ) {
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

        (
            SchemaChunk {
                schema,
                chunk: Chunk::try_new(vec![time.clone().boxed(), child_struct.clone().boxed()])
                    .unwrap(),
            },
            (
                time.boxed(),
                child_struct.boxed(),
                grandchild_struct.boxed(),
                index.boxed(),
            ),
        )
    }

    #[test]
    fn test_get_array() {
        let (test, (time, child_struct, grandchild_struct, index)) = nested_chunk();

        assert_eq!(test.get_array(["time"]), Ok(time));
        assert_eq!(test.get_array(["child"]), Ok(child_struct));
        assert_eq!(
            test.get_array(["child", "grandchild"]),
            Ok(grandchild_struct)
        );
        assert_eq!(
            test.get_array(["child", "grandchild", "index"]),
            Ok(index.clone())
        );
        assert_eq!(
            test.get_table(["child"])
                .unwrap()
                .get_array(["grandchild", "index"]),
            Ok(index.clone())
        );
        assert_eq!(
            test.get_table(["child", "grandchild"])
                .unwrap()
                .get_array(["index"]),
            Ok(index)
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

    #[test]
    fn test_focus() {
        let (test, (time, child_struct, _grandchild_struct, _index)) = nested_chunk();

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["child".to_string()],
                dataset_separator: None
            })
            .unwrap(),
            SchemaChunk::from_struct(child_struct.as_any().downcast_ref().unwrap())
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string()],
                dataset_separator: None
            })
            .unwrap()
            .arrays(),
            vec![vec!["time"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string()],
                dataset_separator: None
            })
            .unwrap()
            .get_array(["time"])
            .unwrap(),
            time
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["child".to_string()],
                dataset_separator: Some(".".to_string())
            })
            .unwrap()
            .arrays(),
            vec![vec!["child.grandchild.index"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string(), "child".to_string()],
                dataset_separator: Some(".".to_string())
            })
            .unwrap()
            .arrays(),
            vec![vec!["time"], vec!["child.grandchild.index"]]
        );
    }

    #[test]
    fn test_nesting() {
        let (test, _) = nested_chunk();

        assert_eq!(
            test.tables(),
            vec![
                vec!["child".to_string()],
                vec!["child".to_string(), "grandchild".to_string()]
            ]
        );

        assert_eq!(
            test.arrays(),
            vec![
                vec!["time".to_string()],
                vec![
                    "child".to_string(),
                    "grandchild".to_string(),
                    "index".to_string()
                ]
            ]
        );

        let child = test.get_table(["child"]).unwrap();
        assert_eq!(child.tables(), vec![vec!["grandchild".to_string()]]);
        assert_eq!(
            child.arrays(),
            vec![vec!["grandchild".to_string(), "index".to_string()]]
        );

        let grandchild = child.get_table(["grandchild"]).unwrap();
        let empty: Vec<Vec<String>> = vec![];
        assert_eq!(grandchild.tables(), empty);
        assert_eq!(grandchild.arrays(), vec![vec!["index".to_string()]]);

        let grandchild = test.get_table(["child", "grandchild"]).unwrap();
        assert_eq!(grandchild.tables(), empty);
        assert_eq!(grandchild.arrays(), vec![vec!["index".to_string()]]);
    }

    #[test]
    fn test_to_from_bytes() {
        let (original, _) = nested_chunk();

        let test = SchemaChunk::from_bytes(&original.to_bytes().unwrap()).unwrap();

        assert_eq!(
            test.tables(),
            vec![
                vec!["child".to_string()],
                vec!["child".to_string(), "grandchild".to_string()]
            ]
        );
    }
}

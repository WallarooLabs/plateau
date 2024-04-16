use std::fmt;
use std::ops::Range;
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    io::Cursor,
};

use arrow2::{
    array::{Array, StructArray},
    chunk::Chunk,
    compute::concatenate::concatenate,
    datatypes::Field,
    io::ipc::{read, write},
};
use regex::Regex;
#[cfg(feature = "rweb")]
use rweb::{openapi::Entity, Schema};
use serde::{Deserialize, Serialize};

use arrow2::array::{FixedSizeListArray, ListArray, PrimitiveArray, Utf8Array};
use arrow2::compute::take::take;
use arrow2::datatypes::{DataType, Metadata};
pub use arrow2::{self, datatypes::Schema as ArrowSchema, error::Error as ArrowError};
use strum::{Display, EnumIter};
use thiserror::Error;
use utoipa::{IntoParams, ToSchema};

pub mod headers {
    pub static ITERATION_STATUS_HEADER: &str = "x-iteration-status";
    pub static MAX_REQUEST_SIZE_HEADER: &str = "x-max-request-size";
}

#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("unsupported type: {0}")]
    Unsupported(String),
    #[error("column {0} must be '{1}'")]
    BadColumn(usize, &'static str),
    #[error("column {0} type {1} != {2}")]
    InvalidColumnType(&'static str, &'static str, &'static str),
    #[error("could not encode 'message' to utf8")]
    FailedEncoding,
    #[error("array lengths do not match")]
    LengthMismatch,
    // this should really never happen...
    #[error("datatype does not match actual type")]
    TypeMismatch,
}

/// Estimate the size of a [Chunk]. The estimate does not include null bitmaps or extent buffers.
pub fn estimate_size(chunk: &SegmentChunk) -> Result<usize, ChunkError> {
    chunk
        .arrays()
        .iter()
        .map(|a| estimate_array_size(a.as_ref()))
        .sum()
}
pub fn estimate_array_size(arr: &dyn Array) -> Result<usize, ChunkError> {
    match arr.data_type() {
        DataType::UInt8 => Ok(arr.len()),
        DataType::UInt16 => Ok(arr.len() * 2),
        DataType::UInt32 => Ok(arr.len() * 4),
        DataType::UInt64 => Ok(arr.len() * 8),
        DataType::Int8 => Ok(arr.len()),
        DataType::Int16 => Ok(arr.len() * 2),
        DataType::Int32 => Ok(arr.len() * 4),
        DataType::Int64 => Ok(arr.len() * 8),
        // XXX - in future versions of arrow2
        // DataType::Int128 => Ok(arr.len() * 16),
        // DataType::Int256 => Ok(arr.len() * 32),
        DataType::Float16 => Ok(arr.len() * 2),
        DataType::Float32 => Ok(arr.len() * 4),
        DataType::Float64 => Ok(arr.len() * 8),
        DataType::Timestamp(_, _) => Ok(arr.len() * 8),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .ok_or(ChunkError::TypeMismatch)
            .map(|arr| arr.values().len()),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<Utf8Array<i64>>()
            .ok_or(ChunkError::TypeMismatch)
            .map(|arr| arr.values().len()),
        DataType::List(_) => arr
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::FixedSizeList(_, _) => arr
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::LargeList(_) => arr
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| estimate_array_size(arr.values().as_ref())),
        DataType::Struct(_) => arr
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(ChunkError::TypeMismatch)
            .and_then(|arr| {
                arr.values()
                    .iter()
                    .map(|inner| estimate_array_size(inner.as_ref()))
                    .sum()
            }),
        t => Err(ChunkError::Unsupported(format!("{:?}", t))),
    }
}

pub fn is_variable_len(data_type: &DataType) -> bool {
    match data_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _) => false,
        // some types like Struct or FixedSizeList could theoretically encapsulate all fixed-size types,
        // but we're just going to call them variable for now to simplify
        _ => true,
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct InsertQuery {
    /// RFC3339 timestamp associated with inserted records
    #[cfg_attr(feature = "clap", arg(short, long))]
    pub time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Insert {
    pub records: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Inserted {
    pub span: Span,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Partitions {
    pub partitions: HashMap<String, Span>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl From<Span> for Range<usize> {
    fn from(value: Span) -> Self {
        value.start..value.end
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Records {
    pub span: Option<Span>,
    pub status: RecordStatus,
    pub records: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, IntoParams, ToSchema)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct DataFocus {
    /// Data sets to return for this query.
    #[serde(default)]
    #[cfg_attr(feature = "clap", arg(default_value = "*", long))]
    pub dataset: Vec<String>,

    /// List of datasets to exclude.
    #[serde(default, rename = "dataset.exclude")]
    #[cfg_attr(feature = "clap", arg(skip))]
    pub exclude: Vec<String>,

    /// When specified, flattens the output data sets into a single table. Uses
    /// the given separator to join nested keys.
    #[serde(default, rename = "dataset.separator")]
    #[cfg_attr(feature = "clap", arg(skip))]
    pub dataset_separator: Option<String>,
}

impl DataFocus {
    pub fn is_some(&self) -> bool {
        !self.dataset.is_empty() || self.dataset_separator.is_some()
    }
}

#[derive(Debug, Default, Deserialize, Serialize, IntoParams)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct RecordQuery {
    /// Start position
    pub start: usize,
    /// Number of records to return (defaults to 1000, maximum of 10000)
    #[cfg_attr(feature = "clap", arg(short, long))]
    pub page_size: Option<usize>,
    /// RFC3339 start time for records (defaults to earliest record)
    #[serde(rename = "time.start")]
    pub start_time: Option<String>,
    /// RFC3339 end time for records (required if start time exists)
    #[serde(rename = "time.end")]
    pub end_time: Option<String>,
    #[serde(flatten)]
    #[cfg_attr(feature = "clap", command(flatten))]
    pub data_focus: DataFocus,
    #[serde(default)]
    #[cfg_attr(feature = "clap", arg(skip))]
    pub partition_filter: PartitionFilter,
}

/// Status of the record request query.
#[derive(Debug, Clone, Display, Serialize, Deserialize, PartialEq, Eq, EnumIter, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
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

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    utoipa::ToSchema,
)]
#[cfg_attr(feature = "rweb", derive(Schema))]
#[serde(rename_all = "lowercase", try_from = "&str")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum TopicIterationOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct TopicIterationQuery {
    /// Number of records to return (defaults to 1000, maximum of 10000)
    #[cfg_attr(feature = "clap", arg(short, long))]
    pub page_size: Option<usize>,
    /// Use reverse iteration to work backwards through the topic
    #[cfg_attr(feature = "clap", arg(short, long))]
    pub order: Option<TopicIterationOrder>,
    /// RFC3339 start time for records (defaults to earliest record)
    #[serde(default, rename = "time.start")]
    pub start_time: Option<String>,
    /// RFC3339 end time for records (required if start time exists)
    #[serde(default, rename = "time.end")]
    pub end_time: Option<String>,
    #[serde(flatten)]
    #[cfg_attr(feature = "clap", clap(flatten))]
    pub data_focus: DataFocus,
    #[serde(default)]
    #[cfg_attr(feature = "clap", clap(skip))]
    pub partition_filter: PartitionFilter,
}

/// An optional filter that can limit the partitions data is taken from.
/// A [`None`] value indicates no filter and that all partitions should be used.
/// PartitionSelector is always specified as a string, but can optionally
/// begin with "regex:" to signify that any partition matching the following string can be converted.
pub type PartitionFilter = Option<Vec<PartitionSelector>>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(from = "String", into = "String")]
pub enum PartitionSelector {
    /// The exact name of a partition.
    String(String),
    /// A string beginning with `regex:`. The suffix will be matched against partition names.
    Regex(Regex),
}

#[cfg(feature = "rweb")]
impl Entity for PartitionSelector {
    fn type_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("partition_filter")
    }

    fn describe(
        _comp_d: &mut rweb::openapi::ComponentDescriptor,
    ) -> rweb::openapi::ComponentOrInlineSchema {
        rweb::openapi::ComponentOrInlineSchema::Inline(rweb::openapi::Schema {
            schema_type: Some(rweb::openapi::Type::String),
            nullable: Some(true),
            ..Default::default()
        })
    }
}

impl fmt::Display for PartitionSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(text) => text.fmt(f),
            Self::Regex(regex) => format_args!("regex:{regex}").fmt(f),
        }
    }
}

impl From<PartitionSelector> for String {
    fn from(value: PartitionSelector) -> Self {
        value.to_string()
    }
}

impl<T> From<T> for PartitionSelector
where
    T: AsRef<str>,
{
    fn from(text: T) -> Self {
        let build_regex = |pattern| {
            regex::RegexBuilder::new(pattern)
                .size_limit(2 << 12)
                .build()
                .ok()
        };
        text.as_ref()
            .strip_prefix("regex:")
            .and_then(build_regex)
            .map_or_else(|| Self::String(text.as_ref().to_string()), Self::Regex)
    }
}

impl PartitionSelector {
    pub fn matches(&self, name: &str) -> bool {
        match self {
            PartitionSelector::String(s) => s == name,
            PartitionSelector::Regex(r) => r.is_match(name),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct TopicIterationReply {
    pub records: Vec<String>,
    #[serde(flatten)]
    pub status: TopicIterationStatus,
}

pub type TopicIterator = HashMap<String, usize>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct TopicIterationStatus {
    pub status: RecordStatus,
    pub next: TopicIterator,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Topics {
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct Topic {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "rweb", derive(Schema))]
pub struct ErrorMessage {
    pub message: String,
    pub code: u16,
}

pub const CONTENT_TYPE_ARROW: &str = "application/vnd.apache.arrow.file";
pub const CONTENT_TYPE_JSON: &str = "application/json";

pub type SegmentChunk = Chunk<Box<dyn Array>>;

/// A [SegmentChunk] packaged with its associated [ArrowSchema].
#[derive(Debug, Clone, PartialEq, ToSchema)]
#[aliases(ArrowSchemaChunk = SchemaChunk<ArrowSchema>)]
pub struct SchemaChunk<S: Borrow<ArrowSchema> + Clone + PartialEq> {
    pub schema: S,
    pub chunk: SegmentChunk,
}
impl<S: Borrow<ArrowSchema> + Clone + PartialEq> SchemaChunk<S> {
    pub fn reverse_inner(&mut self) {
        let arrays = self.chunk.arrays().to_vec();
        if !arrays.is_empty() {
            // build a simple descending index
            let l = arrays[0].len() as u64;
            let idx = PrimitiveArray::from_vec((0..l).rev().collect());

            // apply it to the arrays and replace the underlying chunk
            let reversed: Vec<_> = arrays.iter().map(|a| take(&**a, &idx).unwrap()).collect();
            let rev_segment = SegmentChunk::try_new(reversed).unwrap();
            self.chunk = rev_segment;
        }
    }

    pub fn extend(&mut self, other: Self) -> anyhow::Result<()> {
        if self.schema != other.schema {
            Err(anyhow::anyhow!("schemas do not match"))
        } else {
            let arrays = self
                .chunk
                .arrays()
                .iter()
                .zip(other.chunk.arrays().iter())
                .map(|(a, b)| concatenate(&[a.as_ref(), b.as_ref()]))
                .collect::<arrow2::error::Result<Vec<_>>>()?;
            self.chunk = Chunk::new(arrays);
            Ok(())
        }
    }

    pub fn len(&self) -> usize {
        self.chunk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunk.is_empty()
    }

    /// Checks for [DataType::Null] on any field of the schema, including
    /// nested fields
    pub fn contains_null_type(&self) -> bool {
        contains_null_type(self.schema.borrow())
    }
}

/// A [Vec] of [SegmentChunk], all of the same [ArrowSchema].
#[derive(Debug, Clone, PartialEq)]
pub struct MultiChunk {
    pub schema: ArrowSchema,
    pub chunks: Vec<SegmentChunk>,
}

impl MultiChunk {
    pub fn extend(&mut self, other: Self) -> anyhow::Result<()> {
        anyhow::ensure!(self.schema == other.schema, "schemas do not match");
        self.chunks.extend(other.chunks);
        Ok(())
    }

    pub fn to_schemachunks(self) -> Vec<SchemaChunk<ArrowSchema>> {
        self.chunks
            .into_iter()
            .map(|chunk| SchemaChunk {
                schema: self.schema.clone(),
                chunk,
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

        let paths = focus.dataset.iter().flat_map(|path| {
            if path == "*" {
                self.schema.fields.iter().map(|f| &f.name).collect()
            } else {
                vec![path]
            }
        });

        let exclude: HashSet<&String> = focus.exclude.iter().collect();

        for path in paths {
            let split = focus
                .dataset_separator
                .as_ref()
                .map(|s| path.split(s.as_str()).collect::<Vec<_>>())
                .unwrap_or_else(|| vec![path.as_str()]);
            let arr = self.get_array(split)?;

            if !exclude.contains(path) {
                if let Some(s) = focus.dataset_separator.as_ref() {
                    gather_flat_arrays(&mut fields, &mut arrays, path, arr, s, &exclude);
                } else {
                    // TODO: nullable
                    fields.push(Field::new(path, arr.data_type().clone(), false));
                    arrays.push(arr);
                }
            }
        }

        if arrays.len() == 1 {
            if let Some(s) = arrays[0].as_any().downcast_ref::<StructArray>() {
                let mut data = Self::from_struct(s);
                data.schema.metadata = self.schema.metadata.clone();
                return Ok(data);
            }
        }

        Ok(SchemaChunk {
            chunk: Chunk::new(arrays),
            schema: ArrowSchema {
                fields,
                metadata: self.schema.metadata.clone(),
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

fn contains_null_type(schema: &arrow2::datatypes::Schema) -> bool {
    schema
        .fields
        .iter()
        .flat_map(all_datatypes)
        .any(|d| d == DataType::Null)
}

fn all_datatypes(field: &Field) -> Vec<DataType> {
    match &field.data_type {
        DataType::Struct(fields) | DataType::Union(fields, _, _) => {
            fields.iter().flat_map(all_datatypes).collect()
        }
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => all_datatypes(f),
        DataType::Dictionary(_, f, _) => vec![*f.clone()],
        data_type => vec![data_type.clone()],
    }
}

fn gather_flat_arrays(
    fields: &mut Vec<Field>,
    arrays: &mut Vec<Box<dyn Array>>,
    key: &str,
    arr: Box<dyn Array>,
    separator: &str,
    exclude: &HashSet<&String>,
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

    let mut name: String = path.as_slice().join(separator);
    while let Some(top) = stack.last_mut() {
        match top.next() {
            Some((field, arr)) => match arr.as_any().downcast_ref::<StructArray>() {
                Some(s) => {
                    path.push(field.name.clone());
                    let next_name = path.as_slice().join(separator);
                    if !exclude.contains(&next_name) {
                        name = next_name;
                        stack.push(s.fields().iter().zip(s.values().iter()));
                    } else {
                        path.pop();
                    }
                }
                _ => {
                    let mut field_name = name.clone();
                    field_name.push_str(separator);
                    field_name.push_str(&field.name);

                    fields.push(Field::new(
                        field_name,
                        field.data_type().clone(),
                        field.is_nullable,
                    ));
                    arrays.push(arr.clone());
                }
            },
            None => {
                stack.pop();
                path.pop();
                name = path.as_slice().join(separator);
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

#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct PartitionId {
    pub topic: String,
    pub partition: String,
}

impl PartitionId {
    pub fn new(topic: &str, partition: &str) -> Self {
        PartitionId {
            topic: String::from(topic),
            partition: String::from(partition),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> &str {
        &self.partition
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::datatypes::Schema;

    #[allow(clippy::type_complexity)]
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
            DataType::Struct(vec![
                Field::new("grandchild", grandchild_struct.data_type().clone(), false),
                Field::new("index", index.data_type().clone(), false),
            ]),
            vec![grandchild_struct.clone().boxed(), index.clone().boxed()],
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
    fn test_null_detection() {
        let mut schema = ArrowSchema::default();
        schema.fields.push(Field::new(
            "column",
            DataType::List(Box::new(Field::new("inner_field", DataType::Null, false))),
            true,
        ));
        assert!(super::contains_null_type(&schema));

        let mut schema = ArrowSchema::default();
        schema
            .fields
            .push(Field::new("column", DataType::Float64, false));
        assert!(!super::contains_null_type(&schema));

        let mut schema = ArrowSchema::default();
        schema
            .fields
            .push(Field::new("column", DataType::Float64, true));
        assert!(!super::contains_null_type(&schema));
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
                ..DataFocus::default()
            })
            .unwrap(),
            SchemaChunk::from_struct(child_struct.as_any().downcast_ref().unwrap())
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string()],
                ..DataFocus::default()
            })
            .unwrap()
            .arrays(),
            vec![vec!["time"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string()],
                ..DataFocus::default()
            })
            .unwrap()
            .get_array(["time"])
            .unwrap(),
            time
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["child".to_string()],
                dataset_separator: Some(".".to_string()),
                ..DataFocus::default()
            })
            .unwrap()
            .arrays(),
            vec![vec!["child.grandchild.index"], vec!["child.index"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["time".to_string(), "child".to_string()],
                dataset_separator: Some(".".to_string()),
                ..DataFocus::default()
            })
            .unwrap()
            .arrays(),
            vec![
                vec!["time"],
                vec!["child.grandchild.index"],
                vec!["child.index"]
            ]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["*".to_string()],
                dataset_separator: Some(".".to_string()),
                ..DataFocus::default()
            })
            .unwrap()
            .arrays(),
            vec![
                vec!["time"],
                vec!["child.grandchild.index"],
                vec!["child.index"]
            ]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["*".to_string()],
                dataset_separator: Some(".".to_string()),
                exclude: vec!["time".to_string()]
            })
            .unwrap()
            .arrays(),
            vec![vec!["child.grandchild.index"], vec!["child.index"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["*".to_string()],
                dataset_separator: Some(".".to_string()),
                exclude: vec!["child.grandchild".to_string()]
            })
            .unwrap()
            .arrays(),
            vec![vec!["time"], vec!["child.index"]]
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["*".to_string()],
                dataset_separator: Some(".".to_string()),
                exclude: vec!["child".to_string()]
            })
            .unwrap()
            .arrays(),
            vec![vec!["time"]]
        );
    }

    #[test]
    fn test_focus_metadata() {
        let (mut test, (_time, _child_struct, _grandchild_struct, _index)) = nested_chunk();

        test.schema
            .metadata
            .insert("testing".to_string(), "123".to_string());

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["child".to_string()],
                ..DataFocus::default()
            })
            .unwrap()
            .schema
            .metadata,
            test.schema.metadata
        );

        assert_eq!(
            test.focus(&DataFocus {
                dataset: vec!["child".to_string(), "time".to_string()],
                ..DataFocus::default()
            })
            .unwrap()
            .schema
            .metadata,
            test.schema.metadata
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
                ],
                vec!["child".to_string(), "index".to_string()]
            ]
        );

        let child = test.get_table(["child"]).unwrap();
        assert_eq!(child.tables(), vec![vec!["grandchild".to_string()]]);
        assert_eq!(
            child.arrays(),
            vec![
                vec!["grandchild".to_string(), "index".to_string()],
                vec!["index".to_string()]
            ]
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

    #[test]
    fn partition_selector_string() {
        let ps = PartitionSelector::from("blah");
        assert!(matches!(ps, PartitionSelector::String(text) if text == "blah"));
    }

    #[test]
    fn partition_selector_regex() {
        let ps = PartitionSelector::from("regex:partition-.*");
        assert!(
            matches!(ps, PartitionSelector::Regex(regex) if regex.to_string() == "partition-.*")
        );
    }

    #[test]
    fn partition_selector_invalid_regex() {
        let ps = PartitionSelector::from(r"regex:\p{Unknown}");
        assert!(matches!(ps, PartitionSelector::String(text) if text == r"regex:\p{Unknown}"));
    }

    #[test]
    fn partition_selector_string_display() {
        let ps = PartitionSelector::from("blah");
        assert!(matches!(ps, PartitionSelector::String(_)));
        assert_eq!(ps.to_string(), "blah");
    }

    #[test]
    fn partition_selector_regex_display() {
        let ps = PartitionSelector::from(r"regex:\p{Greek}");
        assert!(matches!(ps, PartitionSelector::Regex(_)));
        assert_eq!(ps.to_string(), r"regex:\p{Greek}");
    }
}

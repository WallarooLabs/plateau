use crate::arrow2::datatypes::Metadata;
use crate::arrow2::io::ipc::{read, write};
use crate::arrow2::io::json as arrow_json;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rweb::{
    body,
    filters::BoxedFilter,
    header,
    http::StatusCode,
    hyper::{self, body::Body},
    openapi::{ComponentDescriptor, ComponentOrInlineSchema, Entity},
    reject, Filter, FromRequest,
};
use rweb::{query, Reply};
use std::borrow::Cow;
use std::io::{Cursor, Write};

use plateau_transport::{
    DataFocus, Insert, InsertQuery, SchemaChunk, SegmentChunk, CONTENT_TYPE_ARROW,
};

use crate::{
    chunk::{new_schema_chunk, LegacyRecords, Record, Schema},
    http::error::ErrorReply,
    limit::LimitedBatch,
};

const CONTENT_TYPE_PANDAS_RECORD: &str = "application/json; format=pandas-records";

pub(crate) struct SchemaChunkRequest(pub(crate) SchemaChunk<Schema>);

impl FromRequest for SchemaChunkRequest {
    type Filter = BoxedFilter<(SchemaChunkRequest,)>;

    fn new() -> Self::Filter {
        let limit = body::content_length_limit(1024 * 1024);

        let time = query().and_then(|query: InsertQuery| async move {
            if let Some(s) = query.time {
                if let Ok(time) = DateTime::parse_from_rfc3339(&s) {
                    Ok(time.with_timezone(&Utc))
                } else {
                    Err(reject::custom(ErrorReply::InvalidQuery))
                }
            } else {
                Ok(Utc::now())
            }
        });

        let json =
            limit
                .and(body::json())
                .and(time)
                .and_then(|insert: Insert, time: _| async move {
                    let records: Vec<_> = insert
                        .records
                        .into_iter()
                        .map(|m| Record {
                            time,
                            message: m.into_bytes(),
                        })
                        .collect();

                    SchemaChunk::try_from(LegacyRecords(records))
                        .map_err(|_| reject::custom(ErrorReply::BadEncoding))
                        .map(SchemaChunkRequest)
                });

        let content_type = header::<String>("content-type").and_then(|content_type| async move {
            if content_type == CONTENT_TYPE_ARROW {
                Ok(())
            } else {
                Err(reject::custom(ErrorReply::CannotAccept(content_type)))
            }
        });

        let chunk =
            limit
                .and(content_type)
                .and(body::bytes())
                .and_then(|_, bytes: Bytes| async move {
                    let mut cursor = Cursor::new(bytes);
                    let metadata = read::read_file_metadata(&mut cursor)
                        .map_err(|e| reject::custom(ErrorReply::Arrow(e)))?;
                    let schema = metadata.schema.clone();
                    let mut reader = read::FileReader::new(cursor, metadata, None, None);
                    if let Some(chunk) = reader.next() {
                        let chunk = chunk.map_err(|e| reject::custom(ErrorReply::Arrow(e)))?;
                        Ok(SchemaChunkRequest(
                            new_schema_chunk(schema, chunk)
                                .map_err(|e| reject::custom(ErrorReply::Chunk(e)))?,
                        ))
                    } else {
                        Err(reject::custom(ErrorReply::EmptyBody))
                    }
                });

        json.or(chunk).unify().boxed()
    }
}

// TODO: this seems unlikely to be correct
impl Entity for SchemaChunkRequest {
    fn type_name() -> Cow<'static, str> {
        Cow::from("SchemaChunk")
    }

    fn describe(_: &mut ComponentDescriptor) -> ComponentOrInlineSchema {
        ComponentOrInlineSchema::Component {
            name: Cow::from("SchemaChunk"),
        }
    }
}

pub(crate) fn to_reply(
    accept: &str,
    batch: LimitedBatch,
    focus: DataFocus,
) -> Result<Box<dyn Reply>, ErrorReply> {
    let mut iter = batch.chunks.into_iter();
    // sigh. this would probably be much easier to implement if/when we
    // refactor SchemaChunk so it holds a Vec of Chunk like LimitedBatch
    // as it is we regenerate the schema and throw it away for each chunk,
    // which can't be efficient.
    let mut schema = batch.schema.unwrap();
    let (first_chunk, mut schema) = if let Some(chunk) = iter.next() {
        let mut chunk = SegmentChunk::from(chunk);
        if focus.is_some() {
            let full = SchemaChunk { schema, chunk };
            let result = full.focus(&focus).map_err(ErrorReply::Path)?;
            schema = result.schema;
            chunk = result.chunk;
        }
        (chunk, schema)
    } else {
        return Err(ErrorReply::EmptyBody);
    };

    let schema_copy = schema.clone();
    let iter = std::iter::once(Ok(first_chunk)).chain(iter.map(|chunk| {
        let chunk = SegmentChunk::from(chunk);

        if focus.is_some() {
            let full = SchemaChunk {
                schema: std::mem::replace(
                    &mut schema,
                    Schema {
                        fields: vec![],
                        metadata: Metadata::default(),
                    },
                ),
                chunk,
            };
            match full.focus(&focus) {
                Ok(result) => {
                    schema = result.schema;
                    Ok(result.chunk)
                }
                Err(e) => Err(ErrorReply::Path(e)),
            }
        } else {
            Ok(chunk)
        }
    }));

    match accept {
        CONTENT_TYPE_ARROW => {
            let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
            let options = write::WriteOptions { compression: None };

            let mut writer = write::FileWriter::new(bytes, schema_copy, None, options);

            writer.start().map_err(ErrorReply::Arrow)?;
            for chunk in iter {
                writer.write(&chunk?, None).map_err(ErrorReply::Arrow)?;
            }
            writer.finish().map_err(ErrorReply::Arrow)?;

            let bytes = writer.into_inner().into_inner();
            Ok(Box::new(
                hyper::Response::builder()
                    .header("Content-Type", CONTENT_TYPE_ARROW)
                    .status(StatusCode::OK)
                    .body::<Body>(bytes.into())
                    .unwrap(),
            ))
        }
        CONTENT_TYPE_PANDAS_RECORD => {
            // ugh. super ugly byte hacking to work around upstream not
            // supporting multiple chunks.
            let mut bytes = vec![];
            let mut first = true;
            write!(&mut bytes, "[").map_err(|_| ErrorReply::Unknown)?;
            for chunk in iter {
                if !first {
                    write!(&mut bytes, ",").map_err(|_| ErrorReply::Unknown)?;
                } else {
                    first = false;
                }
                let mut buf = vec![];
                let chunk = chunk?;
                let mut serializer =
                    arrow_json::write::RecordSerializer::new(schema_copy.clone(), &chunk, vec![]);
                arrow_json::write::write(&mut buf, &mut serializer).map_err(ErrorReply::Arrow)?;
                bytes.extend(&buf[1..buf.len() - 1]);
            }
            write!(&mut bytes, "]").map_err(|_| ErrorReply::Unknown)?;

            Ok(Box::new(
                hyper::Response::builder()
                    .header("Content-Type", CONTENT_TYPE_PANDAS_RECORD)
                    .status(StatusCode::OK)
                    .body::<Body>(bytes.into())
                    .unwrap(),
            ))
        }
        other => Err(ErrorReply::CannotEmit(other.to_string())),
    }
}

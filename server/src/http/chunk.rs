use arrow2::error::Error as ArrowError;
use arrow2::io::ipc::{read, write};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::data_type::ByteArray;
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
use std::io::Cursor;

use plateau_transport::{Insert, InsertQuery, SchemaChunk, SegmentChunk, CONTENT_TYPE_ARROW};

use crate::{
    chunk::{new_schema_chunk, LegacyRecords, Record, Schema},
    http::error::ErrorReply,
    limit::LimitedBatch,
};

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
                            message: ByteArray::from(m.as_str()),
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

pub(crate) fn to_reply(batch: LimitedBatch) -> Result<Box<dyn Reply>, ArrowError> {
    let bytes: Cursor<Vec<u8>> = Cursor::new(vec![]);
    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(bytes, batch.schema.unwrap(), None, options);

    writer.start()?;
    for chunk in batch.chunks {
        writer.write(&SegmentChunk::from(chunk), None)?;
    }
    writer.finish()?;

    let bytes = writer.into_inner().into_inner();
    Ok(Box::new(
        hyper::Response::builder()
            .header("Content-Type", CONTENT_TYPE_ARROW)
            .status(StatusCode::OK)
            .body::<Body>(bytes.into())
            .unwrap(),
    ))
}

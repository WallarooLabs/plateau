use axum::{
    async_trait,
    body::{boxed, Full, HttpBody},
    extract::{
        rejection::{BytesRejection, FailedToBufferBody},
        FromRef, FromRequest,
    },
    headers::ContentType,
    http::{header::CONTENT_TYPE, Request, StatusCode},
    response::Response,
    BoxError, RequestExt as _,
};

use bytes::Bytes;
use std::io::{Cursor, Write};
use std::sync::Arc;

use crate::transport::arrow_ipc::reader::FileReader;
use crate::transport::arrow_ipc::writer::FileWriter;
use crate::transport::arrow_json::{writer::JsonArray, WriterBuilder};
use crate::transport::arrow_schema::{ArrowError, Schema as ArrowSchema};

use crate::transport::{
    headers::ITERATION_STATUS_HEADER, DataFocus, SchemaChunk, SegmentChunk, CONTENT_TYPE_ARROW,
    CONTENT_TYPE_JSON,
};

use crate::data::{
    chunk::{new_schema_chunk, Schema},
    limit::LimitedBatch,
};
use crate::{http::error::ErrorReply, Config};

const CONTENT_TYPE_PANDAS_RECORD: &str = "application/json; format=pandas-records";

pub(crate) struct SchemaChunkRequest(pub(crate) SchemaChunk<Schema>);

#[async_trait]
impl<S, B> FromRequest<S, B> for SchemaChunkRequest
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    Config: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ErrorReply;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let config = Config::from_ref(state);
        let max_append_bytes = config.http.max_append_bytes;

        let content_type = req
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .ok_or(ErrorReply::CannotAccept(
                ContentType::octet_stream().to_string(),
            ))?;

        if content_type == CONTENT_TYPE_ARROW {
            let bytes = match req.with_limited_body() {
                Ok(req) => req.extract::<Bytes, _>(),
                Err(req) => req.extract::<Bytes, _>(),
            }
            .await
            .map_err(|e| {
                if let BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_)) =
                    e
                {
                    return ErrorReply::PayloadTooLarge(max_append_bytes);
                }

                ErrorReply::Arrow(ArrowError::from_external_error(Box::new(e)))
            })?;

            deserialize_request(bytes).await
        } else {
            Err(ErrorReply::CannotAccept(content_type.to_string()))
        }
    }
}

pub(crate) async fn deserialize_request(bytes: Bytes) -> Result<SchemaChunkRequest, ErrorReply> {
    let cursor = Cursor::new(bytes);
    let mut reader = FileReader::try_new(cursor, None).map_err(ErrorReply::Arrow)?;
    let schema = Arc::unwrap_or_clone(reader.schema());
    if let Some(chunk) = reader.next() {
        let mut chunk = new_schema_chunk(schema.clone(), chunk.map_err(ErrorReply::Arrow)?)
            .map_err(ErrorReply::Chunk)?;
        for next_chunk in reader {
            chunk
                .extend(
                    new_schema_chunk(schema.clone(), next_chunk.map_err(ErrorReply::Arrow)?)
                        .map_err(ErrorReply::Chunk)?,
                )
                .map_err(|_| ErrorReply::InvalidSchema)?;
        }
        Ok(SchemaChunkRequest(chunk))
    } else {
        Err(ErrorReply::EmptyBody)
    }
}

pub(crate) fn to_reply(
    accept: Option<&str>,
    batch: LimitedBatch,
    focus: DataFocus,
) -> Result<Response, ErrorReply> {
    let mut iter = batch.chunks.into_iter();
    // TBD - review this in the context of arrow-rs, it may have more efficient functionality for
    // achieving what we need.
    // sigh. this would probably be much easier to implement if/when we
    // refactor SchemaChunk so it holds a Vec of Chunk like LimitedBatch
    // as it is we regenerate the schema and throw it away for each chunk,
    // which can't be efficient.
    let (first_chunk, batch_schema, focused_schema) = if let Some(chunk) = iter.next() {
        let batch_schema = batch.schema.unwrap();
        let mut chunk = SegmentChunk::from(chunk);
        let focused_schema = if focus.is_some() {
            let full = SchemaChunk {
                schema: Arc::new(batch_schema.clone()),
                chunk,
            };
            let result = full.focus(&focus).map_err(ErrorReply::Path)?;
            chunk = result.chunk;
            result.schema
        } else {
            Arc::new(batch_schema.clone())
        };
        (chunk, batch_schema, focused_schema)
    } else {
        return match accept {
            Some(CONTENT_TYPE_ARROW) => {
                let mut bytes = Vec::new();

                let schema = ArrowSchema::empty();

                let mut writer = FileWriter::try_new(&mut bytes, &schema)
                    .map_err(|e| ErrorReply::Arrow(ArrowError::from_external_error(e.into())))?;

                writer.finish().map_err(ErrorReply::Arrow)?;

                Response::builder()
                    .header("Content-Type", CONTENT_TYPE_ARROW)
                    .status(StatusCode::OK)
                    .body(boxed(Full::new(Bytes::from(bytes))))
                    .map_err(|_| ErrorReply::Unknown)
            }
            None | Some("*/*") | Some(CONTENT_TYPE_JSON) | Some(CONTENT_TYPE_PANDAS_RECORD) => {
                Response::builder()
                    .header("Content-Type", CONTENT_TYPE_PANDAS_RECORD)
                    .status(StatusCode::OK)
                    .body(boxed(Full::new(Bytes::from("[]"))))
                    .map_err(|_| ErrorReply::Unknown)
            }
            Some(other) => Err(ErrorReply::CannotEmit(other.to_string())),
        };
    };

    let iter = std::iter::once(Ok(first_chunk)).chain(iter.map(|chunk| {
        let chunk = SegmentChunk::from(chunk);

        if focus.is_some() {
            let full = SchemaChunk {
                schema: Arc::new(batch_schema.clone()),
                chunk,
            };
            full.focus(&focus)
                .map(|result| result.chunk)
                .map_err(ErrorReply::Path)
        } else {
            Ok(chunk)
        }
    }));

    match accept {
        Some(CONTENT_TYPE_ARROW) => {
            let mut bytes = Vec::new();

            let mut writer = FileWriter::try_new(&mut bytes, &focused_schema)
                .map_err(|e| ErrorReply::Arrow(ArrowError::from_external_error(e.into())))?;

            for chunk in iter {
                writer.write(&chunk?).map_err(ErrorReply::Arrow)?;
            }
            writer.finish().map_err(ErrorReply::Arrow)?;

            Response::builder()
                .header("Content-Type", CONTENT_TYPE_ARROW)
                .status(StatusCode::OK)
                .header(
                    ITERATION_STATUS_HEADER,
                    focused_schema
                        .metadata
                        .get("status")
                        .unwrap_or(&"{}".to_string()),
                )
                .body(boxed(Full::new(Bytes::from(bytes))))
                .map_err(|_| ErrorReply::Unknown)
        }
        None | Some("*/*") | Some(CONTENT_TYPE_JSON) | Some(CONTENT_TYPE_PANDAS_RECORD) => {
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
                let chunk = chunk?;

                let builder = WriterBuilder::new().with_explicit_nulls(true);
                let mut writer = builder.build::<_, JsonArray>(Cursor::new(Vec::new()));
                writer.write_batches(&[&chunk]).map_err(ErrorReply::Arrow)?;
                writer.finish().map_err(ErrorReply::Arrow)?;

                let buf = writer.into_inner().into_inner();
                bytes.extend(&buf[1..buf.len().saturating_sub(1)]);
            }
            write!(&mut bytes, "]").map_err(|_| ErrorReply::Unknown)?;

            Response::builder()
                .header(CONTENT_TYPE, CONTENT_TYPE_PANDAS_RECORD)
                .header(
                    ITERATION_STATUS_HEADER,
                    focused_schema
                        .metadata
                        .get("status")
                        .unwrap_or(&"{}".to_string()),
                )
                .status(StatusCode::OK)
                .body(boxed(Full::new(Bytes::from(bytes))))
                .map_err(|_| ErrorReply::Unknown)
        }
        Some(other) => Err(ErrorReply::CannotEmit(other.to_string())),
    }
}

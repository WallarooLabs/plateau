use arrow2::error::Error as ArrowError;
use rweb::*;
use serde::Serialize;
use std::convert::Infallible;
use warp::http::StatusCode;
use warp::reject::Reject;

use crate::chunk::ChunkError;

#[derive(Debug)]
pub(crate) enum ErrorReply {
    Arrow(ArrowError),
    Chunk(ChunkError),
    EmptyBody,
    WriterBusy,
    InvalidQuery,
    InvalidSchema,
    BadEncoding,
    CannotAccept(String),
    CannotEmit(String),
    NoHeartbeat,
    Unknown,
}
impl Reject for ErrorReply {}

#[derive(Schema, Serialize)]
struct ErrorMessage {
    message: String,
    code: u16,
}

pub(crate) async fn emit_error(err: Rejection) -> Result<impl Reply, Infallible> {
    let (code, message) = match err.find::<ErrorReply>() {
        Some(ErrorReply::EmptyBody) => (StatusCode::BAD_REQUEST, "no body provided".to_string()),
        Some(ErrorReply::Arrow(e)) => (StatusCode::BAD_REQUEST, format!("arrow error: {}", e)),
        Some(ErrorReply::Chunk(e)) => (StatusCode::BAD_REQUEST, format!("chunk error: {}", e)),
        Some(ErrorReply::InvalidQuery) => (StatusCode::BAD_REQUEST, "invalid query".to_string()),
        Some(ErrorReply::InvalidSchema) => (StatusCode::BAD_REQUEST, "invalid schema".to_string()),
        Some(ErrorReply::WriterBusy) => (StatusCode::TOO_MANY_REQUESTS, "writer busy".to_string()),
        Some(ErrorReply::BadEncoding) => (
            StatusCode::BAD_REQUEST,
            "could not decode message as utf-8".to_string(),
        ),
        Some(ErrorReply::CannotAccept(content)) => (
            StatusCode::BAD_REQUEST,
            format!("cannot parse Content-Type '{}'", content),
        ),
        Some(ErrorReply::CannotEmit(content)) => (
            StatusCode::BAD_REQUEST,
            format!("cannot emit requested '{}' Accept format", content),
        ),
        Some(ErrorReply::NoHeartbeat) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no heartbeat".to_string(),
        ),
        Some(ErrorReply::Unknown) | None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "unknown error".to_string(),
        ),
    };

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}

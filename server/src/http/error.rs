use crate::arrow2::error::Error as ArrowError;
use plateau_transport::{headers::MAX_REQUEST_SIZE_HEADER, ChunkError, ErrorMessage, PathError};
use rweb::reject::PayloadTooLarge;
use rweb::*;
use std::convert::Infallible;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::reject::Reject;

use crate::config::PlateauConfig;

#[derive(Debug)]
pub(crate) enum ErrorReply {
    Arrow(ArrowError),
    Chunk(ChunkError),
    Path(PathError),
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

pub(crate) async fn emit_error(
    err: Rejection,
    config: Arc<PlateauConfig>,
) -> Result<impl Reply, Infallible> {
    let (code, message) = match err.find::<ErrorReply>() {
        Some(ErrorReply::EmptyBody) => (StatusCode::BAD_REQUEST, "no body provided".to_string()),
        Some(ErrorReply::Arrow(e)) => (StatusCode::BAD_REQUEST, format!("arrow error: {}", e)),
        Some(ErrorReply::Chunk(e)) => (StatusCode::BAD_REQUEST, format!("chunk error: {}", e)),
        Some(ErrorReply::Path(e)) => (StatusCode::BAD_REQUEST, format!("invalid path: {}", e)),
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
        Some(ErrorReply::Unknown) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "unknown error".to_string(),
        ),
        None => {
            if err.find::<PayloadTooLarge>().is_some() {
                (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload too large".to_string(),
                )
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unknown error".to_string(),
                )
            }
        }
    };

    let mut response = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message,
    })
    .into_response();

    if code == 413 {
        response
            .headers_mut()
            .insert(MAX_REQUEST_SIZE_HEADER, config.http.max_append_bytes.into());
    }

    Ok(warp::reply::with_status(response, code))
}

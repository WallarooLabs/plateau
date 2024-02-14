use crate::arrow2::error::Error as ArrowError;
use axum::http::StatusCode;
use plateau_transport::{headers::MAX_REQUEST_SIZE_HEADER, ChunkError, ErrorMessage, PathError};

#[derive(Debug)]
pub(crate) enum ErrorReply {
    Arrow(ArrowError),
    Chunk(ChunkError),
    Path(PathError),
    EmptyBody,
    WriterBusy,
    InvalidQuery,
    InvalidSchema,
    NullTypes,
    BadEncoding,
    CannotAccept(String),
    CannotEmit(String),
    NoHeartbeat,
    InsufficientDiskSpace,
    Unknown,
    PayloadTooLarge(u64),
}

impl axum::response::IntoResponse for ErrorReply {
    fn into_response(self) -> axum::response::Response {
        let (code, message) = match self {
            ErrorReply::EmptyBody => (StatusCode::BAD_REQUEST, "no body provided".to_string()),
            ErrorReply::Arrow(e) => (StatusCode::BAD_REQUEST, format!("arrow error: {}", e)),
            ErrorReply::Chunk(e) => (StatusCode::BAD_REQUEST, format!("chunk error: {}", e)),
            ErrorReply::Path(e) => (StatusCode::BAD_REQUEST, format!("invalid path: {}", e)),
            ErrorReply::InvalidQuery => (StatusCode::BAD_REQUEST, "invalid query".to_string()),
            ErrorReply::InvalidSchema => (StatusCode::BAD_REQUEST, "invalid schema".to_string()),
            ErrorReply::NullTypes => (
                StatusCode::BAD_REQUEST,
                "schema includes null datatypes".to_string(),
            ),
            ErrorReply::WriterBusy => (StatusCode::TOO_MANY_REQUESTS, "writer busy".to_string()),
            ErrorReply::BadEncoding => (
                StatusCode::BAD_REQUEST,
                "could not decode message as utf-8".to_string(),
            ),
            ErrorReply::CannotAccept(content) => (
                StatusCode::BAD_REQUEST,
                format!("cannot parse Content-Type '{}'", content),
            ),
            ErrorReply::CannotEmit(content) => (
                StatusCode::BAD_REQUEST,
                format!("cannot emit requested '{}' Accept format", content),
            ),
            ErrorReply::NoHeartbeat => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "no heartbeat".to_string(),
            ),
            ErrorReply::InsufficientDiskSpace => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "insufficient disk space".to_string(),
            ),
            ErrorReply::Unknown => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unknown error".to_string(),
            ),
            kind => {
                if let ErrorReply::PayloadTooLarge(max_append_bytes) = kind {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        [(MAX_REQUEST_SIZE_HEADER, format!("{max_append_bytes}"))],
                        "payload too large",
                    )
                        .into_response();
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unknown error".to_string(),
                    )
                }
            }
        };

        let response = axum::Json(&ErrorMessage {
            code: code.as_u16(),
            message,
        })
        .into_response();

        (code, response).into_response()
    }
}

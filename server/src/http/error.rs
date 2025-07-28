use crate::arrow2::error::Error as ArrowError;
use axum::http::StatusCode;
use plateau_transport::{headers::MAX_REQUEST_SIZE_HEADER, ChunkError, ErrorMessage, PathError};

#[derive(Debug)]
pub enum ErrorReply {
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
    PayloadTooLarge(usize),
}

impl axum::response::IntoResponse for ErrorReply {
    fn into_response(self) -> axum::response::Response {
        let (code, message) = match self {
            Self::EmptyBody => (StatusCode::BAD_REQUEST, "no body provided".to_string()),
            Self::Arrow(e) => (StatusCode::BAD_REQUEST, format!("arrow error: {e}")),
            Self::Chunk(e) => (StatusCode::BAD_REQUEST, format!("chunk error: {e}")),
            Self::Path(e) => (StatusCode::BAD_REQUEST, format!("invalid path: {e}")),
            Self::InvalidQuery => (StatusCode::BAD_REQUEST, "invalid query".to_string()),
            Self::InvalidSchema => (StatusCode::BAD_REQUEST, "invalid schema".to_string()),
            Self::NullTypes => (
                StatusCode::BAD_REQUEST,
                "schema includes null datatypes".to_string(),
            ),
            Self::WriterBusy => (StatusCode::TOO_MANY_REQUESTS, "writer busy".to_string()),
            Self::BadEncoding => (
                StatusCode::BAD_REQUEST,
                "could not decode message as utf-8".to_string(),
            ),
            Self::CannotAccept(content) => (
                StatusCode::BAD_REQUEST,
                format!("cannot parse Content-Type '{content}'"),
            ),
            Self::CannotEmit(content) => (
                StatusCode::BAD_REQUEST,
                format!("cannot emit requested '{content}' Accept format"),
            ),
            Self::NoHeartbeat => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "no heartbeat".to_string(),
            ),
            Self::InsufficientDiskSpace => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "insufficient disk space".to_string(),
            ),
            Self::Unknown => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "unknown error".to_string(),
            ),
            kind => {
                if let Self::PayloadTooLarge(max_append_bytes) = kind {
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

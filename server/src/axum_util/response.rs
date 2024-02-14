use axum::{
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;

#[derive(Debug)]
pub struct Response<T: Serialize> {
    pub status: StatusCode,
    pub body: T,
}

impl<T: Serialize> IntoResponse for Response<T> {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.body)).into_response()
    }
}

impl<T: Serialize> Response<T> {
    pub fn ok(body: T) -> Self {
        let status = StatusCode::OK;
        Self { status, body }
    }
}

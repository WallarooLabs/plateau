use axum::extract;
use axum::http;
use axum::response;
use serde::de;

use crate::http::ErrorReply;

#[derive(Debug)]
pub struct Query<T>(pub T);

#[derive(Debug)]
#[non_exhaustive]
pub enum QueryRejection {
    FailedToDeserializeQueryString(String),
}

#[axum::async_trait]
impl<T, S> extract::FromRequestParts<S> for Query<T>
where
    T: de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = QueryRejection;

    async fn from_request_parts(
        parts: &mut http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        // A request without a query string carries the same information as one
        // with an empty query string: every parameter takes its default.
        let query = parts.uri.query().unwrap_or_default();
        let config = serde_qs::Config::new(2, false);
        config
            .deserialize_str(query)
            .map(Query)
            .map_err(|e| QueryRejection::FailedToDeserializeQueryString(e.to_string()))
    }
}

impl response::IntoResponse for QueryRejection {
    fn into_response(self) -> response::Response {
        let Self::FailedToDeserializeQueryString(detail) = self;
        ErrorReply::InvalidQueryString(detail).into_response()
    }
}

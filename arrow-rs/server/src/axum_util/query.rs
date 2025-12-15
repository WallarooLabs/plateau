use axum::extract;
use axum::http;
use axum::response;
use serde::de;

#[derive(Debug)]
pub struct Query<T>(pub T);

#[derive(Debug)]
#[non_exhaustive]
pub enum QueryRejection {
    FailedToDeserializeQueryString,
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
        let query = parts
            .uri
            .query()
            .ok_or(QueryRejection::FailedToDeserializeQueryString)?;
        let config = serde_qs::Config::new(2, false);
        config
            .deserialize_str(query)
            .map(Query)
            .map_err(|_| QueryRejection::FailedToDeserializeQueryString)
    }
}

impl response::IntoResponse for QueryRejection {
    fn into_response(self) -> response::Response {
        http::StatusCode::NOT_ACCEPTABLE.into_response()
    }
}

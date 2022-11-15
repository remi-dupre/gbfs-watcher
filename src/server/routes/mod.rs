pub mod stations;

use axum::body::Body;
use axum::http::Request;

use super::error::Error;

pub async fn handle_unmatched_path(request: Request<Body>) -> Error {
    let uri = request.uri().clone();
    let method = request.method().clone();
    Error::UnmatchedPath { uri, method }
}

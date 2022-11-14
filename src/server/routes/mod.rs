pub mod stations;

use std::fmt::Debug;

use axum::body::Body;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::http::{Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::Method;
use serde::Serialize;
use thiserror::Error as ThisError;

use crate::gbfs::models;
use crate::storage::journal;
use crate::util::serialize_with_display;

#[derive(Serialize)]
struct ErrorBody<D: Serialize> {
    status: String,
    status_code: u16,
    msg: String,
    detail: D,
}

#[derive(Debug, Serialize, ThisError)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Error {
    #[error("station does not exist: {station_id}")]
    UnknownStation { station_id: models::StationId },

    #[error("answer is too large, try restraining it with ?from or ?to params")]
    AnswerTooLarge,

    #[error("invalid query parameter")]
    InvalidQueryParameter {
        #[from]
        #[serde(serialize_with = "serialize_with_display")]
        source: QueryRejection,
    },

    #[error("invalid path parameter")]
    InvalidPathParameter {
        #[from]
        #[serde(serialize_with = "serialize_with_display")]
        source: PathRejection,
    },

    #[error("request didn't match any route")]
    UnmatchedPath {
        #[serde(serialize_with = "serialize_with_display")]
        method: Method,
        #[serde(serialize_with = "serialize_with_display")]
        uri: Uri,
    },

    #[error("journal error")]
    Journal {
        #[from]
        source: journal::StationStatusJournalError,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::UnknownStation { .. } | Self::UnmatchedPath { .. } => StatusCode::NOT_FOUND,
            Self::Journal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AnswerTooLarge
            | Self::InvalidQueryParameter { .. }
            | Self::InvalidPathParameter { .. } => StatusCode::BAD_REQUEST,
        };

        let body = ErrorBody {
            status: format!("{status}"),
            status_code: status.into(),
            msg: format!("{self}"),
            detail: self,
        };

        (status, Json(body)).into_response()
    }
}

pub async fn handle_unmatched_path(request: Request<Body>) -> Error {
    let uri = request.uri().clone();
    let method = request.method().clone();
    Error::UnmatchedPath { uri, method }
}

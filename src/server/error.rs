use std::fmt::Debug;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::Method;
use serde::Serialize;
use thiserror::Error as ThisError;

use crate::gbfs::models;
use crate::storage::{dump, journal};
use crate::util::serialize_with_display;

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

    #[error("no dump available on the server")]
    NoDump,

    #[error("error while fetching dump")]
    DumpError {
        #[from]
        source: dump::Error,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::AnswerTooLarge
            | Self::InvalidQueryParameter { .. }
            | Self::InvalidPathParameter { .. } => StatusCode::BAD_REQUEST,
            Self::UnknownStation { .. } | Self::NoDump | Self::UnmatchedPath { .. } => {
                StatusCode::NOT_FOUND
            }
            Self::Journal { .. } | Self::DumpError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        #[derive(Serialize)]
        struct ErrorBody<D: Serialize> {
            status: String,
            status_code: u16,
            msg: String,
            detail: D,
        }

        let body = ErrorBody {
            status: format!("{status}"),
            status_code: status.into(),
            msg: format!("{self}"),
            detail: self,
        };

        (status, Json(body)).into_response()
    }
}

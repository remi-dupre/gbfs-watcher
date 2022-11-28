// gbfs-watcher: API and logger for GBFS endpoints
// Copyright (C) 2022  Rémi Dupré
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::fmt::Debug;
use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::Method;
use serde::Serialize;
use thiserror::Error as ThisError;

use crate::gbfs::models;
use crate::server::state;
use crate::storage::{dump, journal};
use crate::util::log::serialize_with_display;

use super::routes::RouteDoc;

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
        routes: Arc<Vec<RouteDoc>>,
    },

    #[error("journal error")]
    Journal {
        #[from]
        source: journal::StationStatusJournalError,
    },

    #[error("stations error")]
    Stations {
        #[from]
        source: state::stations::Error,
    },

    #[error("no dump available on the server")]
    NoDump,

    #[error("no dump with name {name}")]
    NoDumpWithName { name: String },

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

            Self::UnknownStation { .. }
            | Self::NoDump
            | Self::UnmatchedPath { .. }
            | Error::NoDumpWithName { .. } => StatusCode::NOT_FOUND,

            Self::Journal { .. } | Self::Stations { .. } | Self::DumpError { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
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

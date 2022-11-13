use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::{future, stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

use crate::gbfs::models;
use crate::server::state::State;
use crate::storage::journal;

#[derive(Debug, Serialize, ThisError)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Error {
    #[error("station does not exist: {station_id}")]
    UnknownStation { station_id: models::StationId },
    #[error("answer is too large, try restraining it with ?from or ?to params")]
    AnswerTooLarge,
    #[error("journal error")]
    JournalError {
        #[from]
        source: journal::StationStatusJournalError,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Error::UnknownStation { .. } => StatusCode::NOT_FOUND,
            Error::AnswerTooLarge => StatusCode::BAD_REQUEST,
            Error::JournalError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        #[derive(Serialize)]
        struct Body {
            status: String,
            status_code: u16,
            msg: String,
            detail: Error,
        }

        let body = Body {
            status: format!("{status}"),
            status_code: status.into(),
            msg: format!("{self}"),
            detail: self,
        };

        (status, Json(body)).into_response()
    }
}

#[derive(Serialize)]
pub struct StationDetail {
    pub journal_size: usize,
    pub info: Arc<models::StationInformation>,
    pub current_status: Option<Arc<models::StationStatus>>,
}

pub async fn get_station_detail(
    Path(id): Path<u64>,
    state: Extension<Arc<State>>,
) -> Result<Json<StationDetail>, Error> {
    let info = state
        .stations_info
        .read()
        .await
        .get(&id)
        .ok_or(Error::UnknownStation { station_id: id })?
        .clone();

    let (current_status, journal_size) = {
        if let Some(journal) = state.stations_status.read().await.get(&id) {
            let journal = journal.read().await;
            (journal.last(), journal.len())
        } else {
            (None, 0)
        }
    };

    let resp = StationDetail {
        journal_size,
        info,
        current_status,
    };

    Ok(Json(resp))
}

#[derive(Serialize)]
pub struct Stations {
    pub stations: HashMap<models::StationId, StationDetail>,
}

pub async fn get_stations<'a>(state: Extension<Arc<State>>) -> Json<Stations> {
    let stations = state.stations_info.read().await.clone();
    let stations_status = &state.stations_status.read().await;

    let stations = stream::iter(stations.iter())
        .filter_map(|(station_id, info)| async move {
            let (current_status, journal_size) = {
                if let Some(journal) = stations_status.get(station_id) {
                    let journal = journal.read().await;
                    (journal.last(), journal.len())
                } else {
                    (None, 0)
                }
            };

            Some((
                *station_id,
                StationDetail {
                    journal_size,
                    info: info.clone(),
                    current_status,
                },
            ))
        })
        .collect()
        .await;

    let resp = Stations { stations };
    Json(resp)
}

fn default_max_timestamp() -> models::Timestamp {
    models::Timestamp::MAX
}

#[derive(Deserialize)]
pub struct HistoryQueryParams {
    #[serde(default)]
    from: models::Timestamp,
    #[serde(default = "default_max_timestamp")]
    to: models::Timestamp,
}

#[derive(Serialize)]
pub struct StationHistory {
    #[serde(flatten)]
    pub detail: StationDetail,
    pub history: Vec<Arc<models::StationStatus>>,
}

pub async fn get_station_history(
    Path(id): Path<u64>,
    params: Query<HistoryQueryParams>,
    state: Extension<Arc<State>>,
) -> Result<Json<StationHistory>, Error> {
    let detail = get_station_detail(Path(id), state.clone()).await?.0;
    let station_status = state.stations_status.read().await;

    let journal = station_status
        .get(&id)
        .ok_or(Error::UnknownStation { station_id: id })?
        .read()
        .await;

    let history: Vec<_> = journal
        .iter_from(params.from)
        .await?
        .try_take_while(|x| future::ready(Ok(x.last_reported <= params.to)))
        .take(10_001)
        .try_collect()
        .await?;

    if history.len() > 10_000 {
        return Err(Error::AnswerTooLarge);
    }

    let res = StationHistory { history, detail };
    Ok(Json(res))
}

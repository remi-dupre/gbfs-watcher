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

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query};
use axum::Json;
use futures::{future, stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::Error;
use crate::gbfs::models;
use crate::server::state::State;

#[derive(Serialize)]
pub struct StationDetail {
    pub journal_size: usize,
    pub info: Arc<models::StationInformation>,
    pub current_status: Option<models::StationStatus>,
}

#[derive(Serialize)]
pub struct Stations {
    pub stations: HashMap<models::StationId, StationDetail>,
}

#[derive(Deserialize)]
pub struct HistoryQueryParams {
    #[serde(default = "models::Timestamp::min_value")]
    from: models::Timestamp,
    #[serde(default = "models::Timestamp::max_value")]
    to: models::Timestamp,
}

#[derive(Serialize)]
pub struct StationHistory {
    #[serde(flatten)]
    pub detail: StationDetail,
    pub history: Vec<models::StationStatus>,
}

pub async fn get_stations<'a>(state: Arc<State>) -> Json<Stations> {
    let stations = state.stations_info.read().await.clone();
    let stations_status = &state.stations_status.read().await;

    let stations = stream::iter(stations.iter())
        .filter_map(|(station_id, info)| async move {
            let (current_status, journal_size) = {
                if let Some(journal) = stations_status.get(station_id) {
                    let journal = journal.read().await;
                    (journal.last().copied(), journal.len())
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

pub async fn get_station_detail(
    state: Arc<State>,
    id: Result<Path<u64>, PathRejection>,
) -> Result<Json<StationDetail>, Error> {
    let Path(id) = id?;

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
            (journal.last().copied(), journal.len())
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

pub async fn get_station_history(
    state: Arc<State>,
    id: Result<Path<u64>, PathRejection>,
    params: Result<Query<HistoryQueryParams>, QueryRejection>,
) -> Result<Json<StationHistory>, Error> {
    let Path(id) = id?;
    let params = params?;
    let detail = get_station_detail(state.clone(), Ok(Path(id))).await?.0;
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

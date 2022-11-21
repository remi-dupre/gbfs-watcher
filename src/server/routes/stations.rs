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
use geoutils::Location;
use serde::{Deserialize, Serialize};

use super::Error;
use crate::gbfs::models;
use crate::server::state::State;

const NUM_NEARBY: usize = 10;

#[derive(Serialize)]
pub struct StationNearby {
    pub distance: f64,
    pub station: StationDetail,
}

#[derive(Serialize)]
pub struct StationDetail {
    pub journal_size: usize,
    pub info: Arc<models::StationInformation>,
    pub current_status: Option<models::StationStatus>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub nearby: Vec<StationNearby>,
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
                    nearby: Vec::new(),
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
    let stations_info = state.stations_info.read().await.clone();

    let info = stations_info
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

    let station_loc = Location::new(info.lat, info.lon);
    let by_dist = state.stations_by_dist.read().await.clone();

    let nearby: Vec<_> = stream::iter(by_dist.get(&info.station_id).into_iter().flatten())
        .filter_map(|other| {
            let id = other.station_id;
            let state = state.clone();
            let stations_info = stations_info.clone();

            async move {
                let info = stations_info.get(&id)?.clone();
                let other_loc = Location::new(other.lat, other.lon);
                let distance = station_loc.distance_to(&other_loc).ok()?.meters();

                let (current_status, journal_size) = {
                    if let Some(journal) = state.stations_status.read().await.get(&id) {
                        let journal = journal.read().await;
                        (journal.last().copied(), journal.len())
                    } else {
                        (None, 0)
                    }
                };

                let station = StationDetail {
                    journal_size,
                    info,
                    current_status,
                    nearby: Vec::new(),
                };

                Some(StationNearby { distance, station })
            }
        })
        .take(NUM_NEARBY)
        .collect()
        .await;

    let resp = StationDetail {
        journal_size,
        info,
        current_status,
        nearby,
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

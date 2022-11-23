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
use futures::{future, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use super::Error;
use crate::gbfs::models;
use crate::server::models::StationDetail;
use crate::server::state::State;

/// Number of nearby stations listed in listing queries
const NUM_NEARBY_LIST: usize = 0;

/// Number of nearby stations listed in single queries
const NUM_NEARBY_SINGLE: usize = 10;

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
    let stations: Vec<_> = state.stations.list_station_details(NUM_NEARBY_LIST).await;

    let stations = stations
        .into_iter()
        .map(|x| (x.info.station_id, x))
        .collect();

    Json(Stations { stations })
}

pub async fn get_station_detail(
    state: Arc<State>,
    id: Result<Path<u64>, PathRejection>,
) -> Result<Json<StationDetail>, Error> {
    let Path(id) = id?;

    let resp = state
        .stations
        .get_station_details(id, NUM_NEARBY_SINGLE)
        .await
        .ok_or(Error::UnknownStation { station_id: id })?;

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
    let station_status = state.stations.journals.read().await;

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

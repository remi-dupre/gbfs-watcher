use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::Json;
use futures::{future, stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::gbfs::models;
use crate::server::state::State;

#[derive(Serialize)]
pub struct StationDetail {
    pub journal_size: usize,
    pub info: Arc<models::StationInformation>,
    pub current_status: Option<Arc<models::StationStatus>>,
}

pub async fn get_station_detail(
    id: Path<u64>,
    state: Extension<Arc<State>>,
) -> Result<Json<StationDetail>, StatusCode> {
    let info = state
        .stations_info
        .read()
        .await
        .get(&id)
        .ok_or(StatusCode::NOT_FOUND)?
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
    id: Path<u64>,
    params: Query<HistoryQueryParams>,
    state: Extension<Arc<State>>,
) -> Result<Json<StationHistory>, StatusCode> {
    let detail = get_station_detail(Path(*id), state.clone()).await?.0;
    let station_status = state.stations_status.read().await;

    let journal = station_status
        .get(&id)
        .ok_or(StatusCode::NOT_FOUND)?
        .read()
        .await;

    let history: Vec<_> = journal
        .iter_from(params.from)
        .await
        .expect("TODO")
        .try_take_while(|x| future::ready(Ok(x.last_reported <= params.to)))
        .take(10_001)
        .try_collect()
        .await
        .expect("TODO");

    if history.len() > 10_000 {
        todo!("too large");
    }

    let res = StationHistory { history, detail };
    Ok(Json(res))
}

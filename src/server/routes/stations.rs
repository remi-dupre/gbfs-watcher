use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::Json;
use futures::{stream, StreamExt};
use serde::Serialize;

use crate::gbfs::models;
use crate::server::state::State;

#[derive(Serialize)]
pub struct StationDetail {
    pub journal_size: usize,
    pub info: Arc<models::StationInformation>,
}

#[derive(Serialize)]
pub struct Stations {
    pub stations: HashMap<models::StationId, StationDetail>,
}

pub async fn get_stations(state: Extension<Arc<State>>) -> Json<Stations> {
    let stations = state.stations_info.read().await.clone();
    let stations_status = &state.stations_status.read().await;

    let stations = stream::iter(stations.iter())
        .filter_map(|(station_id, info)| async move {
            Some((
                *station_id,
                StationDetail {
                    journal_size: {
                        if let Some(journal) = stations_status.get(station_id) {
                            journal.read().await.len()
                        } else {
                            0
                        }
                    },
                    info: info.clone(),
                },
            ))
        })
        .collect()
        .await;

    let resp = Stations { stations };
    Json(resp)
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

    let journal_size = {
        if let Some(journal) = state.stations_status.read().await.get(&id) {
            journal.read().await.len()
        } else {
            0
        }
    };

    let resp = StationDetail { journal_size, info };
    Ok(Json(resp))
}

use std::collections::HashMap;
use std::sync::Arc;

use axum::{Extension, Json};
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
pub struct Response {
    pub stations: HashMap<models::StationId, StationDetail>,
}

pub async fn get(state: Extension<Arc<State>>) -> Json<Response> {
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

    let resp = Response { stations };
    Json(resp)
}

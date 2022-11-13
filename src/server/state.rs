use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use thiserror::Error as ThisError;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::gbfs::api::{self, GbfsApi};
use crate::gbfs::models;
use crate::storage::dir_lock::{self, DirLock};
use crate::storage::journal::StationStatusJournal;

/// Update frequency for all stations, in seconds
const UPDATE_STATIONS_STATUS_FREQUENCY: u64 = 120;

pub type AllStationsStatusJournal =
    RwLock<HashMap<models::StationId, RwLock<StationStatusJournal>>>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("could not lock journals directory: {0}")]
    LockError(#[from] dir_lock::Error),

    #[error("error while calling API: {0}")]
    ApiError(#[from] api::Error),
}

pub struct State {
    journals_lock: DirLock,
    pub api: GbfsApi,
    pub stations_info: RwLock<Arc<HashMap<models::StationId, Arc<models::StationInformation>>>>,
    pub stations_status: AllStationsStatusJournal,
}

impl State {
    pub async fn new(api_root_url: &str, journals_path: PathBuf) -> Result<Arc<Self>, Error> {
        let journals_lock = DirLock::lock(journals_path).await?;
        let api = GbfsApi::new(api_root_url).await?;
        let stations_status = AllStationsStatusJournal::default();

        let stations_info = RwLock::new(Arc::new(
            api.get_station_information()
                .await?
                .into_iter()
                .map(|info| (info.station_id, Arc::new(info)))
                .collect(),
        ));

        let state = Arc::new(Self {
            journals_lock,
            api,
            stations_info,
            stations_status,
        });

        tokio::spawn(station_status_update_daemon(state.clone()));
        Ok(state)
    }
}

async fn update_stations_info(state: &State) {
    let stations_info = match state.api.get_station_information().await {
        Ok(resp) => resp
            .into_iter()
            .map(|info| (info.station_id, Arc::new(info)))
            .collect(),
        Err(err) => {
            error!("Could not fetch stations informations: {err}");
            return;
        }
    };

    *state.stations_info.write().await = Arc::new(stations_info);
}

async fn update_stations_status(state: &State) -> usize {
    let error_count = AtomicUsize::new(0);
    let updated_count = AtomicUsize::new(0);

    let total_count = futures::stream::iter(state.api.get_station_status().await.unwrap())
        .map(|status| {
            let error_count = &error_count;
            let updated_count = &updated_count;
            let journals_dir = &state.journals_lock;
            let journals = &state.stations_status;

            async move {
                if !journals.read().await.contains_key(&status.station_id) {
                    let mut guard = journals.write().await;

                    if let Entry::Vacant(e) = guard.entry(status.station_id) {
                        let path = journals_dir.join(format!("{}.journal", status.station_id));

                        let journal = StationStatusJournal::open(path)
                            .await
                            .expect("could not open journal");

                        e.insert(RwLock::new(journal));
                    }
                }

                let journals = journals.read().await;
                let mut journal = journals[&status.station_id].write().await;

                match journal.insert(status).await {
                    Err(err) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        error!("Could not insert in journal: {err}")
                    }
                    Ok(true) => {
                        updated_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {}
                }
            }
        })
        .buffered(64)
        .count()
        .await;

    let updated_count = updated_count.into_inner();
    let error_count = error_count.into_inner();

    if error_count > 0 {
        info!("Updated {updated_count}/{total_count} stations ({error_count} errors)");
    } else {
        info!("Updated {updated_count}/{total_count} stations");
    }

    updated_count
}

async fn station_status_update_daemon(state: Arc<State>) {
    let mut timer = tokio::time::interval(Duration::from_secs(UPDATE_STATIONS_STATUS_FREQUENCY));

    loop {
        timer.tick().await;
        futures::join!(update_stations_status(&state), update_stations_info(&state));
    }
}

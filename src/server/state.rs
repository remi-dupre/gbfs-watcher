use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Future, StreamExt};
use thiserror::Error as ThisError;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

use crate::gbfs::api::{self, GbfsApi};
use crate::gbfs::models;
use crate::storage::dir_lock::{self, DirLock};
use crate::storage::journal::StationStatusJournal;

/// Number of concurent insertions that will be performed on updates
const CONCURENT_JOURNAL_WRITES: usize = 128;

/// Update frequency for all stations status, in seconds
const UPDATE_STATIONS_STATUS_FREQUENCY: Duration = Duration::from_secs(2 * 60); // 2min

/// Update frequency for all stations, in seconds
const UPDATE_STATIONS_INFO_FREQUENCY: Duration = Duration::from_secs(60 * 60); // 1h

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

        spawn_update_daemon(
            Self::update_stations_status,
            UPDATE_STATIONS_STATUS_FREQUENCY,
            state.clone(),
        );

        spawn_update_daemon(
            Self::update_stations_info,
            UPDATE_STATIONS_INFO_FREQUENCY,
            state.clone(),
        );

        Ok(state)
    }

    async fn update_stations_info(self: Arc<Self>) {
        let start_instant = Instant::now();

        let stations_info: HashMap<_, _> = match self.api.get_station_information().await {
            Ok(resp) => resp
                .into_iter()
                .map(|info| (info.station_id, Arc::new(info)))
                .collect(),
            Err(err) => {
                error!("Could not fetch stations informations: {err}");
                return;
            }
        };

        let stations_count = stations_info.len();
        *self.stations_info.write().await = Arc::new(stations_info);
        let total_time = start_instant.elapsed();

        info!(
            stations = stations_count,
            time = format!("{total_time:.2?}"),
            "Updated stations infos",
        );
    }

    async fn update_stations_status(self: Arc<Self>) {
        let start_instant = Instant::now();
        let error_count = AtomicUsize::new(0);
        let updated_count = AtomicUsize::new(0);

        let api_response = match self.api.get_station_status().await {
            Ok(x) => x,
            Err(err) => {
                error!("Could not fetch status from API: {err}");
                return;
            }
        };

        let fetch_api_time = start_instant.elapsed();

        let total_count = futures::stream::iter(api_response)
            .map(|status| {
                let error_count = &error_count;
                let updated_count = &updated_count;
                let journals_dir = &self.journals_lock;
                let journals = &self.stations_status;

                async move {
                    if !journals.read().await.contains_key(&status.station_id) {
                        let mut guard = journals.write().await;

                        if let Entry::Vacant(e) = guard.entry(status.station_id) {
                            let path = journals_dir.join(format!("{}.journal", status.station_id));

                            let journal = match StationStatusJournal::open(path).await {
                                Ok(x) => x,
                                Err(err) => {
                                    error!("Could not open journal: {err}");
                                    return;
                                }
                            };

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
            .buffered(CONCURENT_JOURNAL_WRITES)
            .count()
            .await;

        let total_time = start_instant.elapsed();
        let write_journals_time = total_time - fetch_api_time;
        let updated_count = updated_count.into_inner();
        let error_count = error_count.into_inner();

        info!(
            stations = total_count,
            stations_updated = updated_count,
            fetch_api_time = format!("{fetch_api_time:.2?}"),
            write_journals_time = format!("{write_journals_time:.2?}"),
            errors = {
                if error_count == 0 {
                    None
                } else {
                    Some(error_count)
                }
            },
            "Updated stations status",
        );
    }
}

fn spawn_update_daemon<T, F>(
    mut update_task: T,
    delay: Duration,
    state: Arc<State>,
) -> tokio::task::JoinHandle<()>
where
    T: FnMut(Arc<State>) -> F + Send + 'static,
    F: Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let state = state.clone();
        let mut timer = tokio::time::interval(delay);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            timer.tick().await;
            update_task(state.clone()).await;
        }
    })
}

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

pub mod stations;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::offset::{Local, Utc};
use futures::Future;
use thiserror::Error as ThisError;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

use crate::gbfs::api::{self, GbfsApi};
use crate::gbfs::models;
use crate::storage::dir_lock;
use crate::storage::dump::{self, DumpRegistry};
use crate::storage::journal::{StationStatusJournal, StationStatusJournalError};
use crate::util::non_zero;
use stations::StationRegistry;

/// Update frequency for all stations status, in seconds
const UPDATE_STATIONS_STATUS_FREQUENCY: Duration = Duration::from_secs(2 * 60); // 2min

/// Update frequency for all stations, in seconds
const UPDATE_STATIONS_INFO_FREQUENCY: Duration = Duration::from_secs(60 * 60); // 1h

/// Dump frequency, in seconds
const DUMP_FREQUENCY: i64 = 12 * 60 * 60; // 12h

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("could not lock journals directory: {0}")]
    LockError(#[from] dir_lock::Error),

    #[error("dump registry error: {0}")]
    DumpError(#[from] dump::Error),

    #[error("error while calling API: {0}")]
    ApiError(#[from] api::Error),

    #[error("error while accessing journal: {0}")]
    JournalError(#[from] StationStatusJournalError),

    #[error("error while station info: {0}")]
    StationsError(#[from] stations::Error),
}

pub type AllStationsStatusJournal =
    RwLock<HashMap<models::StationId, RwLock<StationStatusJournal>>>;

pub type RwHashMap<K, V> = RwLock<Arc<HashMap<K, V>>>;

pub struct State {
    api: GbfsApi,
    pub dumps_registry: DumpRegistry,
    pub stations: StationRegistry,
}

impl State {
    pub async fn new(
        api_root_url: &str,
        journals_path: PathBuf,
        dumps_path: PathBuf,
        keep_dumps: usize,
    ) -> Result<Arc<Self>, Error> {
        let state = Arc::new(Self {
            api: GbfsApi::new(api_root_url).await?,
            dumps_registry: DumpRegistry::new(dumps_path, keep_dumps).await?,
            stations: StationRegistry::new(journals_path).await?,
        });

        tokio::join!(
            state.clone().update_stations_info(),
            state.clone().update_stations_status(),
        );

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

        state
            .clone()
            .spawn_update_dumps_daemon(chrono::Duration::seconds(DUMP_FREQUENCY));

        Ok(state)
    }

    // Update daemons

    async fn update_stations_info(self: Arc<Self>) {
        let start_instant = Instant::now();

        let stations_info = match self.api.get_station_information().await {
            Ok(x) => x,
            Err(err) => {
                error!("Could not fetch stations informations: {err}");
                return;
            }
        };

        let fetch_api_time = start_instant.elapsed();
        let counts = self.stations.update_infos(stations_info).await;
        let update_time = start_instant.elapsed() - fetch_api_time;

        info!(
            total = counts.total,
            deleted = non_zero(counts.deleted),
            updated = non_zero(counts.updated),
            errors = non_zero(counts.errors),
            fetch_api_time = format!("{fetch_api_time:.2?}"),
            update_time = format!("{update_time:.2?}"),
            "Updated infos",
        );
    }

    async fn update_stations_status(self: Arc<Self>) {
        let start_instant = Instant::now();

        let api_response = match self.api.get_station_status().await {
            Ok(x) => x,
            Err(err) => {
                error!("Could not fetch status from API: {err}");
                return;
            }
        };

        let fetch_api_time = start_instant.elapsed();
        let counts = self.stations.update_status(api_response).await;
        let write_journals_time = start_instant.elapsed() - fetch_api_time;

        info!(
            total = counts.total,
            deleted = non_zero(counts.deleted),
            updated = non_zero(counts.updated),
            errors = non_zero(counts.errors),
            fetch_api_time = format!("{fetch_api_time:.2?}"),
            write_journals_time = format!("{write_journals_time:.2?}"),
            "Updated status",
        );
    }

    fn spawn_update_dumps_daemon(self: Arc<Self>, dump_frequency: chrono::Duration) {
        tokio::spawn(async move {
            loop {
                let latest = match self.dumps_registry.latest().await {
                    Ok(x) => x,
                    Err(err) => {
                        error!("Could not read last dump date: {err}");
                        return;
                    }
                };

                if let Some((latest_date, _)) = latest {
                    let next_date = latest_date + dump_frequency;
                    let now = Utc::now();

                    if now < next_date {
                        let wait_duration = match (next_date - now).to_std() {
                            Ok(x) => x,
                            Err(err) => {
                                error!("Could not compute next dump date, dumps disable: {err}");
                                return;
                            }
                        };

                        info!("Next dump scheduled on {}", next_date.with_timezone(&Local));
                        tokio::time::sleep(wait_duration).await;
                        info!("Starting scheduled dump");
                    } else {
                        info!("Starting new dump: last ages to {latest_date}: ");
                    }
                } else {
                    info!("Starting initial dump");
                }

                let dump_stream = self.stations.dump().await;

                if let Err(err) = self.dumps_registry.dump(dump_stream).await {
                    error!("Dump failed: {err}");
                }
            }
        });
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
        timer.reset();

        loop {
            timer.tick().await;
            update_task(state.clone()).await;
        }
    })
}

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::offset::{Local, Utc};
use futures::{future, stream, Future, Stream, StreamExt, TryStreamExt};
use thiserror::Error as ThisError;
use tokio::sync::{mpsc, RwLock};
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

use crate::gbfs::api::{self, GbfsApi};
use crate::gbfs::models;
use crate::storage::dir_lock::{self, DirLock};
use crate::storage::dump::{self, DumpRegistry};
use crate::storage::journal::{StationStatusJournal, StationStatusJournalError};

/// Size of the buffer while performing dumps (in number of journal entries)
const DUMP_CACHE_SIZE: usize = 1024;

/// Concurent journals that are being dumped at the same time, note that dumps won't block status
/// updates as long as this value is lower than `CONCURENT_JOURNAL_WRITES`
const CONCURENT_JOURNAL_DUMPS: usize = 4;

/// Number of concurent insertions that will be performed on updates
const CONCURENT_JOURNAL_WRITES: usize = 128;

/// Update frequency for all stations status, in seconds
const UPDATE_STATIONS_STATUS_FREQUENCY: Duration = Duration::from_secs(2 * 60); // 2min

/// Update frequency for all stations, in seconds
const UPDATE_STATIONS_INFO_FREQUENCY: Duration = Duration::from_secs(60 * 60); // 1h

/// Dump frequency, in seconds
const DUMP_FREQUENCY: i64 = 12 * 60 * 60; // 12h

pub type AllStationsStatusJournal =
    RwLock<HashMap<models::StationId, RwLock<StationStatusJournal>>>;

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
}

pub struct State {
    journals_lock: DirLock,
    api: GbfsApi,
    pub dumps_registry: DumpRegistry,
    pub stations_info: RwLock<Arc<HashMap<models::StationId, Arc<models::StationInformation>>>>,
    pub stations_status: AllStationsStatusJournal,
}

impl State {
    pub async fn new(
        api_root_url: &str,
        journals_path: PathBuf,
        dumps_path: PathBuf,
    ) -> Result<Arc<Self>, Error> {
        let journals_lock = DirLock::lock(journals_path).await?;
        let dumps_registry = DumpRegistry::new(dumps_path).await?;
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
            dumps_registry,
            stations_info,
            stations_status,
        });

        // Ensure journals are loaded
        state.clone().update_stations_status().await;

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

    pub async fn dump(self: Arc<Self>) -> impl Stream<Item = Result<models::StationStatus, Error>> {
        let start_ts: u64 = Utc::now()
            .timestamp()
            .try_into()
            .expect("invalid timestamp");

        let (sender, receiver) = mpsc::channel(DUMP_CACHE_SIZE);

        tokio::spawn(async move {
            let stations_status = self.stations_status.read().await;

            stream::iter(stations_status.iter())
                .map(|(station_id, journal)| {
                    let sender = sender.clone();

                    async move {
                        let journal = journal.read().await;

                        let mut iter = match journal.iter().await {
                            Ok(iter) => Box::pin(iter).try_take_while(|obj| {
                                future::ready(Ok(obj.last_reported <= start_ts))
                            }),
                            Err(err) => {
                                error!("Dump failed for station {station_id}: {err}");

                                if sender.send(Err(err.into())).await.is_err() {
                                    return;
                                };

                                return;
                            }
                        };

                        while let Some(obj) = iter.next().await {
                            let obj = obj.map_err(|err| err.into());

                            if sender.send(obj).await.is_err() {
                                return;
                            }
                        }
                    }
                })
                .boxed()
                .buffer_unordered(CONCURENT_JOURNAL_DUMPS)
                .count()
                .await;
        });

        ReceiverStream::new(receiver)
    }

    // Update daemons

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
            .buffer_unordered(CONCURENT_JOURNAL_WRITES)
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

                let dump_stream = self.clone().dump().await;

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

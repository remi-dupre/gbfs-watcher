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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use geoutils::Location;
use thiserror::Error as ThisError;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

use crate::gbfs::models;
use crate::storage::dir_lock::{self, DirLock};
use crate::storage::journal::{StationStatusJournal, StationStatusJournalError};

/// Size of the buffer while performing dumps (in number of journal entries)
const DUMP_CACHE_SIZE: usize = 1024;

/// Concurent journals that are being dumped at the same time, note that dumps won't block status
/// updates as long as this value is lower than `CONCURENT_JOURNAL_WRITES`
const CONCURENT_JOURNAL_DUMPS: usize = 4;

/// Number of concurent insertions that will be performed on updates
const CONCURENT_JOURNAL_WRITES: usize = 128;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("could not lock journals directory: {0}")]
    LockError(#[from] dir_lock::Error),

    #[error("error while accessing journal: {0}")]
    JournalError(#[from] StationStatusJournalError),
}

pub type AllStationsStatusJournal =
    RwLock<HashMap<models::StationId, Arc<RwLock<StationStatusJournal>>>>;

pub type RwHashMap<K, V> = RwLock<Arc<HashMap<K, V>>>;

#[derive(Default)]
pub struct UpdateCounts {
    pub errors: usize,
    pub updated: usize,
    pub deleted: usize,
    pub total: usize,
}

pub struct StationDist {
    pub dist: f64,
    pub station: Arc<models::StationInformation>,
}

pub struct StationRegistry {
    // TODO: I don't want this to be pub
    pub infos: RwHashMap<models::StationId, Arc<models::StationInformation>>,
    pub by_dist: RwHashMap<models::StationId, Vec<StationDist>>,
    pub journals: AllStationsStatusJournal,
    pub journals_lock: DirLock,
}

impl StationRegistry {
    pub async fn new(journals_path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            infos: Default::default(),
            by_dist: Default::default(),
            journals: Default::default(),
            journals_lock: DirLock::lock(journals_path).await?,
        })
    }

    pub async fn update_infos(&self, infos: Vec<models::StationInformation>) -> UpdateCounts {
        let new_infos: HashMap<_, _> = (infos.into_iter())
            .map(Arc::new)
            .map(|info| (info.station_id, info))
            .collect();

        let (deleted, updated) = (self.infos.read().await)
            .values()
            .map(|old| {
                if let Some(new) = new_infos.get(&old.station_id) {
                    if old == new {
                        (0, 0)
                    } else {
                        (0, 1)
                    }
                } else {
                    (1, 0)
                }
            })
            .fold((0, 0), |(a, b), (x, y)| (a + x, b + y));

        let mut errors = 0;

        let new_by_dist = stream::iter(new_infos.values())
            .filter_map(|from| {
                let from_pos = Location::new(from.lat, from.lon);

                let mut by_dist: Vec<_> = (new_infos.values().cloned())
                    .filter(|to| to.station_id != from.station_id)
                    .filter_map(|to| {
                        let to_pos = Location::new(to.lat, to.lon);

                        let Ok(dist) = from_pos.distance_to(&to_pos).map(|d| d.meters()) else {
                            errors += 1;
                            return None;
                        };

                        Some(StationDist { dist, station: to })
                    })
                    .collect();

                by_dist.sort_unstable_by(|x, y| x.dist.total_cmp(&y.dist));

                async move {
                    tokio::task::yield_now().await;
                    Some((from.station_id, by_dist))
                }
            })
            .collect()
            .await;

        let total = new_infos.len();
        *self.infos.write().await = Arc::new(new_infos);
        *self.by_dist.write().await = Arc::new(new_by_dist);

        UpdateCounts {
            errors,
            updated,
            deleted,
            total,
        }
    }

    pub async fn update_status(&self, statuses: Vec<models::StationStatus>) -> UpdateCounts {
        let errors = AtomicUsize::new(0);
        let updated = AtomicUsize::new(0);

        let deleted = {
            let to_retain: HashSet<_> = statuses.iter().map(|status| status.station_id).collect();
            let mut journals = self.journals.write().await;
            let len_before = journals.len();
            journals.retain(move |id, _| to_retain.contains(id));
            let len_after = journals.len();
            len_before - len_after
        };

        futures::stream::iter(statuses)
            .map(|status| {
                let error_count = &errors;
                let updated_count = &updated;
                let journals_dir = &self.journals_lock;
                let journals = &self.journals;

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

                            e.insert(Arc::new(RwLock::new(journal)));
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

        let total = self.journals.read().await.len();

        UpdateCounts {
            errors: errors.into_inner(),
            updated: updated.into_inner(),
            deleted,
            total,
        }
    }

    pub async fn dump(&self) -> impl Stream<Item = Result<models::StationStatus, Error>> {
        let start_ts: u64 = Utc::now()
            .timestamp()
            .try_into()
            .expect("invalid timestamp");

        let (sender, receiver) = mpsc::channel(DUMP_CACHE_SIZE);
        let stations_status = self.journals.read().await.clone();

        tokio::spawn(async move {
            stream::iter(stations_status.iter())
                .for_each_concurrent(CONCURENT_JOURNAL_DUMPS, |(station_id, journal)| {
                    let sender = sender.clone();

                    async move {
                        let journal = journal.read().await;

                        let iter = match journal.iter().await {
                            Ok(x) => x,
                            Err(err) => {
                                error!("Dump failed for station {station_id}: {err}");
                                sender.send(Err(err.into())).await.ok();
                                return;
                            }
                        };

                        iter.try_take_while(|obj| future::ready(Ok(obj.last_reported <= start_ts)))
                            .map(Ok)
                            .try_for_each(|obj| sender.send(obj.map_err(Into::into)))
                            .await
                            .ok();
                    }
                })
                .await;
        });

        ReceiverStream::new(receiver)
    }
}

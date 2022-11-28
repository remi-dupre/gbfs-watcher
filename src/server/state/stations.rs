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

use chrono::{Duration, NaiveDate, Utc};
use futures::{future, stream, StreamExt, TryFuture, TryStreamExt};
use geoutils::Location;
use serde::Serialize;
use thiserror::Error as ThisError;
use tokio::sync::RwLock;
use tracing::error;

use crate::gbfs::models;
use crate::server::models::{StationDetail, WithDist};
use crate::storage::dir_lock::{self, DirLock};
use crate::storage::journal::{StationStatusJournal, StationStatusJournalError};
use crate::util::history::{history_compressed, history_with_intervals};
use crate::util::time::{day_start, day_to_date};

/// Number of concurent insertions that will be performed on updates
const CONCURENT_JOURNAL_WRITES: usize = 128;

/// Number of concurent journals read for estimates
const CONCURENT_ESTIMATE_BUILT: usize = 16;

/// Interval between two points of the estimate
const ESTIMATE_PRECISION: u32 = 3600; // 1h

/// Number of weeks to collect for the estimate
const ESTIMATE_WEEKS: u16 = 2;

/// Numbers of days to compute in advance for estimates
const ESTIMATE_DAYS_FORWARD: i64 = 1;

#[derive(Debug, Serialize, ThisError)]
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

pub struct StationRegistry {
    pub infos: RwHashMap<models::StationId, Arc<models::StationInformation>>,
    pub by_dist: RwHashMap<models::StationId, Vec<WithDist<Arc<models::StationInformation>>>>,
    pub estimates: RwHashMap<(models::StationId, NaiveDate), Arc<Vec<models::StationStatus<f32>>>>,
    pub journals: AllStationsStatusJournal,
    pub journals_lock: DirLock,
}

impl StationRegistry {
    pub async fn new(journals_path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            infos: Default::default(),
            by_dist: Default::default(),
            estimates: Default::default(),
            journals: Default::default(),
            journals_lock: DirLock::lock(journals_path).await?,
        })
    }

    pub async fn get_station_details(
        &self,
        id: models::StationId,
        nearby: usize,
        today_history_precision: u32,
    ) -> Result<Option<StationDetail>, Error> {
        self.get_station_details_impl(id, nearby, today_history_precision, None)
            .await
    }

    pub async fn list_station_details<C: Default + Extend<StationDetail>>(
        &self,
        nearby: usize,
        today_history_precision: u32,
    ) -> Result<C, Error> {
        let infos = self.infos.read().await.clone();

        stream::iter(infos.values().map(Ok))
            .try_filter_map(|info| {
                self.get_station_details_impl(
                    info.station_id,
                    nearby,
                    today_history_precision,
                    Some(info.clone()),
                )
            })
            .try_collect()
            .await
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

                        Some(WithDist { dist, station: to })
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

        let today = day_start().date_naive();

        let new_estimates = stream::iter(new_infos.keys().copied())
            .filter_map(|id| async move {
                let journal = self.journals.read().await.get(&id)?.clone();
                Some((id, journal))
            })
            .boxed()
            .flat_map_unordered(CONCURENT_ESTIMATE_BUILT, |(id, journal)| {
                stream::iter(0..=ESTIMATE_DAYS_FORWARD)
                    .filter_map(move |day_offset| {
                        let journal = journal.clone();
                        let day = today + Duration::days(day_offset);

                        async move {
                            let journal = journal.read().await;

                            let estimate = match estimated_for_day(day, &journal).await {
                                Ok(x) => x,
                                Err(err) => {
                                    error!("Could not build estimate: {err}");
                                    return None;
                                }
                            };

                            let estimate = Arc::new(history_compressed(&estimate));
                            Some(((id, day), estimate))
                        }
                    })
                    .boxed()
            })
            .collect()
            .await;

        let total = new_infos.len();
        *self.infos.write().await = Arc::new(new_infos);
        *self.by_dist.write().await = Arc::new(new_by_dist);
        *self.estimates.write().await = Arc::new(new_estimates);

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

    pub async fn try_for_each_status<S, F, R, E>(&self, init: S, mut read_status: F) -> Result<S, E>
    where
        F: FnMut(S, models::StationStatus) -> R,
        R: TryFuture<Ok = S, Error = E>,
        E: From<Error>,
    {
        let start_ts = Utc::now().timestamp();
        let mut state = init;
        let stations_status: Vec<_> = self.journals.read().await.values().cloned().collect();

        for journal in stations_status {
            let journal = journal.read().await;
            let iter = journal.iter().await.map_err(Error::from)?;

            state = iter
                .try_take_while(|obj| future::ready(Ok(obj.last_reported <= start_ts)))
                .map_err(Error::from)
                .map_err(E::from)
                .try_fold(state, &mut read_status)
                .await?;
        }

        Ok(state)
    }

    async fn get_station_details_impl(
        &self,
        id: models::StationId,
        nearby: usize,
        today_history_precision: u32,
        station_info: Option<Arc<models::StationInformation>>,
    ) -> Result<Option<StationDetail>, Error> {
        let stations_info = &self.infos.read().await.clone();
        let by_dist = &self.by_dist.read().await.clone();

        let details_no_nearby = &|id, info: Option<Arc<models::StationInformation>>| async move {
            let Some(info) = info.or_else(|| stations_info.get(&id).cloned()) else {
                return Ok(None)
            };

            let (current_status, today_history, journal_size) = {
                if let Some(journal) = self.journals.read().await.get(&id) {
                    let journal = journal.read().await;
                    let from = day_start().timestamp() as _;
                    let to = Utc::now().timestamp();

                    let today_history_raw: Vec<_> =
                        journal.iter_from(from).await?.try_collect().await?;

                    let today_history = history_with_intervals(
                        &today_history_raw,
                        from,
                        to,
                        today_history_precision.into(),
                    );

                    let current_status = today_history_raw.last().copied();

                    (
                        current_status,
                        history_compressed(&today_history),
                        journal.len(),
                    )
                } else {
                    (None, vec![], 0)
                }
            };

            let today_estimate = (self.estimates.read().await)
                .get(&(id, day_start().date_naive()))
                .cloned()
                .unwrap_or_default();

            Ok::<_, Error>(Some(StationDetail {
                journal_size,
                info,
                current_status,
                today_history,
                today_estimate,
                nearby: Vec::new(),
            }))
        };

        let Some(mut res) = details_no_nearby(id, station_info).await? else {
            return Ok(None)
        };

        res.nearby = stream::iter(by_dist.get(&id).into_iter().flatten().map(Ok))
            .try_filter_map(|neighbour| async move {
                let Some(station) = details_no_nearby(
                    neighbour.station.station_id,
                    Some(neighbour.station.clone()),
                )
                .await? else { return Ok::<_,Error>(None) };

                Ok(Some(WithDist {
                    dist: neighbour.dist,
                    station,
                }))
            })
            .take(nearby)
            .try_collect()
            .await?;

        Ok(Some(res))
    }
}

async fn estimated_for_day(
    day: NaiveDate,
    journal: &StationStatusJournal,
) -> Result<Vec<models::StationStatus<f32>>, Error> {
    let mut weeks: Vec<_> = stream::iter(1..=ESTIMATE_WEEKS)
        .map(Ok)
        .try_filter_map(|week| async move {
            let start = day_to_date(day - Duration::days(7) * i32::from(week)).timestamp();
            let end = start + 24 * 3600;

            let raw_history: Vec<_> = journal
                .iter_from(start)
                .await?
                .try_take_while(|x| future::ready(Ok(x.last_reported <= end)))
                .try_collect()
                .await?;

            let history =
                history_with_intervals(&raw_history, start, end, ESTIMATE_PRECISION.into());

            Ok::<_, Error>(Some(history))
        })
        .try_collect()
        .await?;

    let Some(mut res) = weeks.pop() else { return Ok(Vec::new()) };

    for other in weeks {
        res.iter_mut().zip(other).for_each(|(acc, x)| *acc += x);
    }

    for acc in &mut res {
        *acc /= ESTIMATE_WEEKS as f32;
        acc.last_reported += i64::from(ESTIMATE_WEEKS) * 7 * 24 * 3600;
    }

    Ok(res)
}

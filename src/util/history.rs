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

use crate::gbfs::models;

pub fn history_with_intervals(
    history: &[models::StationStatus],
    start_ts: models::Timestamp,
    end_ts: models::Timestamp,
    interval: models::Timestamp,
) -> Vec<models::StationStatus<f32>> {
    let mut history: Vec<models::StationStatus<f32>> =
        history.iter().rev().copied().map(Into::into).collect();

    let mut res: Vec<_> = (0..)
        .map(|idx| start_ts + idx * interval)
        .take_while(|curr_ts| *curr_ts < end_ts)
        .filter_map(|curr_ts| {
            let mut curr = *history.last()?;
            let curr_ts_end = std::cmp::min(curr_ts + interval, end_ts);

            // Ensures that current interval is covered by data
            if curr.last_reported >= curr_ts_end {
                return None;
            }

            curr *= 0.;
            curr.last_reported = curr_ts;

            while let Some(&last) = history.last() {
                let intersect_start = std::cmp::max(curr_ts, last.last_reported);

                let intersect_end = {
                    if history.len() >= 2 {
                        let llast = history[history.len() - 2];

                        if llast.last_reported >= curr_ts_end {
                            curr_ts_end
                        } else {
                            history.pop();
                            llast.last_reported
                        }
                    } else {
                        curr_ts_end
                    }
                };

                curr += last
                    * ((intersect_end - intersect_start) as f32 / (curr_ts_end - curr_ts) as f32);

                // If the interval has been covered, we can yield it.
                if intersect_end == curr_ts_end {
                    break;
                }
            }

            Some(curr)
        })
        .collect();

    // Add last point of data which is at exactly the end date
    if let Some(mut last) = history.pop() {
        last.last_reported = end_ts;
        res.push(last);
    }

    res
}

/// Compress consecutive points of same value in the history. If a chain of more than two points
/// share the same counts, the first and last ones will be the only kept.
pub fn history_compressed<T: Copy + PartialEq>(
    history: &[models::StationStatus<T>],
) -> Vec<models::StationStatus<T>> {
    let key = |x: &models::StationStatus<T>| x.num_bikes_available_types;
    let head = history.first().into_iter();
    let tail = history.last().into_iter();

    let body = history
        .windows(3)
        .filter(|w| {
            let x = key(&w[0]);
            let y = key(&w[1]);
            let z = key(&w[2]);
            x != y || y != z
        })
        .map(|w| &w[1]);

    head.chain(body).chain(tail).copied().collect()
}

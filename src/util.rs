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

use std::fmt::Display;

use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serializer;
use tracing::{error, warn};

pub fn serialize_with_display<T: Display, S>(x: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{x}"))
}

/// Return None if input is zero, useful to skip zero values in tracing.
pub fn non_zero<T: Copy + TryInto<i8>>(x: T) -> Option<T> {
    if matches!(x.try_into(), Ok(0)) {
        None
    } else {
        Some(x)
    }
}

pub fn day_to_date(day: NaiveDate) -> DateTime<Local> {
    let datetime = NaiveDateTime::new(day, NaiveTime::from_hms_opt(0, 0, 0).unwrap());

    match datetime.and_local_timezone(Local) {
        chrono::LocalResult::Single(x) => x,
        chrono::LocalResult::Ambiguous(t1, t2) => {
            warn!("Ambiguous start of the day, {t1} or {t2}");
            std::cmp::min(t1, t2)
        }
        chrono::LocalResult::None => {
            error!("Could not compute start of the day");
            Local::now() - Duration::hours(12)
        }
    }
}

/// Return the time at midnight today in local time
pub fn day_start() -> DateTime<Local> {
    let now = Local::now();
    let date = now.date_naive();
    day_to_date(date)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn call_non_zero() {
        assert_eq!(non_zero(0u32), None);
        assert_eq!(non_zero(1u32), Some(1));
        assert_eq!(non_zero(0isize), None);
        assert_eq!(non_zero(-534isize), Some(-534));
    }
}

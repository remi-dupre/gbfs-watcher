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

pub mod dump;
pub mod stations;

use axum::body::Body;
use axum::http::Request;

use super::error::Error;

pub async fn handle_unmatched_path(request: Request<Body>) -> Error {
    let uri = request.uri().clone();
    let method = request.method().clone();
    Error::UnmatchedPath { uri, method }
}

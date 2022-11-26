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

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::StreamBody;
use axum::extract::rejection::PathRejection;
use axum::extract::Path;
use axum::http::header;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Local};
use futures::{future, StreamExt, TryStreamExt};
use serde::Serialize;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::server::error::Error;
use crate::server::state::State;
use crate::storage::dump;

/// Size of the buffer used to stream files
const DUMP_READ_BUFF_SIZE: usize = 16 * 1024 * 1024; // 16 MB

#[derive(Serialize)]
pub struct DumpInfo {
    name: String,
    date: i64,
    size: u64,
}

pub async fn list_dumps(state: Arc<State>) -> Result<Json<Vec<DumpInfo>>, Error> {
    let mut infos: Vec<_> = (state.dumps_registry.iter().await?)
        .try_filter_map(|(date, path)| async move {
            let name = {
                let Some(name) = path.file_name() else { return Ok(None) };
                name.to_string_lossy().to_string()
            };

            let date = date.timestamp();
            let meta = tokio::fs::metadata(&path).await?;
            let size = meta.len();
            let info = DumpInfo { name, date, size };
            Ok(Some(info))
        })
        .try_collect()
        .await?;

    infos.sort_unstable_by_key(|info| info.date);
    Ok(Json(infos))
}

pub async fn dump_by_name(
    state: Arc<State>,
    name: Result<Path<String>, PathRejection>,
) -> Result<impl IntoResponse, Error> {
    let Path(name) = name?;

    let (date, path) = (state.dumps_registry.iter().await?)
        .try_filter(|(_, path)| {
            let res = Some(name.as_str()) == path.file_name().and_then(|name| name.to_str());
            future::ready(res)
        })
        .boxed()
        .try_next()
        .await?
        .ok_or_else(|| Error::NoDumpWithName { name: name.clone() })?;

    serve_dump(date, path).await
}

pub async fn latest_dump(state: Arc<State>) -> Result<impl IntoResponse, Error> {
    let (date, path) = state.dumps_registry.latest().await?.ok_or(Error::NoDump)?;
    serve_dump(date, path).await
}

async fn serve_dump(date: DateTime<Local>, path: PathBuf) -> Result<impl IntoResponse, Error> {
    let file = File::open(&path).await.map_err(dump::Error::from)?;
    let meta = file.metadata().await.map_err(dump::Error::from)?;

    let file_name = path
        .file_name()
        .map(|os_str| os_str.to_string_lossy())
        .unwrap_or_else(|| "dump.json.gz".into());

    let last_modified = date.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let body = {
        let stream = ReaderStream::with_capacity(file, DUMP_READ_BUFF_SIZE);
        StreamBody::new(stream)
    };

    let headers = [
        (header::CONTENT_TYPE, "application/x-gzip".to_string()),
        (header::CONTENT_LENGTH, format!("{}", meta.len())),
        (header::LAST_MODIFIED, last_modified),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{file_name}\""),
        ),
    ];

    Ok((headers, body))
}

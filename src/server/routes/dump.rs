use std::sync::Arc;

use axum::body::StreamBody;
use axum::http::header;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::server::error::Error;
use crate::server::state::State;
use crate::storage::dump;

/// Size of the buffer used to stream files
const DUMP_READ_BUFF_SIZE: usize = 16 * 1024 * 1024; // 16 MB

pub async fn latest_dump(state: Arc<State>) -> Result<impl IntoResponse, Error> {
    let (date, path): (DateTime<Utc>, _) =
        state.dumps_registry.latest().await?.ok_or(Error::NoDump)?;

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

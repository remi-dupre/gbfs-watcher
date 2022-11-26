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

use async_compression::tokio::write::GzipEncoder;
use chrono::{DateTime, Utc};
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::path::PathBuf;
use tempdir::TempDir;
use thiserror::Error as ThisError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, info, warn};

use super::dir_lock::DirLock;
use crate::util::serialize_with_display;

/// Size of the file buffer while writing dumps
const DUMP_BUF_SIZE: usize = 16 * 1024 * 1024; // 16 MB

#[derive(Debug, Serialize, ThisError)]
pub enum Error {
    #[error("dump registry is already locked: {0}")]
    AlreadyLocked(#[from] super::dir_lock::Error),

    #[error("could not serialize dump entry: {0}")]
    Serialization(
        #[from]
        #[serde(serialize_with = "serialize_with_display")]
        serde_json::Error,
    ),

    #[error("I/O error: {0}")]
    IO(
        #[from]
        #[serde(serialize_with = "serialize_with_display")]
        std::io::Error,
    ),
}

pub struct DumpRegistry {
    path: DirLock,
}

impl DumpRegistry {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let path = DirLock::lock(path).await?;
        Ok(Self { path })
    }

    pub async fn init_dump(&self) -> Result<DumpBuilder, Error> {
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let filename = format!("dump_{now}.jsonl.gz");
        DumpBuilder::new(self, filename).await
    }

    pub async fn latest(&self) -> Result<Option<(DateTime<Utc>, PathBuf)>, Error> {
        self.iter()
            .await?
            .try_fold(None, |max, x| {
                let res = std::cmp::max(max, Some(x));
                future::ready(Ok(res))
            })
            .await
    }

    pub async fn iter(
        &self,
    ) -> Result<impl Stream<Item = Result<(DateTime<Utc>, PathBuf), Error>>, Error> {
        let stream = ReadDirStream::new(tokio::fs::read_dir(&*self.path).await?)
            .map(|res| res.map_err(Into::into))
            .try_filter_map(|entry| {
                let res = move || {
                    let filename = entry.file_name();

                    let filename = filename
                        .to_str()?
                        .strip_prefix("dump_")?
                        .strip_suffix(".jsonl.gz")?;

                    let dt = chrono::DateTime::parse_from_rfc3339(filename).ok()?;
                    Some((dt.into(), entry.path()))
                };

                future::ready(Ok(res()))
            });

        Ok(stream)
    }

    pub async fn cleanup(&self, kept: usize) -> Result<usize, Error> {
        if kept == 0 {
            return Ok(0);
        }

        let mut dumps: Vec<_> = self.iter().await?.try_collect().await?;
        dumps.sort_unstable_by_key(|(date, _)| *date);
        let count_removed = dumps.len().saturating_sub(kept);

        stream::iter(dumps)
            .take(count_removed)
            .inspect(|(_, path)| info!("Removing dump at {}", path.display()))
            .map(Ok)
            .try_for_each(|(_, path)| tokio::fs::remove_file(path))
            .await?;

        Ok(count_removed)
    }
}

#[must_use = "a dump builder must be closed"]
pub struct DumpBuilder<'r> {
    registry: &'r DumpRegistry,
    tmp_dir: TempDir,
    filename: String,
    file: GzipEncoder<BufWriter<File>>,
    has_closed: bool,
}

impl<'r> DumpBuilder<'r> {
    async fn new(registry: &'r DumpRegistry, filename: String) -> Result<DumpBuilder<'r>, Error> {
        let tmp_dir = TempDir::new_in(&*registry.path, "dump_part")?;
        let tmp_path = tmp_dir.path().join(&filename);

        let file = {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .await?;

            let file = BufWriter::with_capacity(DUMP_BUF_SIZE, file);
            GzipEncoder::with_quality(file, async_compression::Level::Best)
        };

        Ok(Self {
            registry,
            tmp_dir,
            filename,
            file,
            has_closed: false,
        })
    }

    pub async fn insert<O: Serialize>(mut self, obj: O) -> Result<DumpBuilder<'r>, Error> {
        let json = serde_json::to_string(&obj)?;
        self.file.write_all(json.as_bytes()).await?;
        self.file.write_u8(b'\n').await?;
        Ok(self)
    }

    pub async fn close(mut self) -> Result<(), Error> {
        // Flush the buffer
        self.file.shutdown().await?;

        // Move to target direction
        let tmp_path = self.tmp_dir.path().join(&self.filename);
        let target_path = self.registry.path.join(&self.filename);
        tokio::fs::rename(&tmp_path, &target_path).await?;

        // Now the handler can safely be dropped
        self.has_closed = true;
        Ok(())
    }
}

impl Drop for DumpBuilder<'_> {
    fn drop(&mut self) {
        if !self.has_closed {
            warn!("Dump creation was aborted without calling .close()");
        }
    }
}

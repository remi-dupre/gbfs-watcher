use std::fmt::{Debug, Display};
use std::path::PathBuf;
use std::time::Instant;

use async_compression::tokio::write::GzipEncoder;
use chrono::{DateTime, Utc};
use futures::{future, Stream, StreamExt, TryStreamExt};
use serde::Serialize;
use tempdir::TempDir;
use thiserror::Error as ThisError;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, info};

use super::dir_lock::DirLock;
use crate::util::serialize_with_display;

/// Size of the file buffer while writing dumps
const DUMP_BUF_SIZE: usize = 16 * 1024 * 1024; // 16 MB

#[derive(Debug, Serialize, ThisError)]
pub enum Error {
    #[error("dump registry is already locked: {0}")]
    AlreadyLocked(#[from] super::dir_lock::Error),

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

    pub async fn dump<E, O, S>(&self, mut stream: S) -> Result<(), Error>
    where
        E: Display,
        O: Serialize,
        S: Stream<Item = Result<O, E>> + Unpin,
    {
        let start_instant = Instant::now();
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let filename = format!("dump_{now}.jsonl.gz");
        let tmp_dir = TempDir::new_in(&*self.path, "dump_part")?;
        let tmp_path = tmp_dir.path().join(&filename);

        let mut count: u64 = 0;
        let mut count_errors: u64 = 0;

        {
            let mut file = {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&tmp_path)
                    .await?;

                let file = BufWriter::with_capacity(DUMP_BUF_SIZE, file);
                GzipEncoder::with_quality(file, async_compression::Level::Best)
            };

            while let Some(obj) = stream.next().await {
                let obj = match obj {
                    Ok(x) => x,
                    Err(err) => {
                        error!("Could not retrieve object for dump: {err}");
                        count_errors += 1;
                        continue;
                    }
                };

                let json = match serde_json::to_string(&obj) {
                    Ok(mut x) => {
                        x.push('\n');
                        x
                    }
                    Err(err) => {
                        error!("Could not serialize object for dump: {err}");
                        count_errors += 1;
                        continue;
                    }
                };

                file.write_all(json.as_bytes()).await?;
                count += 1;
            }

            // The encoder does not implicitly flush data
            file.shutdown().await?;
        }

        let path = self.path.join(&filename);
        tokio::fs::rename(&tmp_path, &path).await?;
        let elapsed = start_instant.elapsed();

        info!(
            path = format!("{}", path.display()),
            count = count,
            errors = {
                if count_errors == 0 {
                    None
                } else {
                    Some(count_errors)
                }
            },
            time = format!("{elapsed:.2?}"),
            "Dump finished"
        );

        Ok(())
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
            .try_filter_map(|entry| async move {
                let filename = entry.file_name();
                let filename = filename.to_string_lossy();

                let Some(filename) = filename.strip_prefix("dump_") else {
                    return Ok(None)
                };

                let Some(filename) = filename.strip_suffix(".jsonl.gz") else {
                    return Ok(None)
                };

                let Ok(dt) = chrono::DateTime::parse_from_rfc3339(filename) else {
                    return Ok(None)
                };

                Ok(Some((dt.into(), entry.path())))
            });

        Ok(stream)
    }
}

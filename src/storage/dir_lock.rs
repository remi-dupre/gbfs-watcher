use std::ops::Deref;
use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error as ThisError;
use tokio::fs::OpenOptions;

use crate::util::serialize_with_display;

#[derive(Debug, Serialize, ThisError)]
pub enum Error {
    #[error("directory is already locked by {}", lock_path.display())]
    AlreadyLocked { lock_path: PathBuf },

    #[error("could not create lock in {}: {source}", lock_path.display())]
    IO {
        lock_path: PathBuf,

        #[serde(serialize_with = "serialize_with_display")]
        source: std::io::Error,
    },
}

pub struct DirLock {
    path: PathBuf,
}

impl DirLock {
    pub async fn lock(path: PathBuf) -> Result<Self, Error> {
        let lock_path = path.join(".lock");

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
            .await
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::AlreadyExists => Error::AlreadyLocked {
                    lock_path: lock_path.clone(),
                },
                _ => Error::IO {
                    lock_path: lock_path.clone(),
                    source: err,
                },
            })?;

        Ok(Self { path })
    }
}

impl Deref for DirLock {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        let lock_path = self.path.join(".lock");

        std::fs::remove_file(lock_path).unwrap_or_else(|err| {
            panic!("could not cleanup lock at {}: {err}", self.path.display())
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempdir::TempDir;

    #[tokio::test]
    async fn cannot_lock_missing() {
        let path: PathBuf = "/tmp/this_is_unlikely_to_exist".into();
        assert!(matches!(DirLock::lock(path).await, Err(Error::IO { .. })));
    }

    #[tokio::test]
    async fn cannot_lock_twice() {
        let tempdir = TempDir::new("test-dir_lock").unwrap();
        let _lock_1 = DirLock::lock(tempdir.as_ref().to_path_buf()).await.unwrap();

        assert!(matches!(
            DirLock::lock(tempdir.as_ref().to_path_buf()).await,
            Err(Error::AlreadyLocked { .. })
        ));
    }

    #[tokio::test]
    async fn can_unlock() {
        let tempdir = TempDir::new("test-dir_lock").unwrap();

        {
            DirLock::lock(tempdir.as_ref().to_path_buf()).await.unwrap();
        }

        DirLock::lock(tempdir.as_ref().to_path_buf()).await.unwrap();
    }
}

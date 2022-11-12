use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;

use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use thiserror::Error as ThisError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use tracing::debug;

use crate::gbfs::models;
use crate::storage::binary::Binary;

use super::binary;

/// Number of cached elements at the end of the journal
const JOURNAL_CACHE_SIZE: usize = 4096; // This will fit in 200 MB for 1500 journals of 27 bits
                                        // entries

/// Size of file cache while performing sequencial reads
const SEQ_READ_CACHE_SIZE: usize = 128 * 1024; // 128 kB

pub type StationStatusJournal = Journal<{ binary::STATION_STATUS_BIN_SIZE }, models::StationStatus>;

#[derive(Debug, ThisError)]
pub enum Error<K> {
    #[error("could not open journal: {0}")]
    OpenError(std::io::Error),
    #[error("journal I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("trying to insert an entry small than last one: {last:?} >= {inserted:?}")]
    DecreasingKey { last: K, inserted: K },
}

pub trait JournalObject {
    type Key: Eq + Ord;
    fn get_key(&self) -> Self::Key;
    fn same_entry_as(&self, other: &Self) -> bool;
}

impl JournalObject for models::StationStatus {
    type Key = models::Timestamp;

    fn get_key(&self) -> Self::Key {
        self.last_reported
    }

    fn same_entry_as(&self, other: &Self) -> bool {
        let key = |obj: &Self| {
            (
                obj.station_id,
                obj.num_bikes_available,
                obj.num_docks_available,
                obj.num_docks_disabled,
                obj.is_installed,
                obj.is_returning,
                obj.is_renting,
                obj.num_bikes_available_types,
            )
        };

        key(self) == key(other)
    }
}

pub struct Journal<const BIN_SIZE: usize, T> {
    size: usize,
    path: PathBuf,
    cache: VecDeque<T>,
    _phantom: PhantomData<T>,
}

impl<const BIN_SIZE: usize, T> Journal<BIN_SIZE, T>
where
    T: Clone + Debug + Eq + Binary<BIN_SIZE> + JournalObject,
{
    pub async fn open(path: PathBuf) -> Result<Self, Error<T::Key>> {
        let mut size = 0;
        let mut cache = VecDeque::with_capacity(JOURNAL_CACHE_SIZE);

        match File::open(&path).await {
            Ok(file) => {
                stream_from_current_pos(file)
                    .await?
                    .try_for_each(|obj| {
                        size += 1;
                        push_cache(&mut cache, obj);
                        futures::future::ready(Ok(()))
                    })
                    .await?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(Error::OpenError(err)),
        };

        debug!("Opened {} with {size} entries", path.display());

        Ok(Self {
            size,
            path,
            cache,
            _phantom: PhantomData::default(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn last(&self) -> Option<&T> {
        self.cache.back()
    }

    pub async fn insert(&mut self, obj: T) -> Result<bool, Error<T::Key>> {
        if let Some(last_obj) = self.last() {
            if last_obj.same_entry_as(&obj) {
                return Ok(false);
            } else if last_obj.get_key() >= obj.get_key() {
                return Err(Error::DecreasingKey {
                    last: last_obj.get_key(),
                    inserted: obj.get_key(),
                });
            }
        }

        debug!("New entry: {obj:?}");

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        file.write_all(&obj.serialize()).await?;
        push_cache(&mut self.cache, obj);
        self.size += 1;
        Ok(true)
    }

    pub async fn iter_from(
        &self,
        index: usize,
    ) -> Result<impl Stream<Item = Result<Cow<T>, Error<T::Key>>>, Error<T::Key>> {
        let file_stream = {
            stream::iter({
                if self.len().saturating_sub(index) <= JOURNAL_CACHE_SIZE {
                    None
                } else {
                    let file_index = (index * BIN_SIZE).try_into().expect("invalid file index");
                    let mut file = File::open(&self.path).await?;
                    file.seek(SeekFrom::Start(file_index)).await?;
                    Some(stream_from_current_pos(file).await)
                }
            })
            .try_flatten()
            .map(|res| res.map(Cow::Owned))
        };

        let cache_stream = stream::iter(&self.cache)
            .skip((JOURNAL_CACHE_SIZE + index).saturating_sub(self.len()))
            .map(Cow::Borrowed)
            .map(Ok);

        let stream = file_stream
            .take(self.len() - self.cache.len())
            .chain(cache_stream);

        Ok(stream)
    }

    pub async fn iter(
        &self,
    ) -> Result<impl Stream<Item = Result<Cow<T>, Error<T::Key>>>, Error<T::Key>> {
        self.iter_from(0).await
    }
}

pub async fn stream_from_current_pos<const BIN_SIZE: usize, T: Binary<BIN_SIZE> + JournalObject>(
    reader: impl AsyncRead + Unpin,
) -> Result<impl Stream<Item = Result<T, Error<T::Key>>>, Error<T::Key>> {
    let cached_reader = BufReader::with_capacity(SEQ_READ_CACHE_SIZE, reader);

    let stream = stream::try_unfold(cached_reader, |mut cached_reader| async move {
        let mut buffer = [0; BIN_SIZE];

        match cached_reader.read_exact(&mut buffer).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        Ok(Some((T::deserialize(&buffer), cached_reader)))
    });

    Ok(stream)
}

fn push_cache<T>(cache: &mut VecDeque<T>, obj: T) -> Option<T> {
    let mut poped = None;

    while cache.len() >= JOURNAL_CACHE_SIZE {
        poped = cache.pop_front();
    }

    cache.push_back(obj);
    poped
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::test_journal::{TestObj, TestObjJournal};

    #[tokio::test]
    async fn read_journal() {
        let mut journal = TestObjJournal::new().await;

        let objects: Vec<_> = (0..1024)
            .map(|x| TestObj {
                timestamp: x,
                content: (x * x) as _,
            })
            .collect();

        for obj in &objects {
            journal.insert(obj.clone()).await.unwrap();
        }

        let from_journal: Vec<_> = journal
            .iter()
            .await
            .unwrap()
            .map(|x| x.unwrap().into_owned())
            .collect()
            .await;

        assert_eq!(objects, from_journal);
    }

    #[tokio::test]
    async fn read_journal_with_duplicates() {
        let mut journal = TestObjJournal::new().await;

        let objects: Vec<_> = (0..1024)
            .map(|x| TestObj {
                timestamp: x,
                content: (x * x) as _,
            })
            .collect();

        for obj in &objects {
            journal.insert(obj.clone()).await.unwrap();
            journal.insert(obj.clone()).await.unwrap();
            journal.insert(obj.clone()).await.unwrap();
        }

        let from_journal: Vec<_> = journal
            .iter()
            .await
            .unwrap()
            .map(|x| x.unwrap().into_owned())
            .collect()
            .await;

        assert_eq!(objects, from_journal);
    }
}

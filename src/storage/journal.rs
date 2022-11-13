use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use futures::{future, Future};
use serde::Serialize;
use thiserror::Error as ThisError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use tracing::debug;

use super::binary;
use crate::gbfs::models;
use crate::storage::binary::Binary;
use crate::util::serialize_with_display;

/// Number of cached elements at the end of the journal
const JOURNAL_CACHE_SIZE: usize = 4096; // This will fit in 200 MB for 1500 journals of 27 bits
                                        // entries

/// Size of file cache while performing sequencial reads
const SEQ_READ_CACHE_SIZE: usize = 128 * 1024; // 128 kB

pub type StationStatusJournal = Journal<{ binary::STATION_STATUS_BIN_SIZE }, models::StationStatus>;
pub type StationStatusJournalError = Error<models::StationId>;

#[derive(Debug, Serialize, ThisError)]
pub enum Error<K> {
    #[serde(serialize_with = "serialize_with_display")]
    #[error("could not open journal: {0}")]
    OpenError(std::io::Error),

    #[serde(serialize_with = "serialize_with_display")]
    #[error("journal I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[serde(serialize_with = "serialize_with_display")]
    #[error("file is too large for current architecture: {0}")]
    FileTooLarge(std::num::TryFromIntError),

    #[error("corrupted journal")]
    CorruptedJournal,

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
    cache: VecDeque<Arc<T>>,
    _phantom: PhantomData<T>,
}

impl<const BIN_SIZE: usize, T> Journal<BIN_SIZE, T>
where
    T: Clone + Debug + Eq + Binary<BIN_SIZE> + JournalObject,
{
    pub async fn open(path: PathBuf) -> Result<Self, Error<T::Key>> {
        let (cache, size) = match File::open(&path).await {
            Ok(mut file) => {
                let metadata = file.metadata().await?;
                let file_size: usize = metadata.len().try_into().map_err(Error::FileTooLarge)?;

                if file_size % BIN_SIZE != 0 {
                    return Err(Error::CorruptedJournal);
                }

                let mut cache = VecDeque::with_capacity(JOURNAL_CACHE_SIZE);
                let size = file_size / BIN_SIZE;
                let cache_size = std::cmp::min(size, JOURNAL_CACHE_SIZE);

                file.seek(SeekFrom::End(
                    -i64::try_from(cache_size * BIN_SIZE).expect("invalid file seek"),
                ))
                .await?;

                stream_from_current_pos(file)
                    .await?
                    .try_for_each(|obj| {
                        push_cache(&mut cache, obj);
                        future::ready(Ok(()))
                    })
                    .await?;

                (cache, size)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                (VecDeque::with_capacity(JOURNAL_CACHE_SIZE), 0)
            }
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

    pub fn last(&self) -> Option<Arc<T>> {
        self.cache.back().cloned()
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
        file.flush().await?;
        push_cache(&mut self.cache, obj);
        self.size += 1;
        Ok(true)
    }

    pub async fn iter(
        &self,
    ) -> Result<impl Stream<Item = Result<Arc<T>, Error<T::Key>>> + '_, Error<T::Key>> {
        self.iter_from_index(0).await
    }

    pub async fn iter_from_index(
        &self,
        index: usize,
    ) -> Result<impl Stream<Item = Result<Arc<T>, Error<T::Key>>> + '_, Error<T::Key>> {
        self.iter_from_index_impl(index, || File::open(&self.path))
            .await
    }

    pub async fn iter_from(
        &self,
        lower: T::Key,
    ) -> Result<impl Stream<Item = Result<Arc<T>, Error<T::Key>>> + '_, Error<T::Key>>
    where
        T::Key: std::fmt::Display,
    {
        if self.is_empty() {
            todo!("empty case not yet implemented")
        }

        // TODO: use once_cell so that it is not required to open file when hiting cache only,
        //       may also simplfy iter_from_index_impl?
        let mut file = File::open(&self.path).await?;
        let mut min = 0;
        let mut max = self.len();

        while max - min > 1 {
            let mid = (max + min + 1) / 2;

            let mid_obj = self
                .get_impl(mid, &mut file)
                .await?
                .expect("journal shrank during iteration");

            if mid_obj.get_key() <= lower {
                min = mid;
            } else {
                max = mid;
            }
        }

        self.iter_from_index_impl(min, move || future::ready(Ok(file)))
            .await
    }

    async fn iter_from_index_impl<F>(
        &self,
        index: usize,
        build_file: impl FnOnce() -> F,
    ) -> Result<impl Stream<Item = Result<Arc<T>, Error<T::Key>>> + '_, Error<T::Key>>
    where
        F: Future<Output = Result<File, std::io::Error>>,
    {
        let file_stream = {
            stream::iter({
                if self.len().saturating_sub(index) <= JOURNAL_CACHE_SIZE {
                    None
                } else {
                    let file_index = (index * BIN_SIZE).try_into().expect("invalid file index");
                    let mut file = build_file().await?;
                    file.seek(SeekFrom::Start(file_index)).await?;
                    Some(stream_from_current_pos(file).await)
                }
            })
            .try_flatten()
            .map(|x| x.map(Arc::new))
            .take(self.len().saturating_sub(index + self.cache.len()))
        };

        let cache_stream = stream::iter(&self.cache)
            .skip((index + self.cache.len()).saturating_sub(self.len()))
            .map(|x| Ok(x.clone()));

        let stream = file_stream.chain(cache_stream);
        Ok(stream)
    }

    async fn get_impl(
        &self,
        index: usize,
        file: &mut File,
    ) -> Result<Option<Arc<T>>, Error<T::Key>> {
        Ok({
            if index >= self.len() {
                None
            } else if self.len() - index < self.cache.len() {
                self.cache
                    .get(index - (self.len() - self.cache.len()))
                    .cloned()
            } else {
                let mut buffer = [0; BIN_SIZE];

                file.seek(SeekFrom::Start(
                    (index * BIN_SIZE).try_into().expect("invalid seek"),
                ))
                .await?;

                file.read_exact(&mut buffer).await?;
                let obj = T::deserialize(&buffer);
                Some(Arc::new(obj))
            }
        })
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

fn push_cache<T>(cache: &mut VecDeque<Arc<T>>, obj: T) -> Option<Arc<T>> {
    let mut poped = None;

    while cache.len() >= JOURNAL_CACHE_SIZE {
        poped = cache.pop_front();
    }

    cache.push_back(Arc::new(obj));
    poped
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::test_journal::{TestObj, TestObjJournal};
    use tempdir::TempDir;

    fn build_fake_objects(range: impl Iterator<Item = u64>) -> Vec<TestObj> {
        range
            .map(|x| TestObj {
                timestamp: x,
                content: (x * x) as _,
            })
            .collect()
    }

    async fn test_read_journal(range: impl Iterator<Item = u64>) {
        let mut journal = TestObjJournal::new().await;
        let objects = build_fake_objects(range);

        for obj in &objects {
            journal.insert(obj.clone()).await.unwrap();
        }

        let from_journal: Vec<_> = journal
            .iter()
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        assert_eq!(objects, from_journal);
    }

    #[tokio::test]
    async fn read_journal() {
        test_read_journal(0..64).await
    }

    #[tokio::test]
    async fn read_journal_large() {
        test_read_journal(0..10_000).await
    }

    #[tokio::test]
    async fn reopen_db() {
        let tempdir = TempDir::new("test-journal").unwrap();
        let path = tempdir.path().join("test.journal");
        let objects = build_fake_objects(0..10_000);

        {
            let mut journal = Journal::open(path.clone()).await.unwrap();

            for obj in &objects {
                assert!(journal.insert(obj.clone()).await.unwrap());
            }
        }

        let journal = Journal::open(path).await.unwrap();

        let from_journal: Vec<_> = journal
            .iter()
            .await
            .unwrap()
            .map(|x| x.unwrap())
            .map(|x: Arc<TestObj>| x.as_ref().clone())
            .collect()
            .await;

        assert_eq!(objects, from_journal);
    }

    #[tokio::test]
    async fn read_journal_with_duplicates() {
        let mut journal = TestObjJournal::new().await;
        let objects = build_fake_objects(0..64);

        for obj in &objects {
            assert!(journal.insert(obj.clone()).await.unwrap());
            assert!(!journal.insert(obj.clone()).await.unwrap());
            assert!(!journal.insert(obj.clone()).await.unwrap());
        }

        let from_journal: Vec<_> = journal
            .iter()
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        assert_eq!(objects, from_journal);
    }

    #[tokio::test]
    async fn iter_from_index() {
        let mut journal = TestObjJournal::new().await;
        let objects = build_fake_objects(0..10_000);

        for obj in &objects {
            assert!(journal.insert(obj.clone()).await.unwrap());
        }

        let from_journal_0: Vec<_> = journal
            .iter_from_index(0)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        let from_journal_1000: Vec<_> = journal
            .iter_from_index(1000)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        let from_journal_9990: Vec<_> = journal
            .iter_from_index(9990)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        assert_eq!(objects[0..], from_journal_0);
        assert_eq!(objects[1000..], from_journal_1000);
        assert_eq!(objects[9990..], from_journal_9990);
    }

    #[tokio::test]
    async fn iter_from() {
        let mut journal = TestObjJournal::new().await;
        let objects = build_fake_objects(0..10_000);

        for obj in &objects {
            assert!(journal.insert(obj.clone()).await.unwrap());
        }

        let from_journal_0: Vec<_> = journal
            .iter_from(0)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        let from_journal_1000: Vec<_> = journal
            .iter_from(1000)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        let from_journal_9990: Vec<_> = journal
            .iter_from(9990)
            .await
            .unwrap()
            .map(|x| x.unwrap().as_ref().clone())
            .collect()
            .await;

        assert_eq!(objects[0..], from_journal_0);
        assert_eq!(objects[1000..], from_journal_1000);
        assert_eq!(objects[9990..], from_journal_9990);
    }
}

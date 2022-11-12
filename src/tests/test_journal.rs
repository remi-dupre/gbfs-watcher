use std::fmt::Debug;
use std::io::{Cursor, Write};
use std::ops::{Deref, DerefMut};

use tempdir::TempDir;

use crate::storage::binary::Binary;
use crate::storage::journal::{Journal, JournalObject};

pub type TestObjJournal = TestJournal<24, TestObj>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestObj {
    pub timestamp: u64,
    pub content: u128,
}

impl Binary<24> for TestObj {
    fn serialize(&self) -> [u8; 24] {
        let &Self { timestamp, content } = self;
        let mut buf = Cursor::new([0; 24]);
        buf.write_all(&timestamp.to_be_bytes()).unwrap();
        buf.write_all(&content.to_be_bytes()).unwrap();
        buf.into_inner()
    }

    fn deserialize(buf: &[u8; 24]) -> Self {
        let timestamp = u64::from_be_bytes(buf[..8].try_into().unwrap());
        let content = u128::from_be_bytes(buf[8..].try_into().unwrap());
        Self { timestamp, content }
    }
}

impl JournalObject for TestObj {
    type Key = u64;

    fn get_key(&self) -> Self::Key {
        self.timestamp
    }

    fn same_entry_as(&self, other: &Self) -> bool {
        self.content == other.content
    }
}

pub struct TestJournal<const BIN_SIZE: usize, T> {
    _tempdir: TempDir,
    journal: Journal<BIN_SIZE, T>,
}

impl<const BIN_SIZE: usize, T> Deref for TestJournal<BIN_SIZE, T> {
    type Target = Journal<BIN_SIZE, T>;

    fn deref(&self) -> &Self::Target {
        &self.journal
    }
}

impl<const BIN_SIZE: usize, T> DerefMut for TestJournal<BIN_SIZE, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.journal
    }
}

impl<const BIN_SIZE: usize, T> TestJournal<BIN_SIZE, T>
where
    T: Clone + Debug + Eq + Binary<BIN_SIZE> + JournalObject,
    T::Key: Debug,
{
    pub async fn new() -> TestJournal<BIN_SIZE, T>
    where
        T: Clone + Debug + Eq + Binary<BIN_SIZE> + JournalObject,
        T::Key: Debug,
    {
        let tempdir = TempDir::new("test-journal").expect("could not create tempdir");

        let journal = Journal::open(tempdir.path().join("test.journal"))
            .await
            .expect("could not init new journal");

        TestJournal {
            _tempdir: tempdir,
            journal,
        }
    }
}

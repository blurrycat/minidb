use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use crate::log::{Log, LogError, LogOperation};

#[derive(thiserror::Error, Debug)]
pub enum DBError {
    #[error("underlying WAL error: {0}")]
    WAL(#[from] LogError),
}

pub type DbResult<T> = Result<T, DBError>;

pub(crate) type Collection = BTreeMap<Vec<u8>, Vec<u8>>;

/// Represents an in-memory key-value persisted database.
///
/// The database can store arbitrary binary data, length of keys and values must
/// not exceed what a u64 can fit.
/// Data is persisted to disk using a simple WAL, which is replayed when opening
/// the database directory.
///
// TODO: rewrite this
/// Compaction and snapshots are not yet available, so a database will grow
/// without bounds even when deleting data.
///
/// Note that all data is stored in memory at all times, the larger your database,
/// the larger the memory usage.
#[derive(Debug)]
pub struct Database {
    collection: Collection,
    log: Log,
}

impl Database {
    /// Open a `Database` from the specified `directory`.
    pub fn open(directory: impl AsRef<Path>) -> DbResult<Self> {
        // TODO: actually use the max size parameter here
        let mut log = Log::open(&directory, None)?;
        let mut collection = Collection::new();
        if !log.is_empty()? {
            log.replay(&mut collection)?;
        }
        Ok(Database { collection, log })
    }

    /// Insert `value` at `key` in the database.
    ///
    /// This can fail if writing to the WAL fails.
    pub fn put<K, V>(&mut self, key: K, value: V) -> DbResult<()>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
        self.log.append(LogOperation::Put(&key, &value))?;
        self.collection.insert(key, value);

        Ok(())
    }

    /// Retrieve the value at `key` in the database.
    ///
    /// This returns `None` if `key` is not present in the database.
    pub fn get<K>(&self, key: K) -> Option<&Vec<u8>>
    where
        K: Into<Vec<u8>>,
    {
        self.collection.get(&key.into())
    }

    /// Delete a key from the database.
    ///
    /// This can fail if writing to the WAL fails.
    pub fn delete<K>(&mut self, key: K) -> DbResult<()>
    where
        K: Into<Vec<u8>>,
    {
        let key = key.into();
        // We only care about appending the log if we actually had a key to delete
        if self.collection.remove(&key).is_some() {
            self.log.append(LogOperation::Delete(&key))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_put() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut db = Database::open(tempdir.path()).unwrap();

        db.put("key", "value").unwrap();
        assert_eq!(db.get("key"), Some(&b"value".to_vec()))
    }

    #[test]
    fn test_database_delete() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut db = Database::open(tempdir.path()).unwrap();

        db.put("key", "value").unwrap();
        db.delete("key").unwrap();
        assert_eq!(db.get("key"), None)
    }

    #[test]
    fn test_database_replay() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut db = Database::open(tempdir.path()).unwrap();

        db.put("key", "value").unwrap();
        db.put("key2", "value2").unwrap();
        db.delete("key").unwrap();

        // Simulate reading an existing DB from disk
        drop(db);
        let db = Database::open(tempdir.path()).unwrap();
        assert_eq!(db.get("key"), None);
        assert_eq!(db.get("key2"), Some(&b"value2".to_vec()));
    }
}

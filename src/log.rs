use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::db::Collection;

const MAIN_WAL_FILENAME: &str = "main.wal";

#[derive(thiserror::Error, Debug)]
pub enum LogError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("de/serialization error: {0}")]
    Encoding(#[from] bincode::Error),

    #[error("path '{0}' is not valid unicode")]
    InvalidPath(PathBuf),
    #[error("path '{0}' is not a directory")]
    NotADirectory(PathBuf),
}

pub type LogResult<T> = Result<T, LogError>;

/// An owned version for a LogOperation for deserializing purposes.
#[derive(Debug, PartialEq, Eq, Deserialize)]
enum OwnedLogOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Represents an operation to be applied to the log.
#[derive(Debug, Serialize)]
pub enum LogOperation<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
}

#[allow(dead_code)]
impl<'a> LogOperation<'a> {
    fn to_owned(&self) -> OwnedLogOperation {
        match self {
            LogOperation::Put(key, value) => OwnedLogOperation::Put(key.to_vec(), value.to_vec()),
            LogOperation::Delete(key) => OwnedLogOperation::Delete(key.to_vec()),
        }
    }
}

/// A simple Write-Ahead Log persisted to disk in a directory.
///
/// The WAL can be rotated so that older data can rest on-disk. Over time this
/// data will accumulate, and can be compacted into a single new snapshot containing
/// all currently active data, clearing out any deleted data from snapshots.
#[derive(Debug)]
pub struct Log {
    path: PathBuf,
    current_snapshot: RefCell<u8>,
    current_file: File,
    max_log_size: Option<u64>,
}

impl Log {
    /// Open a directory in which the WAL will be kept.
    ///
    /// This will create or open a `main.wal` file inside that directory.
    pub fn open(path: impl AsRef<Path>, max_log_size: Option<u64>) -> LogResult<Self> {
        // Ensure path is valid unicode (so that we can safely unwrap it down the line)
        if path.as_ref().to_str().is_none() {
            return Err(LogError::InvalidPath(path.as_ref().into()));
        }
        // Ensure path is a directory
        if !path.as_ref().is_dir() {
            return Err(LogError::NotADirectory(path.as_ref().into()));
        }

        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path.as_ref().join(MAIN_WAL_FILENAME))?;

        let log = Log {
            current_file: file,
            current_snapshot: RefCell::new(0),
            path: path.as_ref().into(),
            max_log_size,
        };
        log.update_snapshot_count()?;

        Ok(log)
    }

    fn list_snapshots(&self) -> LogResult<Vec<PathBuf>> {
        // This is safe because we have already checked in `open()` that the path
        // is valid unicode.
        let pattern = self.path.join("snapshot_*.wal");
        let pattern = pattern.to_str().unwrap();
        // TODO: check that this is safe. it should be since the pattern is a valid path in the first place
        let snapshots = glob::glob(pattern).unwrap();

        let snapshots: Vec<PathBuf> = snapshots.filter_map(|path| path.ok()).collect();

        Ok(snapshots)
    }

    fn update_snapshot_count(&self) -> LogResult<()> {
        let snapshots = self.list_snapshots()?;

        // TODO: actually check that this fits in a u8
        self.current_snapshot.replace(snapshots.len() as u8);

        Ok(())
    }

    /// Append a log operation to the WAL.
    ///
    /// This method may rotate the current WAL file if it is bigger than the
    /// optional limit.
    pub fn append(&mut self, op: LogOperation) -> LogResult<()> {
        // If the current main log file is bigger than the limit we set, rotate it
        if let Some(max_log_size) = self.max_log_size {
            if self.main_log_size()? >= max_log_size {
                self.rotate()?;
            }
        }
        write_operation(op, &mut self.current_file)?;
        // Ensure data is flushed to disk before returning from the operation
        self.current_file.sync_data()?;

        Ok(())
    }

    /// Return `true` if the WAL is empty.
    ///
    /// This checks for any snapshots present in the log directory, and will check
    /// all their sizes.
    pub fn is_empty(&self) -> LogResult<bool> {
        let mut total_size = 0;

        let snapshots = self.list_snapshots()?;
        for snapshot in snapshots {
            total_size += snapshot.metadata()?.len();
        }

        total_size += self.current_file.metadata()?.len();

        Ok(total_size == 0)
    }

    fn main_log_size(&self) -> LogResult<u64> {
        Ok(self.current_file.metadata()?.len())
    }

    /// Replay the WAL into the specified `collection`.
    pub fn replay(&mut self, collection: &mut Collection) -> LogResult<()> {
        let snapshots = self.list_snapshots()?;
        for snapshot in snapshots.into_iter() {
            replay_file(snapshot, collection)?;
        }

        replay_file(self.path.join(MAIN_WAL_FILENAME), collection)?;

        Ok(())
    }

    /// Rotate the log file by renaming it and creating a new one in its place.
    ///
    /// This may perform a compaction if we have reached the maximum number of
    /// snapshots in the directory (255).
    pub fn rotate(&mut self) -> LogResult<()> {
        let current_snapshot = *self.current_snapshot.borrow();
        let main_path = self.path.join(MAIN_WAL_FILENAME);
        let new_path = self
            .path
            .join(format!("snapshot_{:0>3}.wal", current_snapshot));
        std::fs::rename(&main_path, new_path)?;

        if *self.current_snapshot.borrow() < u8::MAX {
            self.current_snapshot.replace(current_snapshot + 1);
        } else {
            self.compact()?;
        }

        let new_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(main_path)?;
        self.current_file = new_file;

        Ok(())
    }

    /// Compact all snapshots into a single new snapshot.
    ///
    /// This process will prune any data that has been deleted, and only retain
    /// the Put operations.
    ///
    /// Note this compaction process will never touch the main log file, you
    /// should rotate it beforehand if you care about pruning all deleted data.
    pub fn compact(&self) -> LogResult<()> {
        let snapshots = self.list_snapshots()?;

        let mut collection = Collection::new();
        for snapshot in snapshots.iter() {
            replay_file(snapshot, &mut collection)?;
        }

        let tmp_snapshot_path = self.path.join("tmp_snapshot.wal");
        let tmp_snapshot = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_snapshot_path)?;
        // This should help reducing the amount of write syscalls we make
        let mut bufwriter = BufWriter::new(&tmp_snapshot);

        for (key, value) in collection.into_iter() {
            let op = LogOperation::Put(&key, &value);
            write_operation(op, &mut bufwriter)?;
        }

        // Flush all data to disk
        tmp_snapshot.sync_all()?;

        // Remove all old snapshots now that all data is safely stored to disk
        for snapshot in snapshots.iter() {
            std::fs::remove_file(snapshot)?;
        }

        // Reset the snapshot counter since we're going to write the first one again
        self.current_snapshot.replace(1);
        // Rename the new compacted snapshot as the 0 snapshot
        std::fs::rename(tmp_snapshot_path, self.path.join("snapshot_000.wal"))?;

        Ok(())
    }
}

fn write_operation<W>(op: LogOperation, writer: &mut W) -> LogResult<()>
where
    W: Write + Seek,
{
    writer.seek(io::SeekFrom::End(0))?;

    bincode::serialize_into(writer, &op)?;
    Ok(())
}

/// Replay a single log file into the specified collection.
fn replay_file(path: impl AsRef<Path>, collection: &mut Collection) -> LogResult<()> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut bufreader = BufReader::new(file.try_clone()?);

    loop {
        let op: OwnedLogOperation = match bincode::deserialize_from(bufreader.by_ref()) {
            Ok(op) => op,
            Err(e) => match *e {
                // We have reached the end of the Log file, stop the replay
                bincode::ErrorKind::Io(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                _ => return Err(e.into()),
            },
        };
        match op {
            OwnedLogOperation::Put(key, value) => collection.insert(key, value),
            OwnedLogOperation::Delete(key) => collection.remove(&key),
        };
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, io::Read, os::unix::prelude::OsStringExt};

    use super::*;

    #[test]
    fn test_log_op_put() {
        let op = LogOperation::Put(b"key", b"value");
        let serialized_op = bincode::serialize(&op).unwrap();
        assert_eq!(
            serialized_op,
            [
                0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 0, 0, 0, 0, 118, 97,
                108, 117, 101
            ]
        );
        let deserialized_op: OwnedLogOperation = bincode::deserialize(&serialized_op).unwrap();
        assert_eq!(deserialized_op, op.to_owned());
    }

    #[test]
    fn test_log_op_delete() {
        let op = LogOperation::Delete(b"key");
        let serialized_op = bincode::serialize(&op).unwrap();
        assert_eq!(
            bincode::serialize(&op).unwrap(),
            [1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 107, 101, 121]
        );
        let deserialized_op: OwnedLogOperation = bincode::deserialize(&serialized_op).unwrap();
        assert_eq!(deserialized_op, op.to_owned());
    }

    #[test]
    fn test_log_invalid_path() {
        let path = vec![b'/', 0xC3];
        let path = PathBuf::from(OsString::from_vec(path));
        let open_result = Log::open(&path, None);

        let expected_error = LogError::InvalidPath(path);

        assert!(open_result.is_err());
        assert_eq!(
            open_result.unwrap_err().to_string(),
            expected_error.to_string()
        );
    }

    #[test]
    fn test_log_not_a_directory() {
        let path = tempfile::NamedTempFile::new().unwrap();
        let path = path.path();
        let open_result = Log::open(path, None);

        let expected_error = LogError::NotADirectory(path.into());

        assert!(open_result.is_err());
        assert_eq!(
            open_result.unwrap_err().to_string(),
            expected_error.to_string()
        );
    }

    #[test]
    fn test_log_append() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log = Log::open(tempdir.path(), None).unwrap();

        let op = LogOperation::Put(b"key", b"value");
        let op_bytes = bincode::serialize(&op).unwrap();

        log.append(op).unwrap();

        // Read the underlying file and compare its contents to the serialized operation
        let mut buf = vec![];
        log.current_file.seek(io::SeekFrom::Start(0)).unwrap();
        log.current_file.read_to_end(&mut buf).unwrap();

        assert_eq!(op_bytes, buf);
    }

    #[test]
    fn test_log_file_replay() {
        let mut collection = Collection::new();
        replay_file("./tests/data/replay.wal", &mut collection).unwrap();

        assert_eq!(collection.get(&b"key".to_vec()), None);
        assert_eq!(collection.get(&b"key1".to_vec()), Some(&"value1".into()));
    }

    // TODO: this test passes but is completely wrong now that we use serde/bincode
    #[test]
    fn test_log_file_replay_invalid_op() {
        let mut collection = Collection::new();
        let replay_result = replay_file("./tests/data/invalid_op.wal", &mut collection);
        let expected_error = LogError::Encoding(bincode::Error::new(bincode::ErrorKind::Custom(
            "invalid value: integer `2`, expected variant index 0 <= i < 2".into(),
        )));
        assert!(replay_result.is_err());
        assert_eq!(
            replay_result.unwrap_err().to_string(),
            expected_error.to_string()
        )
    }

    #[test]
    fn test_log_rotate() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log = Log::open(tempdir.path(), None).unwrap();

        let op = LogOperation::Put(b"key", b"value");
        let op_bytes = bincode::serialize(&op).unwrap();
        log.append(op).unwrap();

        log.rotate().unwrap();

        let main_metadata = tempdir.path().join(MAIN_WAL_FILENAME).metadata().unwrap();
        let snapshot_metadata = tempdir.path().join("snapshot_000.wal").metadata().unwrap();

        assert!(main_metadata.is_file());
        assert_eq!(main_metadata.len(), 0);
        assert!(snapshot_metadata.is_file());
        assert_eq!(snapshot_metadata.len(), op_bytes.len() as u64);
    }

    #[test]
    fn test_log_rotate_replay() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log = Log::open(tempdir.path(), None).unwrap();

        let op = LogOperation::Put(b"key", b"value");
        log.append(op).unwrap();
        let op = LogOperation::Put(b"key_to_delete", b"value");
        log.append(op).unwrap();

        log.rotate().unwrap();

        let op = LogOperation::Delete(b"key_to_delete");
        log.append(op).unwrap();

        let mut collection = Collection::new();
        log.replay(&mut collection).unwrap();

        assert_eq!(collection.get(&b"key_to_delete".to_vec()), None);
        assert_eq!(collection.get(&b"key".to_vec()), Some(&"value".into()));
    }

    #[test]
    fn test_log_compaction() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log = Log::open(tempdir.path(), None).unwrap();

        let op = LogOperation::Put(b"key", b"value");
        let op1_bytes = bincode::serialize(&op).unwrap();
        log.append(op).unwrap();

        let op = LogOperation::Put(b"key_to_delete", b"value");
        log.append(op).unwrap();

        log.rotate().unwrap();

        let op = LogOperation::Delete(b"key_to_delete");
        log.append(op).unwrap();

        let op = LogOperation::Put(b"key2", b"value2");
        let op2_bytes = bincode::serialize(&op).unwrap();
        log.append(op).unwrap();

        log.rotate().unwrap();
        log.compact().unwrap();

        let main_metadata = tempdir.path().join(MAIN_WAL_FILENAME).metadata().unwrap();
        let snapshot_metadata = tempdir.path().join("snapshot_000.wal").metadata().unwrap();

        let snapshot_list = log.list_snapshots().unwrap();
        assert_eq!(snapshot_list.len(), 1);

        assert!(main_metadata.is_file());
        assert_eq!(main_metadata.len(), 0);
        assert!(snapshot_metadata.is_file());
        assert_eq!(
            snapshot_metadata.len(),
            (op1_bytes.len() + op2_bytes.len()) as u64
        );
    }

    #[test]
    fn test_log_auto_rotate() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut log = Log::open(tempdir.path(), Some(1)).unwrap(); // Should rotate after every insert

        let op = LogOperation::Put(b"key1", b"value");
        log.append(op).unwrap();

        let op = LogOperation::Put(b"key2", b"value");
        log.append(op).unwrap();

        let op = LogOperation::Put(b"key3", b"value");
        log.append(op).unwrap();

        assert!(!log.is_empty().unwrap());
        assert_eq!(*log.current_snapshot.borrow(), 2);
    }

    // #[test]
    // fn test_log_auto_compact() {
    //     todo!("auto compact")
    // }
}

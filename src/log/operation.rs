use serde::{Deserialize, Serialize};

use crate::db::Collection;

/// An owned version for a LogOperation for deserializing purposes.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub enum OwnedLogOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl OwnedLogOperation {
    pub fn as_log_operation(&self) -> LogOperation {
        match self {
            OwnedLogOperation::Put(key, value) => LogOperation::Put(key, value),
            OwnedLogOperation::Delete(key) => LogOperation::Delete(key),
        }
    }

    pub fn apply(&self, collection: &mut Collection) {
        self.as_log_operation().apply(collection)
    }
}

/// Represents an operation to be applied to the log.
#[derive(Debug, Serialize)]
pub enum LogOperation<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
}

#[allow(dead_code)]
impl<'a> LogOperation<'a> {
    pub fn to_owned(&self) -> OwnedLogOperation {
        match self {
            LogOperation::Put(key, value) => OwnedLogOperation::Put(key.to_vec(), value.to_vec()),
            LogOperation::Delete(key) => OwnedLogOperation::Delete(key.to_vec()),
        }
    }

    pub fn apply(&self, collection: &mut Collection) {
        match self {
            LogOperation::Put(key, value) => collection.insert(key.to_vec(), value.to_vec()),
            LogOperation::Delete(key) => collection.remove(*key),
        };
    }
}

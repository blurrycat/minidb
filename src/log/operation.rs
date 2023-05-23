use serde::{Deserialize, Serialize};

use crate::db::Collection;

/// An owned version for a LogOperation for deserializing purposes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
#[derive(Debug, Clone, Serialize)]
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

#[cfg(test)]
mod tests {
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
}

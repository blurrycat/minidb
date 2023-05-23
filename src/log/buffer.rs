use std::{
    io::{Seek, Write},
    time::{Duration, Instant},
};

use super::{write_operation, LogOperation, LogResult, OwnedLogOperation};

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub operation: OwnedLogOperation,
    instant: Instant,
}

impl<'a> From<LogOperation<'a>> for Entry {
    fn from(operation: LogOperation<'a>) -> Self {
        Entry {
            operation: operation.to_owned(),
            instant: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct OperationBuffer {
    capacity: usize,
    flush_every: Duration,
    entries: Vec<Entry>,
}

impl OperationBuffer {
    pub fn new(capacity: usize, flush_every: Duration) -> Self {
        OperationBuffer {
            capacity,
            flush_every,
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn iter(&self) -> std::slice::Iter<Entry> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn push<W>(&mut self, entry: Entry, writer: &mut W) -> LogResult<()>
    where
        W: Write + Seek,
    {
        // If we would exceed the capacity, flush the buffer
        if self.len() + 1 > self.capacity {
            self.flush(writer)?;
        }

        // If we haven't flushed the buffer in some time, do it
        if let Some(last_entry) = self.entries.last() {
            if Instant::now() > last_entry.instant + self.flush_every {
                self.flush(writer)?;
            }
        }

        self.entries.push(entry);

        Ok(())
    }

    pub fn flush<W>(&mut self, writer: &mut W) -> LogResult<()>
    where
        W: Write + Seek,
    {
        for op in self.entries.drain(..) {
            write_operation(op.operation.as_log_operation(), writer)?;
        }

        // Ensure data is flushed to disk before returning from the operation
        writer.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_push() {
        let mut dummy_file = tempfile::tempfile().unwrap();
        let mut buffer = OperationBuffer::new(10, Duration::from_secs(30));

        let op = LogOperation::Put(b"key", b"value");
        let entry: Entry = op.into();

        buffer.push(entry.clone(), &mut dummy_file).unwrap();

        assert_eq!(buffer.entries.len(), 1);
        assert_eq!(buffer.entries[0], entry);
    }

    #[test]
    fn test_buffer_flush() {
        let mut dummy_file = tempfile::tempfile().unwrap();
        let mut buffer = OperationBuffer::new(10, Duration::from_secs(30));

        let op = LogOperation::Put(b"key", b"value");

        buffer.push(op.into(), &mut dummy_file).unwrap();
        buffer.flush(&mut dummy_file).unwrap();

        let metadata = dummy_file.metadata().unwrap();
        assert_ne!(metadata.len(), 0);
        assert_eq!(buffer.entries.len(), 0);
    }

    #[test]
    fn test_buffer_auto_flush_capacity() {
        let mut dummy_file = tempfile::tempfile().unwrap();
        let mut buffer = OperationBuffer::new(3, Duration::from_secs(30));

        let op = LogOperation::Put(b"key", b"value");
        buffer.push(op.clone().into(), &mut dummy_file).unwrap();
        buffer.push(op.clone().into(), &mut dummy_file).unwrap();
        buffer.push(op.clone().into(), &mut dummy_file).unwrap();
        // This one should force a flush
        buffer.push(op.into(), &mut dummy_file).unwrap();

        let metadata = dummy_file.metadata().unwrap();
        assert_ne!(metadata.len(), 0);
        // There still should be one item in the buffer (the last one we added)
        assert_eq!(buffer.entries.len(), 1);
    }

    #[test]
    fn test_buffer_auto_flush_time() {
        let mut dummy_file = tempfile::tempfile().unwrap();
        let mut buffer = OperationBuffer::new(10, Duration::from_millis(250));

        let op = LogOperation::Put(b"key", b"value");
        buffer.push(op.clone().into(), &mut dummy_file).unwrap();

        // Sleep longer than the max flush interval
        std::thread::sleep(Duration::from_millis(300));

        // This one should force a flush since it's been some time now
        buffer.push(op.into(), &mut dummy_file).unwrap();

        let metadata = dummy_file.metadata().unwrap();
        assert_ne!(metadata.len(), 0);
        // There still should be one item in the buffer (the last one we added)
        assert_eq!(buffer.entries.len(), 1);
    }
}

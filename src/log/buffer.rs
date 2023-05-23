use std::{
    io::{Seek, Write},
    time::{Duration, Instant},
};

use super::{write_operation, LogOperation, LogResult, OwnedLogOperation};

#[derive(Debug)]
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

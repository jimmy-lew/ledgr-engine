//! # MemTable
//!
//! An ordered in-memory buffer.  Transactions are keyed by `(timestamp, id)`
//! so that when the table is flushed to an SSTable the rows are in timestamp
//! order, which improves zone-map effectiveness and makes merging cheaper.
//!
//! When `size_bytes` exceeds `FLUSH_THRESHOLD` the engine triggers a flush.

use std::collections::BTreeMap;

use crate::models::Transaction;

/// Flush when the MemTable holds more than ~4 MiB of data.
pub const FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Composite key that provides timestamp-ordered iteration.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
struct MemKey(u64 /* ts */, u64 /* id */);

pub struct MemTable {
    data: BTreeMap<MemKey, Transaction>,
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Insert a transaction.  Returns the new approximate size in bytes.
    pub fn insert(&mut self, tx: Transaction) -> usize {
        // Approximate: fixed fields + description UTF-8
        let size = 8 + 8 + 8 + 1 + 8 + 4 + tx.description.len();
        self.size_bytes += size;
        self.data.insert(MemKey(tx.timestamp, tx.id), tx);
        self.size_bytes
    }

    /// True when the table should be flushed to disk.
    pub fn needs_flush(&self) -> bool {
        self.size_bytes >= FLUSH_THRESHOLD_BYTES
    }

    /// Drain all rows in timestamp order (ready for SSTable flush).
    pub fn drain_sorted(&mut self) -> Vec<Transaction> {
        let rows: Vec<Transaction> = self.data.values().cloned().collect();
        self.data.clear();
        self.size_bytes = 0;
        rows
    }

    /// Non-destructive iterator for in-memory query paths.
    pub fn iter_sorted(&self) -> impl Iterator<Item = &Transaction> {
        self.data.values()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

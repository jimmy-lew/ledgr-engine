//! # Sparse Timestamp Index
//!
//! A sparse index stores one summary entry for every `SPARSE_FACTOR` rows
//! (default: 64).  This is the same technique used by Cassandra, Parquet,
//! and several LSM engines.
//!
//! ## Why sparse, not dense?
//!
//! A **dense** index has one entry per row and grows linearly with the
//! dataset.  A **sparse** index has 1/64th the entries while still bounding
//! the worst-case scan cost to 64 rows per "miss."  For columnar data where
//! a single sequential read of 64 × 8 bytes = 512 bytes is essentially free
//! in cache terms, sparse indexing is the right trade-off.
//!
//! ## On-Disk Format  (at `file_header.segments_end_offset`)
//!
//! ```text
//! [8 B]  entry_count  u64
//! [entry_count × 16 B]
//!   per entry:
//!     [8 B]  timestamp       u64  (of the indexed row)
//!     [8 B]  global_row_idx  u64  (0-based across all segments)
//! ```
//!
//! ## Query: `find_start_row(target_ts)`
//!
//! Binary-search the entries for the last entry with `timestamp <= target_ts`.
//! The result's `global_row_idx` is a safe lower bound: we may need to scan
//! backward at most `SPARSE_FACTOR - 1` rows from there (or just start
//! scanning forward from that row for the first matching timestamp).
//!
//! Because transactions are flushed in timestamp order (MemTable is sorted
//! before flushing), the sparse index is always in ascending order.

use std::io::{Read, Seek, SeekFrom, Write};
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use crate::error::Result;

/// One sparse index entry (16 bytes on disk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparseEntry {
    /// The timestamp of the globally `global_row_idx`-th transaction.
    pub timestamp:      u64,
    /// Zero-based index across all segments (not per-segment).
    pub global_row_idx: u64,
}

/// One sparse entry per this many rows.
pub const SPARSE_FACTOR: u64 = 64;

/// In-memory sparse index.
#[derive(Debug, Default, Clone)]
pub struct SparseIndex {
    pub entries: Vec<SparseEntry>,
}

impl SparseIndex {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    /// Build a fresh sparse index from an ordered slice of `(timestamp, global_row_idx)`.
    ///
    /// Called during flush after all segments are written.  The timestamps must
    /// already be in non-decreasing order (guaranteed by MemTable sort-on-flush).
    pub fn build(ordered_rows: &[(u64, u64)]) -> Self {
        let entries = ordered_rows
            .iter()
            .enumerate()
            .filter(|(i, _)| (*i as u64) % SPARSE_FACTOR == 0)
            .map(|(_, &(ts, idx))| SparseEntry { timestamp: ts, global_row_idx: idx })
            .collect();
        Self { entries }
    }

    // ── Disk I/O ───────────────────────────────────────────────────────────

    /// Write the index to `w` starting at the current position.
    /// Returns the number of bytes written.
    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        let count = self.entries.len() as u64;
        w.write_u64::<LE>(count)?;
        for e in &self.entries {
            w.write_u64::<LE>(e.timestamp)?;
            w.write_u64::<LE>(e.global_row_idx)?;
        }
        Ok(8 + self.entries.len() * 16)
    }

    /// Read from `r` at absolute file offset `offset`.
    pub fn read_from<R: Read + Seek>(r: &mut R, offset: u64, count: u64) -> Result<Self> {
        r.seek(SeekFrom::Start(offset))?;
        let stored_count = r.read_u64::<LE>()?;
        debug_assert_eq!(stored_count, count, "sparse index count mismatch");
        let n = stored_count as usize;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let timestamp      = r.read_u64::<LE>()?;
            let global_row_idx = r.read_u64::<LE>()?;
            entries.push(SparseEntry { timestamp, global_row_idx });
        }
        Ok(Self { entries })
    }

    // ── Query ──────────────────────────────────────────────────────────────

    /// Return the `global_row_idx` of the **first sparse entry** whose
    /// timestamp is `>= start_ts`.
    ///
    /// The caller should start scanning rows from this index (or from 0 if
    /// the result is None, meaning all entries are after `start_ts`).
    ///
    /// ## Complexity
    /// O(log n) binary search over the sparse entries.
    pub fn lower_bound_row(&self, start_ts: u64) -> u64 {
        if self.entries.is_empty() { return 0; }

        // Find the last sparse entry with timestamp <= start_ts.
        // That entry's global_row_idx is a safe starting point.
        match self.entries.binary_search_by_key(&start_ts, |e| e.timestamp) {
            Ok(pos) => {
                // Exact match – use this entry directly
                self.entries[pos].global_row_idx
            }
            Err(0) => {
                // All entries are after start_ts; start from row 0
                0
            }
            Err(pos) => {
                // entries[pos-1].timestamp < start_ts <= entries[pos].timestamp
                // Start from the earlier sparse boundary; a forward scan of at
                // most SPARSE_FACTOR rows will find the exact first match.
                self.entries[pos - 1].global_row_idx
            }
        }
    }

    /// Return the `global_row_idx` of the **last sparse entry** whose
    /// timestamp is `<= end_ts`.
    ///
    /// Used to bound the upper end of a range scan.
    pub fn upper_bound_row(&self, end_ts: u64) -> Option<u64> {
        if self.entries.is_empty() { return None; }
        let pos = match self.entries.binary_search_by_key(&end_ts, |e| e.timestamp) {
            Ok(pos)  => pos,
            Err(pos) => {
                if pos == 0 { return None; }  // all entries after end_ts
                pos - 1
            }
        };
        Some(self.entries[pos].global_row_idx + SPARSE_FACTOR)
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    // ── Merge (used when new rows are appended) ───────────────────────────

    /// Extend the index with new rows starting at `first_new_global_idx`.
    /// `new_rows` is a slice of `(timestamp, global_row_idx)` for the rows
    /// just flushed (already in timestamp order).
    pub fn extend(&mut self, new_rows: &[(u64, u64)]) {
        for (i, &(ts, global_idx)) in new_rows.iter().enumerate() {
            if global_idx % SPARSE_FACTOR == 0 || (i == 0 && self.entries.is_empty()) {
                self.entries.push(SparseEntry { timestamp: ts, global_row_idx: global_idx });
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod sparse_tests {
    use super::*;

    fn build_test_index(n: u64) -> (SparseIndex, Vec<(u64, u64)>) {
        let rows: Vec<(u64, u64)> = (0..n).map(|i| (i * 100, i)).collect();
        let idx = SparseIndex::build(&rows);
        (idx, rows)
    }

    #[test]
    fn build_creates_correct_entries() {
        let (idx, _) = build_test_index(256);
        // 256 rows / SPARSE_FACTOR(64) = 4 entries (rows 0, 64, 128, 192)
        assert_eq!(idx.len(), 4);
        assert_eq!(idx.entries[0].global_row_idx, 0);
        assert_eq!(idx.entries[1].global_row_idx, 64);
        assert_eq!(idx.entries[2].global_row_idx, 128);
        assert_eq!(idx.entries[3].global_row_idx, 192);
    }

    #[test]
    fn lower_bound_exact_match() {
        let (idx, _) = build_test_index(256);
        // Row 64 has timestamp 64*100 = 6400
        let row = idx.lower_bound_row(6400);
        assert_eq!(row, 64);
    }

    #[test]
    fn lower_bound_between_entries() {
        let (idx, _) = build_test_index(256);
        // Query ts=7000 falls between entry[1] (ts=6400, row=64) and entry[2] (ts=12800, row=128)
        // Should return row 64 (the safe lower bound)
        let row = idx.lower_bound_row(7_000);
        assert_eq!(row, 64);
    }

    #[test]
    fn lower_bound_before_all_entries() {
        let (idx, _) = build_test_index(256);
        let row = idx.lower_bound_row(0);
        assert_eq!(row, 0);
    }

    #[test]
    fn roundtrip_via_cursor() {
        use std::io::Cursor;
        let (idx, _) = build_test_index(192);
        let mut buf  = Cursor::new(Vec::new());
        let n_bytes  = idx.write_to(&mut buf).unwrap();
        assert_eq!(n_bytes, 8 + idx.len() * 16);

        let restored = SparseIndex::read_from(&mut buf, 0, idx.len() as u64).unwrap();
        assert_eq!(restored.entries, idx.entries);
    }
}

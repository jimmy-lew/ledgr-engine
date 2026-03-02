//! # Indexes
//!
//! ## BitmapIndex  (for `transaction_type`)
//!
//! Each distinct value maps to a **roaring-style** dense bitmap over
//! SSTable row positions.  Because `transaction_type` is a 1-bit enum
//! this degenerates to two bitsets – perfect for compressed storage.
//!
//! ```text
//!  value → BitVec over row positions
//!  DEBIT  → [1,0,1,1,0,…]
//!  CREDIT → [0,1,0,0,1,…]
//! ```
//!
//! ## AccountIndex  (for `account_id`)
//!
//! A standard `HashMap<account_id, Vec<RowRef>>` where `RowRef` identifies
//! the SSTable file and row index.  For range queries the entries are also
//! maintained in a `BTreeMap` to support ordered traversal.

use std::collections::{BTreeMap, HashMap};

use crate::models::TransactionType;

// ──────────────────────────────────────────────────────────────────────────────
// Shared Row Reference
// ──────────────────────────────────────────────────────────────────────────────

/// Points to a specific row in a specific SSTable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowRef {
    /// Zero-based SSTable sequence number (matches the file naming scheme).
    pub sstable_seq: u64,
    /// Zero-based row index within that SSTable.
    pub row_index:   u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Bitmap Index
// ──────────────────────────────────────────────────────────────────────────────

/// A simple in-memory bitmap index over `transaction_type`.
///
/// Internally each value maps to a `Vec<u64>` where each `u64` is a 64-bit
/// word of the bitmap (word 0 = rows 0-63, word 1 = rows 64-127, …).
pub struct BitmapIndex {
    /// `bitmaps[tx_type_u8]` → bitmap words
    bitmaps: HashMap<u8, Vec<u64>>,
    /// Total rows indexed (needed to allocate bitmap words).
    row_count: u64,
}

impl BitmapIndex {
    pub fn new() -> Self {
        Self { bitmaps: HashMap::new(), row_count: 0 }
    }

    /// Record that row `row_idx` in this index has `tx_type`.
    pub fn set(&mut self, row_idx: u64, tx_type: TransactionType) {
        let key = tx_type as u8;
        let word_idx  = (row_idx / 64) as usize;
        let bit_shift = row_idx % 64;

        let bitmap = self.bitmaps.entry(key).or_default();
        // Grow if needed
        if bitmap.len() <= word_idx {
            bitmap.resize(word_idx + 1, 0u64);
        }
        bitmap[word_idx] |= 1u64 << bit_shift;

        if row_idx >= self.row_count {
            self.row_count = row_idx + 1;
        }
    }

    /// Return an iterator over all row indices that match `tx_type`.
    pub fn matching_rows(&self, tx_type: TransactionType) -> Vec<u64> {
        let key = tx_type as u8;
        match self.bitmaps.get(&key) {
            None => vec![],
            Some(words) => {
                let mut result = Vec::new();
                for (word_idx, &word) in words.iter().enumerate() {
                    if word == 0 { continue; }
                    for bit in 0..64u64 {
                        if word & (1u64 << bit) != 0 {
                            result.push(word_idx as u64 * 64 + bit);
                        }
                    }
                }
                result
            }
        }
    }

    /// Count bits set for a given `tx_type` (O(popcount) with HW intrinsic).
    pub fn count(&self, tx_type: TransactionType) -> u64 {
        self.bitmaps
            .get(&(tx_type as u8))
            .map(|w| w.iter().map(|&x| x.count_ones() as u64).sum())
            .unwrap_or(0)
    }

    /// Merge another bitmap index (used when compacting SSTables).
    pub fn merge(&mut self, other: &BitmapIndex, row_offset: u64) {
        for (&key, words) in &other.bitmaps {
            for (word_idx, &word) in words.iter().enumerate() {
                if word == 0 { continue; }
                for bit in 0..64u64 {
                    if word & (1u64 << bit) != 0 {
                        let global_row = row_offset + word_idx as u64 * 64 + bit;
                        let tx_type = TransactionType::from_u8(key)
                            .unwrap_or(TransactionType::Debit);
                        self.set(global_row, tx_type);
                    }
                }
            }
        }
    }
}

impl Default for BitmapIndex { fn default() -> Self { Self::new() } }

// ──────────────────────────────────────────────────────────────────────────────
// Account (B-Tree / Hash) Index
// ──────────────────────────────────────────────────────────────────────────────

/// Dual-structure index for `account_id`:
///
/// - `HashMap` for O(1) exact-match lookups.
/// - `BTreeMap` wrapping the same values for ordered range scans.
pub struct AccountIndex {
    /// Fast exact-match path.
    hash_map:  HashMap<u64, Vec<RowRef>>,
    /// Ordered path for range queries (`account_id BETWEEN a AND b`).
    btree_map: BTreeMap<u64, Vec<RowRef>>,
}

impl AccountIndex {
    pub fn new() -> Self {
        Self { hash_map: HashMap::new(), btree_map: BTreeMap::new() }
    }

    /// Record a row reference for `account_id`.
    pub fn insert(&mut self, account_id: u64, row: RowRef) {
        self.hash_map.entry(account_id).or_default().push(row.clone());
        self.btree_map.entry(account_id).or_default().push(row);
    }

    /// Return all `RowRef`s for an exact `account_id`.  O(1) amortised.
    pub fn get(&self, account_id: u64) -> &[RowRef] {
        self.hash_map.get(&account_id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Return all `RowRef`s for account IDs in `[lo, hi]`.  O(log n + k).
    pub fn range(&self, lo: u64, hi: u64) -> Vec<&RowRef> {
        self.btree_map
            .range(lo..=hi)
            .flat_map(|(_, refs)| refs.iter())
            .collect()
    }

    pub fn account_count(&self) -> usize { self.hash_map.len() }
}

impl Default for AccountIndex { fn default() -> Self { Self::new() } }

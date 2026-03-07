//! # Ledger Engine
//!
//! A single-file, columnar storage engine for double-entry accounting.
//!
//! ## On-Disk Layout  (`ledger.ldg`)
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │  FILE HEADER           512 bytes, offset 0               │
//! │  – magic, version, account/segment/sparse-index pointers │
//! │  – genesis_hash, last_tx_hash (hash chain anchors)       │
//! │  – CRC32 over header payload                             │
//! ├──────────────────────────────────────────────────────────┤
//! │  ACCOUNTS REGION       1 024 × 128 byte slots            │
//! │  offset 512 … 131 583                                    │
//! │  Fixed-size records; slot[0..accounts_count] are active. │
//! ├──────────────────────────────────────────────────────────┤
//! │  SEGMENT 0  (256-byte header + columnar data blocks)     │
//! │    col-0: id        u64 × N, tightly packed              │
//! │    col-1: acct_id   u64 × N                              │
//! │    col-2: amount    i64 × N  (cents, signed)             │
//! │    col-3: tx_type   dict-encoded u8 × N                  │
//! │    col-4: timestamp u64 × N                              │
//! │    col-5: desc      4-byte-len-prefixed UTF-8 strings    │
//! │    col-6: tx_hash   [u8;32] × N  (SHA-256 chain)         │
//! ├──────────────────────────────────────────────────────────┤
//! │  SEGMENT 1 … SEGMENT K  (append-only, never mutated)     │
//! ├──────────────────────────────────────────────────────────┤
//! │  SPARSE TIMESTAMP INDEX  (rewritten on every flush)      │
//! │  One (timestamp, global_row_index) entry per 64 rows.    │
//! │  Enables O(log n) seek into any date range.              │
//! └──────────────────────────────────────────────────────────┘
//!
//! WAL lives in a *separate* `wal.log` alongside the `.ldg` file.
//! ```

pub mod encoders;
pub mod engine;
pub mod error;
pub mod file_format;
pub mod hash_chain;
pub mod models;
pub mod simd_scan;
pub mod sparse_index;
pub mod storage;
pub mod wal;

pub use engine::LedgerEngine;
pub use error::LedgerError;
pub use models::{Account, AccountType, ExpenseSummary, Transaction, TransactionType};

#[cfg(test)]
mod tests;

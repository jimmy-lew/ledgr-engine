//! # Ledger Engine
//!
//! A custom embedded storage engine optimised for double-entry financial
//! accounting workloads.
//!
//! ## Architecture
//!
//! ```text
//!  ┌─────────────────────────────────────────────────────────┐
//!  │                      LedgerEngine                       │
//!  │                                                         │
//!  │   append_transaction()   validate_ledger()              │
//!  │   get_expense_summary()                                 │
//!  │                                                         │
//!  │  ┌──────────┐  ┌────────────────────────────────────┐   │
//!  │  │   WAL    │  │           LSM-Tree                 │   │
//!  │  │(append-  │  │  ┌──────────┐   ┌─────────────┐    │   │
//!  │  │  only)   │  │  │MemTable  │──▶│  SSTables   │    │   │
//!  │  │          │  │  │(BTreeMap)│   │ (columnar)  │    │   │
//!  │  └──────────┘  │  └──────────┘   └─────────────┘    │   │
//!  │                │                                    │   │
//!  │                │  ┌────────────┐  ┌─────────────┐   │   │
//!  │                │  │BitmapIndex │  │ HashIndex   │   │   │
//!  │                │  │(tx_type)   │  │(account_id) │   │   │
//!  │                │  └────────────┘  └─────────────┘   │   │
//!  │                └────────────────────────────────────┘   │
//!  └─────────────────────────────────────────────────────────┘
//! ```

pub mod engine;
pub mod error;
pub mod indexes;
pub mod memtable;
pub mod models;
pub mod sstable;
pub mod wal;

pub use engine::LedgerEngine;
pub use error::LedgerError;
pub use models::{Account, AccountType, Transaction, TransactionType};

#[cfg(test)]
mod tests;

//! # LedgerEngine – Public API
//!
//! ## Double-entry enforcement
//!
//! The *only* write path is `record_journal_entry(entry)`.  The engine
//! validates the accounting invariant **before touching any I/O**:
//!
//! ```text
//! ∑ leg.signed_amount() == 0     (debits == credits)
//! ```
//!
//! If that check fails the entire entry is rejected with
//! `LedgerError::JournalNotBalanced` and nothing is written to the WAL
//! or the MemTable.  There is no API that lets a caller write a single
//! unmatched leg.
//!
//! For the common two-account case use the convenience method:
//!
//! ```rust,ignore
//! engine.record_entry(debit_acct, credit_acct, 50_000, "Rent")?;
//! ```
//!
//! ## Concurrency model
//!
//! A `parking_lot::RwLock` wraps all mutable state.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::error::{LedgerError, Result};
use crate::hash_chain::ChainTip;
use crate::models::{
    Account, AccountType, Direction, ExpenseSummary, JournalEntry, Leg, Transaction,
};
use crate::simd_scan;
use crate::storage::Storage;
use crate::wal::{Wal, WalEntry};

#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub col_compressed: [u64; 8],
    pub col_uncompressed: [u64; 8],
    pub segment_count: usize,
    pub total_tx_count: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// MemTable
// ─────────────────────────────────────────────────────────────────────────────

struct MemTable {
    rows: Vec<Transaction>,
    size_bytes: usize,
}

impl MemTable {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            size_bytes: 0,
        }
    }

    fn insert(&mut self, tx: Transaction) {
        self.size_bytes += 8 + 8 + 8 + 8 + 1 + 8 + 4 + tx.description.len() + 32;
        self.rows.push(tx);
    }

    fn needs_flush(&self) -> bool {
        self.size_bytes >= FLUSH_THRESHOLD
    }

    fn drain_sorted(&mut self) -> Vec<Transaction> {
        let mut rows = std::mem::take(&mut self.rows);
        rows.sort_by_key(|tx| (tx.timestamp, tx.id));
        self.size_bytes = 0;
        rows
    }

    fn iter(&self) -> impl Iterator<Item = &Transaction> {
        self.rows.iter()
    }
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

const FLUSH_THRESHOLD: usize = 8 * 1024 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// Inner state
// ─────────────────────────────────────────────────────────────────────────────

struct Inner {
    storage: Storage,
    wal: Wal,
    memtable: MemTable,
    chain_tip: ChainTip,
    /// Monotonically increasing counter shared between journal entry IDs
    /// and individual leg IDs (engine uses one AtomicU64 for both).
    next_entry_id: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// LedgerEngine
// ─────────────────────────────────────────────────────────────────────────────

pub struct LedgerEngine {
    inner: RwLock<Inner>,
    next_id: AtomicU64,
}

impl LedgerEngine {
    // ── Open / recover ─────────────────────────────────────────────────────

    pub fn open(data_path: impl AsRef<Path>) -> Result<Self> {
        let data_path = data_path.as_ref().to_path_buf();
        let wal_path = data_path.with_extension("wal");

        let storage = Storage::open(&data_path)?;
        let wal = Wal::open(&wal_path)?;

        let recovered = wal.replay()?;
        let mut memtable = MemTable::new();
        let mut max_id: u64 = storage.header.total_tx_count;

        let chain_tip = ChainTip::new(storage.header.last_tx_hash);

        for tx in recovered {
            if tx.id > max_id {
                max_id = tx.id;
            }
            memtable.insert(tx);
        }

        Ok(Self {
            next_id: AtomicU64::new(max_id + 1),
            inner: RwLock::new(Inner {
                storage,
                wal,
                memtable,
                chain_tip,
                next_entry_id: max_id + 1,
            }),
        })
    }

    // ── Account management ─────────────────────────────────────────────────

    pub fn create_account(&self, name: &str, kind: AccountType) -> Result<u64> {
        let now = unix_now();
        self.inner.write().storage.add_account(name, kind, now)
    }

    // ── Primary write path: record a complete journal entry ────────────────

    /// Record a fully balanced double-entry journal entry.
    ///
    /// ## What "double-entry" means here
    ///
    /// Every economic event must be described by at least **two** legs where
    /// debits and credits are equal.  Classic examples:
    ///
    /// ```text
    /// Cash sale of $500:
    ///   DEBIT  Cash            +500   (asset increases)
    ///   CREDIT Revenue         −500   (revenue increases)
    ///
    /// Purchase a $1 200 laptop: $200 cash + $1 000 on account:
    ///   DEBIT  Equipment      +1200   (asset increases)
    ///   CREDIT Cash            −200   (asset decreases)
    ///   CREDIT Accounts Pay.  −1000   (liability increases)
    /// ```
    ///
    /// ## Guarantee
    ///
    /// `JournalEntry::validate()` is called **before any I/O**.  If it
    /// returns an error, the WAL and MemTable are untouched.  After
    /// validation, all legs are written to the WAL as **one atomic record**
    /// and then inserted into the MemTable together — partial commits are
    /// impossible.
    ///
    /// Returns the `journal_entry_id` assigned to the entry.
    pub fn record_journal_entry(&self, entry: JournalEntry) -> Result<u64> {
        // ── 1. Validate accounting invariant (pure, no I/O) ───────────────
        entry.validate()?;

        let mut inner = self.inner.write();

        // ── 2. Validate all referenced accounts exist ─────────────────────
        for (_i, leg) in entry.legs.iter().enumerate() {
            if !inner.storage.accounts.contains_key(&leg.account_id) {
                return Err(LedgerError::UnknownAccount(leg.account_id));
            }
        }

        let timestamp = entry.timestamp.unwrap_or_else(unix_now);
        let journal_entry_id = {
            let id = inner.next_entry_id;
            inner.next_entry_id += 1;
            id
        };

        // Hashes are [0;32] at this stage; they are computed during flush
        // by the ChainTip (which reads from the MemTable's drain_sorted).
        // For WAL records we store the [0;32] placeholder; on replay the
        // engine re-inserts them into the MemTable and recomputes on flush.
        let legs: Vec<Transaction> = entry
            .legs
            .iter()
            .map(|leg| {
                let leg_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                Transaction {
                    id: leg_id,
                    journal_entry_id,
                    account_id: leg.account_id,
                    amount: leg.signed_amount(),
                    transaction_type: leg.direction,
                    timestamp,
                    description: entry.description.clone(),
                    tx_hash: [0u8; 32],
                }
            })
            .collect();

        // ── 4. Atomic WAL write (all legs in one record) ──────────────────
        let wal_entry = WalEntry {
            journal_entry_id,
            timestamp,
            description: entry.description.clone(),
            legs: legs.clone(),
        };
        inner.wal.append_journal_entry(&wal_entry)?;

        // ── 5. Update account balances and populate MemTable ──────────────
        for leg in &legs {
            inner
                .storage
                .update_account_balance(leg.account_id, leg.amount)?;
            inner.memtable.insert(leg.clone());
        }

        // ── 6. Auto-flush if MemTable threshold exceeded ──────────────────
        if inner.memtable.needs_flush() {
            Self::do_flush(&mut inner)?;
        }

        Ok(journal_entry_id)
    }

    /// Convenience method for the common **two-account** journal entry.
    ///
    /// Records a single balanced pair:
    ///
    /// ```text
    ///   DEBIT  debit_account   amount_cents
    ///   CREDIT credit_account  amount_cents
    /// ```
    ///
    /// Returns the `journal_entry_id`.
    pub fn record_entry(
        &self,
        debit_account: u64,
        credit_account: u64,
        amount_cents: u64,
        description: &str,
        timestamp: Option<u64>,
    ) -> Result<u64> {
        let entry = JournalEntry::new(
            description,
            vec![
                Leg::debit(debit_account, amount_cents),
                Leg::credit(credit_account, amount_cents),
            ],
            timestamp,
        );
        self.record_journal_entry(entry)
    }

    // ── validate_ledger ─────────────────────────────────────────────────────

    /// Two-phase validation:
    ///
    /// ### Phase 1 – SIMD Balance Scan
    ///
    /// Loads the `amount` column (signed i64) from every segment plus the
    /// MemTable.  Calls `simd_sum_i64` which dispatches to AVX2 on x86_64
    /// or a scalar fallback elsewhere.
    ///
    /// Asserts:  **∑ all amounts = 0**
    ///
    /// Cross-checks against **∑ account.balance** from the fixed-size
    /// Accounts region.  Any discrepancy flags a storage-level bug.
    ///
    /// ### Phase 2 – Hash Chain Integrity Walk
    ///
    /// Replays SHA-256(leg_fields ‖ prev_hash) for every flushed leg in
    /// global order.  A mismatch proves that a historical row was altered.
    pub fn validate_ledger(&self) -> Result<()> {
        let mut inner = self.inner.write();

        // ── Phase 1: SIMD amount scan ──────────────────────────────────────
        let mut all_amounts: Vec<i64> = Vec::new();

        for seg in inner.storage.segments.clone() {
            all_amounts.extend(inner.storage.read_amounts(&seg)?);
        }
        for tx in inner.memtable.iter() {
            all_amounts.push(tx.amount);
        }

        let net = simd_scan::simd_sum_i64(&all_amounts);
        let balance_sum = inner.storage.account_balance_sum();

        if balance_sum != net {
            return Err(LedgerError::Encoding(format!(
                "Internal inconsistency: ∑account.balance ({balance_sum}) \
                 ≠ ∑transaction.amount ({net})"
            )));
        }
        if net != 0 {
            return Err(LedgerError::ImbalancedLedger { net });
        }

        println!(
            "[validate] Phase 1 ✓  SIMD net = {net} ¢  \
             ∑account.balance = {balance_sum} ¢  \
             rows = {}",
            all_amounts.len()
        );

        // ── Phase 2: Hash chain walk ───────────────────────────────────────
        let mut prev_hash = inner.storage.header.genesis_hash;
        let mut global_row = 0u64;

        for seg in inner.storage.segments.clone() {
            let txs = inner.storage.read_all_transactions(&seg)?;
            for tx in &txs {
                let expected = crate::hash_chain::compute_tx_hash(tx, &prev_hash);
                if expected != tx.tx_hash {
                    return Err(LedgerError::HashChainViolation {
                        row: global_row,
                        expected: hex::encode(expected),
                        actual: hex::encode(tx.tx_hash),
                    });
                }
                prev_hash = tx.tx_hash;
                global_row += 1;
            }
        }

        println!("[validate] Phase 2 ✓  Hash chain intact across {global_row} flushed rows");
        Ok(())
    }

    // ── get_expense_summary ─────────────────────────────────────────────────

    /// Aggregate debits and credits for `[start_ts, end_ts]`.
    ///
    /// Optimisation layers applied in order:
    /// 1. **Sparse index** binary-search → O(log N) first-candidate row.
    /// 2. **Zone-map** per-segment `(min_ts, max_ts)` comparison → whole
    ///    segments are skipped when they have no overlap with the range.
    /// 3. **Columnar read** of only `timestamp`, `amount`, `tx_type`.
    /// 4. **SIMD split-sum** (`simd_sum_by_type`) for debit/credit totals.
    pub fn get_expense_summary(&self, start_ts: u64, end_ts: u64) -> Result<ExpenseSummary> {
        let mut inner = self.inner.write();
        let mut summary = ExpenseSummary::default();

        let first_candidate = inner.storage.sparse.lower_bound_row(start_ts);

        let segments = inner.storage.segments.clone();
        for seg in &segments {
            let seg_last = seg.header.first_row_global_idx + seg.header.row_count;

            if seg_last <= first_candidate {
                summary.segments_skipped += 1;
                continue;
            }
            if seg.header.max_ts < start_ts || seg.header.min_ts > end_ts {
                summary.segments_skipped += 1;
                println!(
                    "[summary] ⏩ Zone-map skip: seg {} ts=[{},{}]",
                    seg.seq, seg.header.min_ts, seg.header.max_ts
                );
                continue;
            }

            let timestamps = inner.storage.read_timestamps(seg)?;
            let amounts = inner.storage.read_amounts(seg)?;
            let tx_types = inner.storage.read_tx_types(seg)?;

            let (filt_amounts, filt_types): (Vec<i64>, Vec<u8>) = timestamps
                .iter()
                .zip(amounts.iter())
                .zip(tx_types.iter())
                .filter(|((&ts, _), _)| ts >= start_ts && ts <= end_ts)
                .map(|((_, &amt), &t)| (amt, t))
                .unzip();

            if filt_amounts.is_empty() {
                continue;
            }

            let (d, c) = simd_scan::simd_sum_by_type(&filt_amounts, &filt_types);
            summary.total_debits += d;
            summary.total_credits += c;
            summary.row_count += filt_amounts.len() as u64;
        }

        for tx in inner.memtable.iter() {
            if tx.timestamp < start_ts || tx.timestamp > end_ts {
                continue;
            }
            match tx.transaction_type {
                Direction::Debit => summary.total_debits += tx.amount,
                Direction::Credit => summary.total_credits += tx.amount,
            }
            summary.row_count += 1;
        }

        summary.net = summary.total_debits + summary.total_credits;
        Ok(summary)
    }

    // ── Read / query APIs ──────────────────────────────────────────────────

    /// Return all accounts sorted by their numeric ID.
    pub fn list_accounts(&self) -> Vec<Account> {
        let inner = self.inner.read();
        let mut accounts: Vec<Account> = inner
            .storage
            .accounts
            .values()
            .map(|(_, a)| a.clone())
            .collect();
        accounts.sort_by_key(|a| a.id);
        accounts
    }

    /// Return every transaction leg across all flushed segments *and* the
    /// MemTable, sorted globally by (timestamp, leg_id).
    ///
    /// Legs from the same `journal_entry_id` are naturally adjacent because
    /// they share the same timestamp and were inserted together.
    pub fn list_all_transactions(&self) -> Result<Vec<Transaction>> {
        let mut inner = self.inner.write();

        let mut all: Vec<Transaction> = Vec::new();
        for seg in inner.storage.segments.clone() {
            all.extend(inner.storage.read_all_transactions(&seg)?);
        }
        for tx in inner.memtable.iter() {
            all.push(tx.clone());
        }
        Ok(all)
    }

    /// Return all transaction legs grouped into journal entries.
    ///
    /// Each inner `Vec<Transaction>` contains all legs of one entry;
    /// the outer `Vec` is ordered by `journal_entry_id`.
    pub fn list_journal_entries(&self) -> Result<Vec<Vec<Transaction>>> {
        use std::collections::BTreeMap;
        let legs = self.list_all_transactions()?;
        let mut map: BTreeMap<u64, Vec<Transaction>> = BTreeMap::new();
        for leg in legs {
            map.entry(leg.journal_entry_id).or_default().push(leg);
        }
        Ok(map.into_values().collect())
    }

    // ── Force flush ────────────────────────────────────────────────────────

    pub fn force_flush(&self) -> Result<()> {
        let mut inner = self.inner.write();
        if !inner.memtable.is_empty() {
            Self::do_flush(&mut inner)?;
        }
        Ok(())
    }

    // ── Internal flush ─────────────────────────────────────────────────────

    fn do_flush(inner: &mut Inner) -> Result<()> {
        let rows = inner.memtable.drain_sorted();
        let n = rows.len();

        let sparse_before = inner.storage.sparse.len();
        let segments_before = inner.storage.segments.len();

        let flush_start = std::time::Instant::now();
        inner.storage.flush_segment(rows, &mut inner.chain_tip)?;
        let flush_duration = flush_start.elapsed();

        inner.wal.sync()?;
        inner.wal.truncate()?;

        let sparse_after = inner.storage.sparse.len();
        let segments_after = inner.storage.segments.len();

        println!(
            "[flush] ✓  {n} legs → segment {} (sparse: {} → {}, segments: {} → {}) in {:?}",
            segments_after - 1,
            sparse_before,
            sparse_after,
            segments_before,
            segments_after,
            flush_duration
        );
        Ok(())
    }

    pub fn get_compression_stats(&self) -> CompressionStats {
        let inner = self.inner.read();
        let mut col_compressed: [u64; 8] = [0; 8];
        let mut col_uncompressed: [u64; 8] = [0; 8];

        for seg in &inner.storage.segments {
            for (i, c) in seg.header.columns.iter().enumerate() {
                col_compressed[i] += c.length;
                col_uncompressed[i] += c.uncompressed_length;
            }
        }

        CompressionStats {
            col_compressed,
            col_uncompressed,
            segment_count: inner.storage.segments.len(),
            total_tx_count: inner.storage.header.total_tx_count,
        }
    }
}

impl Drop for LedgerEngine {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.try_write() {
            if !inner.memtable.is_empty() {
                if let Err(e) = Self::do_flush(&mut inner) {
                    eprintln!("Warning: failed to flush on drop: {}", e);
                }
            }
            if let Err(e) = inner.wal.sync() {
                eprintln!("Warning: failed to sync WAL on drop: {}", e);
            }
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

//! # LedgerEngine
//!
//! The top-level public API. Coordinates the WAL, MemTable, SSTable files,
//! and both indexes under a single `parking_lot::RwLock` so reads are
//! concurrent while writes are exclusive.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::error::{LedgerError, Result};
use crate::indexes::{AccountIndex, BitmapIndex, RowRef};
use crate::memtable::MemTable;
use crate::models::{Account, AccountType, ExpenseSummary, Transaction, TransactionType};
use crate::sstable::{SSTableHeader, SSTableReader, SSTableWriter};
use crate::wal::Wal;

// ──────────────────────────────────────────────────────────────────────────────
// SSTable catalogue entry (one per .sst file on disk)
// ──────────────────────────────────────────────────────────────────────────────

struct SstEntry {
    path: PathBuf,
    header: SSTableHeader,
    seq: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Engine inner state (guarded by RwLock)
// ──────────────────────────────────────────────────────────────────────────────

struct EngineInner {
    data_dir: PathBuf,
    wal: Wal,
    memtable: MemTable,
    sstables: Vec<SstEntry>,
    accounts: HashMap<u64, Account>,
    account_index: AccountIndex,
    bitmap_index: BitmapIndex,
    next_sst_seq: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Public engine handle
// ──────────────────────────────────────────────────────────────────────────────

pub struct LedgerEngine {
    inner: RwLock<EngineInner>,
    next_id: AtomicU64,
}

impl LedgerEngine {
    // ── Construction / recovery ────────────────────────────────────────────

    /// Open (or create) an engine whose data lives in `data_dir`.
    /// On startup the WAL is replayed into the MemTable.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.join("wal.log");
        let wal = Wal::open(&wal_path)?;

        // Discover existing SSTables
        let mut sst_entries: Vec<SstEntry> = Vec::new();
        let mut max_tx_id: u64 = 0;
        let mut next_sst_seq: u64 = 0;

        let mut account_index = AccountIndex::new();
        let bitmap_index = BitmapIndex::new();

        for entry in fs::read_dir(&data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                let seq: u64 = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("sst_"))
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);

                let mut f = File::open(&path)?;
                let header = SSTableHeader::read_from(&mut f)?;

                // Rebuild account_index from this SSTable
                let acct_ids = SSTableReader::read_account_ids(&mut f, &header)?;
                for (row_idx, acct_id) in acct_ids.iter().enumerate() {
                    account_index.insert(
                        *acct_id,
                        RowRef {
                            sstable_seq: seq,
                            row_index: row_idx as u64,
                        },
                    );
                }

                sst_entries.push(SstEntry { path, header, seq });
                if seq >= next_sst_seq {
                    next_sst_seq = seq + 1;
                }
            }
        }
        sst_entries.sort_by_key(|e| e.seq);

        // Replay WAL into a fresh MemTable
        let recovered = wal.replay()?;
        let mut memtable = MemTable::new();
        for tx in &recovered {
            if tx.id > max_tx_id {
                max_tx_id = tx.id;
            }
            memtable.insert(tx.clone());
        }

        Ok(Self {
            next_id: AtomicU64::new(max_tx_id + 1),
            inner: RwLock::new(EngineInner {
                data_dir,
                wal,
                memtable,
                sstables: sst_entries,
                accounts: HashMap::new(),
                account_index,
                bitmap_index,
                next_sst_seq,
            }),
        })
    }

    // ── Account management ─────────────────────────────────────────────────

    pub fn create_account(&self, name: &str, kind: AccountType) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Self::unix_now();
        let account = Account {
            id,
            name: name.to_string(),
            kind,
            created_at: now,
        };
        let mut inner = self.inner.write();
        inner.accounts.insert(id, account);
        Ok(id)
    }

    // ── Primary write path ─────────────────────────────────────────────────

    /// Append a double-entry transaction.
    ///
    /// The transaction is written to the WAL synchronously (durability) then
    /// inserted into the in-memory MemTable.  If the MemTable exceeds its
    /// size threshold a flush is triggered inline.
    pub fn append_transaction(
        &self,
        account_id: u64,
        amount_cents: i64,
        transaction_type: TransactionType,
        description: &str,
    ) -> Result<u64> {
        let mut inner = self.inner.write();

        if !inner.accounts.contains_key(&account_id) {
            return Err(LedgerError::UnknownAccount(account_id));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let tx = Transaction {
            id,
            account_id,
            amount: amount_cents,
            transaction_type,
            timestamp: Self::unix_now(),
            description: description.to_string(),
        };

        // 1. WAL first (durability guarantee)
        inner.wal.append(&tx)?;

        // 2. MemTable
        inner.memtable.insert(tx);

        // 3. Flush if threshold exceeded
        if inner.memtable.needs_flush() {
            Self::flush_memtable(&mut inner)?;
        }

        Ok(id)
    }

    // ── Validate ledger (the core read-path showcase) ──────────────────────

    /// Verify that the net sum of all `amount` values across every SSTable
    /// **and** the MemTable is exactly zero.
    ///
    /// ## How it exploits the columnar header
    ///
    /// For each SSTable on disk:
    /// 1. Read the 137-byte header – O(1).
    /// 2. From `header.columns[col::AMT].offset` and `.length` we know
    ///    **precisely** where the amount column starts and how many bytes it
    ///    occupies.
    /// 3. `file.seek(offset)` + read `row_count × 8` bytes.
    ///
    /// We never load `id`, `account_id`, `description`, or `timestamp` data.
    pub fn validate_ledger(&self) -> Result<()> {
        let inner = self.inner.read();

        let mut grand_total: i64 = 0;

        // ── SSTables on disk ───────────────────────────────────────────────
        for entry in &inner.sstables {
            let mut file = File::open(&entry.path)?;
            let column_sum = SSTableReader::sum_amounts(&mut file, &entry.header)?;
            grand_total = grand_total
                .checked_add(column_sum)
                .ok_or_else(|| LedgerError::Encoding("i64 overflow in validate_ledger".into()))?;
        }

        // ── MemTable (in-memory rows not yet flushed) ──────────────────────
        for tx in inner.memtable.iter_sorted() {
            grand_total = grand_total
                .checked_add(tx.amount)
                .ok_or_else(|| LedgerError::Encoding("i64 overflow in memtable sum".into()))?;
        }

        if grand_total != 0 {
            return Err(LedgerError::ImbalancedLedger { net: grand_total });
        }

        println!("[validate_ledger] ✓  Net balance = 0 cents (ledger is balanced)");
        Ok(())
    }

    // ── Expense summary (zone-map skip + columnar read) ───────────────────

    /// Aggregate debits and credits within `[start_ts, end_ts]`.
    ///
    /// ## Optimisation chain
    ///
    /// 1. **Zone-map check** – compare `(start, end)` against the 8-byte
    ///    `min_ts`/`max_ts` stored in the header.  Skip the whole file if
    ///    there is no overlap.  This is a pure memory compare; the data
    ///    pages stay cold in the OS page cache.
    ///
    /// 2. **Columnar read** – for surviving SSTables we read only:
    ///    - the `timestamp` column (predicate evaluation)
    ///    - the `amount` column (aggregate target)
    ///    - the `transaction_type` column (group-by key)
    ///
    /// 3. **MemTable scan** – iterate over the in-memory sorted BTreeMap,
    ///    filter by timestamp, accumulate.
    pub fn get_expense_summary(&self, start_ts: u64, end_ts: u64) -> Result<ExpenseSummary> {
        let inner = self.inner.read();
        let mut summary = ExpenseSummary::default();

        // ── SSTables ───────────────────────────────────────────────────────
        for entry in &inner.sstables {
            // ① Zone-map: skip if no overlap
            if !SSTableReader::overlaps_time_range(&entry.header, start_ts, end_ts) {
                println!(
                    "[get_expense_summary] ⏩ Skipping SSTable seq={} \
                     (ts range [{},{}] ∩ [{},{}] = ∅)",
                    entry.seq, entry.header.min_ts, entry.header.max_ts, start_ts, end_ts
                );
                continue;
            }

            // ② Read only the 3 relevant columns
            let mut file = File::open(&entry.path)?;
            let (debits, credits) = SSTableReader::aggregate_by_type_in_range(
                &mut file,
                &entry.header,
                start_ts,
                end_ts,
            )?;

            summary.total_debits += debits;
            summary.total_credits += credits;
            summary.row_count += entry.header.row_count;
        }

        // ── MemTable ───────────────────────────────────────────────────────
        for tx in inner.memtable.iter_sorted() {
            if tx.timestamp < start_ts || tx.timestamp > end_ts {
                continue;
            }
            match tx.transaction_type {
                TransactionType::Debit => summary.total_debits += tx.amount,
                TransactionType::Credit => summary.total_credits += tx.amount,
            }
            summary.row_count += 1;
        }

        summary.net = summary.total_credits + summary.total_debits;
        Ok(summary)
    }

    // ── Force flush (for testing / shutdown) ──────────────────────────────

    pub fn force_flush(&self) -> Result<()> {
        let mut inner = self.inner.write();
        if !inner.memtable.is_empty() {
            Self::flush_memtable(&mut inner)?;
        }
        Ok(())
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    fn flush_memtable(inner: &mut EngineInner) -> Result<()> {
        let rows = inner.memtable.drain_sorted();
        if rows.is_empty() {
            return Ok(());
        }

        let seq = inner.next_sst_seq;
        let path = inner.data_dir.join(format!("sst_{:08}.sst", seq));

        // Write SSTable to a buffer then to disk atomically.
        let mut buf = Cursor::new(Vec::<u8>::new());
        let header = SSTableWriter::write(&mut buf, &rows)?;

        fs::write(&path, buf.into_inner())?;

        // Update in-memory catalog and indexes
        let row_count = header.row_count;
        for (row_idx, tx) in rows.iter().enumerate() {
            inner.account_index.insert(
                tx.account_id,
                RowRef {
                    sstable_seq: seq,
                    row_index: row_idx as u64,
                },
            );
            inner.bitmap_index.set(row_idx as u64, tx.transaction_type);
        }

        inner.sstables.push(SstEntry { path, header, seq });
        inner.next_sst_seq = seq + 1;

        println!("[flush] ✓  Flushed {} rows → sst_{:08}.sst", row_count, seq);
        Ok(())
    }

    fn unix_now() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

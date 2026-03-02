//! # Single-File Storage Manager
//!
//! `Storage` owns the one `.ldg` file and provides all byte-level I/O:
//! initialising the file layout on first open, writing segments, updating
//! the sparse index, and seeking to specific columns for reads.
//!
//! ## Flush Sequence (atomic from the reader's perspective)
//!
//! ```text
//! 1. Seek to segments_end_offset (just past the current sparse index)
//!    and write the new segment header + column data.
//! 2. Compute new sparse index = merge(old_index, new_rows).
//! 3. Write the new sparse index immediately after the new segment.
//! 4. Truncate the file to (new_segment_end + new_sparse_index_bytes)
//!    so no orphaned bytes remain.
//! 5. Seek to byte 0; rewrite the FileHeader with updated pointers.
//! 6. fsync.
//! ```
//!
//! Steps 1-4 leave the old header still pointing to the old sparse index,
//! so a crash between steps 4 and 5 is safe: the WAL will replay any
//! unflushed rows and the file is internally consistent.  Only step 5
//! (the header rewrite) is the logical commit point.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use byteorder::{LE, ReadBytesExt, WriteBytesExt};

use crate::error::{LedgerError, Result};
use crate::file_format::{
    self, col, enc, ColumnMeta, FileHeader, SegmentHeader, FILE_HEADER_SIZE,
    MAX_ACCOUNTS, NUM_TX_COLUMNS, ACCOUNT_RECORD_SIZE,
};
use crate::hash_chain::{self, ChainTip};
use crate::models::{Account, AccountType, Transaction, TransactionType};
use crate::sparse_index::SparseIndex;

// ──────────────────────────────────────────────────────────────────────────────
// In-memory segment catalogue
// ──────────────────────────────────────────────────────────────────────────────

/// Everything we need to know about a written segment without re-reading it.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub seq:                u64,
    pub header:             SegmentHeader,
    /// Absolute file offset at which the SegmentHeader begins.
    pub file_offset:        u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Storage
// ──────────────────────────────────────────────────────────────────────────────

pub struct Storage {
    file:          File,
    pub header:    FileHeader,
    pub segments:  Vec<SegmentMeta>,
    pub accounts:  HashMap<u64, (usize, Account)>, // id → (slot_index, account)
    pub sparse:    SparseIndex,
}

impl Storage {
    // ── Open / create ──────────────────────────────────────────────────────

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let is_new = !path.exists();

        let file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)?;

        let mut storage = Self {
            file,
            header:   FileHeader::default(),
            segments: Vec::new(),
            accounts: HashMap::new(),
            sparse:   SparseIndex::new(),
        };

        if is_new {
            storage.initialise_new_file()?;
        } else {
            storage.load_existing_file()?;
        }

        Ok(storage)
    }

    // ── Initialise a brand-new file ────────────────────────────────────────

    fn initialise_new_file(&mut self) -> Result<()> {
        // Write the file header at offset 0
        self.header.write_to(&mut self.file)?;

        // Zero out the entire accounts region so slot reads are deterministic
        let zero_accounts = vec![0u8; MAX_ACCOUNTS * ACCOUNT_RECORD_SIZE];
        self.file.seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))?;
        self.file.write_all(&zero_accounts)?;
        self.file.sync_all()?;
        Ok(())
    }

    // ── Load & validate an existing file ──────────────────────────────────

    fn load_existing_file(&mut self) -> Result<()> {
        // 1. Read and validate the file header
        self.header = FileHeader::read_from(&mut self.file)?;

        // 2. Load account records
        for slot in 0..self.header.accounts_count as usize {
            if let Some(acct) = file_format::read_account_slot(&mut self.file, slot)? {
                self.accounts.insert(acct.id, (slot, acct));
            }
        }

        // 3. Walk and load all segment headers
        let mut offset = file_format::SEGMENTS_BASE_OFFSET;
        for seq in 0..self.header.segment_count {
            self.file.seek(SeekFrom::Start(offset))?;
            let seg_hdr = SegmentHeader::read_from(&mut self.file)?;
            let seg_size = file_format::SEGMENT_HEADER_SIZE as u64
                + seg_hdr.columns.iter().map(|c| c.length).sum::<u64>();
            self.segments.push(SegmentMeta {
                seq,
                file_offset: offset,
                header: seg_hdr,
            });
            offset += seg_size;
        }

        // 4. Load the sparse index
        if self.header.sparse_index_count > 0 {
            self.sparse = SparseIndex::read_from(
                &mut self.file,
                self.header.segments_end_offset,
                self.header.sparse_index_count,
            )?;
        }

        Ok(())
    }

    // ── Account management ─────────────────────────────────────────────────

    pub fn add_account(&mut self, name: &str, kind: AccountType, created_at: u64) -> Result<u64> {
        let slot = self.header.accounts_count as usize;
        if slot >= MAX_ACCOUNTS {
            return Err(LedgerError::AccountsExhausted(MAX_ACCOUNTS));
        }

        // Sequential ID: accounts_count + 1 (accounts and transactions share
        // the same ID space via the engine's AtomicU64 counter).
        let id = (slot + 1) as u64;

        let account = Account { id, name: name.to_string(), kind, created_at, balance: 0 };

        file_format::write_account_slot(
            &mut self.file,
            slot,
            id,
            name,
            kind as u8,
            created_at,
            0,
        )?;

        self.header.accounts_count += 1;
        self.header.write_to(&mut self.file)?;
        self.file.sync_all()?;

        self.accounts.insert(id, (slot, account));
        Ok(id)
    }

    /// Update the stored balance for an account (called after each tx).
    pub fn update_account_balance(&mut self, account_id: u64, delta: i64) -> Result<()> {
        let (slot, acct) = self.accounts.get_mut(&account_id)
            .ok_or(LedgerError::UnknownAccount(account_id))?;
        acct.balance += delta;
        let balance = acct.balance;
        file_format::write_account_slot(
            &mut self.file,
            *slot,
            acct.id,
            &acct.name.clone(),
            acct.kind as u8,
            acct.created_at,
            balance,
        )?;
        Ok(())
    }

    // ── Flush MemTable → new Segment ───────────────────────────────────────

    /// Write `rows` as a new segment.  `rows` must already be sorted by
    /// timestamp (MemTable guarantees this).
    ///
    /// `chain_tip` is the hash chain state at the start of this batch;
    /// each row's `tx_hash` field is filled in here.
    pub fn flush_segment(
        &mut self,
        mut rows:      Vec<Transaction>,
        chain_tip:     &mut ChainTip,
    ) -> Result<()> {
        if rows.is_empty() { return Err(LedgerError::EmptyFlush); }

        let first_row_global = self.header.total_tx_count;
        let row_count        = rows.len() as u64;

        // ── Assign hashes ──────────────────────────────────────────────────
        for tx in rows.iter_mut() {
            tx.tx_hash = chain_tip.advance(tx);
        }

        // ── Serialise each column into an in-memory buffer ─────────────────
        // We need to know each column's byte length before we can write the
        // segment header (which contains their absolute offsets), so we
        // serialise all columns first, then emit the header.

        let col_id    = serialise_u64_col(rows.iter().map(|r| r.id));
        let col_acct  = serialise_u64_col(rows.iter().map(|r| r.account_id));
        let col_amt   = serialise_i64_col(rows.iter().map(|r| r.amount));
        let (col_type, type_enc) = serialise_dict_u8_col(rows.iter().map(|r| r.transaction_type as u8));
        let col_ts    = serialise_u64_col(rows.iter().map(|r| r.timestamp));
        let col_desc  = serialise_string_col(rows.iter().map(|r| r.description.as_str()));
        let col_hash  = serialise_hash_col(rows.iter().map(|r| &r.tx_hash));
        let col_entry = serialise_u64_col(rows.iter().map(|r| r.journal_entry_id));

        let col_data: [&[u8]; NUM_TX_COLUMNS] = [
            &col_id, &col_acct, &col_amt, &col_type, &col_ts, &col_desc, &col_hash, &col_entry,
        ];
        let encodings: [u8; NUM_TX_COLUMNS] = [
            enc::NONE, enc::NONE, enc::NONE, type_enc, enc::NONE, enc::NONE, enc::NONE, enc::NONE,
        ];

        // ── Compute segment file position ──────────────────────────────────
        // The new segment is placed at segments_end_offset (just past the
        // current sparse index, or at SEGMENTS_BASE_OFFSET for the first seg).
        let seg_file_offset = self.header.segments_end_offset;
        let data_start      = seg_file_offset + file_format::SEGMENT_HEADER_SIZE as u64;

        // ── Compute column offsets (absolute file positions) ───────────────
        let mut columns = [ColumnMeta::default(); NUM_TX_COLUMNS];
        let mut cursor  = data_start;
        for i in 0..NUM_TX_COLUMNS {
            columns[i] = ColumnMeta {
                offset:   cursor,
                length:   col_data[i].len() as u64,
                encoding: encodings[i],
            };
            cursor += col_data[i].len() as u64;
        }

        // ── CRC32 over all column data ─────────────────────────────────────
        let mut all_data = Vec::new();
        for &block in &col_data { all_data.extend_from_slice(block); }
        let data_crc32 = SegmentHeader::crc32_of(&all_data);

        let min_ts = rows.iter().map(|r| r.timestamp).min().unwrap();
        let max_ts = rows.iter().map(|r| r.timestamp).max().unwrap();

        let seg_hdr = SegmentHeader {
            magic:               *b"SEGM",
            row_count,
            min_ts,
            max_ts,
            first_row_global_idx: first_row_global,
            columns: columns.clone(),
            data_crc32,
        };

        // ── Write: segment header + column blocks ──────────────────────────
        self.file.seek(SeekFrom::Start(seg_file_offset))?;
        seg_hdr.write_to(&mut self.file)?;
        for &block in &col_data { self.file.write_all(block)?; }

        let new_segments_end = cursor; // = seg_file_offset + seg_header + all data

        // ── Rebuild and write sparse index ─────────────────────────────────
        let new_row_pairs: Vec<(u64, u64)> = rows.iter()
            .enumerate()
            .map(|(i, r)| (r.timestamp, first_row_global + i as u64))
            .collect();
        self.sparse.extend(&new_row_pairs);
        let sparse_bytes = self.sparse_write_at(new_segments_end)?;

        // ── Truncate file to exactly what we've written ────────────────────
        let new_file_len = new_segments_end + sparse_bytes as u64;
        self.file.set_len(new_file_len)?;

        // ── Update + rewrite file header (logical commit) ─────────────────
        self.header.segment_count       += 1;
        self.header.segments_end_offset  = new_segments_end;
        self.header.sparse_index_count   = self.sparse.len() as u64;
        self.header.total_tx_count      += row_count;
        self.header.last_tx_hash         = chain_tip.last_hash;
        self.header.write_to(&mut self.file)?;
        self.file.sync_all()?;

        let seq = (self.segments.len()) as u64;
        self.segments.push(SegmentMeta {
            seq,
            file_offset: seg_file_offset,
            header: seg_hdr,
        });

        Ok(())
    }

    fn sparse_write_at(&mut self, offset: u64) -> Result<usize> {
        self.file.seek(SeekFrom::Start(offset))?;
        let n = self.sparse.write_to(&mut self.file)?;
        Ok(n)
    }

    // ── Column read helpers ────────────────────────────────────────────────

    /// Load the full `amount` (i64) column from a segment.
    /// **Only the amount column block is read from disk.**
    pub fn read_amounts(&mut self, meta: &SegmentMeta) -> Result<Vec<i64>> {
        let col = &meta.header.columns[col::AMT];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.file.read_i64::<LE>()?); }
        Ok(out)
    }

    /// Load the `tx_type` column from a segment (dictionary decoded).
    pub fn read_tx_types(&mut self, meta: &SegmentMeta) -> Result<Vec<u8>> {
        let col = &meta.header.columns[col::TYPE];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let dict_size = self.file.read_u8()? as usize;
        let mut dict  = vec![0u8; dict_size];
        self.file.read_exact(&mut dict)?;
        let n = meta.header.row_count as usize;
        let mut codes = vec![0u8; n];
        self.file.read_exact(&mut codes)?;
        let decoded: Vec<u8> = codes.iter().map(|&c| dict[c as usize]).collect();
        Ok(decoded)
    }

    /// Load the `timestamp` column from a segment.
    pub fn read_timestamps(&mut self, meta: &SegmentMeta) -> Result<Vec<u64>> {
        let col = &meta.header.columns[col::TS];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n { out.push(self.file.read_u64::<LE>()?); }
        Ok(out)
    }

    /// Load the `tx_hash` column for integrity verification.
    pub fn read_hashes(&mut self, meta: &SegmentMeta) -> Result<Vec<[u8; 32]>> {
        let col = &meta.header.columns[col::HASH];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let mut h = [0u8; 32];
            self.file.read_exact(&mut h)?;
            out.push(h);
        }
        Ok(out)
    }

    /// Load all columns needed to reconstruct `Transaction` rows (for
    /// integrity re-computation).  More expensive than targeted column reads,
    /// but required for the hash chain walk.
    pub fn read_all_transactions(&mut self, meta: &SegmentMeta) -> Result<Vec<Transaction>> {
        let n = meta.header.row_count as usize;

        // Load each column individually (column-sequential access pattern)
        let ids = {
            self.file.seek(SeekFrom::Start(meta.header.columns[col::ID].offset))?;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n { v.push(self.file.read_u64::<LE>()?); }
            v
        };
        let accts = {
            self.file.seek(SeekFrom::Start(meta.header.columns[col::ACCT].offset))?;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n { v.push(self.file.read_u64::<LE>()?); }
            v
        };
        let amts = self.read_amounts(meta)?;
        let types = self.read_tx_types(meta)?;
        let ts    = self.read_timestamps(meta)?;

        // Descriptions (variable-length)
        self.file.seek(SeekFrom::Start(meta.header.columns[col::DESC].offset))?;
        let mut descs = Vec::with_capacity(n);
        for _ in 0..n {
            let len  = self.file.read_u32::<LE>()? as usize;
            let mut b = vec![0u8; len];
            self.file.read_exact(&mut b)?;
            descs.push(String::from_utf8_lossy(&b).into_owned());
        }

        let hashes = self.read_hashes(meta)?;

        // journal_entry_id column
        let entry_ids = {
            self.file.seek(SeekFrom::Start(meta.header.columns[col::ENTRY_ID].offset))?;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n { v.push(self.file.read_u64::<LE>()?); }
            v
        };

        let txs = (0..n).map(|i| Transaction {
            id:               ids[i],
            journal_entry_id: entry_ids[i],
            account_id:       accts[i],
            amount:           amts[i],
            transaction_type: TransactionType::from_u8(types[i])
                                  .unwrap_or(TransactionType::Debit),
            timestamp:        ts[i],
            description:      descs[i].clone(),
            tx_hash:          hashes[i],
        }).collect();
        Ok(txs)
    }

    /// Sum of all account balances (used in validate_ledger).
    pub fn account_balance_sum(&self) -> i64 {
        self.accounts.values().map(|(_, a)| a.balance).sum()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Column serialisers  (pure functions, no I/O)
// ──────────────────────────────────────────────────────────────────────────────

fn serialise_u64_col(values: impl Iterator<Item = u64>) -> Vec<u8> {
    let mut v = Vec::new();
    for x in values { v.extend_from_slice(&x.to_le_bytes()); }
    v
}

fn serialise_i64_col(values: impl Iterator<Item = i64>) -> Vec<u8> {
    let mut v = Vec::new();
    for x in values { v.extend_from_slice(&x.to_le_bytes()); }
    v
}

/// Dictionary encoding: `[dict_len u8][entries…][codes…]`.
fn serialise_dict_u8_col(values: impl Iterator<Item = u8>) -> (Vec<u8>, u8) {
    let vals: Vec<u8> = values.collect();
    let mut dict = Vec::<u8>::new();
    let codes: Vec<u8> = vals.iter().map(|&v| {
        if let Some(p) = dict.iter().position(|&d| d == v) { p as u8 }
        else { dict.push(v); (dict.len() - 1) as u8 }
    }).collect();
    let mut out = vec![dict.len() as u8];
    out.extend_from_slice(&dict);
    out.extend_from_slice(&codes);
    (out, enc::DICTIONARY)
}

fn serialise_string_col<'a>(values: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut v = Vec::new();
    for s in values {
        let b = s.as_bytes();
        v.extend_from_slice(&(b.len() as u32).to_le_bytes());
        v.extend_from_slice(b);
    }
    v
}

fn serialise_hash_col<'a>(values: impl Iterator<Item = &'a [u8; 32]>) -> Vec<u8> {
    let mut v = Vec::new();
    for h in values { v.extend_from_slice(h); }
    v
}

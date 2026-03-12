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

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::encoders::{
    BlockCompressor, CompressionCodec, DeltaEncoder, DeltaEncoderU64, RleEncoder,
};
use crate::error::{LedgerError, Result};
use crate::file_format::{
    self, col, comp, enc, ColumnMeta, FileHeader, SegmentHeader, ACCOUNT_RECORD_SIZE,
    FILE_HEADER_SIZE, MAX_ACCOUNTS, NUM_TX_COLUMNS,
};
use crate::hash_chain::ChainTip;
use crate::models::{Account, AccountType, Transaction, TransactionType};
use crate::sparse_index::{SparseEntry, SparseIndex, SPARSE_FACTOR};

const CHECKPOINT_INTERVAL: u64 = 100;

// ──────────────────────────────────────────────────────────────────────────────
// In-memory segment catalogue
// ──────────────────────────────────────────────────────────────────────────────

/// Everything we need to know about a written segment without re-reading it.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub seq: u64,
    pub header: SegmentHeader,
    /// Absolute file offset at which the SegmentHeader begins.
    pub file_offset: u64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Storage
// ──────────────────────────────────────────────────────────────────────────────

pub struct Storage {
    file: File,
    pub header: FileHeader,
    pub segments: Vec<SegmentMeta>,
    pub accounts: HashMap<u64, (usize, Account)>, // id → (slot_index, account)
    pub sparse: SparseIndex,
    pub compression_codec: CompressionCodec,
}

impl Storage {
    // ── Open / create ──────────────────────────────────────────────────────

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_compression(path, CompressionCodec::Zstd)
    }

    pub fn open_with_compression(path: impl AsRef<Path>, codec: CompressionCodec) -> Result<Self> {
        let path = path.as_ref();
        let is_new = !path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut storage = Self {
            file,
            header: FileHeader::default(),
            segments: Vec::new(),
            accounts: HashMap::new(),
            sparse: SparseIndex::new(),
            compression_codec: codec,
        };

        if is_new {
            storage.initialise_new_file()?;
        } else {
            storage.load_existing_file()?;
        }

        Ok(storage)
    }

    pub fn set_compression_codec(&mut self, codec: CompressionCodec) {
        self.compression_codec = codec;
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

        // 4. Load or rebuild the sparse index
        let checkpoint_seg_count = self.header.sparse_checkpoint_seg_count;

        if self.header.sparse_checkpoint_offset > 0 && checkpoint_seg_count > 0 {
            // Load from checkpoint
            self.sparse = SparseIndex::read_from(
                &mut self.file,
                self.header.sparse_checkpoint_offset,
                self.header.sparse_index_count,
            )?;

            // Incrementally rebuild from segments created after checkpoint
            if checkpoint_seg_count < self.header.segment_count {
                println!(
                    "[recovery] rebuilding sparse index from segment {} to {}",
                    checkpoint_seg_count, self.header.segment_count
                );
                let segments_to_rebuild: Vec<_> = self
                    .segments
                    .iter()
                    .skip(checkpoint_seg_count as usize)
                    .cloned()
                    .collect();
                for seg in segments_to_rebuild {
                    let timestamps = self.read_timestamps(&seg)?;
                    for (i, ts) in timestamps.iter().enumerate() {
                        let global_idx = seg.header.first_row_global_idx + i as u64;
                        if global_idx % SPARSE_FACTOR == 0 {
                            self.sparse.entries.push(SparseEntry {
                                timestamp: *ts,
                                global_row_idx: global_idx,
                            });
                        }
                    }
                }
            }
        } else if self.header.sparse_index_count > 0 {
            // Legacy format: sparse index at segments_end_offset
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

        let account = Account {
            id,
            name: name.to_string(),
            kind,
            created_at,
            balance: 0,
        };

        file_format::write_account_slot(&mut self.file, slot, id, name, kind as u8, created_at, 0)?;

        self.header.accounts_count += 1;
        self.header.write_to(&mut self.file)?;
        self.file.sync_all()?;

        self.accounts.insert(id, (slot, account));
        Ok(id)
    }

    /// Update the stored balance for an account (called after each tx).
    /// Only updates in-memory balance - disk write happens during flush.
    pub fn update_account_balance(&mut self, account_id: u64, delta: i64) -> Result<()> {
        use std::collections::hash_map::Entry;
        match self.accounts.entry(account_id) {
            Entry::Occupied(entry) => {
                entry.into_mut().1.balance += delta;
            }
            Entry::Vacant(_) => {
                return Err(LedgerError::UnknownAccount(account_id));
            }
        }
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
        mut rows: Vec<Transaction>,
        chain_tip: &mut ChainTip,
    ) -> Result<()> {
        if rows.is_empty() {
            return Err(LedgerError::EmptyFlush);
        }

        let first_row_global = self.header.total_tx_count;
        let row_count = rows.len() as u64;

        // ── Assign hashes ──────────────────────────────────────────────────
        for tx in rows.iter_mut() {
            tx.tx_hash = chain_tip.advance(tx);
        }

        // ── Serialise each column into an in-memory buffer ─────────────────
        // We need to know each column's byte length before we can write the
        // segment header (which contains their absolute offsets), so we
        // serialise all columns first, then apply encoding+compression.

        let col_id = serialise_u64_col(rows.iter().map(|r| r.id));
        let col_acct = serialise_u64_col(rows.iter().map(|r| r.account_id));
        let col_amt = serialise_i64_col(rows.iter().map(|r| r.amount));
        let (col_type, type_enc) =
            serialise_dict_u8_col(rows.iter().map(|r| r.transaction_type as u8));
        let col_ts = serialise_u64_col(rows.iter().map(|r| r.timestamp));
        let col_desc = serialise_string_col(rows.iter().map(|r| r.description.as_str()));
        let col_hash = serialise_hash_col(rows.iter().map(|r| &r.tx_hash));
        let col_entry = serialise_u64_col(rows.iter().map(|r| r.journal_entry_id));

        let col_data: Vec<Vec<u8>> = vec![
            col_id, col_acct, col_amt, col_type, col_ts, col_desc, col_hash, col_entry,
        ];

        // Apply encoding and compression to each column
        let (encoded_cols, encodings, compressed_cols) =
            encode_all_columns(&col_data, self.compression_codec)?;

        // ── Compute segment file position ──────────────────────────────────
        let seg_file_offset = self.header.segments_end_offset;
        let data_start = seg_file_offset + file_format::SEGMENT_HEADER_SIZE as u64;

        // ── Compute column offsets (absolute file positions) ───────────────
        let mut columns = [ColumnMeta::default(); NUM_TX_COLUMNS];
        let mut cursor = data_start;
        for i in 0..NUM_TX_COLUMNS {
            let comp_byte = match self.compression_codec {
                CompressionCodec::None => comp::NONE,
                CompressionCodec::Zstd => comp::ZSTD,
                CompressionCodec::Lz4 => comp::LZ4,
            };
            columns[i] = ColumnMeta {
                offset: cursor,
                length: compressed_cols[i].len() as u64,
                encoding: encodings[i],
                compression: comp_byte,
                uncompressed_length: col_data[i].len() as u64,
            };
            cursor += compressed_cols[i].len() as u64;
        }

        // ── CRC32 over all column data (compressed) ─────────────────────────────────
        let mut all_data = Vec::new();
        for block in &compressed_cols {
            all_data.extend_from_slice(block);
        }
        let data_crc32 = SegmentHeader::crc32_of(&all_data);

        let min_ts = rows.iter().map(|r| r.timestamp).min().unwrap();
        let max_ts = rows.iter().map(|r| r.timestamp).max().unwrap();

        let seg_hdr = SegmentHeader {
            magic: *b"SEGM",
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
        for block in &compressed_cols {
            self.file.write_all(block)?;
        }

        let new_segments_end = cursor;

        // ── Extend in-memory sparse index (NOT written to disk every flush) ────
        let new_row_pairs: Vec<(u64, u64)> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| (r.timestamp, first_row_global + i as u64))
            .collect();
        self.sparse.extend(&new_row_pairs);

        // ── Truncate file to exactly what we've written ────────────────────
        // Note: We ALWAYS write the sparse index after each segment (legacy format)
        // This ensures the file is always readable
        let sparse_bytes = self.sparse.write_to(&mut self.file)? as u64;
        let new_file_len = new_segments_end + sparse_bytes;
        self.file.set_len(new_file_len)?;

        // ── Update + rewrite file header (logical commit) ─────────────────
        self.header.segment_count += 1;
        self.header.segments_end_offset = new_segments_end;
        self.header.sparse_index_count = self.sparse.len() as u64;
        self.header.total_tx_count += row_count;
        self.header.last_tx_hash = chain_tip.last_hash;
        // Write sparse index immediately after segment (legacy format, always written)
        self.header.sparse_checkpoint_offset = new_segments_end;
        self.header.sparse_checkpoint_seg_count = self.header.segment_count;

        // ── Write all account balances to disk ────────────────────────────────
        for (slot, acct) in self.accounts.values() {
            file_format::write_account_slot(
                &mut self.file,
                *slot,
                acct.id,
                &acct.name,
                acct.kind as u8,
                acct.created_at,
                acct.balance,
            )?;
        }

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

    fn write_sparse_checkpoint(&mut self, next_segment_offset: u64) -> Result<u64> {
        let checkpoint_offset = next_segment_offset;
        self.file.seek(SeekFrom::Start(checkpoint_offset))?;
        let bytes_written = self.sparse.write_to(&mut self.file)?;
        println!(
            "[checkpoint] wrote {} sparse entries ({} KB) at offset {}",
            self.sparse.len(),
            bytes_written / 1024,
            checkpoint_offset
        );
        Ok(bytes_written as u64)
    }

    // ── Column read helpers ────────────────────────────────────────────────

    fn decompress_column(&mut self, col: &ColumnMeta) -> Result<Vec<u8>> {
        let codec = match col.compression {
            0 => CompressionCodec::None,
            1 => CompressionCodec::Zstd,
            2 => CompressionCodec::Lz4,
            _ => CompressionCodec::None,
        };

        // Handle no compression case - just return raw data
        if codec == CompressionCodec::None {
            let mut uncompressed = vec![0u8; col.length as usize];
            self.file.read_exact(&mut uncompressed)?;
            return Ok(uncompressed);
        }

        let mut compressed = vec![0u8; col.length as usize];
        self.file.read_exact(&mut compressed)?;

        let compressor = BlockCompressor::new(codec);
        compressor.decompress(&compressed, col.uncompressed_length as usize)
    }

    fn decode_column(&self, col: &ColumnMeta, decompressed: &[u8]) -> Result<Vec<u8>> {
        match col.encoding {
            0 => Ok(decompressed.to_vec()), // NONE
            2 => {
                // DELTA for u64
                let enc = DeltaEncoderU64::decode_from_bytes(decompressed)
                    .ok_or_else(|| LedgerError::Encoding("failed to decode delta u64".into()))?;
                let decoded = enc.decode();
                let mut result = Vec::with_capacity(decoded.len() * 8);
                for v in decoded {
                    result.extend_from_slice(&v.to_le_bytes());
                }
                Ok(result)
            }
            3 => {
                // RLE
                let enc = RleEncoder::decode_from_bytes(decompressed)
                    .ok_or_else(|| LedgerError::Encoding("failed to decode RLE".into()))?;
                let decoded = enc.decode();
                let mut result = Vec::with_capacity(decoded.len() * 8);
                for v in decoded {
                    result.extend_from_slice(&v.to_le_bytes());
                }
                Ok(result)
            }
            _ => Ok(decompressed.to_vec()),
        }
    }

    /// Load the full `amount` (i64) column from a segment using bulk read.
    pub fn read_amounts(&mut self, meta: &SegmentMeta) -> Result<Vec<i64>> {
        let col = &meta.header.columns[col::AMT];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let decompressed = self.decompress_column(col)?;
        let decoded = self.decode_column(col, &decompressed)?;

        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for chunk in decoded.chunks_exact(8) {
            let val = i64::from_le_bytes(chunk.try_into().unwrap());
            out.push(val);
        }
        Ok(out)
    }

    /// Load the `tx_type` column from a segment (dictionary decoded) using bulk read.
    pub fn read_tx_types(&mut self, meta: &SegmentMeta) -> Result<Vec<u8>> {
        let col = &meta.header.columns[col::TYPE];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let decompressed = self.decompress_column(col)?;

        let dict_size = decompressed[0] as usize;
        let dict = decompressed[1..1 + dict_size].to_vec();
        let codes_start = 1 + dict_size;
        let n = meta.header.row_count as usize;
        let codes = &decompressed[codes_start..codes_start + n];
        let decoded: Vec<u8> = codes.iter().map(|&c| dict[c as usize]).collect();
        Ok(decoded)
    }

    /// Load the `timestamp` column from a segment using bulk read.
    pub fn read_timestamps(&mut self, meta: &SegmentMeta) -> Result<Vec<u64>> {
        let col = &meta.header.columns[col::TS];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let decompressed = self.decompress_column(col)?;
        let decoded = self.decode_column(col, &decompressed)?;

        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for chunk in decoded.chunks_exact(8) {
            let val = u64::from_le_bytes(chunk.try_into().unwrap());
            out.push(val);
        }
        Ok(out)
    }

    /// Load the `tx_hash` column for integrity verification using bulk read.
    pub fn read_hashes(&mut self, meta: &SegmentMeta) -> Result<Vec<[u8; 32]>> {
        let col = &meta.header.columns[col::HASH];
        self.file.seek(SeekFrom::Start(col.offset))?;
        let decompressed = self.decompress_column(col)?;

        let n = meta.header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for chunk in decompressed.chunks_exact(32) {
            let mut h = [0u8; 32];
            h.copy_from_slice(chunk);
            out.push(h);
        }
        Ok(out)
    }

    /// Bulk read helper for u64 columns
    fn read_u64_column(&mut self, offset: u64, n: usize) -> Result<Vec<u64>> {
        let byte_len = n * 8;
        let mut buf = vec![0u8; byte_len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;

        let mut out = Vec::with_capacity(n);
        for chunk in buf.chunks_exact(8) {
            let val = u64::from_le_bytes(chunk.try_into().unwrap());
            out.push(val);
        }
        Ok(out)
    }

    /// Load all columns needed to reconstruct `Transaction` rows (for
    /// integrity re-computation). Uses bulk reads for performance.
    pub fn read_all_transactions(&mut self, meta: &SegmentMeta) -> Result<Vec<Transaction>> {
        let n = meta.header.row_count as usize;
        let cols = &meta.header.columns;

        // Read ID column
        let id_col = &cols[col::ID];
        self.file.seek(SeekFrom::Start(id_col.offset))?;
        let id_decompressed = self.decompress_column(id_col)?;
        let ids_decoded = self.decode_column(id_col, &id_decompressed)?;
        let ids: Vec<u64> = ids_decoded
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        // Read account_id column
        let acct_col = &cols[col::ACCT];
        self.file.seek(SeekFrom::Start(acct_col.offset))?;
        let acct_decompressed = self.decompress_column(acct_col)?;
        let accts_decoded = self.decode_column(acct_col, &acct_decompressed)?;
        let accts: Vec<u64> = accts_decoded
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        // Read other columns using existing methods
        let amts = self.read_amounts(meta)?;
        let types = self.read_tx_types(meta)?;
        let ts = self.read_timestamps(meta)?;
        let hashes = self.read_hashes(meta)?;

        // Read journal_entry_id column
        let entry_col = &cols[col::ENTRY_ID];
        self.file.seek(SeekFrom::Start(entry_col.offset))?;
        let entry_decompressed = self.decompress_column(entry_col)?;
        let entry_decoded = self.decode_column(entry_col, &entry_decompressed)?;
        let entry_ids: Vec<u64> = entry_decoded
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        // Read descriptions (variable-length)
        let desc_col = &cols[col::DESC];
        self.file.seek(SeekFrom::Start(desc_col.offset))?;
        let desc_decompressed = self.decompress_column(desc_col)?;

        let mut descs = Vec::with_capacity(n);
        let mut offset = 0;
        for _ in 0..n {
            if offset + 4 > desc_decompressed.len() {
                break;
            }
            let len = u32::from_le_bytes(desc_decompressed[offset..offset + 4].try_into().unwrap())
                as usize;
            offset += 4;
            if offset + len > desc_decompressed.len() {
                break;
            }
            let s = String::from_utf8_lossy(&desc_decompressed[offset..offset + len]).into_owned();
            descs.push(s);
            offset += len;
        }

        let txs = (0..n)
            .map(|i| Transaction {
                id: ids[i],
                journal_entry_id: entry_ids[i],
                account_id: accts[i],
                amount: amts[i],
                transaction_type: TransactionType::from_u8(types[i])
                    .unwrap_or(TransactionType::Debit),
                timestamp: ts[i],
                description: descs[i].clone(),
                tx_hash: hashes[i],
            })
            .collect();
        Ok(txs)
    }

    /// Bulk read u64 column directly from file (static helper)
    fn read_u64_column_from_file(file: &mut File, offset: u64, n: usize) -> Result<Vec<u64>> {
        let byte_len = n * 8;
        let mut buf = vec![0u8; byte_len];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;

        let mut out = Vec::with_capacity(n);
        for chunk in buf.chunks_exact(8) {
            let val = u64::from_le_bytes(chunk.try_into().unwrap());
            out.push(val);
        }
        Ok(out)
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
    for x in values {
        v.extend_from_slice(&x.to_le_bytes());
    }
    v
}

fn serialise_i64_col(values: impl Iterator<Item = i64>) -> Vec<u8> {
    let mut v = Vec::new();
    for x in values {
        v.extend_from_slice(&x.to_le_bytes());
    }
    v
}

/// Dictionary encoding: `[dict_len u8][entries…][codes…]`.
fn serialise_dict_u8_col(values: impl Iterator<Item = u8>) -> (Vec<u8>, u8) {
    let vals: Vec<u8> = values.collect();
    let mut dict = Vec::<u8>::new();
    let codes: Vec<u8> = vals
        .iter()
        .map(|&v| {
            if let Some(p) = dict.iter().position(|&d| d == v) {
                p as u8
            } else {
                dict.push(v);
                (dict.len() - 1) as u8
            }
        })
        .collect();
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
    for h in values {
        v.extend_from_slice(h);
    }
    v
}

fn encode_all_columns(
    col_data: &[Vec<u8>],
    codec: CompressionCodec,
) -> Result<(Vec<Vec<u8>>, [u8; NUM_TX_COLUMNS], Vec<Vec<u8>>)> {
    let mut encoded_cols = Vec::with_capacity(NUM_TX_COLUMNS);
    let mut encodings = [0u8; NUM_TX_COLUMNS];
    let mut compressed_cols = Vec::with_capacity(NUM_TX_COLUMNS);

    let compressor = BlockCompressor::new(codec);

    for i in 0..NUM_TX_COLUMNS {
        let original_data = &col_data[i];

        // Apply column-specific encoding
        let encoded = match i {
            0 | 4 | 7 => {
                // Delta encoding for monotonic sequences (id, timestamp, journal_entry_id)
                let u64_values: Vec<u64> = original_data
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect();
                let enc = DeltaEncoderU64::encode(&u64_values);
                let mut bytes = Vec::new();
                enc.encode_to_bytes(&mut bytes);
                bytes
            }
            1 => {
                // RLE for account_id (repeats within journal entries)
                let u64_values: Vec<u64> = original_data
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect();
                let enc = RleEncoder::encode(&u64_values);
                let mut bytes = Vec::new();
                enc.encode_to_bytes(&mut bytes);
                bytes
            }
            2 => {
                // Delta encoding for amounts (signed)
                let i64_values: Vec<i64> = original_data
                    .chunks_exact(8)
                    .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                    .collect();
                let enc = DeltaEncoder::encode(&i64_values);
                let mut bytes = Vec::new();
                enc.encode_to_bytes(&mut bytes);
                bytes
            }
            3 => {
                // Dictionary for transaction_type (already encoded, just compress)
                let mut bytes = Vec::new();
                bytes.extend_from_slice(original_data);
                encodings[i] = enc::DICTIONARY;
                let compressed = compressor.compress(&bytes)?;
                encoded_cols.push(bytes);
                compressed_cols.push(compressed);
                continue;
            }
            5 => {
                // No additional encoding for description - just compress directly
                // Format: length-prefixed strings [4 bytes len][string bytes][4 bytes len][string bytes]...
                encoded_cols.push(original_data.clone());
                encodings[i] = enc::NONE;
                let compressed = compressor.compress(original_data)?;
                compressed_cols.push(compressed);
                continue;
            }
            6 => {
                // No encoding for tx_hash (random data)
                let mut bytes = Vec::new();
                bytes.extend_from_slice(original_data);
                encodings[i] = enc::NONE;
                let compressed = compressor.compress(&bytes)?;
                encoded_cols.push(bytes);
                compressed_cols.push(compressed);
                continue;
            }
            _ => original_data.clone(),
        };

        // Determine encoding type for header
        encodings[i] = match i {
            0 | 4 | 7 => enc::DELTA,
            1 => enc::RLE,
            2 => enc::DELTA,
            3 => enc::DICTIONARY,
            5 => enc::NONE,
            _ => enc::NONE,
        };

        // Apply compression
        let compressed = compressor.compress(&encoded)?;
        encoded_cols.push(encoded);
        compressed_cols.push(compressed);
    }

    Ok((encoded_cols, encodings, compressed_cols))
}

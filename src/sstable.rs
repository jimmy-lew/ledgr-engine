//! # SSTable – Columnar On-Disk Format
//!
//! ## File Layout (byte-by-byte specification)
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────┐
//! │                        HEADER  (0x8B bytes)                    │
//! │                                                                │
//! │  0x00–0x03  [4 B]   Magic bytes          "LEDG"                │
//! │  0x04       [1 B]   Version              0x01                  │
//! │  0x05–0x0C  [8 B]   Row count            u64 LE                │
//! │  0x0D–0x14  [8 B]   Min timestamp        u64 LE                │
//! │  0x15–0x1C  [8 B]   Max timestamp        u64 LE                │
//! │  0x1D–0x20  [4 B]   Header CRC32         u32 LE  ← over bytes  │
//! │                                           0x00–0x1C            │
//! │  0x21–0x22  [2 B]   RESERVED / padding                         │
//! │  0x23–0x88  [6×17B] Column metadata array                      │
//! │    per column (17 bytes):                                      │
//! │      [8 B]  Absolute file offset of column block  u64 LE       │
//! │      [8 B]  Byte length of column block            u64 LE      │
//! │      [1 B]  Encoding: 0=None 1=Dictionary 2=RLE                │
//! │                                                                │
//! │  Total header = 4+1+8+8+8+4+2+(6×17) = 137 bytes (0x89)        │
//! └────────────────────────────────────────────────────────────────┘
//! ┌────────────────────────────────────────────────────────────────┐
//! │  COLUMN BLOCK 0 – id          (u64 × row_count, tightly packed)│
//! │  COLUMN BLOCK 1 – account_id  (u64 × row_count)                │
//! │  COLUMN BLOCK 2 – amount      (i64 × row_count, cents)         │
//! │  COLUMN BLOCK 3 – tx_type     (dict-encoded u8 × row_count)    │
//! │  COLUMN BLOCK 4 – timestamp   (u64 × row_count)                │
//! │  COLUMN BLOCK 5 – description (len-prefixed UTF-8 strings)     │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Column Index Constants
//! | Idx | Name             |
//! |-----|------------------|
//! |  0  | id               |
//! |  1  | account_id       |
//! |  2  | amount           |
//! |  3  | transaction_type |
//! |  4  | timestamp        |
//! |  5  | description      |

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use crc32fast::Hasher as Crc32Hasher;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::{LedgerError, Result};
use crate::models::{Transaction, TransactionType};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

pub const MAGIC: [u8; 4] = *b"LEDG";
pub const VERSION: u8 = 0x01;
pub const NUM_COLUMNS: usize = 6;
pub const HEADER_SIZE: usize = 4 + 1 + 8 + 8 + 8 + 4 + 2 + NUM_COLUMNS * 17; // 137

/// Column indices – use these everywhere to avoid magic numbers.
pub mod col {
    pub const ID: usize = 0;
    pub const ACCT: usize = 1;
    pub const AMT: usize = 2;
    pub const TYPE: usize = 3;
    pub const TS: usize = 4;
    pub const DESC: usize = 5;
}

/// Encoding byte stored in column metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    None = 0,
    Dictionary = 1,
    Rle = 2,
}

impl Encoding {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Dictionary,
            2 => Self::Rle,
            _ => Self::None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Core Header Structs
// ──────────────────────────────────────────────────────────────────────────────

/// 17-byte per-column metadata block.
#[derive(Debug, Clone, Copy, Default)]
pub struct ColumnMeta {
    /// Absolute byte offset inside the file where this column's data starts.
    pub offset: u64,
    /// Byte length of this column's data block.
    pub length: u64,
    /// Compression / encoding applied to this block.
    pub encoding: u8,
}

/// The fixed-size SSTable header (137 bytes on disk).
///
/// Constructed in memory, then serialised to disk.  On read the checksum is
/// verified before any field is trusted.
#[derive(Debug, Clone)]
pub struct SSTableHeader {
    // 0x00–0x03
    pub magic: [u8; 4],
    // 0x04
    pub version: u8,
    // 0x05–0x0C
    pub row_count: u64,
    // 0x0D–0x14
    pub min_ts: u64,
    // 0x15–0x1C
    pub max_ts: u64,
    // 0x1D–0x20  (CRC32 is computed over bytes 0x00..=0x1C, i.e. the 29 bytes
    //              before this field, then stored here as u32 LE)
    pub checksum: u32,
    // 0x21–0x22  2 reserved bytes (zero)
    // 0x23–0x88  column metadata
    pub columns: [ColumnMeta; NUM_COLUMNS],
}

impl SSTableHeader {
    /// Compute the CRC32 over the bytes that precede the checksum field.
    /// Those are: magic(4) + version(1) + row_count(8) + min_ts(8) + max_ts(8) = 29 bytes.
    pub fn compute_checksum(
        magic: &[u8; 4],
        version: u8,
        row_count: u64,
        min_ts: u64,
        max_ts: u64,
    ) -> u32 {
        let mut h = Crc32Hasher::new();
        h.update(magic);
        h.update(&[version]);
        h.update(&row_count.to_le_bytes());
        h.update(&min_ts.to_le_bytes());
        h.update(&max_ts.to_le_bytes());
        h.finalize()
    }

    // ── Serialisation ──────────────────────────────────────────────────────

    /// Write the header to any `Write + Seek` sink.
    /// Returns the number of bytes written (always `HEADER_SIZE`).
    pub fn write_to<W: Write + Seek>(&self, w: &mut W) -> Result<usize> {
        // 0x00 – magic
        w.write_all(&self.magic)?;
        // 0x04 – version
        w.write_u8(self.version)?;
        // 0x05 – row_count
        w.write_u64::<LE>(self.row_count)?;
        // 0x0D – min_ts
        w.write_u64::<LE>(self.min_ts)?;
        // 0x15 – max_ts
        w.write_u64::<LE>(self.max_ts)?;
        // 0x1D – checksum
        w.write_u32::<LE>(self.checksum)?;
        // 0x21 – 2 reserved bytes
        w.write_all(&[0u8; 2])?;
        // 0x23 – column metadata array
        for col in &self.columns {
            w.write_u64::<LE>(col.offset)?;
            w.write_u64::<LE>(col.length)?;
            w.write_u8(col.encoding)?;
        }
        debug_assert_eq!(
            w.stream_position()? as usize,
            HEADER_SIZE,
            "header size drift – update HEADER_SIZE constant"
        );
        Ok(HEADER_SIZE)
    }

    // ── Deserialisation ────────────────────────────────────────────────────

    /// Read and validate the header from any `Read` source.
    ///
    /// Fails fast with `LedgerError::BadMagic` or
    /// `LedgerError::ChecksumMismatch` before trusting any other field.
    pub fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        // 0x00 – magic
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(LedgerError::BadMagic);
        }

        // 0x04 – version
        let version = r.read_u8()?;
        if version != VERSION {
            return Err(LedgerError::UnsupportedVersion(version));
        }

        // 0x05 – row_count
        let row_count = r.read_u64::<LE>()?;
        // 0x0D – min_ts
        let min_ts = r.read_u64::<LE>()?;
        // 0x15 – max_ts
        let max_ts = r.read_u64::<LE>()?;

        // 0x1D – checksum (verify before proceeding)
        let stored_checksum = r.read_u32::<LE>()?;
        let computed = Self::compute_checksum(&magic, version, row_count, min_ts, max_ts);
        if stored_checksum != computed {
            return Err(LedgerError::ChecksumMismatch {
                stored: stored_checksum,
                computed,
            });
        }

        // 0x21 – 2 reserved bytes (ignore)
        let mut _reserved = [0u8; 2];
        r.read_exact(&mut _reserved)?;

        // 0x23 – column metadata
        let mut columns = [ColumnMeta::default(); NUM_COLUMNS];
        for col in columns.iter_mut() {
            col.offset = r.read_u64::<LE>()?;
            col.length = r.read_u64::<LE>()?;
            col.encoding = r.read_u8()?;
        }

        Ok(Self {
            magic,
            version,
            row_count,
            min_ts,
            max_ts,
            checksum: stored_checksum,
            columns,
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SSTable Writer
// ──────────────────────────────────────────────────────────────────────────────

/// Builds and writes a complete SSTable from a slice of transactions.
///
/// The writer:
/// 1. Serialises each column block in order.
/// 2. Records each block's absolute file offset and byte length.
/// 3. Writes the header (with checksum and zone-map timestamps) at byte 0.
pub struct SSTableWriter;

impl SSTableWriter {
    pub fn write<W: Write + Seek>(w: &mut W, rows: &[Transaction]) -> Result<SSTableHeader> {
        if rows.is_empty() {
            return Err(LedgerError::Encoding("Cannot write empty SSTable".into()));
        }

        // ── Step 1: reserve space for the header ───────────────────────────
        w.seek(SeekFrom::Start(0))?;
        w.write_all(&[0u8; HEADER_SIZE])?; // placeholder

        // ── Step 2: write column blocks sequentially ───────────────────────
        let mut columns = [ColumnMeta::default(); NUM_COLUMNS];

        // COLUMN 0 – id (u64, no encoding)
        columns[col::ID] = Self::write_col_u64(w, rows.iter().map(|r| r.id))?;

        // COLUMN 1 – account_id (u64, no encoding)
        columns[col::ACCT] = Self::write_col_u64(w, rows.iter().map(|r| r.account_id))?;

        // COLUMN 2 – amount (i64, cents, no encoding)
        columns[col::AMT] = Self::write_col_i64(w, rows.iter().map(|r| r.amount))?;

        // COLUMN 3 – transaction_type (dictionary encoded)
        //   Dictionary: [dict_size u8][entry0 u8]…[entryN u8][codes…]
        //   For a binary enum this compresses to almost nothing.
        columns[col::TYPE] =
            Self::write_col_dict_u8(w, rows.iter().map(|r| r.transaction_type as u8))?;

        // COLUMN 4 – timestamp (u64, no encoding)
        columns[col::TS] = Self::write_col_u64(w, rows.iter().map(|r| r.timestamp))?;

        // COLUMN 5 – description (length-prefixed UTF-8, no encoding)
        columns[col::DESC] =
            Self::write_col_strings(w, rows.iter().map(|r| r.description.as_str()))?;

        // ── Step 3: compute zone-map & checksum; patch header ─────────────
        let min_ts = rows.iter().map(|r| r.timestamp).min().unwrap();
        let max_ts = rows.iter().map(|r| r.timestamp).max().unwrap();
        let row_count = rows.len() as u64;

        let checksum = SSTableHeader::compute_checksum(&MAGIC, VERSION, row_count, min_ts, max_ts);

        let header = SSTableHeader {
            magic: MAGIC,
            version: VERSION,
            row_count,
            min_ts,
            max_ts,
            checksum,
            columns,
        };

        // Seek back to byte 0 and overwrite the placeholder with real header.
        w.seek(SeekFrom::Start(0))?;
        header.write_to(w)?;

        Ok(header)
    }

    // ── Column serialisers ─────────────────────────────────────────────────

    fn write_col_u64<W: Write + Seek>(
        w: &mut W,
        values: impl Iterator<Item = u64>,
    ) -> Result<ColumnMeta> {
        let offset = w.stream_position()?;
        for v in values {
            w.write_u64::<LE>(v)?;
        }
        let end = w.stream_position()?;
        Ok(ColumnMeta {
            offset,
            length: end - offset,
            encoding: Encoding::None as u8,
        })
    }

    fn write_col_i64<W: Write + Seek>(
        w: &mut W,
        values: impl Iterator<Item = i64>,
    ) -> Result<ColumnMeta> {
        let offset = w.stream_position()?;
        for v in values {
            w.write_i64::<LE>(v)?;
        }
        let end = w.stream_position()?;
        Ok(ColumnMeta {
            offset,
            length: end - offset,
            encoding: Encoding::None as u8,
        })
    }

    /// Dictionary encoding for low-cardinality u8 columns (e.g. transaction_type).
    ///
    /// On-disk layout:
    /// ```text
    /// [1 B dict_size][dict_size × 1 B values][row_count × 1 B codes]
    /// ```
    fn write_col_dict_u8<W: Write + Seek>(
        w: &mut W,
        values: impl Iterator<Item = u8>,
    ) -> Result<ColumnMeta> {
        let vals: Vec<u8> = values.collect();
        // Build dictionary (order of first appearance)
        let mut dict: Vec<u8> = Vec::new();
        let codes: Vec<u8> = vals
            .iter()
            .map(|&v| {
                if let Some(pos) = dict.iter().position(|&d| d == v) {
                    pos as u8
                } else {
                    dict.push(v);
                    (dict.len() - 1) as u8
                }
            })
            .collect();

        let offset = w.stream_position()?;
        w.write_u8(dict.len() as u8)?;
        w.write_all(&dict)?;
        w.write_all(&codes)?;
        let end = w.stream_position()?;
        Ok(ColumnMeta {
            offset,
            length: end - offset,
            encoding: Encoding::Dictionary as u8,
        })
    }

    /// Variable-length strings: each string is preceded by a 4-byte LE length prefix.
    fn write_col_strings<'a, W: Write + Seek>(
        w: &mut W,
        values: impl Iterator<Item = &'a str>,
    ) -> Result<ColumnMeta> {
        let offset = w.stream_position()?;
        for s in values {
            let bytes = s.as_bytes();
            w.write_u32::<LE>(bytes.len() as u32)?;
            w.write_all(bytes)?;
        }
        let end = w.stream_position()?;
        Ok(ColumnMeta {
            offset,
            length: end - offset,
            encoding: Encoding::None as u8,
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SSTable Reader  –  the column-seeking read path
// ──────────────────────────────────────────────────────────────────────────────

/// Stateless reader that uses header metadata to seek **directly** to columns
/// without loading the entire file.
pub struct SSTableReader;

impl SSTableReader {
    // ── Zone-map check ─────────────────────────────────────────────────────

    /// Return `true` if this SSTable can contain rows inside `[start, end]`.
    /// Uses only the two 8-byte zone-map fields in the header – O(1), no data read.
    pub fn overlaps_time_range(header: &SSTableHeader, start: u64, end: u64) -> bool {
        header.min_ts <= end && header.max_ts >= start
    }

    // ── Amount column ──────────────────────────────────────────────────────

    /// Seek directly to the amount column block and sum every value.
    ///
    /// This is the hot path for `validate_ledger`.  We touch **only** the 8 bytes
    /// of column metadata at `header.columns[col::AMT]` and then the `row_count × 8`
    /// bytes of the amount block itself – nothing else is read from disk.
    pub fn sum_amounts<RS: Read + Seek>(file: &mut RS, header: &SSTableHeader) -> Result<i64> {
        let meta = &header.columns[col::AMT];

        // ── SEEK ── jump directly over all preceding columns
        file.seek(SeekFrom::Start(meta.offset))?;

        let mut total: i64 = 0;
        for _ in 0..header.row_count {
            total = total
                .checked_add(file.read_i64::<LE>()?)
                .ok_or_else(|| LedgerError::Encoding("i64 overflow summing amounts".into()))?;
        }
        Ok(total)
    }

    // ── Amount + type columns (for expense summary) ────────────────────────

    /// Read the `amount` and `transaction_type` columns, filtered by a time range.
    ///
    /// Because we need to correlate row positions across two non-adjacent columns,
    /// we:
    ///   1. Read the `timestamp` column into a bitmask of matching row indices.
    ///   2. Seek to `amount` and read only the matching offsets.
    ///   3. Seek to `transaction_type` and decode only the matching rows.
    ///
    /// For very large SSTables a more sophisticated approach would use SIMD or
    /// vectorised predicate evaluation; this implementation is deliberately clear.
    pub fn aggregate_by_type_in_range<RS: Read + Seek>(
        file: &mut RS,
        header: &SSTableHeader,
        start: u64,
        end: u64,
    ) -> Result<(i64, i64)> {
        // returns (total_debits, total_credits)
        let n = header.row_count as usize;

        // ── Pass 1: read timestamps → build predicate bitmask ─────────────
        let ts_meta = &header.columns[col::TS];
        file.seek(SeekFrom::Start(ts_meta.offset))?;

        let mut matching: Vec<bool> = Vec::with_capacity(n);
        for _ in 0..n {
            let ts = file.read_u64::<LE>()?;
            matching.push(ts >= start && ts <= end);
        }

        // ── Pass 2: read amounts for matching rows ─────────────────────────
        let amt_meta = &header.columns[col::AMT];
        file.seek(SeekFrom::Start(amt_meta.offset))?;

        let mut amounts: Vec<i64> = Vec::with_capacity(n);
        for _ in 0..n {
            amounts.push(file.read_i64::<LE>()?);
        }

        // ── Pass 3: decode transaction_type (dictionary encoded) ───────────
        let type_meta = &header.columns[col::TYPE];
        file.seek(SeekFrom::Start(type_meta.offset))?;

        let dict_size = file.read_u8()? as usize;
        let mut dict = vec![0u8; dict_size];
        file.read_exact(&mut dict)?;
        let mut codes = vec![0u8; n];
        file.read_exact(&mut codes)?;

        // ── Aggregate ──────────────────────────────────────────────────────
        let mut total_debits: i64 = 0;
        let mut total_credits: i64 = 0;

        for i in 0..n {
            if !matching[i] {
                continue;
            }
            let raw_type = dict.get(codes[i] as usize).copied().unwrap_or(0);
            match TransactionType::from_u8(raw_type) {
                Some(TransactionType::Debit) => total_debits += amounts[i],
                Some(TransactionType::Credit) => total_credits += amounts[i],
                None => {}
            }
        }

        Ok((total_debits, total_credits))
    }

    /// Read the `account_id` column and return a vec aligned with row indices.
    /// Used by the B-Tree / Hash index rebuild path.
    pub fn read_account_ids<RS: Read + Seek>(
        file: &mut RS,
        header: &SSTableHeader,
    ) -> Result<Vec<u64>> {
        let meta = &header.columns[col::ACCT];
        file.seek(SeekFrom::Start(meta.offset))?;
        let n = header.row_count as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(file.read_u64::<LE>()?);
        }
        Ok(out)
    }
}

//! # Single-File Format Specification
//!
//! All multi-byte integers are **little-endian**.
//!
//! ## File Header (512 bytes, always at offset 0)
//!
//! ```text
//! Offset   Size   Field
//! ──────  ──────  ──────────────────────────────────────────────────────
//! 0x000      4    Magic bytes: "LDGR"
//! 0x004      1    Version: 0x01
//! 0x005      3    Reserved (zero)
//! 0x008      8    accounts_count         u64 – active account slots
//! 0x010      8    segment_count          u64 – number of written segments
//! 0x018      8    segments_end_offset    u64 – byte where last segment ends
//! 0x020      8    sparse_index_count     u64 – number of sparse entries
//! 0x028      8    total_tx_count         u64 – grand total rows across all segs
//! 0x030     32    genesis_hash           [u8;32] – prev pointer for row 0
//! 0x050     32    last_tx_hash           [u8;32] – hash of most-recent tx
//! 0x070      4    header_crc32           CRC32 over bytes [0x000..0x070)
//! 0x074      8    sparse_checkpoint_offset u64 – file offset of sparse checkpoint (0 = none)
//! 0x07C      8    sparse_checkpoint_seg_count u64 – segment count at checkpoint
//! 0x084    140    padding (zeroes)
//! ──────  ─────   Total = 512 bytes  (0x200)
//! ```
//!
//! ## Accounts Region  (offsets 512 … 131 583)
//!
//! 1 024 fixed slots × 128 bytes each.
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ───────────────────────────────────
//! 0x00    1     is_active  (0 = empty, 1 = occupied)
//! 0x01    8     id         u64
//! 0x09    1     kind       AccountType u8
//! 0x0A    8     created_at u64
//! 0x12    8     balance    i64  (cents; running total)
//! 0x1A    2     name_len   u16
//! 0x1C   64     name       UTF-8, null-padded to 64 bytes
//! 0x5C   36     padding
//! ──────  ────  Total = 128 bytes
//! ```
//!
//! ## Segment Header (256 bytes, immediately before column data)
//!
//! ```text
//! Offset  Size  Field
//! ──────  ────  ─────────────────────────────────────────────────────────
//! 0x00    4     Magic bytes: "SEGM"
//! 0x04    8     row_count              u64
//! 0x0C    8     min_ts                 u64 (zone map lo)
//! 0x14    8     max_ts                 u64 (zone map hi)
//! 0x1C    8     first_row_global_idx   u64 (global index of row 0)
//! 0x24   64     col_offsets[8]         u64 each – absolute file offsets
//! 0x64   64     col_lengths[8]         u64 each – byte lengths
//! 0xA4    8     col_encodings[8]       u8 each
//! 0xAC    4     data_crc32             CRC32 over all column data bytes
//! 0xB0   80     padding
//! ──────  ────  Total = 256 bytes (0x100)
//! ```
//!
//! ## Column Layout (8 columns per segment)
//!
//! | idx | name             | encoding  | element size |
//! |-----|------------------|-----------|--------------|
//! |  0  | id               | None      | 8 bytes      |
//! |  1  | account_id       | None      | 8 bytes      |
//! |  2  | amount           | None      | 8 bytes      |
//! |  3  | transaction_type | Dict(u8)  | 1 byte code  |
//! |  4  | timestamp        | None      | 8 bytes      |
//! |  5  | description      | None      | 4+N bytes    |
//! |  6  | tx_hash          | None      | 32 bytes     |
//! |  7  | journal_entry_id | None      | 8 bytes      | ← groups legs together
//!
//! ## Sparse Timestamp Index (at `segments_end_offset`)
//!
//! ```text
//! [8 bytes: count u64]
//! [count × 16 bytes: (timestamp u64, global_row_idx u64)]
//! ```

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use crc32fast::Hasher as Crc32Hasher;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::{LedgerError, Result};

// ──────────────────────────────────────────────────────────────────────────────
// Layout constants
// ──────────────────────────────────────────────────────────────────────────────

pub const FILE_MAGIC: [u8; 4] = *b"LDGR";
pub const SEGMENT_MAGIC: [u8; 4] = *b"SEGM";
pub const VERSION: u8 = 0x01;

pub const FILE_HEADER_SIZE: usize = 512;
pub const ACCOUNT_RECORD_SIZE: usize = 128;
pub const MAX_ACCOUNTS: usize = 1_024;
pub const ACCOUNTS_REGION_SIZE: usize = MAX_ACCOUNTS * ACCOUNT_RECORD_SIZE; // 131 072
/// Byte offset where the first segment begins.
pub const SEGMENTS_BASE_OFFSET: u64 = (FILE_HEADER_SIZE + ACCOUNTS_REGION_SIZE) as u64; // 131 584

pub const SEGMENT_HEADER_SIZE: usize = 256;
pub const NUM_TX_COLUMNS: usize = 8;

/// Encoding byte values stored in `col_encodings`.
pub mod enc {
    pub const NONE: u8 = 0;
    pub const DICTIONARY: u8 = 1;
}

/// Column index constants – never use raw numbers.
pub mod col {
    pub const ID: usize = 0;
    pub const ACCT: usize = 1;
    pub const AMT: usize = 2;
    pub const TYPE: usize = 3;
    pub const TS: usize = 4;
    pub const DESC: usize = 5;
    pub const HASH: usize = 6;
    /// All legs of the same `JournalEntry` share this value.
    /// Enables grouping legs back into their originating entry.
    pub const ENTRY_ID: usize = 7;
}

// ──────────────────────────────────────────────────────────────────────────────
// File Header
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FileHeader {
    // 0x000
    pub magic: [u8; 4],
    // 0x004
    pub version: u8,
    // 0x008
    pub accounts_count: u64,
    // 0x010
    pub segment_count: u64,
    // 0x018
    /// Byte offset at which the current sparse index begins
    /// (and at which the next new segment will be written BEFORE the index
    /// is moved to follow it).
    pub segments_end_offset: u64,
    // 0x020
    pub sparse_index_count: u64,
    // 0x028
    pub total_tx_count: u64,
    // 0x030
    pub genesis_hash: [u8; 32],
    // 0x050
    pub last_tx_hash: [u8; 32],
    // 0x070  CRC32 over bytes [0x000 .. 0x070)
    pub header_crc32: u32,
    // 0x074  File offset of sparse index checkpoint (0 = no checkpoint)
    pub sparse_checkpoint_offset: u64,
    // 0x07C  Segment count when checkpoint was written
    pub sparse_checkpoint_seg_count: u64,
}

impl FileHeader {
    pub fn new() -> Self {
        Self {
            magic: FILE_MAGIC,
            version: VERSION,
            accounts_count: 0,
            segment_count: 0,
            segments_end_offset: SEGMENTS_BASE_OFFSET,
            sparse_index_count: 0,
            total_tx_count: 0,
            genesis_hash: [0u8; 32],
            last_tx_hash: [0u8; 32],
            header_crc32: 0,
            sparse_checkpoint_offset: 0,
            sparse_checkpoint_seg_count: 0,
        }
    }

    // ── CRC helpers ────────────────────────────────────────────────────────

    fn build_payload_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(0x084);
        v.extend_from_slice(&self.magic);
        v.push(self.version);
        v.extend_from_slice(&[0u8; 3]); // reserved
        v.extend_from_slice(&self.accounts_count.to_le_bytes());
        v.extend_from_slice(&self.segment_count.to_le_bytes());
        v.extend_from_slice(&self.segments_end_offset.to_le_bytes());
        v.extend_from_slice(&self.sparse_index_count.to_le_bytes());
        v.extend_from_slice(&self.total_tx_count.to_le_bytes());
        v.extend_from_slice(&self.genesis_hash);
        v.extend_from_slice(&self.last_tx_hash);
        v.extend_from_slice(&self.sparse_checkpoint_offset.to_le_bytes());
        v.extend_from_slice(&self.sparse_checkpoint_seg_count.to_le_bytes());
        debug_assert_eq!(v.len(), 0x080);
        v.resize(0x084, 0u8); // pad to 132 bytes for CRC computation
        v
    }

    pub fn compute_crc(payload: &[u8]) -> u32 {
        let mut h = Crc32Hasher::new();
        h.update(payload);
        h.finalize()
    }

    // ── Serialise ──────────────────────────────────────────────────────────

    pub fn write_to<W: Write + Seek>(&mut self, w: &mut W) -> Result<()> {
        let payload = self.build_payload_bytes();
        self.header_crc32 = Self::compute_crc(&payload);

        w.seek(SeekFrom::Start(0))?;
        w.write_all(&payload)?;
        w.write_u32::<LE>(self.header_crc32)?;
        // Padding to 512 bytes: payload(0x84) + crc(4) = 136; pad = 376
        let written = 0x084usize + 4;
        w.write_all(&vec![0u8; FILE_HEADER_SIZE - written])?;
        Ok(())
    }

    // ── Deserialise ────────────────────────────────────────────────────────

    pub fn read_from<R: Read + Seek>(r: &mut R) -> Result<Self> {
        r.seek(SeekFrom::Start(0))?;

        // Read the full 512-byte block for CRC verification
        let mut raw = [0u8; FILE_HEADER_SIZE];
        r.read_exact(&mut raw)?;

        // Validate magic
        if &raw[0..4] != b"LDGR" {
            return Err(LedgerError::BadMagic);
        }

        let version = raw[4];
        if version != VERSION {
            return Err(LedgerError::UnsupportedVersion(version));
        }

        // Verify CRC over [0..0x084)
        let stored_crc = u32::from_le_bytes(raw[0x084..0x088].try_into().unwrap());
        let computed = Self::compute_crc(&raw[..0x084]);
        if stored_crc != computed {
            return Err(LedgerError::HeaderChecksumMismatch {
                stored: stored_crc,
                computed,
            });
        }

        fn read_u64(src: &[u8], off: usize) -> u64 {
            u64::from_le_bytes(src[off..off + 8].try_into().unwrap())
        }

        let mut genesis_hash = [0u8; 32];
        let mut last_tx_hash = [0u8; 32];
        genesis_hash.copy_from_slice(&raw[0x030..0x050]);
        last_tx_hash.copy_from_slice(&raw[0x050..0x070]);

        Ok(Self {
            magic: raw[0..4].try_into().unwrap(),
            version,
            accounts_count: read_u64(&raw, 0x008),
            segment_count: read_u64(&raw, 0x010),
            segments_end_offset: read_u64(&raw, 0x018),
            sparse_index_count: read_u64(&raw, 0x020),
            total_tx_count: read_u64(&raw, 0x028),
            genesis_hash,
            last_tx_hash,
            header_crc32: stored_crc,
            sparse_checkpoint_offset: read_u64(&raw, 0x074),
            sparse_checkpoint_seg_count: read_u64(&raw, 0x07C),
        })
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Account Record (128 bytes on disk)
// ──────────────────────────────────────────────────────────────────────────────

pub fn write_account_slot<W: Write + Seek>(
    w: &mut W,
    slot_index: usize,
    id: u64,
    name: &str,
    kind: u8,
    created_at: u64,
    balance: i64,
) -> Result<()> {
    let offset = FILE_HEADER_SIZE as u64 + (slot_index * ACCOUNT_RECORD_SIZE) as u64;
    w.seek(SeekFrom::Start(offset))?;

    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len().min(64) as u16;

    w.write_u8(1)?; // is_active
    w.write_u64::<LE>(id)?;
    w.write_u8(kind)?;
    w.write_u64::<LE>(created_at)?;
    w.write_i64::<LE>(balance)?;
    w.write_u16::<LE>(name_len)?;

    let mut name_buf = [0u8; 64];
    name_buf[..name_len as usize].copy_from_slice(&name_bytes[..name_len as usize]);
    w.write_all(&name_buf)?;

    w.write_all(&[0u8; 36])?; // padding
    Ok(())
}

pub fn read_account_slot<R: Read + Seek>(
    r: &mut R,
    slot_index: usize,
) -> Result<Option<crate::models::Account>> {
    use crate::models::{Account, AccountType};

    let offset = FILE_HEADER_SIZE as u64 + (slot_index * ACCOUNT_RECORD_SIZE) as u64;
    r.seek(SeekFrom::Start(offset))?;

    let is_active = r.read_u8()?;
    if is_active == 0 {
        return Ok(None);
    }

    let id = r.read_u64::<LE>()?;
    let kind_byte = r.read_u8()?;
    let created_at = r.read_u64::<LE>()?;
    let balance = r.read_i64::<LE>()?;
    let name_len = r.read_u16::<LE>()? as usize;

    let mut name_buf = [0u8; 64];
    r.read_exact(&mut name_buf)?;
    let name = String::from_utf8_lossy(&name_buf[..name_len]).into_owned();

    let mut _pad = [0u8; 36];
    r.read_exact(&mut _pad)?;

    let kind = AccountType::from_u8(kind_byte).unwrap_or(AccountType::Asset);

    Ok(Some(Account {
        id,
        name,
        kind,
        created_at,
        balance,
    }))
}

// ──────────────────────────────────────────────────────────────────────────────
// Segment Header (256 bytes)
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Copy)]
pub struct ColumnMeta {
    /// Absolute file offset of this column's data block.
    pub offset: u64,
    /// Byte length of this column's data block.
    pub length: u64,
    /// Encoding type (enc::NONE or enc::DICTIONARY).
    pub encoding: u8,
}

#[derive(Debug, Clone)]
pub struct SegmentHeader {
    // 0x00
    pub magic: [u8; 4],
    // 0x04
    pub row_count: u64,
    // 0x0C
    pub min_ts: u64,
    // 0x14
    pub max_ts: u64,
    // 0x1C
    pub first_row_global_idx: u64,
    // 0x24  col_offsets[7]
    // 0x5C  col_lengths[7]
    // 0x94  col_encodings[7]
    pub columns: [ColumnMeta; NUM_TX_COLUMNS],
    // 0x9B
    pub data_crc32: u32,
    // 0x9F  padding
}

impl SegmentHeader {
    pub fn write_to<W: Write + Seek>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.magic)?;
        w.write_u64::<LE>(self.row_count)?;
        w.write_u64::<LE>(self.min_ts)?;
        w.write_u64::<LE>(self.max_ts)?;
        w.write_u64::<LE>(self.first_row_global_idx)?;

        for c in &self.columns {
            w.write_u64::<LE>(c.offset)?;
        }
        for c in &self.columns {
            w.write_u64::<LE>(c.length)?;
        }
        for c in &self.columns {
            w.write_u8(c.encoding)?;
        }

        w.write_u32::<LE>(self.data_crc32)?;
        w.write_all(&[0u8; 80])?; // padding: 256 - (4+8+8+8+8 + 8*8 + 8*8 + 8 + 4) = 80
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;

        let row_count = r.read_u64::<LE>()?;
        let min_ts = r.read_u64::<LE>()?;
        let max_ts = r.read_u64::<LE>()?;
        let first_row_global_idx = r.read_u64::<LE>()?;

        let mut offsets = [0u64; NUM_TX_COLUMNS];
        let mut lengths = [0u64; NUM_TX_COLUMNS];
        let mut encodings = [0u8; NUM_TX_COLUMNS];

        for o in offsets.iter_mut() {
            *o = r.read_u64::<LE>()?;
        }
        for l in lengths.iter_mut() {
            *l = r.read_u64::<LE>()?;
        }
        for e in encodings.iter_mut() {
            *e = r.read_u8()?;
        }

        let data_crc32 = r.read_u32::<LE>()?;
        let mut _pad = [0u8; 80];
        r.read_exact(&mut _pad)?;

        let mut columns = [ColumnMeta::default(); NUM_TX_COLUMNS];
        for (i, c) in columns.iter_mut().enumerate() {
            c.offset = offsets[i];
            c.length = lengths[i];
            c.encoding = encodings[i];
        }

        Ok(Self {
            magic,
            row_count,
            min_ts,
            max_ts,
            first_row_global_idx,
            columns,
            data_crc32,
        })
    }

    /// Compute CRC32 over a block of raw column bytes.
    pub fn crc32_of(data: &[u8]) -> u32 {
        let mut h = Crc32Hasher::new();
        h.update(data);
        h.finalize()
    }
}

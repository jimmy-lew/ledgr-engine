//! # Write-Ahead Log  (`ledger.wal`)
//!
//! The WAL is the *only* separate file from the main `.ldg` database.
//!
//! ## Atomic journal-entry records
//!
//! The critical design point: **all legs of one `JournalEntry` are written
//! as a single WAL record**.  This means:
//!
//! - Either *all* legs of an entry are replayed on recovery, or *none* are.
//! - A crash mid-write leaves an incomplete record whose CRC will not match,
//!   so it (and everything after it) is discarded.
//! - It is therefore impossible for the WAL to replay only half of a
//!   double-entry pair — the atomicity guarantee is in the record boundary.
//!
//! ## Record Layout
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │ 0x00       [1 B]   Record type: 0x02 = JournalEntry      │
//! │ 0x01–0x04  [4 B]   CRC32 over entire payload             │
//! │ 0x05–0x08  [4 B]   Payload length (bytes)                │
//! │ 0x09–…     [N B]   Payload:                              │
//! │              [8 B]   journal_entry_id  u64               │
//! │              [8 B]   timestamp         u64               │
//! │              [4 B]   desc_len          u32               │
//! │              [M B]   description       UTF-8             │
//! │              [2 B]   leg_count         u16               │
//! │              [leg_count × leg_record]                    │
//! │                leg_record:                               │
//! │                  [8 B]  leg_id         u64               │
//! │                  [8 B]  account_id     u64               │
//! │                  [8 B]  amount (signed i64, cents)       │
//! │                  [1 B]  direction      u8                │
//! │                  [32B]  tx_hash        [u8;32]           │
//! └──────────────────────────────────────────────────────────┘
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32Hasher;

use crate::error::{LedgerError, Result};
use crate::models::{Direction, Transaction};

const RECORD_JOURNAL: u8 = 0x02;

// ─────────────────────────────────────────────────────────────────────────────
// WAL journal-entry batch  (all legs together)
// ─────────────────────────────────────────────────────────────────────────────

/// All legs of one `JournalEntry`, pre-populated with their assigned IDs
/// and hashes, ready to be atomically written to the WAL then the MemTable.
pub struct WalEntry {
    pub journal_entry_id: u64,
    pub timestamp:        u64,
    pub description:      String,
    pub legs:             Vec<Transaction>,
}

pub struct Wal {
    path:        PathBuf,
    writer:      BufWriter<File>,
    byte_offset: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path  = path.as_ref().to_path_buf();
        let file  = OpenOptions::new().create(true).append(true).open(&path)?;
        let byte_offset = file.metadata()?.len();
        Ok(Self { path, writer: BufWriter::new(file), byte_offset })
    }

    // ── Write ──────────────────────────────────────────────────────────────

    /// Write ALL legs of a journal entry as one atomic record and fsync.
    ///
    /// Either the entire record lands on durable storage or the CRC will
    /// flag it as corrupt on replay — there is no intermediate state where
    /// only some legs exist in the WAL.
    pub fn append_journal_entry(&mut self, entry: &WalEntry) -> Result<()> {
        let payload = Self::serialise(entry)?;
        let crc     = crc32(&payload);

        self.writer.write_u8(RECORD_JOURNAL)?;
        self.writer.write_u32::<LE>(crc)?;
        self.writer.write_u32::<LE>(payload.len() as u32)?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        self.byte_offset += 1 + 4 + 4 + payload.len() as u64;
        Ok(())
    }

    // ── Replay ─────────────────────────────────────────────────────────────

    /// Read all intact records from the WAL.  Returns a flat list of
    /// `Transaction` legs in the order they were written.
    pub fn replay(&self) -> Result<Vec<Transaction>> {
        let mut file   = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut legs   = Vec::new();
        let mut offset = 0u64;

        loop {
            let rtype = match file.read_u8() {
                Ok(b)  => b,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };
            if rtype != RECORD_JOURNAL {
                return Err(LedgerError::WalCorruption { offset });
            }

            let stored_crc = file.read_u32::<LE>()?;
            let plen       = file.read_u32::<LE>()? as usize;
            let mut payload = vec![0u8; plen];

            match file.read_exact(&mut payload) {
                Ok(_)  => {}
                Err(_) => break,   // truncated write — drop this record
            }

            if crc32(&payload) != stored_crc {
                // Corrupt record — stop here
                break;
            }

            let entry = Self::deserialise(&payload)?;
            legs.extend(entry.legs);
            offset += 1 + 4 + 4 + plen as u64;
        }
        Ok(legs)
    }

    // ── Truncate after successful flush to .ldg ────────────────────────────

    pub fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        let f = self.writer.get_ref();
        f.set_len(0)?;
        f.sync_all()?;
        self.byte_offset = 0;
        Ok(())
    }

    // ── Serialisation ──────────────────────────────────────────────────────

    fn serialise(entry: &WalEntry) -> Result<Vec<u8>> {
        let desc = entry.description.as_bytes();
        let mut v = Vec::new();

        v.extend_from_slice(&entry.journal_entry_id.to_le_bytes());
        v.extend_from_slice(&entry.timestamp.to_le_bytes());
        v.extend_from_slice(&(desc.len() as u32).to_le_bytes());
        v.extend_from_slice(desc);
        v.extend_from_slice(&(entry.legs.len() as u16).to_le_bytes());

        for leg in &entry.legs {
            v.extend_from_slice(&leg.id.to_le_bytes());
            v.extend_from_slice(&leg.account_id.to_le_bytes());
            v.extend_from_slice(&leg.amount.to_le_bytes());
            v.push(leg.transaction_type as u8);
            v.extend_from_slice(&leg.tx_hash);
        }
        Ok(v)
    }

    fn deserialise(buf: &[u8]) -> Result<WalEntry> {
        let mut pos: usize = 0;

        let journal_entry_id = read_u64(buf, &mut pos);
        let timestamp        = read_u64(buf, &mut pos);
        let desc_len         = read_u32(buf, &mut pos) as usize;
        let description      = String::from_utf8_lossy(&buf[pos..pos+desc_len]).into_owned();
        pos += desc_len;
        let leg_count        = read_u16(buf, &mut pos) as usize;

        let mut legs = Vec::with_capacity(leg_count);
        for _ in 0..leg_count {
            let leg_id     = read_u64(buf, &mut pos);
            let account_id = read_u64(buf, &mut pos);
            let amount     = read_i64(buf, &mut pos);
            let dir_byte   = buf[pos]; pos += 1;
            let direction  = Direction::from_u8(dir_byte)
                .ok_or_else(|| LedgerError::Encoding("bad direction in WAL".into()))?;
            let mut tx_hash = [0u8; 32];
            tx_hash.copy_from_slice(&buf[pos..pos+32]);
            pos += 32;

            legs.push(Transaction {
                id:               leg_id,
                journal_entry_id,
                account_id,
                amount,
                transaction_type: direction,
                timestamp,
                description:      description.clone(),
                tx_hash,
            });
        }

        Ok(WalEntry { journal_entry_id, timestamp, description, legs })
    }
}

// ── Byte-level helpers ─────────────────────────────────────────────────────

fn crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}
fn read_u64(buf: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_le_bytes(buf[*pos..*pos+8].try_into().unwrap()); *pos += 8; v
}
fn read_i64(buf: &[u8], pos: &mut usize) -> i64 {
    let v = i64::from_le_bytes(buf[*pos..*pos+8].try_into().unwrap()); *pos += 8; v
}
fn read_u32(buf: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes(buf[*pos..*pos+4].try_into().unwrap()); *pos += 4; v
}
fn read_u16(buf: &[u8], pos: &mut usize) -> u16 {
    let v = u16::from_le_bytes(buf[*pos..*pos+2].try_into().unwrap()); *pos += 2; v
}

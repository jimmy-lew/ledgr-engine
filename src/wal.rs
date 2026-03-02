//! # Write-Ahead Log (WAL)
//!
//! The WAL provides **durability**: every `append_transaction` call is
//! synchronously flushed here before the MemTable is updated.  On restart the
//! engine replays the WAL to rebuild any MemTable entries that were not yet
//! flushed to an SSTable.
//!
//! ## Record Layout
//!
//! ```text
//! ┌────────────────────────────────────────────────────────┐
//! │ 0x00        [1 B]  Record type  (0x01 = Transaction)  │
//! │ 0x01–0x04   [4 B]  Payload CRC32                      │
//! │ 0x05–0x08   [4 B]  Payload length (bytes)             │
//! │ 0x09–…      [N B]  Payload (serialised Transaction)   │
//! └────────────────────────────────────────────────────────┘
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32Hasher;

use crate::error::{LedgerError, Result};
use crate::models::Transaction;

const RECORD_TYPE_TX: u8 = 0x01;

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    byte_offset: u64,
}

impl Wal {
    /// Open (or create) the WAL at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let byte_offset = file.metadata()?.len();
        Ok(Self { path, writer: BufWriter::new(file), byte_offset })
    }

    /// Append a transaction record and **fsync**.
    /// Returns the byte offset at which this record begins.
    pub fn append(&mut self, tx: &Transaction) -> Result<u64> {
        let payload = Self::serialise_tx(tx)?;
        let crc = Self::crc32(&payload);
        let start = self.byte_offset;

        self.writer.write_u8(RECORD_TYPE_TX)?;
        self.writer.write_u32::<LE>(crc)?;
        self.writer.write_u32::<LE>(payload.len() as u32)?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        // Safety: call sync_all via the underlying file
        // (BufWriter::get_ref gives us &File)
        self.writer.get_ref().sync_all()?;

        self.byte_offset += 1 + 4 + 4 + payload.len() as u64;
        Ok(start)
    }

    /// Replay all records from the WAL.  Used during engine startup.
    pub fn replay(&self) -> Result<Vec<Transaction>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut txs = Vec::new();
        let mut offset: u64 = 0;

        loop {
            let record_type = match file.read_u8() {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };
            if record_type != RECORD_TYPE_TX {
                return Err(LedgerError::WalCorruption { offset });
            }
            let stored_crc    = file.read_u32::<LE>()?;
            let payload_len   = file.read_u32::<LE>()? as usize;
            let mut payload   = vec![0u8; payload_len];
            file.read_exact(&mut payload)?;

            let computed = Self::crc32(&payload);
            if stored_crc != computed {
                return Err(LedgerError::WalCorruption { offset });
            }

            txs.push(Self::deserialise_tx(&payload)?);
            offset += 1 + 4 + 4 + payload_len as u64;
        }
        Ok(txs)
    }

    // ── Serialisation ──────────────────────────────────────────────────────

    fn serialise_tx(tx: &Transaction) -> Result<Vec<u8>> {
        let desc_bytes = tx.description.as_bytes();
        // Fixed: id(8)+acct(8)+amt(8)+type(1)+ts(8) = 33
        // Variable: desc_len(4)+desc(N)
        let mut buf = Vec::with_capacity(33 + 4 + desc_bytes.len());
        buf.extend_from_slice(&tx.id.to_le_bytes());
        buf.extend_from_slice(&tx.account_id.to_le_bytes());
        buf.extend_from_slice(&tx.amount.to_le_bytes());
        buf.push(tx.transaction_type as u8);
        buf.extend_from_slice(&tx.timestamp.to_le_bytes());
        buf.extend_from_slice(&(desc_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(desc_bytes);
        Ok(buf)
    }

    fn deserialise_tx(buf: &[u8]) -> Result<Transaction> {
        use std::convert::TryInto;
        let id         = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let account_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let amount     = i64::from_le_bytes(buf[16..24].try_into().unwrap());
        let tx_type    = crate::models::TransactionType::from_u8(buf[24])
            .ok_or_else(|| LedgerError::Encoding("invalid tx type in WAL".into()))?;
        let timestamp  = u64::from_le_bytes(buf[25..33].try_into().unwrap());
        let desc_len   = u32::from_le_bytes(buf[33..37].try_into().unwrap()) as usize;
        let description = String::from_utf8_lossy(&buf[37..37 + desc_len]).into_owned();
        Ok(Transaction { id, account_id, amount, transaction_type: tx_type, timestamp, description })
    }

    fn crc32(data: &[u8]) -> u32 {
        let mut h = Crc32Hasher::new();
        h.update(data);
        h.finalize()
    }
}

//! # Write-Ahead Log  (`wal.log`)
//!
//! The WAL is the *only* separate file from the main `.ldg` database.
//! Its sole purpose is crash durability: every `append_transaction` is
//! fsynced here before the MemTable is updated.  On recovery the WAL is
//! replayed to rebuild the in-memory state.
//!
//! Once a flush to the `.ldg` file completes successfully the WAL is
//! **truncated** (reset to zero length) so it doesn't grow unboundedly.
//!
//! ## Record Layout
//!
//! ```text
//! [1 B]  type  (0x01 = Transaction)
//! [4 B]  CRC32 over payload
//! [4 B]  payload_len (bytes)
//! [N B]  payload
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32Hasher;

use crate::error::{LedgerError, Result};
use crate::models::Transaction;

const RECORD_TX: u8 = 0x01;

pub struct Wal {
    path:        PathBuf,
    writer:      BufWriter<File>,
    byte_offset: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let byte_offset = file.metadata()?.len();
        Ok(Self { path, writer: BufWriter::new(file), byte_offset })
    }

    /// Append `tx` to the WAL and fsync.  Returns the start offset.
    pub fn append(&mut self, tx: &Transaction) -> Result<u64> {
        let payload = Self::serialise(tx)?;
        let crc     = crc32(&payload);
        let start   = self.byte_offset;

        self.writer.write_u8(RECORD_TX)?;
        self.writer.write_u32::<LE>(crc)?;
        self.writer.write_u32::<LE>(payload.len() as u32)?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        self.byte_offset += 1 + 4 + 4 + payload.len() as u64;
        Ok(start)
    }

    /// Replay all records from disk.  Used on startup for crash recovery.
    pub fn replay(&self) -> Result<Vec<Transaction>> {
        let mut file   = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut txs    = Vec::new();
        let mut offset = 0u64;

        loop {
            let rtype = match file.read_u8() {
                Ok(b)  => b,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };
            if rtype != RECORD_TX {
                return Err(LedgerError::WalCorruption { offset });
            }
            let stored_crc  = file.read_u32::<LE>()?;
            let plen        = file.read_u32::<LE>()? as usize;
            let mut payload = vec![0u8; plen];
            file.read_exact(&mut payload)?;

            if crc32(&payload) != stored_crc {
                return Err(LedgerError::WalCorruption { offset });
            }

            txs.push(Self::deserialise(&payload)?);
            offset += 1 + 4 + 4 + plen as u64;
        }
        Ok(txs)
    }

    /// Truncate the WAL after a successful flush to the main file.
    pub fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        let f = self.writer.get_ref();
        f.set_len(0)?;
        f.sync_all()?;
        self.byte_offset = 0;
        Ok(())
    }

    fn serialise(tx: &Transaction) -> Result<Vec<u8>> {
        let desc = tx.description.as_bytes();
        let mut v = Vec::with_capacity(8+8+8+1+8+4+desc.len()+32);
        v.extend_from_slice(&tx.id.to_le_bytes());
        v.extend_from_slice(&tx.account_id.to_le_bytes());
        v.extend_from_slice(&tx.amount.to_le_bytes());
        v.push(tx.transaction_type as u8);
        v.extend_from_slice(&tx.timestamp.to_le_bytes());
        v.extend_from_slice(&(desc.len() as u32).to_le_bytes());
        v.extend_from_slice(desc);
        v.extend_from_slice(&tx.tx_hash);
        Ok(v)
    }

    fn deserialise(buf: &[u8]) -> Result<Transaction> {
        use crate::models::TransactionType;
        let id         = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let account_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let amount     = i64::from_le_bytes(buf[16..24].try_into().unwrap());
        let tx_type    = TransactionType::from_u8(buf[24])
            .ok_or_else(|| LedgerError::Encoding("bad tx_type in WAL".into()))?;
        let timestamp  = u64::from_le_bytes(buf[25..33].try_into().unwrap());
        let desc_len   = u32::from_le_bytes(buf[33..37].try_into().unwrap()) as usize;
        let description = String::from_utf8_lossy(&buf[37..37+desc_len]).into_owned();
        let hash_off    = 37 + desc_len;
        let mut tx_hash = [0u8; 32];
        tx_hash.copy_from_slice(&buf[hash_off..hash_off+32]);
        Ok(Transaction {
            id, account_id, amount, transaction_type: tx_type,
            timestamp, description, tx_hash,
        })
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

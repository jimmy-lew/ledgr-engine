use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SSTable magic bytes mismatch – file may be corrupt or wrong format")]
    BadMagic,

    #[error("SSTable version {0} is not supported (expected 1)")]
    UnsupportedVersion(u8),

    #[error("Header checksum mismatch: stored={stored:#010x}, computed={computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },

    #[error("Ledger balance violation: net amount is {net} (expected 0)")]
    ImbalancedLedger { net: i64 },

    #[error("WAL corruption detected at byte offset {offset}")]
    WalCorruption { offset: u64 },

    #[error("Attempted to reference unknown account id={0}")]
    UnknownAccount(u64),

    #[error("Column index {0} out of range (max 5)")]
    ColumnOutOfRange(u8),

    #[error("Encoding error: {0}")]
    Encoding(String),
}

pub type Result<T> = std::result::Result<T, LedgerError>;

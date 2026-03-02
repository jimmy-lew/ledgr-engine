use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("File magic bytes mismatch – not a valid .ldg file")]
    BadMagic,

    #[error("Unsupported file version {0} (expected 1)")]
    UnsupportedVersion(u8),

    #[error("File header CRC mismatch: stored={stored:#010x} computed={computed:#010x}")]
    HeaderChecksumMismatch { stored: u32, computed: u32 },

    #[error("Segment {seq} CRC mismatch: stored={stored:#010x} computed={computed:#010x}")]
    SegmentChecksumMismatch { seq: u64, stored: u32, computed: u32 },

    #[error("Hash chain broken at global row {row}: expected {expected} got {actual}")]
    HashChainViolation { row: u64, expected: String, actual: String },

    #[error("Ledger imbalanced: SIMD net amount sum = {net} cents (expected 0)")]
    ImbalancedLedger { net: i64 },

    #[error("Account capacity exhausted (max {0} accounts)")]
    AccountsExhausted(usize),

    #[error("Unknown account id={0}")]
    UnknownAccount(u64),

    #[error("MemTable is empty – nothing to flush")]
    EmptyFlush,

    #[error("WAL corruption at byte offset {offset}")]
    WalCorruption { offset: u64 },

    #[error("Encoding error: {0}")]
    Encoding(String),
}

pub type Result<T> = std::result::Result<T, LedgerError>;

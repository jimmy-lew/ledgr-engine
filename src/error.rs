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
    SegmentChecksumMismatch {
        seq: u64,
        stored: u32,
        computed: u32,
    },

    #[error("Hash chain broken at global row {row}: expected {expected} got {actual}")]
    HashChainViolation {
        row: u64,
        expected: String,
        actual: String,
    },

    #[error("Ledger imbalanced: SIMD net amount sum = {net} cents (expected 0)")]
    ImbalancedLedger { net: i64 },

    #[error("Account capacity exhausted (max {0} accounts)")]
    AccountsExhausted(usize),

    #[error("Unknown account id={0}")]
    UnknownAccount(u64),

    /// Fired when a `JournalEntry` is submitted with fewer than 2 legs.
    /// A journal entry needs at least one debit leg AND one credit leg.
    #[error(
        "Journal entry requires at least 2 legs (got {got}). \
         Every transaction must come FROM somewhere and go TO somewhere."
    )]
    JournalTooFewLegs { got: usize },

    /// Fired when ∑debit amounts ≠ ∑credit amounts.
    /// The entry is rejected entirely — no legs are written to the WAL.
    #[error(
        "Journal entry is not balanced: debits={debits} cents, credits={credits} cents \
         (difference = {diff} cents). ∑Debits − ∑Credits must equal 0.",
        diff = (*debits as i64 - *credits as i64).abs()
    )]
    JournalNotBalanced { debits: u64, credits: u64 },

    /// Fired when a leg references the same account as another leg in the
    /// same journal entry and the combination is economically invalid
    /// (e.g. debiting and crediting the exact same account for the exact
    /// same amount — a no-op entry that shouldn't be recorded).
    #[error(
        "Journal entry leg {leg_index} references account {account_id} which appears \
         in another leg with the same direction — this produces a no-op entry."
    )]
    JournalNoOp { leg_index: usize, account_id: u64 },

    #[error("MemTable is empty – nothing to flush")]
    EmptyFlush,

    #[error("WAL corruption at byte offset {offset}")]
    WalCorruption { offset: u64 },

    #[error("Encoding error: {0}")]
    Encoding(String),
}

pub type Result<T> = std::result::Result<T, LedgerError>;

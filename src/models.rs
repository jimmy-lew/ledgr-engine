// ──────────────────────────────────────────────────────────────────────────────
// Primitive enums
// ──────────────────────────────────────────────────────────────────────────────

/// Direction of a single ledger leg.
///
/// Accounting sign convention (stored in the `tx_type` column):
///
/// | Account kind | Debit effect | Credit effect |
/// |--------------|-------------|---------------|
/// | Asset        | Increase    | Decrease      |
/// | Liability    | Decrease    | Increase      |
/// | Equity       | Decrease    | Increase      |
/// | Revenue      | Decrease    | Increase      |
/// | Expense      | Increase    | Decrease      |
///
/// The engine is sign-convention agnostic: it only enforces that
/// **∑all leg amounts = 0** per journal entry.  The `amount` field on a
/// `Leg` is always a **positive** magnitude; the `direction` field carries
/// the sign semantics.  Internally `Transaction.amount` is stored as a
/// *signed* i64 (Debit = negative, Credit = positive) so that a simple
/// `SUM(amount)` over all rows yields 0 for a balanced ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Direction {
    Debit = 0,
    Credit = 1,
}

impl Direction {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Debit),
            1 => Some(Self::Credit),
            _ => None,
        }
    }
    /// Signed multiplier: debits are stored as negative amounts.
    pub fn sign(self) -> i64 {
        match self {
            Self::Debit => -1,
            Self::Credit => 1,
        }
    }
}

// Keep the old name as an alias so simd_scan and other modules compile unchanged.
pub type TransactionType = Direction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccountType {
    Asset = 0,
    Liability = 1,
    Equity = 2,
    Revenue = 3,
    Expense = 4,
}

impl AccountType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Asset),
            1 => Some(Self::Liability),
            2 => Some(Self::Equity),
            3 => Some(Self::Revenue),
            4 => Some(Self::Expense),
            _ => None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Double-entry journal structures
// ──────────────────────────────────────────────────────────────────────────────

/// A single leg of a journal entry.
///
/// `amount` is a **positive** magnitude in cents.  The sign is determined
/// by `direction` and applied when the leg is stored as a `Transaction` row.
#[derive(Debug, Clone)]
pub struct Leg {
    pub account_id: u64,
    pub amount: u64, // always positive; sign comes from `direction`
    pub direction: Direction,
}

impl Leg {
    /// Convenience constructor for a debit leg.
    pub fn debit(account_id: u64, amount_cents: u64) -> Self {
        Self {
            account_id,
            amount: amount_cents,
            direction: Direction::Debit,
        }
    }

    /// Convenience constructor for a credit leg.
    pub fn credit(account_id: u64, amount_cents: u64) -> Self {
        Self {
            account_id,
            amount: amount_cents,
            direction: Direction::Credit,
        }
    }

    /// The signed amount as stored in the `amount` column.
    /// Debit legs are stored as negative values so that ∑ over all legs = 0.
    pub fn signed_amount(&self) -> i64 {
        self.direction.sign() * self.amount as i64
    }
}

/// A complete double-entry journal entry.
///
/// ## Invariant (enforced by `record_journal_entry` before any I/O)
///
/// ```text
/// ∑ leg.signed_amount()  ==  0
/// ```
///
/// Equivalently:  ∑ debit amounts  ==  ∑ credit amounts
///
/// A journal entry must have **at least two legs** (one debit, one credit).
/// Complex entries (split transactions) may have more, e.g.:
///
/// ```text
/// Purchase of a $1 200 laptop on a $200 deposit + $1 000 payable:
///
///   DEBIT   Equipment         $1 200   ← asset increases
///   CREDIT  Cash                $200   ← asset decreases
///   CREDIT  Accounts Payable  $1 000   ← liability increases
///
///   Net: –1200 + 200 + 1000 = 0  ✓
/// ```
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Human-readable memo shared by all legs.
    pub description: String,
    /// All legs.  Must sum to zero (debit totals = credit totals).
    /// Order is significant: legs are stored in this order; they share a
    /// common `journal_entry_id` column value so they can be grouped later.
    pub legs: Vec<Leg>,
    /// Optional timestamp (Unix epoch seconds). If None, uses current time.
    pub timestamp: Option<u64>,
}

impl JournalEntry {
    pub fn new(description: impl Into<String>, legs: Vec<Leg>) -> Self {
        Self {
            description: description.into(),
            legs,
            timestamp: None,
        }
    }

    pub fn with_timestamp(description: impl Into<String>, legs: Vec<Leg>, timestamp: u64) -> Self {
        Self {
            description: description.into(),
            legs,
            timestamp: Some(timestamp),
        }
    }

    /// Validate the accounting invariant without touching any I/O.
    ///
    /// Returns `Ok(())` when:
    /// - There are at least 2 legs.
    /// - All referenced accounts are distinct for clarity (warning only –
    ///   the engine enforces the balance, not uniqueness).
    /// - ∑ signed_amount() == 0.
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::LedgerError;

        if self.legs.len() < 2 {
            return Err(LedgerError::JournalTooFewLegs {
                got: self.legs.len(),
            });
        }

        let net: i64 = self.legs.iter().map(|l| l.signed_amount()).sum();
        if net != 0 {
            let total_debits: u64 = self
                .legs
                .iter()
                .filter(|l| l.direction == Direction::Debit)
                .map(|l| l.amount)
                .sum();
            let total_credits: u64 = self
                .legs
                .iter()
                .filter(|l| l.direction == Direction::Credit)
                .map(|l| l.amount)
                .sum();
            return Err(LedgerError::JournalNotBalanced {
                debits: total_debits,
                credits: total_credits,
            });
        }

        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Storage-layer row (one row per leg, columnar)
// ──────────────────────────────────────────────────────────────────────────────

/// An immutable storage row representing **one leg** of a journal entry.
///
/// Multiple `Transaction` rows share the same `journal_entry_id`, which lets
/// any query reconstruct the full double-entry context of any leg.
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Globally unique leg ID (monotonically increasing).
    pub id: u64,
    /// Groups all legs of the same `JournalEntry` together.
    pub journal_entry_id: u64,
    pub account_id: u64,
    /// Signed cents: negative for debits, positive for credits.
    /// Storing signed values means `∑ all amounts = 0` for any balanced ledger.
    pub amount: i64,
    pub transaction_type: Direction,
    pub timestamp: u64,
    pub description: String,
    /// SHA-256( leg_fields ‖ prev_tx_hash ). Chains across ALL legs in
    /// insertion order, not per-journal-entry, so the hash chain covers the
    /// entire history.
    pub tx_hash: [u8; 32],
}

// ──────────────────────────────────────────────────────────────────────────────
// Account
// ──────────────────────────────────────────────────────────────────────────────

/// A ledger account.
#[derive(Debug, Clone)]
pub struct Account {
    pub id: u64,
    pub name: String,
    pub kind: AccountType,
    pub created_at: u64,
    /// Running signed balance in cents.
    /// ∑(all account.balance) must equal 0 for a balanced ledger.
    pub balance: i64,
}

// ──────────────────────────────────────────────────────────────────────────────
// Query result types
// ──────────────────────────────────────────────────────────────────────────────

/// Result of `get_expense_summary`.
#[derive(Debug, Default)]
pub struct ExpenseSummary {
    pub total_debits: i64,
    pub total_credits: i64,
    pub net: i64,
    pub row_count: u64,
    pub segments_skipped: u64,
}

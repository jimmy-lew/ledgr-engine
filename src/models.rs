/// Direction of a ledger entry.
/// Stored as a dictionary-encoded u8 in the `tx_type` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TransactionType {
    Debit  = 0,
    Credit = 1,
}

impl TransactionType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v { 0 => Some(Self::Debit), 1 => Some(Self::Credit), _ => None }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccountType {
    Asset     = 0,
    Liability = 1,
    Equity    = 2,
    Revenue   = 3,
    Expense   = 4,
}

impl AccountType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Asset), 1 => Some(Self::Liability),
            2 => Some(Self::Equity), 3 => Some(Self::Revenue),
            4 => Some(Self::Expense), _ => None,
        }
    }
}

/// A ledger account (parent entity, row-oriented).
#[derive(Debug, Clone)]
pub struct Account {
    pub id:         u64,
    pub name:       String,
    pub kind:       AccountType,
    pub created_at: u64,     // Unix timestamp
    /// Running balance in cents (sum of all transactions on this account).
    /// For a balanced ledger ∑(account.balance) = 0.
    pub balance:    i64,
}

/// An immutable transaction row.
///
/// `amount` is stored in the **smallest currency unit** (cents).
/// By convention: credits are **positive**, debits are **negative**.
/// This means the net sum of all transaction amounts in a balanced
/// double-entry ledger is always exactly **zero**.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id:               u64,
    pub account_id:       u64,
    pub amount:           i64,   // cents; credit = +, debit = –
    pub transaction_type: TransactionType,
    pub timestamp:        u64,   // Unix seconds
    pub description:      String,
    /// SHA-256( tx_fields ‖ prev_tx_hash ).
    /// All-zeros for the genesis transaction's prev pointer.
    pub tx_hash:          [u8; 32],
}

/// Result of `get_expense_summary`.
#[derive(Debug, Default)]
pub struct ExpenseSummary {
    pub total_debits:  i64,   // sum of all debit amounts (negative cents)
    pub total_credits: i64,   // sum of all credit amounts (positive cents)
    pub net:           i64,   // total_debits + total_credits (0 if balanced)
    pub row_count:     u64,
    pub sstables_skipped: u64,  // how many segments the zone-map pruned
}

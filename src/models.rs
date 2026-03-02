/// The two sides of a double-entry ledger entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TransactionType {
    Debit  = 0,
    Credit = 1,
}

impl TransactionType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Debit),
            1 => Some(Self::Credit),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debit  => "DEBIT",
            Self::Credit => "CREDIT",
        }
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

/// A ledger account (parent entity).
#[derive(Debug, Clone)]
pub struct Account {
    pub id:         u64,
    pub name:       String,
    pub kind:       AccountType,
    pub created_at: u64,   // Unix timestamp (seconds)
}

/// An immutable transaction row.
///
/// `amount` is stored in the **smallest currency unit** (e.g. cents / pence)
/// as a signed i64 so we never touch floating-point arithmetic.
/// Credits are positive, debits are negative by convention.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id:               u64,
    pub account_id:       u64,
    pub amount:           i64,   // in cents; credits +, debits –
    pub transaction_type: TransactionType,
    pub timestamp:        u64,   // Unix timestamp (seconds)
    pub description:      String,
}

/// Summary returned by `get_expense_summary`.
#[derive(Debug, Default)]
pub struct ExpenseSummary {
    pub total_debits:  i64,
    pub total_credits: i64,
    pub net:           i64,
    pub row_count:     u64,
}

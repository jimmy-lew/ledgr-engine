use ledger_engine::models::{AccountType, JournalEntry, Leg};
use ledger_engine::LedgerEngine;
use napi::{Error, Result};
use napi_derive::napi;
use serde::Deserialize;

#[derive(Deserialize)]
struct LegInputJson {
    account_id: i64,
    amount: i64,
    is_credit: bool,
}

#[napi]
pub struct Engine {
    inner: LedgerEngine,
}

#[napi]
impl Engine {
    #[napi(constructor)]
    pub fn new(path: String) -> Result<Engine> {
        let inner = LedgerEngine::open(&path)
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;
        Ok(Engine { inner })
    }

    #[napi]
    pub fn create_account(&self, name: String, kind: String) -> Result<i64> {
        let account_type = match kind.to_lowercase().as_str() {
            "asset" => AccountType::Asset,
            "liability" => AccountType::Liability,
            "equity" => AccountType::Equity,
            "revenue" => AccountType::Revenue,
            "expense" => AccountType::Expense,
            _ => {
                return Err(Error::new(
                    napi::Status::GenericFailure,
                    format!("Unknown account type: {}", kind),
                ))
            }
        };
        self.inner
            .create_account(&name, account_type)
            .map(|id| id as i64)
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    #[napi]
    pub fn record_entry(
        &self,
        debit_account: i64,
        credit_account: i64,
        amount_cents: i64,
        description: String,
    ) -> Result<i64> {
        if debit_account < 0 || credit_account < 0 || amount_cents < 0 {
            return Err(Error::new(
                napi::Status::GenericFailure,
                "Account IDs and amount must be non-negative".to_string(),
            ));
        }
        self.inner
            .record_entry(
                debit_account as u64,
                credit_account as u64,
                amount_cents as u64,
                &description,
            )
            .map(|id| id as i64)
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    #[napi]
    pub fn record_journal_entry(&self, description: String, legs_json: String) -> Result<i64> {
        let parsed: Vec<LegInputJson> = serde_json::from_str(&legs_json).map_err(|e| {
            Error::new(
                napi::Status::GenericFailure,
                format!("Invalid legs JSON: {}", e),
            )
        })?;

        let mut journal_legs = Vec::new();

        for leg in parsed {
            if leg.account_id < 0 || leg.amount < 0 {
                return Err(Error::new(
                    napi::Status::GenericFailure,
                    "Account ID and amount must be non-negative".to_string(),
                ));
            }

            let ledger_leg = if leg.is_credit {
                Leg::credit(leg.account_id as u64, leg.amount as u64)
            } else {
                Leg::debit(leg.account_id as u64, leg.amount as u64)
            };
            journal_legs.push(ledger_leg);
        }

        let journal_entry = JournalEntry::new(&description, journal_legs);
        self.inner
            .record_journal_entry(journal_entry)
            .map(|id| id as i64)
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    #[napi]
    pub fn list_accounts(&self) -> Vec<AccountOutput> {
        self.inner
            .list_accounts()
            .into_iter()
            .map(|a| AccountOutput {
                id: a.id as i64,
                name: a.name,
                kind: format!("{:?}", a.kind).to_lowercase(),
                balance: a.balance,
                created_at: a.created_at as i64,
            })
            .collect()
    }

    #[napi]
    pub fn list_all_transactions(&self) -> Result<Vec<TransactionOutput>> {
        let txs = self
            .inner
            .list_all_transactions()
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;
        Ok(txs
            .into_iter()
            .map(|t| TransactionOutput {
                id: t.id as i64,
                journal_entry_id: t.journal_entry_id as i64,
                account_id: t.account_id as i64,
                amount: t.amount,
                transaction_type: format!("{:?}", t.transaction_type).to_lowercase(),
                timestamp: t.timestamp as i64,
                description: t.description,
                tx_hash: faster_hex::hex_string(&t.tx_hash),
            })
            .collect())
    }

    #[napi]
    pub fn list_journal_entries(&self) -> Result<Vec<Vec<TransactionOutput>>> {
        let entries = self
            .inner
            .list_journal_entries()
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                entry
                    .into_iter()
                    .map(|t| TransactionOutput {
                        id: t.id as i64,
                        journal_entry_id: t.journal_entry_id as i64,
                        account_id: t.account_id as i64,
                        amount: t.amount,
                        transaction_type: format!("{:?}", t.transaction_type).to_lowercase(),
                        timestamp: t.timestamp as i64,
                        description: t.description,
                        tx_hash: faster_hex::hex_string(&t.tx_hash),
                    })
                    .collect()
            })
            .collect())
    }

    #[napi]
    pub fn get_expense_summary(&self, start_ts: i64, end_ts: i64) -> Result<ExpenseSummaryOutput> {
        if start_ts < 0 || end_ts < 0 {
            return Err(Error::new(
                napi::Status::GenericFailure,
                "Timestamps must be non-negative".to_string(),
            ));
        }
        let summary = self
            .inner
            .get_expense_summary(start_ts as u64, end_ts as u64)
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;
        Ok(ExpenseSummaryOutput {
            total_debits: summary.total_debits,
            total_credits: summary.total_credits,
            net: summary.net,
            row_count: summary.row_count as i64,
            segments_skipped: summary.segments_skipped as i64,
        })
    }

    #[napi]
    pub fn validate_ledger(&self) -> Result<()> {
        self.inner
            .validate_ledger()
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))
    }

    #[napi]
    pub fn force_flush(&self) -> Result<()> {
        self.inner
            .force_flush()
            .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))
    }
}

#[napi]
pub struct AccountOutput {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub balance: i64,
    pub created_at: i64,
}

#[napi]
pub struct TransactionOutput {
    pub id: i64,
    pub journal_entry_id: i64,
    pub account_id: i64,
    pub amount: i64,
    pub transaction_type: String,
    pub timestamp: i64,
    pub description: String,
    pub tx_hash: String,
}

#[napi]
pub struct ExpenseSummaryOutput {
    pub total_debits: i64,
    pub total_credits: i64,
    pub net: i64,
    pub row_count: i64,
    pub segments_skipped: i64,
}

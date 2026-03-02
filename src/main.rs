//! Runnable demo – shows the full write and read cycle.
//! `cargo run` in the ledger-engine directory.

use ledger_engine::models::AccountType;
use ledger_engine::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), LedgerError> {
    let tmp_dir = std::env::temp_dir().join("ledger_demo");
    std::fs::create_dir_all(&tmp_dir).ok();
    println!("=== Ledger Engine Demo ===");
    println!("Data directory: {}", tmp_dir.display());

    let engine = LedgerEngine::open(&tmp_dir)?;

    // ── Set up accounts ────────────────────────────────────────────────────
    let cash_id = engine.create_account("Cash", AccountType::Asset)?;
    let revenue_id = engine.create_account("Revenue", AccountType::Revenue)?;
    let expense_id = engine.create_account("Operating Expenses", AccountType::Expense)?;
    let equity_id = engine.create_account("Retained Earnings", AccountType::Equity)?;

    println!("\n[accounts] Created: cash={cash_id}, revenue={revenue_id}, expense={expense_id}, equity={equity_id}");

    // ── Record balanced double-entry transactions ──────────────────────────
    //   Each economic event has TWO legs that must net to zero.

    // Event 1: Customer pays $1,200 → cash in, revenue recognised
    engine.append_transaction(
        cash_id,
        120_000,
        TransactionType::Credit,
        "Customer payment #1001",
    )?;
    engine.append_transaction(
        revenue_id,
        -120_000,
        TransactionType::Debit,
        "Revenue recognised #1001",
    )?;

    // Event 2: Pay rent $800
    engine.append_transaction(expense_id, 80_000, TransactionType::Debit, "Rent – March")?;
    engine.append_transaction(cash_id, -80_000, TransactionType::Credit, "Rent – March")?;

    // Event 3: Pay salaries $950
    engine.append_transaction(
        expense_id,
        95_000,
        TransactionType::Debit,
        "Salaries – March",
    )?;
    engine.append_transaction(
        cash_id,
        -95_000,
        TransactionType::Credit,
        "Salaries – March",
    )?;

    // Event 4: Founder equity injection $5,000
    engine.append_transaction(
        cash_id,
        500_000,
        TransactionType::Credit,
        "Equity injection",
    )?;
    engine.append_transaction(
        equity_id,
        -500_000,
        TransactionType::Debit,
        "Equity injection",
    )?;

    println!("\n[write] 8 transaction legs recorded.");

    // ── Validate in MemTable ───────────────────────────────────────────────
    println!("\n--- validate_ledger() [MemTable] ---");
    engine.validate_ledger()?;

    // ── Flush to SSTable ───────────────────────────────────────────────────
    println!("\n--- force_flush() → SSTable ---");
    engine.force_flush()?;

    // ── Validate again (SSTable path; demonstrates column seek) ───────────
    println!("\n--- validate_ledger() [SSTable] ---");
    engine.validate_ledger()?;

    // ── Expense summary ────────────────────────────────────────────────────
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    println!("\n--- get_expense_summary(past month) ---");
    let summary = engine.get_expense_summary(now - 30 * 86_400, now)?;
    println!(
        "  Debits  (outflows): {:>10} cents ({:.2} USD)",
        summary.total_debits,
        summary.total_debits as f64 / 100.0
    );
    println!(
        "  Credits (inflows):  {:>10} cents ({:.2} USD)",
        summary.total_credits,
        summary.total_credits as f64 / 100.0
    );
    println!("  Net:                {:>10} cents", summary.net);
    println!("  Rows scanned:       {:>10}", summary.row_count);

    println!("\n=== Demo complete ===");
    Ok(())
}

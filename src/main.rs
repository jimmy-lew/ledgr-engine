//! Runnable demo showing all major engine features.
//! `cargo run --release` in the ledger-engine directory.

use ledger_engine::*;
use ledger_engine::models::AccountType;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), LedgerError> {
    let dbpath = std::env::temp_dir().join("demo.ldg");
    // Clean up previous run
    let _ = std::fs::remove_file(&dbpath);
    let _ = std::fs::remove_file(dbpath.with_extension("wal"));

    println!("╔══════════════════════════════════════════════╗");
    println!("║     Single-File Columnar Ledger Engine       ║");
    println!("╚══════════════════════════════════════════════╝");
    println!("Database file: {}", dbpath.display());

    let engine = LedgerEngine::open(&dbpath)?;

    // ── Account setup ──────────────────────────────────────────────────────
    let cash      = engine.create_account("Cash",               AccountType::Asset)?;
    let ar        = engine.create_account("Accounts Receivable",AccountType::Asset)?;
    let revenue   = engine.create_account("Revenue",            AccountType::Revenue)?;
    let expense   = engine.create_account("Operating Expenses", AccountType::Expense)?;
    let equity    = engine.create_account("Retained Earnings",  AccountType::Equity)?;

    println!("\n[accounts] {cash}=Cash  {ar}=AR  {revenue}=Revenue  \
              {expense}=Expense  {equity}=Equity\n");

    // ── Double-entry transactions ──────────────────────────────────────────
    // Each event has two legs; the pair must sum to zero.

    // Event 1: Founder injects $10,000 cash
    engine.append_transaction(cash,   1_000_000, TransactionType::Credit, "Founder equity injection")?;
    engine.append_transaction(equity,-1_000_000, TransactionType::Debit,  "Founder equity injection")?;

    // Event 2: Customer invoiced $2,400 (AR)
    engine.append_transaction(ar,       240_000, TransactionType::Debit,  "Invoice #101")?;
    engine.append_transaction(revenue, -240_000, TransactionType::Credit, "Invoice #101")?;

    // Event 3: Customer pays invoice
    engine.append_transaction(cash,    240_000, TransactionType::Credit, "Payment of Invoice #101")?;
    engine.append_transaction(ar,     -240_000, TransactionType::Credit, "Payment clears AR")?;

    // Event 4: Pay office rent $1,200
    engine.append_transaction(expense,  120_000, TransactionType::Debit,  "Rent – April")?;
    engine.append_transaction(cash,    -120_000, TransactionType::Credit, "Rent – April")?;

    // Event 5: Pay salaries $3,800
    engine.append_transaction(expense,  380_000, TransactionType::Debit,  "Salaries – April")?;
    engine.append_transaction(cash,    -380_000, TransactionType::Credit, "Salaries – April")?;

    println!("[write] 10 transaction legs written to WAL + MemTable");

    // ── Validate while still in MemTable ──────────────────────────────────
    println!("\n─── Phase 1: validate in MemTable ───");
    engine.validate_ledger()?;

    // ── Flush to the single .ldg file ─────────────────────────────────────
    println!("\n─── Flush to disk ───");
    engine.force_flush()?;

    // Confirm WAL was truncated
    let wal_size = std::fs::metadata(dbpath.with_extension("wal"))
        .map(|m| m.len()).unwrap_or(0);
    println!("[wal] WAL size after flush = {wal_size} bytes (should be 0)");

    let ldg_size = std::fs::metadata(&dbpath).unwrap().len();
    println!("[file] .ldg file size = {ldg_size} bytes");

    // ── Validate from disk (SIMD scan + hash chain walk) ──────────────────
    println!("\n─── Phase 2: validate from disk (SIMD + hash chain) ───");
    engine.validate_ledger()?;

    // ── Expense summary ───────────────────────────────────────────────────
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let summary = engine.get_expense_summary(now - 3600, now + 3600)?;
    println!("\n─── Expense Summary (last hour) ───");
    println!("  Debits:          {:>12} ¢  (= ${:.2})", summary.total_debits,  summary.total_debits  as f64 / 100.0);
    println!("  Credits:         {:>12} ¢  (= ${:.2})", summary.total_credits, summary.total_credits as f64 / 100.0);
    println!("  Net:             {:>12} ¢", summary.net);
    println!("  Rows scanned:    {:>12}", summary.row_count);
    println!("  Segments skipped:{:>12}", summary.sstables_skipped);

    println!("\n✓  Demo complete — all invariants hold");
    Ok(())
}

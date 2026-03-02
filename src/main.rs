//! Runnable demo: `cargo run --release`

use ledger_engine::*;
use ledger_engine::models::AccountType;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), LedgerError> {
    let dbpath = std::env::temp_dir().join("demo.ldg");
    let _ = std::fs::remove_file(&dbpath);
    let _ = std::fs::remove_file(dbpath.with_extension("wal"));

    println!("╔══════════════════════════════════════════════════╗");
    println!("║   Single-File Double-Entry Columnar Ledger       ║");
    println!("╚══════════════════════════════════════════════════╝");
    println!("DB: {}\n", dbpath.display());

    let engine = LedgerEngine::open(&dbpath)?;

    // ── Chart of accounts ─────────────────────────────────────────────────
    let cash      = engine.create_account("Cash",               AccountType::Asset)?;
    let ar        = engine.create_account("Accounts Receivable",AccountType::Asset)?;
    let equipment = engine.create_account("Equipment",          AccountType::Asset)?;
    let payable   = engine.create_account("Accounts Payable",   AccountType::Liability)?;
    let equity    = engine.create_account("Retained Earnings",  AccountType::Equity)?;
    let revenue   = engine.create_account("Revenue",            AccountType::Revenue)?;
    let rent_exp  = engine.create_account("Rent Expense",       AccountType::Expense)?;
    let sal_exp   = engine.create_account("Salaries Expense",   AccountType::Expense)?;

    // ── Journal entries ───────────────────────────────────────────────────

    // J1: Founder puts in $10 000 cash
    //     DEBIT  Cash $10 000
    //     CREDIT Retained Earnings $10 000
    engine.record_simple_entry(cash, equity, 1_000_000, "Founder equity injection")?;
    println!("[J1] Equity injection $10 000");

    // J2: Purchase $1 200 laptop — $200 cash + $1 000 on account
    //     DEBIT  Equipment       $1 200
    //     CREDIT Cash              $200
    //     CREDIT Accounts Payable $1 000
    engine.record_journal_entry(JournalEntry::new(
        "Laptop purchase",
        vec![
            Leg::debit(equipment,  120_000),
            Leg::credit(cash,       20_000),
            Leg::credit(payable,   100_000),
        ],
    ))?;
    println!("[J2] Laptop purchase  $1 200  (split: $200 cash + $1 000 payable)");

    // J3: Invoice customer $2 400
    //     DEBIT  Accounts Receivable $2 400
    //     CREDIT Revenue             $2 400
    engine.record_simple_entry(ar, revenue, 240_000, "Invoice #1001")?;
    println!("[J3] Customer invoice  $2 400");

    // J4: Customer pays invoice — AR clears to cash
    //     DEBIT  Cash               $2 400
    //     CREDIT Accounts Receivable$2 400
    engine.record_simple_entry(cash, ar, 240_000, "Payment of Invoice #1001")?;
    println!("[J4] Customer payment  $2 400");

    // J5: Pay rent $1 200
    //     DEBIT  Rent Expense $1 200
    //     CREDIT Cash         $1 200
    engine.record_simple_entry(rent_exp, cash, 120_000, "Rent – April")?;
    println!("[J5] Rent              $1 200");

    // J6: Pay salaries $3 800
    //     DEBIT  Salaries Expense $3 800
    //     CREDIT Cash             $3 800
    engine.record_simple_entry(sal_exp, cash, 380_000, "Salaries – April")?;
    println!("[J6] Salaries          $3 800");

    // ── Demonstrate that an unbalanced entry is REJECTED ──────────────────
    println!("\n--- Attempting unbalanced entry ---");
    match engine.record_journal_entry(JournalEntry::new(
        "Attempted fraud",
        vec![
            Leg::credit(cash,    999_999),
            Leg::debit(equity, 1_000_000),   // $0.01 off
        ],
    )) {
        Err(e) => println!("[REJECTED ✓] {e}"),
        Ok(_)  => println!("[ERROR] Unbalanced entry was accepted — this is a bug!"),
    }

    // ── Validate in MemTable ──────────────────────────────────────────────
    println!("\n--- validate_ledger (MemTable) ---");
    engine.validate_ledger()?;

    // ── Flush → single .ldg file ──────────────────────────────────────────
    println!("\n--- force_flush → disk ---");
    engine.force_flush()?;

    let wal_size = std::fs::metadata(dbpath.with_extension("wal"))
        .map(|m| m.len()).unwrap_or(0);
    let ldg_size = std::fs::metadata(&dbpath).unwrap().len();
    println!("WAL size after flush = {wal_size} B  (expected 0)");
    println!(".ldg file size       = {ldg_size} B");

    // ── Validate from disk (SIMD + hash chain) ────────────────────────────
    println!("\n--- validate_ledger (disk: SIMD + hash chain) ---");
    engine.validate_ledger()?;

    // ── Expense summary ───────────────────────────────────────────────────
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let s   = engine.get_expense_summary(now - 3600, now + 3600)?;
    println!("\n--- Expense Summary ---");
    println!("  Debits:            {:>12} ¢  (${:.2})", s.total_debits,  s.total_debits  as f64 / 100.0);
    println!("  Credits:           {:>12} ¢  (${:.2})", s.total_credits, s.total_credits as f64 / 100.0);
    println!("  Net:               {:>12} ¢", s.net);
    println!("  Legs scanned:      {:>12}", s.row_count);
    println!("  Segments skipped:  {:>12}", s.segments_skipped);

    println!("\n✓  All invariants satisfied");
    Ok(())
}

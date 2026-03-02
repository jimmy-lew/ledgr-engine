//! Runnable demo: `cargo run --release`

use ledger_engine::models::{Account, AccountType, Direction, JournalEntry, Leg, Transaction};
use ledger_engine::*;
use std::time::{SystemTime, UNIX_EPOCH};

// ─────────────────────────────────────────────────────────────────────────────
// Display helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Format a signed cent amount as a dollar string with sign, e.g. "+$1,200.00"
fn fmt_dollars(cents: i64) -> String {
    let sign = if cents < 0 { "-" } else { "+" };
    let abs = cents.unsigned_abs();
    let dollars = abs / 100;
    let cents_r = abs % 100;
    format!("{sign}${dollars},{cents_r:02}") // simple thousands would need more work;
                                             // keeping it readable for a demo
}

/// Format a Unix timestamp as "YYYY-MM-DD HH:MM:SS"
fn fmt_ts(ts: u64) -> String {
    // Manual UTC breakdown — no external date crate needed
    let secs = ts;
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86_400;

    // Days since 1970-01-01 → Gregorian calendar
    let mut year = 1970u32;
    let mut rem = days as u32;
    loop {
        let dy = if is_leap(year) { 366 } else { 365 };
        if rem < dy {
            break;
        }
        rem -= dy;
        year += 1;
    }
    let months = [
        31u32,
        if is_leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u32;
    for dm in &months {
        if rem < *dm {
            break;
        }
        rem -= dm;
        month += 1;
    }
    let day = rem + 1;
    format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02}")
}

fn is_leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn account_type_str(k: &AccountType) -> &'static str {
    match k {
        AccountType::Asset => "Asset",
        AccountType::Liability => "Liability",
        AccountType::Equity => "Equity",
        AccountType::Revenue => "Revenue",
        AccountType::Expense => "Expense",
    }
}

fn direction_str(d: Direction) -> &'static str {
    match d {
        Direction::Debit => "DR",
        Direction::Credit => "CR",
    }
}

/// Horizontal rule of given width
fn hr(n: usize) -> String {
    "─".repeat(n)
}

// ─────────────────────────────────────────────────────────────────────────────
// Print accounts table
// ─────────────────────────────────────────────────────────────────────────────

fn print_accounts(accounts: &[Account]) {
    //  ID │ Type      │ Balance         │ Name
    let col_name = accounts
        .iter()
        .map(|a| a.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let w = 4 + 3 + 9 + 3 + 16 + 3 + col_name;

    println!("\n┌{}┐", hr(w));
    println!("│ {:^width$} │", "ACCOUNTS", width = w - 2);
    println!("├{0}┬{1}┬{2}┬{3}┤", hr(4), hr(11), hr(18), hr(col_name + 2));
    println!(
        "│ {:<2} │ {:<9} │ {:>16} │ {:<col_name$} │",
        "ID",
        "Type",
        "Balance",
        "Name",
        col_name = col_name
    );
    println!("├{0}┼{1}┼{2}┼{3}┤", hr(4), hr(11), hr(18), hr(col_name + 2));

    for a in accounts {
        let bal_str = format!("{:>10.2}", a.balance as f64 / 100.0);
        println!(
            "│ {:<2} │ {:<9} │ {:>16} │ {:<col_name$} │",
            a.id,
            account_type_str(&a.kind),
            bal_str,
            a.name,
            col_name = col_name
        );
    }

    // Totals row — ∑ of all balances must be 0
    let total: i64 = accounts.iter().map(|a| a.balance).sum();
    println!("├{0}┴{1}┴{2}┴{3}┤", hr(4), hr(11), hr(18), hr(col_name + 2));
    println!(
        "│ {:<width$} │",
        format!("NET (must = 0.00):  {:>10.2}", total as f64 / 100.0),
        width = w - 2
    );
    println!("└{}┘", hr(w));
}

// ─────────────────────────────────────────────────────────────────────────────
// Print journal entries table
// ─────────────────────────────────────────────────────────────────────────────

fn print_journal_entries(entries: &[Vec<Transaction>], accounts: &[Account]) {
    // Build id → name lookup
    let name_of = |id: u64| -> &str {
        accounts
            .iter()
            .find(|a| a.id == id)
            .map(|a| a.name.as_str())
            .unwrap_or("?")
    };

    let max_desc = entries
        .iter()
        .flat_map(|e| e.iter())
        .map(|t| t.description.len())
        .max()
        .unwrap_or(11)
        .max(11);
    let max_acct = accounts
        .iter()
        .map(|a| a.name.len())
        .max()
        .unwrap_or(7)
        .max(7);

    // Col widths: EntryID(7) | LegID(5) | Timestamp(19) | DR/CR(5) | Account(max_acct) | Amount(14) | Desc(max_desc)
    let w_eid = 7usize;
    let w_lid = 5usize;
    let w_ts = 19usize;
    let w_dir = 5usize;
    let w_acct = max_acct;
    let w_amt = 14usize;
    let w_desc = max_desc;
    let total_w =
        w_eid + 3 + w_lid + 3 + w_ts + 3 + w_dir + 3 + w_acct + 3 + w_amt + 3 + w_desc + 2;

    println!("\n┌{}┐", hr(total_w));
    println!(
        "│ {:^width$} │",
        "JOURNAL ENTRIES  (all legs)",
        width = total_w - 2
    );
    println!(
        "├{0}┬{1}┬{2}┬{3}┬{4}┬{5}┬{6}┤",
        hr(w_eid + 2),
        hr(w_lid + 2),
        hr(w_ts + 2),
        hr(w_dir + 2),
        hr(w_acct + 2),
        hr(w_amt + 2),
        hr(w_desc + 2)
    );
    println!("│ {:<w_eid$} │ {:<w_lid$} │ {:<w_ts$} │ {:<w_dir$} │ {:<w_acct$} │ {:>w_amt$} │ {:<w_desc$} │",
        "Entry", "Leg", "Timestamp", "D/C", "Account", "Amount (USD)", "Description",
        w_eid=w_eid, w_lid=w_lid, w_ts=w_ts, w_dir=w_dir,
        w_acct=w_acct, w_amt=w_amt, w_desc=w_desc);
    println!(
        "├{0}┼{1}┼{2}┼{3}┼{4}┼{5}┼{6}┤",
        hr(w_eid + 2),
        hr(w_lid + 2),
        hr(w_ts + 2),
        hr(w_dir + 2),
        hr(w_acct + 2),
        hr(w_amt + 2),
        hr(w_desc + 2)
    );

    for (i, entry_legs) in entries.iter().enumerate() {
        // Separator between journal entries (after the first)
        if i > 0 {
            println!(
                "├{0}┼{1}┼{2}┼{3}┼{4}┼{5}┼{6}┤",
                hr(w_eid + 2),
                hr(w_lid + 2),
                hr(w_ts + 2),
                hr(w_dir + 2),
                hr(w_acct + 2),
                hr(w_amt + 2),
                hr(w_desc + 2)
            );
        }

        let entry_id = entry_legs[0].journal_entry_id;
        let is_first_row = |row: usize| row == 0;

        for (row, leg) in entry_legs.iter().enumerate() {
            let eid_cell = if is_first_row(row) {
                format!("{:<w_eid$}", entry_id, w_eid = w_eid)
            } else {
                " ".repeat(w_eid)
            };
            let ts_cell = if is_first_row(row) {
                fmt_ts(leg.timestamp)
            } else {
                " ".repeat(w_ts)
            };
            let desc_cell = if is_first_row(row) {
                format!("{:<w_desc$}", leg.description, w_desc = w_desc)
            } else {
                " ".repeat(w_desc)
            };

            let amt_str = format!("{:>10.2}", leg.amount as f64 / 100.0);

            println!(
                "│ {} │ {:<w_lid$} │ {:<w_ts$} │ {} │ {:<w_acct$} │ {:>w_amt$} │ {} │",
                eid_cell,
                leg.id,
                ts_cell,
                format!(
                    "{:^w_dir$}",
                    direction_str(leg.transaction_type),
                    w_dir = w_dir
                ),
                name_of(leg.account_id),
                amt_str,
                desc_cell,
                w_lid = w_lid,
                w_ts = w_ts,
                w_acct = w_acct,
                w_amt = w_amt
            );
        }

        // Per-entry subtotal check
        let entry_net: i64 = entry_legs.iter().map(|l| l.amount).sum();
        let net_str = format!("{:>10.2}", entry_net as f64 / 100.0);
        let balanced = if entry_net == 0 {
            "✓ balanced"
        } else {
            "✗ UNBALANCED"
        };
        // println!(
        //     "│ {} │ {:<w_lid$} │ {:<w_ts$} │ {:<w_dir$} │ {:<w_acct$} │ {:>w_amt$} │ {:<w_desc$} │",
        //     " ".repeat(w_eid),
        //     "",
        //     "",
        //     "",
        //     format!("{:>w_acct$}", "Entry total:", w_acct = w_acct),
        //     format!("{net_str} {balanced}"),
        //     "",
        //     w_lid = w_lid,
        //     w_ts = w_ts,
        //     w_dir = w_dir,
        //     w_acct = w_acct,
        //     w_amt = w_amt,
        //     w_desc = w_desc
        // );
    }

    println!(
        "└{0}┴{1}┴{2}┴{3}┴{4}┴{5}┴{6}┘",
        hr(w_eid + 2),
        hr(w_lid + 2),
        hr(w_ts + 2),
        hr(w_dir + 2),
        hr(w_acct + 2),
        hr(w_amt + 2),
        hr(w_desc + 2)
    );

    // Grand totals
    let all_legs: Vec<&Transaction> = entries.iter().flat_map(|e| e.iter()).collect();
    let total_dr: i64 = all_legs
        .iter()
        .filter(|l| l.transaction_type == Direction::Debit)
        .map(|l| l.amount)
        .sum();
    let total_cr: i64 = all_legs
        .iter()
        .filter(|l| l.transaction_type == Direction::Credit)
        .map(|l| l.amount)
        .sum();
    let grand_net = total_dr + total_cr;
    println!(
        "  Total DR: {:>10.2}   Total CR: {:>10.2}   Net: {:>10.2}  {}",
        total_dr as f64 / 100.0,
        total_cr as f64 / 100.0,
        grand_net as f64 / 100.0,
        if grand_net == 0 {
            "✓ ledger balanced"
        } else {
            "✗ LEDGER UNBALANCED"
        }
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Print raw leg listing (flat, chronological)
// ─────────────────────────────────────────────────────────────────────────────

fn print_transaction_log(txs: &[Transaction], accounts: &[Account]) {
    let name_of = |id: u64| -> &str {
        accounts
            .iter()
            .find(|a| a.id == id)
            .map(|a| a.name.as_str())
            .unwrap_or("?")
    };

    let max_acct = accounts
        .iter()
        .map(|a| a.name.len())
        .max()
        .unwrap_or(7)
        .max(7);
    let max_desc = txs
        .iter()
        .map(|t| t.description.len())
        .max()
        .unwrap_or(11)
        .max(11);
    let w = 5 + 3 + 7 + 3 + 19 + 3 + 5 + 3 + max_acct + 3 + 12 + 3 + 63 + 2;

    println!("\n┌{}┐", hr(w));
    println!(
        "│ {:^width$} │",
        "RAW TRANSACTION LOG  (one row per leg, chronological)",
        width = w - 2
    );
    println!(
        "├{0}┬{1}┬{2}┬{3}┬{4}┬{5}┬{6}┤",
        hr(7),
        hr(9),
        hr(21),
        hr(7),
        hr(max_acct + 2),
        hr(14),
        hr(65)
    );
    println!(
        "│ {:<5} │ {:<7} │ {:<19} │ {:<5} │ {:<max_acct$} │ {:>12} │ {:<63} │",
        "LegID",
        "EntryID",
        "Timestamp",
        "D/C",
        "Account",
        "Amount",
        "tx_hash (SHA-256, first 31 hex chars…)",
        max_acct = max_acct
    );
    println!(
        "├{0}┼{1}┼{2}┼{3}┼{4}┼{5}┼{6}┤",
        hr(7),
        hr(9),
        hr(21),
        hr(7),
        hr(max_acct + 2),
        hr(14),
        hr(65)
    );

    for tx in txs {
        let hash_preview = hex::encode(&tx.tx_hash[..16]); // 32 hex chars of the 32-byte hash
        let null_hash = tx.tx_hash == [0u8; 32];
        let hash_str = if null_hash {
            "(pending flush)                 ".to_string()
        } else {
            format!("{hash_preview}…")
        };

        println!(
            "│ {:<5} │ {:<7} │ {} │ {:^5} │ {:<max_acct$} │ {:>12.2} │ {:<63} │",
            tx.id,
            tx.journal_entry_id,
            fmt_ts(tx.timestamp),
            direction_str(tx.transaction_type),
            name_of(tx.account_id),
            tx.amount as f64 / 100.0,
            hash_str,
            max_acct = max_acct
        );
    }

    println!(
        "└{0}┴{1}┴{2}┴{3}┴{4}┴{5}┴{6}┘",
        hr(7),
        hr(9),
        hr(21),
        hr(7),
        hr(max_acct + 2),
        hr(14),
        hr(65)
    );
    println!("  {} legs total", txs.len());
}

// ─────────────────────────────────────────────────────────────────────────────
// Storage metadata panel
// ─────────────────────────────────────────────────────────────────────────────

fn print_storage_meta(dbpath: &std::path::Path) {
    let ldg_size = std::fs::metadata(dbpath).map(|m| m.len()).unwrap_or(0);
    let wal_size = std::fs::metadata(dbpath.with_extension("wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!("\n┌───────────────────────────────────┐");
    println!("│        STORAGE METADATA           │");
    println!("├───────────────────────────────────┤");
    println!(
        "│ File: {:>27} │",
        dbpath.file_name().unwrap_or_default().to_string_lossy()
    );
    println!("│ .ldg size: {:>20} B │", ldg_size);
    println!("│ .wal size: {:>20} B │", wal_size);
    println!("└───────────────────────────────────┘");
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

fn main() -> Result<(), LedgerError> {
    let dbpath = std::env::temp_dir().join("demo.ldg");
    let _ = std::fs::remove_file(&dbpath);
    let _ = std::fs::remove_file(dbpath.with_extension("wal"));

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║        Single-File Double-Entry Columnar Ledger          ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("DB: {}\n", dbpath.display());

    let engine = LedgerEngine::open(&dbpath)?;

    // ── Chart of accounts ─────────────────────────────────────────────────
    let cash = engine.create_account("Cash", AccountType::Asset)?;
    let ar = engine.create_account("Accounts Receivable", AccountType::Asset)?;
    let equipment = engine.create_account("Equipment", AccountType::Asset)?;
    let payable = engine.create_account("Accounts Payable", AccountType::Liability)?;
    let equity = engine.create_account("Retained Earnings", AccountType::Equity)?;
    let revenue = engine.create_account("Revenue", AccountType::Revenue)?;
    let rent_exp = engine.create_account("Rent Expense", AccountType::Expense)?;
    let sal_exp = engine.create_account("Salaries Expense", AccountType::Expense)?;

    // ── Journal entries ───────────────────────────────────────────────────

    // J1: Founder injects $10 000 cash
    engine.record_simple_entry(cash, equity, 1_000_000, "Founder equity injection")?;

    // J2: Buy $1 200 laptop — $200 cash + $1 000 on account (3-way split)
    engine.record_journal_entry(JournalEntry::new(
        "Laptop purchase",
        vec![
            Leg::debit(equipment, 120_000),
            Leg::credit(cash, 20_000),
            Leg::credit(payable, 100_000),
        ],
    ))?;

    // J3: Invoice customer $2 400
    engine.record_simple_entry(ar, revenue, 240_000, "Invoice #1001")?;

    // J4: Customer pays invoice
    engine.record_simple_entry(cash, ar, 240_000, "Payment of Invoice #1001")?;

    // J5: Pay rent $1 200
    engine.record_simple_entry(rent_exp, cash, 120_000, "Rent – April")?;

    // J6: Pay salaries $3 800
    engine.record_simple_entry(sal_exp, cash, 380_000, "Salaries – April")?;

    // ── Show state BEFORE flush (data lives in MemTable + WAL) ────────────
    println!("═══════════════════════════════════════════════════════════");
    println!("  STATE AFTER WRITES  (MemTable — not yet flushed to disk)");
    println!("═══════════════════════════════════════════════════════════");

    let accounts = engine.list_accounts();
    print_accounts(&accounts);

    let entries = engine.list_journal_entries()?;
    print_journal_entries(&entries, &accounts);

    let txs = engine.list_all_transactions()?;
    print_transaction_log(&txs, &accounts);

    // ── Rejected entry demo ───────────────────────────────────────────────
    println!("\n--- Attempting unbalanced entry (should be rejected) ---");
    match engine.record_journal_entry(JournalEntry::new(
        "Attempted fraud",
        vec![Leg::credit(cash, 999_999), Leg::debit(equity, 1_000_000)],
    )) {
        Err(e) => println!("[REJECTED ✓] {e}"),
        Ok(_) => println!("[BUG] Unbalanced entry was accepted!"),
    }

    // ── Validate in-memory ────────────────────────────────────────────────
    println!("\n--- validate_ledger (MemTable) ---");
    engine.validate_ledger()?;

    // ── Flush to the single .ldg file ─────────────────────────────────────
    println!("\n--- force_flush → .ldg file ---");
    engine.force_flush()?;

    // ── Show state AFTER flush (data is now in columnar segments on disk) ──
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  STATE AFTER FLUSH   (read back from columnar segments)");
    println!("═══════════════════════════════════════════════════════════");

    let accounts = engine.list_accounts();
    print_accounts(&accounts);

    let entries = engine.list_journal_entries()?;
    print_journal_entries(&entries, &accounts);

    let txs = engine.list_all_transactions()?;
    print_transaction_log(&txs, &accounts);

    // ── Final validation ──────────────────────────────────────────────────
    println!("\n--- validate_ledger (disk: SIMD scan + SHA-256 hash chain) ---");
    engine.validate_ledger()?;

    // ── Expense summary ───────────────────────────────────────────────────
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let s = engine.get_expense_summary(now - 3600, now + 3600)?;
    println!("\n┌─────────────────────────────────────┐");
    println!("│           EXPENSE SUMMARY           │");
    println!("├─────────────────────────────────────┤");
    println!(
        "│  Total Debits:  {:>18.2}  │",
        s.total_debits as f64 / 100.0
    );
    println!(
        "│  Total Credits: {:>18.2}  │",
        s.total_credits as f64 / 100.0
    );
    println!("│  Net:           {:>18.2}  │", s.net as f64 / 100.0);
    println!("│  Legs scanned:  {:>18}  │", s.row_count);
    println!("│  Segs skipped:  {:>18}  │", s.segments_skipped);
    println!("└─────────────────────────────────────┘");

    print_storage_meta(&dbpath);

    println!("\n✓  All done — ledger is balanced");
    Ok(())
}

use clap::{Parser, Subcommand};
use comfy_table::{presets::UTF8_FULL_CONDENSED, Table};
use rustyline::history::FileHistory;
use rustyline::Editor;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::engine::LedgerEngine;
use crate::error::LedgerError;
use crate::models::{AccountType, Direction};

const DEFAULT_LEDGER_FILE: &str = "ledger.ldb";

#[derive(Parser)]
#[command(name = "ldb")]
#[command(version = "0.1.0")]
#[command(about = "Double-entry accounting ledger", long_about = None)]
pub struct Cli {
    #[arg(short, long, global = true)]
    pub file: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Init {
        #[arg(long = "force")]
        force: bool,
    },
    Account {
        #[command(subcommand)]
        action: AccountCommands,
    },
    #[command(alias = "acct")]
    Accounts,
    Register {
        description: String,
        amount: u64,
        debit: String,
        credit: String,
        #[arg(short, long)]
        date: Option<String>,
    },
    #[command(alias = "bal")]
    Balance,
    #[command(alias = "is")]
    IncomeStatement,
    #[command(alias = "entries")]
    Print,
    Validate,
    Stats,
    Shell,
    Benchmark {
        #[arg(default_value = "10000")]
        num_entries: usize,
    },
}

#[derive(Subcommand)]
pub enum AccountCommands {
    Add { name: String, kind: String },
    List,
}

fn get_ledger_path(cli_file: &Option<PathBuf>) -> PathBuf {
    if let Some(path) = cli_file {
        return path.clone();
    }
    if let Ok(path) = std::env::var("LEDGER_FILE") {
        return PathBuf::from(path);
    }
    if let Some(data_dir) = dirs::data_local_dir() {
        return data_dir.join("ldb").join(DEFAULT_LEDGER_FILE);
    }
    PathBuf::from(DEFAULT_LEDGER_FILE)
}

fn parse_account_type(s: &str) -> Result<AccountType, String> {
    match s.to_lowercase().as_str() {
        "asset" | "a" => Ok(AccountType::Asset),
        "liability" | "liab" | "l" => Ok(AccountType::Liability),
        "equity" | "e" => Ok(AccountType::Equity),
        "revenue" | "rev" | "r" => Ok(AccountType::Revenue),
        "expense" | "exp" | "x" => Ok(AccountType::Expense),
        _ => Err(format!("Unknown account type: {}", s)),
    }
}

fn parse_date(s: &str) -> Result<u64, String> {
    let parts: Vec<u32> = s
        .split('-')
        .map(|p| p.parse().map_err(|_| format!("Invalid date: {}", s)))
        .collect::<Result<Vec<_>, _>>()?;

    if parts.len() != 3 {
        return Err(format!("Invalid date format: {}. Use YYYY-MM-DD", s));
    }

    let (year, month, day) = (parts[0], parts[1], parts[2]);

    if month < 1 || month > 12 {
        return Err(format!("Invalid month: {}", month));
    }

    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => return Err(format!("Invalid month: {}", month)),
    };

    if day < 1 || day > days_in_month {
        return Err(format!("Invalid day: {} for month {}", day, month));
    }

    let days_before_year: u64 = (1970..year)
        .map(|y| if is_leap(y) { 366 } else { 365 })
        .sum();
    let days_before_month: u64 = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        .iter()
        .take((month - 1) as usize)
        .sum();
    let days = days_before_year + days_before_month + (day - 1) as u64;

    Ok(days * 86400)
}

fn is_leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn fmt_ts(ts: u64) -> String {
    let days = ts / 86_400;
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
    format!("{year:04}-{month:02}-{day:02}")
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_amount(cents: u64) -> String {
    let dollars = cents / 100;
    let remainder = cents % 100;
    format!("${}.{:02}", dollars, remainder)
}

pub fn run() -> Result<(), LedgerError> {
    let cli = Cli::parse();

    let path = get_ledger_path(&cli.file);

    match cli.command {
        Commands::Init { force } => cmd_init(&path, force),
        Commands::Account { action } => cmd_account(&path, &action),
        Commands::Accounts => cmd_accounts(&path),
        Commands::Register {
            description,
            amount,
            debit,
            credit,
            date,
        } => cmd_register(
            &path,
            &description,
            amount,
            &debit,
            &credit,
            date.as_deref(),
        ),
        Commands::Balance => cmd_balance(&path),
        Commands::IncomeStatement => cmd_income_statement(&path),
        Commands::Print => cmd_print(&path),
        Commands::Validate => cmd_validate(&path),
        Commands::Stats => cmd_stats(&path),
        Commands::Shell => run_shell(&path),
        Commands::Benchmark { num_entries } => cmd_benchmark(num_entries),
    }
}

fn cmd_init(path: &PathBuf, force: bool) -> Result<(), LedgerError> {
    if path.exists() && !force {
        return Err(LedgerError::Encoding(format!(
            "Ledger file already exists: {}. Use --force to overwrite.",
            path.display()
        )));
    }

    if path.exists() && force {
        std::fs::remove_file(path).ok();
        std::fs::remove_file(path.with_extension("wal")).ok();
    }

    let engine = LedgerEngine::open(path)?;

    println!("Initialized ledger: {}", path.display());
    println!("  Accounts region: 1024 slots");
    println!("  Write-ahead log: {}.wal", path.display());

    drop(engine);

    Ok(())
}

fn cmd_account(path: &PathBuf, action: &AccountCommands) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;

    match action {
        AccountCommands::Add { name, kind } => {
            let account_type = parse_account_type(kind).map_err(|e| LedgerError::Encoding(e))?;

            let id = engine.create_account(name, account_type)?;
            println!("Created account: {} ({:?}) id={}", name, account_type, id);
            Ok(())
        }
        AccountCommands::List => {
            let accounts = engine.list_accounts();
            if accounts.is_empty() {
                println!("No accounts defined.");
            } else {
                for a in &accounts {
                    println!("  {:2} {:10} {}", a.id, format!("{:?}", a.kind), a.name);
                }
            }
            Ok(())
        }
    }
}

fn cmd_accounts(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;
    let accounts = engine.list_accounts();

    if accounts.is_empty() {
        println!("No accounts defined.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL_CONDENSED);
    table.set_header(["ID", "Type", "Balance", "Name"]);

    for a in &accounts {
        let bal = a.balance as f64 / 100.0;
        table.add_row([
            a.id.to_string(),
            format!("{:?}", a.kind),
            format!("{:.2}", bal),
            a.name.clone(),
        ]);
    }

    let total: f64 = accounts.iter().map(|a| a.balance as f64).sum::<f64>() / 100.0;

    println!("{}", table);
    println!("{:>4} {:>10} {:>15.2} {}", "", "TOTAL", total, "");

    Ok(())
}

fn cmd_register(
    path: &PathBuf,
    description: &str,
    amount: u64,
    debit: &str,
    credit: &str,
    date: Option<&str>,
) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;

    let accounts = engine.list_accounts();
    let accounts_map: HashMap<_, _> = accounts
        .iter()
        .map(|a| (a.name.to_lowercase(), a.id))
        .collect();

    let debit_id = *accounts_map
        .get(&debit.to_lowercase())
        .ok_or_else(|| LedgerError::Encoding(format!("Account not found: {}", debit)))?;
    let credit_id = *accounts_map
        .get(&credit.to_lowercase())
        .ok_or_else(|| LedgerError::Encoding(format!("Account not found: {}", credit)))?;

    let timestamp = if let Some(d) = date {
        Some(parse_date(d).map_err(|e| LedgerError::Encoding(e))?)
    } else {
        None
    };

    let journal_entry_id =
        engine.record_entry(debit_id, credit_id, amount, description, timestamp)?;

    println!(
        "Posted {} to {} (dr) <-> {} (cr)",
        format_amount(amount),
        debit,
        credit
    );
    println!("  Journal entry id: {}", journal_entry_id);

    Ok(())
}

fn cmd_balance(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;
    let accounts = engine.list_accounts();

    if accounts.is_empty() {
        println!("No accounts defined.");
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL_CONDENSED);
    table.set_header(["ID", "Type", "Balance", "Name"]);

    for a in &accounts {
        let display_balance = match a.kind {
            AccountType::Asset | AccountType::Expense => -a.balance,
            AccountType::Liability | AccountType::Equity | AccountType::Revenue => a.balance,
        };
        let bal_str = format!("{:.2}", display_balance as f64 / 100.0);

        table.add_row([
            a.id.to_string(),
            format!("{:?}", a.kind),
            bal_str,
            a.name.clone(),
        ]);
    }

    println!("\n{:-^50}", " BALANCE ");
    println!("{:^50}", format!("{} accounts", accounts.len()));
    println!("{}", table);

    let net: i64 = accounts.iter().map(|a| a.balance).sum();
    println!(
        "NET: {:>13.2}  {}",
        net as f64 / 100.0,
        if net == 0 {
            "✓ balanced"
        } else {
            "✗ UNBALANCED"
        }
    );

    Ok(())
}

fn cmd_income_statement(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;
    let accounts = engine.list_accounts();
    let all_txs = engine.list_all_transactions()?;

    let mut account_balances: HashMap<u64, i64> = HashMap::new();
    for tx in &all_txs {
        *account_balances.entry(tx.account_id).or_insert(0) += tx.amount;
    }

    let revenue_accounts: Vec<_> = accounts
        .iter()
        .filter(|a| matches!(a.kind, AccountType::Revenue))
        .collect();
    let expense_accounts: Vec<_> = accounts
        .iter()
        .filter(|a| matches!(a.kind, AccountType::Expense))
        .collect();

    let w = 50;
    println!("\n{:─^width$}", " INCOME STATEMENT ", width = w);

    println!("\nRevenue:");
    if revenue_accounts.is_empty() {
        println!("  (none)");
    } else {
        let mut total = 0i64;
        for a in &revenue_accounts {
            let bal = *account_balances.get(&a.id).unwrap_or(&0);
            total += bal;
            println!("  {:30} {:>15.2}", a.name, bal as f64 / 100.0);
        }
        println!(
            "  {:30} ─{:>14}",
            "-".repeat(30),
            format_amount(total.unsigned_abs())
        );
        println!("  {:30} {:>15.2}", "Total Revenue", total as f64 / 100.0);
    }

    println!("\nExpenses:");
    if expense_accounts.is_empty() {
        println!("  (none)");
    } else {
        let mut total = 0i64;
        for a in &expense_accounts {
            let bal = -(*account_balances.get(&a.id).unwrap_or(&0));
            total += bal;
            println!("  {:30} {:>15.2}", a.name, bal as f64 / 100.0);
        }
        println!(
            "  {:30} ─{:>14}",
            "-".repeat(30),
            format_amount(total.unsigned_abs())
        );
        println!("  {:30} {:>15.2}", "Total Expenses", total as f64 / 100.0);
    }

    let revenue_total: i64 = revenue_accounts
        .iter()
        .map(|a| *account_balances.get(&a.id).unwrap_or(&0))
        .sum();
    let expense_total: i64 = expense_accounts
        .iter()
        .map(|a| -(*account_balances.get(&a.id).unwrap_or(&0)))
        .sum();
    let net_income = revenue_total - expense_total;

    println!("\n{}", "─".repeat(w));
    let label = if net_income >= 0 {
        "Net Income"
    } else {
        "Net Loss"
    };
    println!("  {:30} {:>15.2}", label, net_income as f64 / 100.0);
    println!("{}", "─".repeat(w));

    Ok(())
}

fn cmd_print(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;
    let accounts = engine.list_accounts();
    let entries = engine.list_journal_entries()?;

    let name_of = |id: u64| -> String {
        accounts
            .iter()
            .find(|a| a.id == id)
            .map(|a| a.name.clone())
            .unwrap_or_else(|| format!("?({})", id))
    };

    if entries.is_empty() {
        println!("No journal entries.");
        return Ok(());
    }

    for entry in &entries {
        if let Some(first) = entry.first() {
            println!(
                "{} {:30} {:>12}",
                fmt_ts(first.timestamp),
                first.description,
                ""
            );
        }
        for leg in entry {
            let amt = leg.amount.abs() as f64 / 100.0;
            let dr_cr = if leg.transaction_type == Direction::Debit {
                "DR"
            } else {
                "CR"
            };
            println!(
                "    {:4} {:>3} {:>15.2} {}",
                leg.id,
                dr_cr,
                amt,
                name_of(leg.account_id)
            );
        }
        println!();
    }

    Ok(())
}

fn cmd_validate(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    println!("Validating ledger: {}", path.display());
    let engine = LedgerEngine::open(path)?;
    engine.validate_ledger()?;
    println!("\n✓ Ledger is valid and balanced");

    Ok(())
}

fn cmd_stats(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    let engine = LedgerEngine::open(path)?;
    let stats = engine.get_compression_stats();

    let field_names = [
        "id",
        "account_id",
        "amount",
        "transaction_type",
        "timestamp",
        "description",
        "tx_hash",
        "journal_entry_id",
    ];

    let total_compressed: u64 = stats.col_compressed.iter().sum();
    let total_uncompressed: u64 = stats.col_uncompressed.iter().sum();

    println!("\n{:=<80}", "");
    println!("{:^80}", "COMPRESSION STATISTICS");
    println!("{:=<80}", "");
    println!();

    let mut table = Table::new();
    table.load_preset(UTF8_FULL_CONDENSED);
    table.set_header(["Field", "Uncompressed", "Compressed", "Ratio", "Savings"]);

    for i in 0..8 {
        let name = field_names[i];
        let uncomp = stats.col_uncompressed[i];
        let comp = stats.col_compressed[i];
        let ratio = if uncomp > 0 {
            comp as f64 / uncomp as f64
        } else {
            0.0
        };
        let savings = if uncomp > 0 { 1.0 - ratio } else { 0.0 };
        table.add_row([
            name.to_string(),
            format_size(uncomp),
            format_size(comp),
            format!("{:.1}%", ratio * 100.0),
            format!("{:.1}%", savings * 100.0),
        ]);
    }

    println!("{}", table);

    let total_ratio = if total_uncompressed > 0 {
        total_compressed as f64 / total_uncompressed as f64
    } else {
        0.0
    };
    let total_savings = if total_uncompressed > 0 {
        1.0 - total_ratio
    } else {
        0.0
    };
    println!(
        "TOTAL {:>15} {:>15} {:>9.1}% {:>11.1}%",
        format_size(total_uncompressed),
        format_size(total_compressed),
        total_ratio * 100.0,
        total_savings * 100.0
    );
    println!();

    let file_meta = std::fs::metadata(path)?;
    println!("File size on disk: {}", format_size(file_meta.len()));
    println!(
        "Data size: {} ({} segments, {} transactions)",
        format_size(total_compressed),
        stats.segment_count,
        stats.total_tx_count
    );
    println!();

    Ok(())
}

fn run_shell(path: &PathBuf) -> Result<(), LedgerError> {
    if !path.exists() {
        return Err(LedgerError::Encoding(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        )));
    }

    println!("Ledger Engine Interactive Shell");
    println!("Type 'help' for available commands, 'exit' to quit.");

    let history_path = path.with_extension("history");
    let mut rl = Editor::<(), FileHistory>::new()
        .map_err(|e| LedgerError::Encoding(format!("Failed to init shell: {}", e)))?;

    if std::fs::metadata(&history_path).is_ok() {
        let _ = rl.load_history(&history_path);
    }

    loop {
        let readline = rl.readline("ldb> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line == "exit" || line == "quit" || line == "q" {
                    break;
                }

                let _ = rl.add_history_entry(line);

                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Err(e) = handle_shell_command(path, &parts) {
                    eprintln!("Error: {}", e);
                }
            }
            Err(rustyline::error::ReadlineError::Eof)
            | Err(rustyline::error::ReadlineError::Interrupted) => {
                break;
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);

    Ok(())
}

fn handle_shell_command(path: &PathBuf, parts: &[&str]) -> Result<(), LedgerError> {
    let cmd = parts.first().copied().unwrap_or("");

    match cmd {
        "help" => {
            println!("Available commands:");
            println!("  accounts              - List all accounts");
            println!("  balance               - Show account balances");
            println!("  register <desc> <amt> <debit> <credit> - Record a transaction");
            println!("  print                  - Show journal entries");
            println!("  stats                  - Show compression statistics");
            println!("  validate               - Validate ledger integrity");
            println!("  exit                   - Exit shell");
            Ok(())
        }
        "accounts" | "acct" => cmd_accounts(path),
        "balance" | "bal" => cmd_balance(path),
        "register" | "post" => {
            if parts.len() < 5 {
                return Err(LedgerError::Encoding(
                    "Usage: register <description> <amount> <debit> <credit>".to_string(),
                ));
            }
            let description = parts[1];
            let amount: u64 = parts[2]
                .parse()
                .map_err(|_| LedgerError::Encoding(format!("Invalid amount: {}", parts[2])))?;
            let debit = parts[3];
            let credit = parts[4];
            cmd_register(path, description, amount, debit, credit, None)
        }
        "print" | "entries" => cmd_print(path),
        "stats" => cmd_stats(path),
        "validate" => cmd_validate(path),
        _ => {
            println!(
                "Unknown command: {}. Type 'help' for available commands.",
                cmd
            );
            Ok(())
        }
    }
}

fn cmd_benchmark(num_entries: usize) -> Result<(), LedgerError> {
    let dbpath = std::path::PathBuf::from("benchmark.ldb");
    let _ = std::fs::remove_file(&dbpath);
    let _ = std::fs::remove_file(dbpath.with_extension("wal"));

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    LOAD TEST BENCHMARK                   ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("Entries: {}\n", num_entries);

    let engine = LedgerEngine::open(&dbpath)?;

    let cash = engine.create_account("Cash", AccountType::Asset)?;
    let equity = engine.create_account("Equity", AccountType::Equity)?;
    let revenue = engine.create_account("Revenue", AccountType::Revenue)?;
    let expense = engine.create_account("Expense", AccountType::Expense)?;

    let start = std::time::Instant::now();

    for i in 0..num_entries {
        let amount: u64 = ((i as i64 % 1000 + 1).unsigned_abs()) * 100;

        if i % 4 == 0 {
            engine.record_entry(cash, equity, amount, "Capital injection", None)?;
        } else if i % 4 == 1 {
            engine.record_entry(revenue, cash, amount, "Sale", None)?;
        } else {
            engine.record_entry(expense, cash, amount, "Expense", None)?;
        }
    }

    let write_time = start.elapsed();
    println!("Wrote {} entries in {:.2?}", num_entries, write_time);
    println!(
        "  (WAL + MemTable insert per entry: {:.2?})",
        write_time / num_entries as u32
    );

    let flush_start = std::time::Instant::now();
    engine.force_flush()?;
    let flush_time = flush_start.elapsed();
    println!("Flushed to disk in {:.2?}", flush_time);

    let total_legs = num_entries * 2;
    println!("Total legs written: {}", total_legs);

    let total_time = start.elapsed();
    println!("\n┌─────────────────────────────────────┐");
    println!("│           BENCHMARK RESULTS         │");
    println!("├─────────────────────────────────────┤");
    println!("│  Total entries: {:>19} │", num_entries);
    println!("│  Total legs: {:>22} │", num_entries * 2);
    println!("│  Write time: {:>22?} │", write_time);
    println!("│  Flush time: {:>22?} │", flush_time);
    println!("│  Total time: {:>22?} │", total_time);
    println!(
        "│  Throughput: {:>20}/s │",
        num_entries as f64 / total_time.as_secs_f64()
    );
    println!("└─────────────────────────────────────┘");

    let metadata = std::fs::metadata(&dbpath).unwrap();
    println!("\nDatabase size: {} bytes", metadata.len());

    Ok(())
}

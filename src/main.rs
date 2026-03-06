use ledger_engine::models::Direction;
use ledger_engine::*;
use std::env;
use std::path::PathBuf;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_LEDGER_FILE: &str = "ledger.ldb";

fn main() {
    let args: Vec<String> = env::args().collect();
    let prog = args.get(0).map(|s| s.as_str()).unwrap_or("ldb");

    if args.len() < 2 {
        print_help(prog);
        std::process::exit(1);
    }

    let cmd = &args[1];

    match cmd.as_str() {
        "help" | "-h" | "--help" => {
            print_help(prog);
        }
        "version" | "--version" | "-V" => {
            println!("ldb {}", VERSION);
        }
        "init" => {
            if let Err(e) = cmd_init(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "account" => {
            if let Err(e) = cmd_account(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "register" | "post" => {
            if let Err(e) = cmd_register(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "balance" | "bal" => {
            if let Err(e) = cmd_balance(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "incomestatement" | "is" => {
            if let Err(e) = cmd_income_statement(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "accounts" | "acct" => {
            if let Err(e) = cmd_accounts(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "print" | "entries" => {
            if let Err(e) = cmd_print(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        "validate" => {
            if let Err(e) = cmd_validate(&args) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("Unknown command: {}\n", cmd);
            print_help(prog);
            std::process::exit(1);
        }
    }
}

fn print_help(prog: &str) {
    println!("ldb - double-entry accounting ledger");
    println!();
    println!("Usage:");
    println!("  {} <command> [options] [args]", prog);
    println!();
    println!("Commands:");
    println!("  init                     Initialize a new ledger file");
    println!("  account [add] <name> <type>  Create a new account");
    println!("  accounts                 List all accounts");
    println!("  register <desc> <amt> <debit> <credit> [-d YYYY-MM-DD]");
    println!("                           Record a transaction");
    println!("  balance                  Show account balances");
    println!("  incomestatement          Show income statement");
    println!("  print                    Show all journal entries");
    println!("  validate                 Validate ledger integrity");
    println!("  version                  Show version");
    println!("  help                     Show this help");
    println!();
    println!("Account types:");
    println!("  asset, liability, equity, revenue, expense");
    println!();
    println!("Examples:");
    println!("  {} init", prog);
    println!("  {} account add Cash asset", prog);
    println!("  {} account add Employer liability", prog);
    println!("  {} account add \"Salary Income\" revenue", prog);
    println!(
        "  {} register \"Earned salary\" 3000 Employer \"Salary Income\" -d 2024-01-15",
        prog
    );
    println!(
        "  {} register \"Got paid\" 3000 Cash Employer -d 2024-01-20",
        prog
    );
    println!("  {} balance", prog);
    println!("  {} incomestatement", prog);
}

fn get_ledger_path() -> PathBuf {
    if let Ok(path) = env::var("LEDGER_FILE") {
        return PathBuf::from(path);
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

fn cmd_init(args: &[String]) -> Result<(), String> {
    let path = if let Some(pos) = args.iter().position(|a| a == "-f" || a == "--file") {
        if pos + 1 < args.len() {
            PathBuf::from(&args[pos + 1])
        } else {
            return Err("Missing file path after -f".to_string());
        }
    } else {
        get_ledger_path()
    };

    if path.exists() {
        return Err(format!("Ledger file already exists: {}", path.display()));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;

    println!("Initialized ledger: {}", path.display());
    println!("  Accounts region: 1024 slots");
    println!("  Write-ahead log: {}.wal", path.display());

    std::mem::forget(engine);
    Ok(())
}

fn cmd_account(args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;

    if args.len() == 2 {
        let accounts = engine.list_accounts();
        if accounts.is_empty() {
            println!("No accounts defined.");
        } else {
            println!("Accounts:");
            for a in &accounts {
                println!("  {:2} {:10} {}", a.id, format!("{:?}", a.kind), a.name);
            }
        }
        return Ok(());
    }

    let subcmd = args.get(2).map(|s| s.as_str()).unwrap_or("add");
    match subcmd {
        "add" | "create" => {
            if args.len() < 5 {
                return Err("Usage: ldb account add <name> <type>".to_string());
            }
            let name = &args[3];
            let type_str = &args[4];
            let kind = parse_account_type(type_str)?;

            let id = engine
                .create_account(name, kind)
                .map_err(|e| e.to_string())?;
            println!("Created account: {} ({:?}) id={}", name, kind, id);
            Ok(())
        }
        _ => Err(format!("Unknown account subcommand: {}", subcmd)),
    }
}

fn cmd_accounts(_args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;
    let accounts = engine.list_accounts();

    if accounts.is_empty() {
        println!("No accounts defined.");
        return Ok(());
    }

    let _name_width = accounts
        .iter()
        .map(|a| a.name.len())
        .max()
        .unwrap_or(10)
        .max(10);
    let header = format!("{:>4} {:>10} {:>15} {}", "ID", "Type", "Balance", "Name");
    let sep = "-".repeat(header.len());

    println!("{}", header);
    println!("{}", sep);

    for a in &accounts {
        let bal = a.balance as f64 / 100.0;
        println!(
            "{:>4} {:>10} {:>15.2} {}",
            a.id,
            format!("{:?}", a.kind),
            bal,
            a.name
        );
    }

    println!("{}", sep);
    let total: f64 = accounts.iter().map(|a| a.balance as f64).sum::<f64>() / 100.0;
    println!("{:>4} {:>10} {:>15.2} {}", "", "TOTAL", total, "");

    Ok(())
}

fn cmd_register(args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    if args.len() < 5 {
        return Err(
            "Usage: ldb register <description> <amount> <debit-account> <credit-account> [-d YYYY-MM-DD]"
                .to_string(),
        );
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;

    let description = &args[2];
    let amount_str = &args[3];
    let debit_name = &args[4];
    let credit_name = &args[5];

    let mut timestamp: Option<u64> = None;
    if let Some(pos) = args.iter().position(|a| a == "-d" || a == "--date") {
        if pos + 1 < args.len() {
            timestamp = Some(parse_date(&args[pos + 1])?);
        }
    }

    let amount: u64 = amount_str
        .replace(',', "")
        .parse()
        .map_err(|_| format!("Invalid amount: {}", amount_str))?;

    let accounts = engine.list_accounts();
    let accounts_map: std::collections::HashMap<_, _> = accounts
        .iter()
        .map(|a| (a.name.to_lowercase(), a.id))
        .collect();

    let debit_id = *accounts_map
        .get(&debit_name.to_lowercase())
        .ok_or_else(|| format!("Account not found: {}", debit_name))?;
    let credit_id = *accounts_map
        .get(&credit_name.to_lowercase())
        .ok_or_else(|| format!("Account not found: {}", credit_name))?;

    let journal_entry_id = engine
        .record_entry_with_timestamp(debit_id, credit_id, amount, description, timestamp)
        .map_err(|e| e.to_string())?;

    println!(
        "Posted {} to {} (dr) <-> {} (cr)",
        format_amount(amount),
        debit_name,
        credit_name
    );
    println!("  Journal entry id: {}", journal_entry_id);

    Ok(())
}

fn cmd_balance(_args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;
    let mut accounts = engine.list_accounts();

    if accounts.is_empty() {
        println!("No accounts defined.");
        return Ok(());
    }

    let all_txs = engine.list_all_transactions().map_err(|e| e.to_string())?;
    for tx in &all_txs {
        if let Some(acct) = accounts.iter_mut().find(|a| a.id == tx.account_id) {
            acct.balance += tx.amount;
        }
    }

    let name_width = accounts
        .iter()
        .map(|a| a.name.len())
        .max()
        .unwrap_or(10)
        .max(10);
    let w = 4 + 3 + 10 + 3 + 15 + 3 + name_width;

    println!("\n{:─^width$}", " BALANCE ", width = w);
    println!(
        "│ {:^width$} │",
        format!("{} accounts", accounts.len()),
        width = w - 2
    );
    println!(
        "├{0}┬{1}┬{2}┬{3}┤",
        "─".repeat(4),
        "─".repeat(10),
        "─".repeat(15),
        "─".repeat(name_width + 2)
    );
    println!(
        "│ {:<2} │ {:<8} │ {:>13} │ {:<width$} │",
        "ID",
        "Type",
        "Balance",
        "Name",
        width = name_width
    );
    println!(
        "├{0}┼{1}┼{2}┼{3}┤",
        "─".repeat(4),
        "─".repeat(10),
        "─".repeat(15),
        "─".repeat(name_width + 2)
    );

    let mut asset_total = 0i64;
    let mut liab_total = 0i64;
    let mut equity_total = 0i64;
    let mut revenue_total = 0i64;
    let mut expense_total = 0i64;

    for a in &accounts {
        let display_balance = match a.kind {
            AccountType::Asset | AccountType::Expense => -a.balance,
            AccountType::Liability | AccountType::Equity | AccountType::Revenue => a.balance,
        };
        let bal_str = format!("{:>13.2}", display_balance as f64 / 100.0);
        println!(
            "│ {:<2} │ {:<8} │ {:>13} │ {:<width$} │",
            a.id,
            format!("{:?}", a.kind),
            bal_str,
            a.name,
            width = name_width
        );

        match a.kind {
            AccountType::Asset => asset_total += a.balance,
            AccountType::Liability => liab_total += a.balance,
            AccountType::Equity => equity_total += a.balance,
            AccountType::Revenue => revenue_total += a.balance,
            AccountType::Expense => expense_total += a.balance,
        }
    }

    println!(
        "├{0}┴{1}┴{2}┴{3}┤",
        "─".repeat(4),
        "─".repeat(10),
        "─".repeat(15),
        "─".repeat(name_width + 2)
    );

    let net = asset_total + liab_total + equity_total + revenue_total + expense_total;
    println!(
        "│ {:<width$} │",
        format!(
            "NET: {:>13.2}  {}",
            net as f64 / 100.0,
            if net == 0 {
                "✓ balanced"
            } else {
                "✗ UNBALANCED"
            }
        ),
        width = w - 2
    );
    println!("└{}┘", "─".repeat(w));

    Ok(())
}

fn cmd_income_statement(_args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;
    let accounts = engine.list_accounts();
    let all_txs = engine.list_all_transactions().map_err(|e| e.to_string())?;

    let mut account_balances: std::collections::HashMap<u64, i64> =
        std::collections::HashMap::new();
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

fn cmd_print(_args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;
    let accounts = engine.list_accounts();
    let entries = engine.list_journal_entries().map_err(|e| e.to_string())?;

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

fn cmd_validate(_args: &[String]) -> Result<(), String> {
    let path = get_ledger_path();
    if !path.exists() {
        return Err(format!(
            "Ledger not found: {}. Run 'ldb init' first.",
            path.display()
        ));
    }

    println!("Validating ledger: {}", path.display());
    let engine = LedgerEngine::open(&path).map_err(|e| e.to_string())?;
    engine.validate_ledger().map_err(|e| e.to_string())?;
    println!("\n✓ Ledger is valid and balanced");

    Ok(())
}

fn format_amount(cents: u64) -> String {
    let dollars = cents / 100;
    let remainder = cents % 100;
    format!("${}.{:02}", dollars, remainder)
}

fn fmt_ts(ts: u64) -> String {
    let secs = ts;
    let days = secs / 86_400;

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

fn is_leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
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

    let days_before_year = (1970..year)
        .map(|y| if is_leap(y) { 366 } else { 365 })
        .sum::<u64>();
    let days_before_month: u64 = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        .iter()
        .take((month - 1) as usize)
        .sum();
    let days = days_before_year + days_before_month + (day - 1) as u64;

    Ok(days * 86400)
}

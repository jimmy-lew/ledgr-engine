#[cfg(test)]
mod tests {
    use crate::file_format::*;
    use crate::hash_chain::{compute_tx_hash, genesis_hash};
    use crate::models::*;
    use crate::simd_scan;
    use crate::sparse_index::SparseIndex;
    use crate::*;
    use std::io::Cursor;

    // ── Helpers ────────────────────────────────────────────────────────────

    fn make_raw_tx(
        id: u64,
        entry_id: u64,
        acct: u64,
        amount: i64,
        dir: Direction,
        ts: u64,
    ) -> Transaction {
        Transaction {
            id,
            journal_entry_id: entry_id,
            account_id: acct,
            amount,
            transaction_type: dir,
            timestamp: ts,
            description: format!("tx-{id}"),
            tx_hash: [0u8; 32],
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 1. JournalEntry validation – accounting invariant
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn journal_entry_balanced_passes() {
        let entry = JournalEntry::new(
            "Rent payment",
            vec![Leg::debit(1, 120_000), Leg::credit(2, 120_000)],
            None,
        );
        assert!(entry.validate().is_ok());
    }

    #[test]
    fn journal_entry_unbalanced_rejected() {
        let entry = JournalEntry::new(
            "Bad entry",
            vec![
                Leg::debit(1, 100_000),
                Leg::credit(2, 90_000), // ← $100 debit, $90 credit — doesn't balance
            ],
            None,
        );
        match entry.validate() {
            Err(LedgerError::JournalNotBalanced { debits, credits }) => {
                assert_eq!(debits, 100_000);
                assert_eq!(credits, 90_000);
            }
            other => panic!("expected JournalNotBalanced, got {:?}", other),
        }
    }

    #[test]
    fn journal_entry_single_leg_rejected() {
        let entry = JournalEntry::new("Orphan", vec![Leg::debit(1, 500)], None);
        assert!(matches!(
            entry.validate(),
            Err(LedgerError::JournalTooFewLegs { got: 1 })
        ));
    }

    #[test]
    fn journal_entry_empty_rejected() {
        let entry = JournalEntry::new("Empty", vec![], None);
        assert!(matches!(
            entry.validate(),
            Err(LedgerError::JournalTooFewLegs { got: 0 })
        ));
    }

    #[test]
    fn journal_entry_split_three_legs_balanced() {
        // Equipment $1200 = Cash $200 + Payable $1000
        let entry = JournalEntry::new(
            "Laptop purchase",
            vec![
                Leg::debit(10, 120_000),  // Equipment ↑
                Leg::credit(20, 20_000),  // Cash ↓
                Leg::credit(30, 100_000), // Accounts Payable ↑
            ],
            None,
        );
        assert!(entry.validate().is_ok());
        // Verify net
        let net: i64 = entry.legs.iter().map(|l| l.signed_amount()).sum();
        assert_eq!(net, 0);
    }

    #[test]
    fn leg_signed_amounts_correct() {
        assert_eq!(Leg::debit(1, 500).signed_amount(), -500);
        assert_eq!(Leg::credit(1, 500).signed_amount(), 500);
    }

    // ──────────────────────────────────────────────────────────────────────
    // 2. Engine rejects unbalanced entries before any I/O
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn engine_rejects_unbalanced_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path().join("l.ldg")).unwrap();
        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
        let rev = engine
            .create_account("Revenue", AccountType::Revenue)
            .unwrap();

        let result = engine.record_journal_entry(JournalEntry::new(
            "Unbalanced",
            vec![
                Leg::debit(cash, 1_000),
                Leg::credit(rev, 999), // 1 cent off
            ],
            None,
        ));
        assert!(matches!(
            result,
            Err(LedgerError::JournalNotBalanced { .. })
        ));
    }

    #[test]
    fn engine_rejects_single_leg_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path().join("l.ldg")).unwrap();
        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();

        let result = engine.record_journal_entry(JournalEntry::new(
            "Orphan debit",
            vec![Leg::debit(cash, 5_000)],
            None,
        ));
        assert!(matches!(result, Err(LedgerError::JournalTooFewLegs { .. })));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 3. Engine records valid two-leg and three-leg entries
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn engine_two_leg_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path().join("l.ldg")).unwrap();
        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
        let rev = engine
            .create_account("Revenue", AccountType::Revenue)
            .unwrap();

        let entry_id = engine
            .record_entry(cash, rev, 50_000, "Cash sale", None)
            .unwrap();
        assert!(entry_id > 0);
        engine.validate_ledger().unwrap();
    }

    #[test]
    fn engine_three_leg_split_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path().join("l.ldg")).unwrap();

        let equip = engine
            .create_account("Equipment", AccountType::Asset)
            .unwrap();
        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
        let payable = engine
            .create_account("Accounts Payable", AccountType::Liability)
            .unwrap();

        // Buy $1 200 laptop: $200 cash + $1 000 on credit
        engine
            .record_journal_entry(JournalEntry::new(
                "Laptop purchase",
                vec![
                    Leg::debit(equip, 120_000),
                    Leg::credit(cash, 20_000),
                    Leg::credit(payable, 100_000),
                ],
                None,
            ))
            .unwrap();

        engine.validate_ledger().unwrap();
    }

    // ──────────────────────────────────────────────────────────────────────
    // 4. validate_ledger – balanced & imbalanced ledgers
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn validate_ledger_balanced() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path().join("l.ldg")).unwrap();
        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
        let equity = engine
            .create_account("Equity", AccountType::Equity)
            .unwrap();
        let exp = engine
            .create_account("Expense", AccountType::Expense)
            .unwrap();

        engine
            .record_entry(cash, equity, 1_000_000, "Equity injection", None)
            .unwrap();
        engine
            .record_entry(exp, cash, 500_000, "Operating costs", None)
            .unwrap();

        engine.validate_ledger().unwrap();
        engine.force_flush().unwrap();
        engine.validate_ledger().unwrap(); // again from disk
    }

    // ──────────────────────────────────────────────────────────────────────
    // 5. Hash chain – tamper detection
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn hash_chain_avalanche() {
        let genesis = genesis_hash();
        let tx0 = make_raw_tx(1, 10, 1, -500, Direction::Debit, 100);
        let tx1 = make_raw_tx(2, 10, 2, 500, Direction::Credit, 100);

        let h0 = compute_tx_hash(&tx0, &genesis);
        let h1 = compute_tx_hash(&tx1, &h0);

        // Tamper tx0's amount
        let mut tx0_bad = tx0.clone();
        tx0_bad.amount = -1;
        let h0_bad = compute_tx_hash(&tx0_bad, &genesis);

        // tx1's hash computed from the tampered h0 must differ
        let h1_bad = compute_tx_hash(&tx1, &h0_bad);

        assert_ne!(h0, h0_bad, "tampered tx must have different hash");
        assert_ne!(h1, h1_bad, "downstream hash must cascade differently");
    }

    // ──────────────────────────────────────────────────────────────────────
    // 6. File header round-trip
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn file_header_roundtrip() {
        let mut hdr = FileHeader::new();
        hdr.accounts_count = 7;
        hdr.total_tx_count = 500;

        let mut buf = Cursor::new(vec![0u8; FILE_HEADER_SIZE]);
        hdr.write_to(&mut buf).unwrap();
        assert_eq!(buf.get_ref().len(), FILE_HEADER_SIZE);

        buf.set_position(0);
        let r = FileHeader::read_from(&mut buf).unwrap();
        assert_eq!(r.accounts_count, 7);
        assert_eq!(r.total_tx_count, 500);
        assert_eq!(&r.magic, b"LDGR");
    }

    #[test]
    fn file_header_crc_protects_corruption() {
        let mut hdr = FileHeader::new();
        let mut buf = Cursor::new(vec![0u8; FILE_HEADER_SIZE]);
        hdr.write_to(&mut buf).unwrap();
        buf.get_mut()[0x028] ^= 0x01; // flip bit in total_tx_count
        buf.set_position(0);
        assert!(matches!(
            FileHeader::read_from(&mut buf),
            Err(LedgerError::HeaderChecksumMismatch { .. })
        ));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 7. Sparse index
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn sparse_index_lower_bound() {
        let rows: Vec<(u64, u64)> = (0u64..320).map(|i| (i * 100, i)).collect();
        let idx = SparseIndex::build(&rows);
        assert_eq!(idx.entries.len(), 5); // 0, 64, 128, 192, 256
        assert_eq!(idx.lower_bound_row(6400), 64);
        assert_eq!(idx.lower_bound_row(7_000), 64);
        assert_eq!(idx.lower_bound_row(0), 0);
    }

    // ──────────────────────────────────────────────────────────────────────
    // 8. SIMD scan correctness
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn simd_sum_balanced() {
        let amounts: Vec<i64> = vec![-500, 500, -1000, 1000, -200, 200];
        assert_eq!(simd_scan::simd_sum_i64(&amounts), 0);
    }

    // ──────────────────────────────────────────────────────────────────────
    // 9. Crash recovery via WAL replay
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn crash_recovery_preserves_balance() {
        let tmp = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");

        {
            let engine = LedgerEngine::open(&dbpath).unwrap();
            let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
            let equity = engine
                .create_account("Equity", AccountType::Equity)
                .unwrap();
            // Write but do NOT flush — WAL holds the data
            engine
                .record_entry(cash, equity, 100_000, "Equity", None)
                .unwrap();
        }

        // Reopen — WAL is replayed automatically
        let engine = LedgerEngine::open(&dbpath).unwrap();
        engine.validate_ledger().unwrap();
    }

    // ──────────────────────────────────────────────────────────────────────
    // 10. Single-file assertion: WAL empty after flush
    // ──────────────────────────────────────────────────────────────────────

    #[test]
    fn wal_truncated_after_flush() {
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");
        let engine = LedgerEngine::open(&dbpath).unwrap();

        let cash = engine.create_account("Cash", AccountType::Asset).unwrap();
        let rev = engine.create_account("Rev", AccountType::Revenue).unwrap();
        engine.record_entry(cash, rev, 1_000, "test", None).unwrap();
        engine.force_flush().unwrap();

        let wal_len = fs::metadata(dbpath.with_extension("wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(wal_len, 0, "WAL must be truncated after a successful flush");
    }
}

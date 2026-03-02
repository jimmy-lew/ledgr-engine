#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use ledger_engine::*;
    use ledger_engine::file_format::*;
    use ledger_engine::models::*;
    use ledger_engine::hash_chain::{compute_tx_hash, genesis_hash};
    use ledger_engine::sparse_index::{SparseIndex, SPARSE_FACTOR};
    use ledger_engine::simd_scan;

    // ── Helpers ────────────────────────────────────────────────────────────

    fn make_tx(id: u64, account_id: u64, amount: i64, tt: TransactionType, ts: u64) -> Transaction {
        Transaction {
            id, account_id, amount, transaction_type: tt,
            timestamp: ts,
            description: format!("tx-{id}"),
            tx_hash: [0u8; 32],
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 1. File header round-trip and CRC
    // ──────────────────────────────────────────────────────────────────────
    #[test]
    fn file_header_roundtrip() {
        let mut hdr = FileHeader::new();
        hdr.accounts_count = 42;
        hdr.total_tx_count = 1_000_000;
        hdr.last_tx_hash   = [0xABu8; 32];

        let mut buf = Cursor::new(vec![0u8; FILE_HEADER_SIZE]);
        hdr.write_to(&mut buf).unwrap();
        assert_eq!(buf.get_ref().len(), FILE_HEADER_SIZE);

        buf.set_position(0);
        let restored = FileHeader::read_from(&mut buf).unwrap();
        assert_eq!(restored.accounts_count, 42);
        assert_eq!(restored.total_tx_count, 1_000_000);
        assert_eq!(restored.last_tx_hash,   [0xABu8; 32]);
        assert_eq!(&restored.magic,          b"LDGR");
    }

    #[test]
    fn file_header_detects_bad_magic() {
        let mut hdr = FileHeader::new();
        let mut buf = Cursor::new(vec![0u8; FILE_HEADER_SIZE]);
        hdr.write_to(&mut buf).unwrap();
        buf.get_mut()[0] = 0xFF;  // corrupt magic
        buf.set_position(0);
        assert!(matches!(FileHeader::read_from(&mut buf), Err(LedgerError::BadMagic)));
    }

    #[test]
    fn file_header_detects_crc_corruption() {
        let mut hdr = FileHeader::new();
        hdr.total_tx_count = 5;
        let mut buf = Cursor::new(vec![0u8; FILE_HEADER_SIZE]);
        hdr.write_to(&mut buf).unwrap();
        // Flip a bit in total_tx_count (offset 0x028)
        buf.get_mut()[0x028] ^= 0x01;
        buf.set_position(0);
        assert!(matches!(
            FileHeader::read_from(&mut buf),
            Err(LedgerError::HeaderChecksumMismatch { .. })
        ));
    }

    // ──────────────────────────────────────────────────────────────────────
    // 2. Hash chain – correct chaining and tamper detection
    // ──────────────────────────────────────────────────────────────────────
    #[test]
    fn hash_chain_builds_correctly() {
        let mut prev = genesis_hash();
        let txs: Vec<Transaction> = (1..=5)
            .map(|i| make_tx(i, 1, i as i64 * 100, TransactionType::Credit, i * 1000))
            .collect();

        let mut hashes = Vec::new();
        for tx in &txs {
            let h = compute_tx_hash(tx, &prev);
            hashes.push(h);
            prev = h;
        }

        // Each hash must be different
        for i in 0..hashes.len() - 1 {
            assert_ne!(hashes[i], hashes[i+1], "consecutive hashes must differ");
        }
    }

    #[test]
    fn hash_chain_detects_amount_change() {
        let prev  = genesis_hash();
        let mut tx = make_tx(1, 1, 1000, TransactionType::Credit, 100);

        let original_hash = compute_tx_hash(&tx, &prev);

        // Tamper with the amount
        tx.amount = 999_999;
        let tampered_hash = compute_tx_hash(&tx, &prev);

        assert_ne!(original_hash, tampered_hash,
            "changing amount must produce a different hash");
    }

    #[test]
    fn hash_chain_avalanche_effect() {
        // Changing an early transaction must cascade to change all subsequent hashes
        let txs: Vec<Transaction> = (1..=4)
            .map(|i| make_tx(i, 1, i as i64 * 10, TransactionType::Debit, i * 500))
            .collect();

        // Compute honest chain
        let mut prev = genesis_hash();
        let mut honest: Vec<[u8; 32]> = Vec::new();
        for tx in &txs { let h = compute_tx_hash(tx, &prev); honest.push(h); prev = h; }

        // Tamper tx[0] and recompute from there
        let mut tampered_tx0 = txs[0].clone();
        tampered_tx0.amount = 999;
        let mut prev_t = genesis_hash();
        let mut tampered: Vec<[u8; 32]> = Vec::new();
        let altered = compute_tx_hash(&tampered_tx0, &prev_t);
        tampered.push(altered); prev_t = altered;
        for tx in &txs[1..] { let h = compute_tx_hash(tx, &prev_t); tampered.push(h); prev_t = h; }

        // All four hashes must differ
        for i in 0..4 {
            assert_ne!(honest[i], tampered[i],
                "hash at position {i} should differ after tampering position 0");
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // 3. Sparse index queries
    // ──────────────────────────────────────────────────────────────────────
    #[test]
    fn sparse_index_lower_bound() {
        // 320 rows with timestamps 0, 100, 200, …, 31900
        let rows: Vec<(u64, u64)> = (0u64..320).map(|i| (i * 100, i)).collect();
        let idx = SparseIndex::build(&rows);

        // Expected entries at rows 0, 64, 128, 192, 256
        assert_eq!(idx.entries.len(), 5);

        // Query ts=6400 (row 64) → exact match at sparse entry 1
        assert_eq!(idx.lower_bound_row(6400), 64);

        // Query ts=7000 (between rows 70 and 71) → returns row 64 (safe lower bound)
        assert_eq!(idx.lower_bound_row(7_000), 64);

        // Query ts=0 → row 0
        assert_eq!(idx.lower_bound_row(0), 0);

        // Query ts=999_999 → last sparse entry (row 256)
        assert_eq!(idx.lower_bound_row(999_999), 256);
    }

    #[test]
    fn sparse_index_skips_segments_correctly() {
        // Simulates 192 rows; we want only those with ts ∈ [5000, 10000].
        // Rows 0–63: ts 0–6300; rows 64–127: ts 6400–12700
        let rows: Vec<(u64, u64)> = (0u64..192).map(|i| (i * 100, i)).collect();
        let idx  = SparseIndex::build(&rows);

        let start_row = idx.lower_bound_row(5_000);
        // Row with ts 5000 is row 50, but sparse lower bound snaps to row 0
        // (the only entry before row 64 is row 0 with ts=0)
        assert!(start_row <= 50, "lower bound must not overshoot the start");
    }

    // ──────────────────────────────────────────────────────────────────────
    // 4. SIMD scan – basic arithmetic correctness
    // ──────────────────────────────────────────────────────────────────────
    #[test]
    fn simd_sum_balanced_amounts() {
        let amts: Vec<i64> = vec![-100, 100, -200, 200, -50, 50, -1000, 1000];
        assert_eq!(simd_scan::simd_sum_i64(&amts), 0);
    }

    #[test]
    fn simd_split_sum_large() {
        let n = 10_000usize;
        let amounts:  Vec<i64> = (0..n).map(|i| if i % 2 == 0 { -(i as i64 + 1) } else { i as i64 + 1 }).collect();
        let tx_types: Vec<u8>  = (0..n).map(|i| (i % 2) as u8).collect();

        let (scalar_d, scalar_c) = {
            let mut d = 0i64; let mut c = 0i64;
            for (i, (&a, &t)) in amounts.iter().zip(tx_types.iter()).enumerate() {
                if t == 0 { d += a; } else { c += a; }
            }
            (d, c)
        };

        let (simd_d, simd_c) = simd_scan::simd_sum_by_type(&amounts, &tx_types);
        assert_eq!(simd_d, scalar_d);
        assert_eq!(simd_c, scalar_c);
    }

    // ──────────────────────────────────────────────────────────────────────
    // 5. End-to-end engine: append → flush → validate → query
    // ──────────────────────────────────────────────────────────────────────
    #[test]
    fn engine_end_to_end_balanced() {
        let tmp    = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");
        let engine = LedgerEngine::open(&dbpath).unwrap();

        let cash    = engine.create_account("Cash",    AccountType::Asset).unwrap();
        let expense = engine.create_account("Expense", AccountType::Expense).unwrap();
        let revenue = engine.create_account("Revenue", AccountType::Revenue).unwrap();

        // Balanced: revenue +$1 200, cash +$1 200
        engine.append_transaction(cash,    120_000, TransactionType::Credit, "Sale proceeds").unwrap();
        engine.append_transaction(revenue,-120_000, TransactionType::Debit,  "Revenue recognised").unwrap();

        // Balanced: expense +$300, cash –$300
        engine.append_transaction(expense,  30_000, TransactionType::Debit,  "Rent").unwrap();
        engine.append_transaction(cash,    -30_000, TransactionType::Credit, "Rent payment").unwrap();

        // In-MemTable validation
        engine.validate_ledger().unwrap();

        // Flush to disk, then re-validate (exercises the SSTable read path)
        engine.force_flush().unwrap();
        engine.validate_ledger().unwrap();
    }

    #[test]
    fn engine_detects_imbalance() {
        let tmp    = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");
        let engine = LedgerEngine::open(&dbpath).unwrap();

        let acct = engine.create_account("Orphan", AccountType::Asset).unwrap();
        engine.append_transaction(acct, 50_000, TransactionType::Credit, "Ghost credit").unwrap();

        match engine.validate_ledger() {
            Err(LedgerError::ImbalancedLedger { net }) => {
                assert_eq!(net, 50_000, "net should equal the orphaned amount");
            }
            other => panic!("expected ImbalancedLedger, got {:?}", other),
        }
    }

    #[test]
    fn engine_expense_summary_with_zone_skip() {
        let tmp    = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");
        let engine = LedgerEngine::open(&dbpath).unwrap();

        let acct = engine.create_account("Test", AccountType::Asset).unwrap();

        // Use manual timestamp injection is not directly possible through
        // the public API (timestamp = now), so we just verify the summary
        // returns sensible totals across all time.
        engine.append_transaction(acct, -500, TransactionType::Debit,  "Out").unwrap();
        engine.append_transaction(acct,  500, TransactionType::Credit, "In").unwrap();

        engine.force_flush().unwrap();

        let summary = engine.get_expense_summary(0, u64::MAX).unwrap();
        assert_eq!(summary.total_debits  + summary.total_credits, 0);
        assert_eq!(summary.net, 0);
        assert_eq!(summary.row_count, 2);
    }

    #[test]
    fn engine_survives_crash_recovery() {
        let tmp    = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");

        // First session: write some transactions but do NOT flush
        {
            let engine = LedgerEngine::open(&dbpath).unwrap();
            let acct = engine.create_account("Recovery", AccountType::Equity).unwrap();
            engine.append_transaction(acct, -200, TransactionType::Debit,  "pre-crash debit").unwrap();
            engine.append_transaction(acct,  200, TransactionType::Credit, "pre-crash credit").unwrap();
            // Drop without flushing – WAL has the data, .ldg has only the account
        }

        // Second session: replay WAL, verify recovery
        {
            let engine = LedgerEngine::open(&dbpath).unwrap();
            // After WAL replay the MemTable should be populated
            engine.validate_ledger().unwrap();   // should balance
        }
    }

    #[test]
    fn single_file_contains_all_data() {
        use std::fs;
        let tmp    = tempfile::tempdir().unwrap();
        let dbpath = tmp.path().join("ledger.ldg");
        let engine = LedgerEngine::open(&dbpath).unwrap();

        let acct = engine.create_account("SoleAccount", AccountType::Asset).unwrap();
        engine.append_transaction(acct, 1_000, TransactionType::Credit, "deposit").unwrap();
        engine.append_transaction(acct,-1_000, TransactionType::Debit,  "withdrawal").unwrap();
        engine.force_flush().unwrap();

        // The WAL should be empty after flush (truncated)
        let wal_path = dbpath.with_extension("wal");
        let wal_len  = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(wal_len, 0, "WAL must be empty after successful flush");

        // The single .ldg file must exist and contain the data
        let ldg_size = fs::metadata(&dbpath).unwrap().len();
        assert!(
            ldg_size >= file_format::SEGMENTS_BASE_OFFSET,
            ".ldg file must be at least as large as the accounts region"
        );
    }
}

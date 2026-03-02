//! Integration tests for the ledger storage engine.

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use ledger_engine::*;
    use ledger_engine::sstable::*;
    use ledger_engine::models::*;

    // ── Helper: build a minimal valid Transaction ──────────────────────────

    fn make_tx(id: u64, account_id: u64, amount: i64, tx_type: TransactionType, ts: u64) -> Transaction {
        Transaction {
            id,
            account_id,
            amount,
            transaction_type: tx_type,
            timestamp: ts,
            description: format!("Test transaction {}", id),
        }
    }

    // ────────────────────────────────────────────────────────────────────────
    // 1. Header round-trip: write → read → verify every field
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn header_roundtrip() {
        let checksum = SSTableHeader::compute_checksum(&MAGIC, VERSION, 100, 1000, 2000);
        let header = SSTableHeader {
            magic:     MAGIC,
            version:   VERSION,
            row_count: 100,
            min_ts:    1000,
            max_ts:    2000,
            checksum,
            columns:   [ColumnMeta::default(); NUM_COLUMNS],
        };

        let mut buf = Cursor::new(Vec::<u8>::new());
        header.write_to(&mut buf).unwrap();

        // Verify exact byte count
        assert_eq!(buf.get_ref().len(), HEADER_SIZE, "header must be exactly {HEADER_SIZE} bytes");

        // Seek back and deserialise
        buf.set_position(0);
        let parsed = SSTableHeader::read_from(&mut buf).unwrap();

        assert_eq!(parsed.magic,     MAGIC);
        assert_eq!(parsed.version,   VERSION);
        assert_eq!(parsed.row_count, 100);
        assert_eq!(parsed.min_ts,    1000);
        assert_eq!(parsed.max_ts,    2000);
        assert_eq!(parsed.checksum,  checksum);
    }

    // ────────────────────────────────────────────────────────────────────────
    // 2. Corrupted magic bytes → BadMagic error
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn header_bad_magic_detected() {
        let checksum = SSTableHeader::compute_checksum(&MAGIC, VERSION, 1, 0, 0);
        let header = SSTableHeader {
            magic: MAGIC, version: VERSION, row_count: 1,
            min_ts: 0, max_ts: 0, checksum,
            columns: [ColumnMeta::default(); NUM_COLUMNS],
        };

        let mut buf = Cursor::new(Vec::<u8>::new());
        header.write_to(&mut buf).unwrap();

        // Corrupt the magic bytes
        buf.get_mut()[0] = 0xFF;

        buf.set_position(0);
        let result = SSTableHeader::read_from(&mut buf);
        assert!(matches!(result, Err(LedgerError::BadMagic)));
    }

    // ────────────────────────────────────────────────────────────────────────
    // 3. Corrupted header payload → ChecksumMismatch error
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn header_checksum_mismatch_detected() {
        let checksum = SSTableHeader::compute_checksum(&MAGIC, VERSION, 42, 100, 200);
        let header = SSTableHeader {
            magic: MAGIC, version: VERSION, row_count: 42,
            min_ts: 100, max_ts: 200, checksum,
            columns: [ColumnMeta::default(); NUM_COLUMNS],
        };

        let mut buf = Cursor::new(Vec::<u8>::new());
        header.write_to(&mut buf).unwrap();

        // Flip a bit in the row_count field (offset 5)
        buf.get_mut()[5] ^= 0xFF;

        buf.set_position(0);
        let result = SSTableHeader::read_from(&mut buf);
        assert!(matches!(result, Err(LedgerError::ChecksumMismatch { .. })));
    }

    // ────────────────────────────────────────────────────────────────────────
    // 4. Full SSTable write → read; verify column offsets and amount sums
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn sstable_write_and_sum_amounts() {
        let rows = vec![
            make_tx(1, 10, -5000, TransactionType::Debit,  1_000_000),
            make_tx(2, 10,  5000, TransactionType::Credit, 1_000_001),
            make_tx(3, 20, -3000, TransactionType::Debit,  1_000_002),
            make_tx(4, 20,  3000, TransactionType::Credit, 1_000_003),
        ];

        let mut buf = Cursor::new(Vec::<u8>::new());
        let header = SSTableWriter::write(&mut buf, &rows).unwrap();

        // Header sanity
        assert_eq!(header.row_count, 4);
        assert_eq!(header.min_ts,    1_000_000);
        assert_eq!(header.max_ts,    1_000_003);

        // Column offsets must be non-overlapping and in ascending order
        for i in 0..NUM_COLUMNS - 1 {
            let a = &header.columns[i];
            let b = &header.columns[i + 1];
            assert!(
                a.offset + a.length <= b.offset,
                "column {} overlaps column {}",
                i, i + 1
            );
        }

        // Amount column: seek directly and sum
        buf.set_position(0);
        let sum = SSTableReader::sum_amounts(&mut buf, &header).unwrap();
        assert_eq!(sum, 0, "balanced test data must sum to 0");

        // Encoding: transaction_type must be dictionary encoded
        assert_eq!(header.columns[col::TYPE].encoding, Encoding::Dictionary as u8);
        // All others (id, account_id, amount, timestamp) must be None
        assert_eq!(header.columns[col::ID  ].encoding, Encoding::None as u8);
        assert_eq!(header.columns[col::AMT ].encoding, Encoding::None as u8);
    }

    // ────────────────────────────────────────────────────────────────────────
    // 5. Zone-map filtering
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn zone_map_overlap() {
        let rows = vec![
            make_tx(1, 1, 100, TransactionType::Credit, 1_000),
            make_tx(2, 1, -100, TransactionType::Debit, 2_000),
        ];
        let mut buf = Cursor::new(Vec::<u8>::new());
        let header = SSTableWriter::write(&mut buf, &rows).unwrap();

        assert!(SSTableReader::overlaps_time_range(&header, 500,   1_500));  // overlaps
        assert!(SSTableReader::overlaps_time_range(&header, 1_000, 2_000));  // exact match
        assert!(!SSTableReader::overlaps_time_range(&header, 2_001, 3_000)); // after
        assert!(!SSTableReader::overlaps_time_range(&header, 0,     999));   // before
    }

    // ────────────────────────────────────────────────────────────────────────
    // 6. Expense summary aggregation
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn aggregate_by_type_in_range() {
        let rows = vec![
            make_tx(1, 1, -1000, TransactionType::Debit,  100),
            make_tx(2, 1,  1000, TransactionType::Credit, 200),
            make_tx(3, 1, -2000, TransactionType::Debit,  300),
            make_tx(4, 1,  2000, TransactionType::Credit, 400),
            make_tx(5, 1, -9999, TransactionType::Debit,  999), // outside range
        ];

        let mut buf = Cursor::new(Vec::<u8>::new());
        let header = SSTableWriter::write(&mut buf, &rows).unwrap();

        buf.set_position(0);
        let (debits, credits) =
            SSTableReader::aggregate_by_type_in_range(&mut buf, &header, 100, 400).unwrap();

        assert_eq!(debits,  -3000);
        assert_eq!(credits,  3000);
    }

    // ────────────────────────────────────────────────────────────────────────
    // 7. Bitmap index – set / count / matching_rows
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn bitmap_index_operations() {
        use ledger_engine::indexes::BitmapIndex;

        let mut idx = BitmapIndex::new();
        idx.set(0,  TransactionType::Debit);
        idx.set(1,  TransactionType::Credit);
        idx.set(63, TransactionType::Debit);
        idx.set(64, TransactionType::Debit);   // second word

        assert_eq!(idx.count(TransactionType::Debit),  3);
        assert_eq!(idx.count(TransactionType::Credit), 1);

        let debit_rows = idx.matching_rows(TransactionType::Debit);
        assert_eq!(debit_rows, vec![0, 63, 64]);
    }

    // ────────────────────────────────────────────────────────────────────────
    // 8. End-to-end engine: append → validate → summarise
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn engine_end_to_end() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path()).unwrap();

        let asset_acct   = engine.create_account("Cash",    AccountType::Asset).unwrap();
        let expense_acct = engine.create_account("Expense", AccountType::Expense).unwrap();

        // Double-entry: debit cash -500, credit expense +500 (net = 0)
        engine.append_transaction(asset_acct,   -500, TransactionType::Debit,  "Office supplies").unwrap();
        engine.append_transaction(expense_acct,  500, TransactionType::Credit, "Office supplies").unwrap();

        // Another balanced pair
        engine.append_transaction(asset_acct,   -1000, TransactionType::Debit,  "Rent").unwrap();
        engine.append_transaction(expense_acct,  1000, TransactionType::Credit, "Rent").unwrap();

        // Ledger must balance in the MemTable
        engine.validate_ledger().unwrap();

        // Force flush to SSTable then validate again (hits the disk path)
        engine.force_flush().unwrap();
        engine.validate_ledger().unwrap();

        // Expense summary over all time
        let summary = engine.get_expense_summary(0, u64::MAX).unwrap();
        assert_eq!(summary.total_debits,  -1500);
        assert_eq!(summary.total_credits,  1500);
        assert_eq!(summary.net, 0);
    }

    // ────────────────────────────────────────────────────────────────────────
    // 9. Imbalanced ledger is caught
    // ────────────────────────────────────────────────────────────────────────
    #[test]
    fn engine_detects_imbalance() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = LedgerEngine::open(tmp.path()).unwrap();

        let acct = engine.create_account("Cash", AccountType::Asset).unwrap();
        engine.append_transaction(acct, -500, TransactionType::Debit, "Orphaned debit").unwrap();

        // Only one side of the entry → net = -500
        let result = engine.validate_ledger();
        assert!(matches!(result, Err(LedgerError::ImbalancedLedger { net: -500 })));
    }
}

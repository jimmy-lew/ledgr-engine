# Ledgr Engine - Database Specification

## Overview

**Ledgr Engine** is a single-file, columnar storage engine designed for double-entry financial ledgers. It implements a write-ahead log (WAL), append-only storage with periodic flushing to columnar segments, sparse timestamp indexing for efficient range queries, and a tamper-evident hash chain for data integrity verification.

This document provides a detailed technical specification of the database architecture, file format, and core components.

---

## 1. Architecture Overview

### 1.1 High-Level Design

The engine follows an architecture similar to log-structured merge (LSM) trees, with the following components:

1. **Write-Ahead Log (WAL)** - A separate `.wal` file that ensures durability of uncommitted writes
2. **MemTable** - An in-memory buffer that accumulates transactions before flushing
3. **Storage** - Columnar segments stored in a single `.ldg` file
4. **Sparse Index** - Timestamp-based index for efficient range queries
5. **Hash Chain** - SHA-256 based tamper-evident linkage across all transactions

### 1.2 File Structure

```
┌─────────────────────────────────────────────────────────────┐
│  ledger.ldg                                                 │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ FILE HEADER (512 bytes)                               │  │
│  │ - Magic, version, pointers                            │  │
│  │ - Hash chain anchors (genesis_hash, last_tx_hash)     │  │
│  │ - CRC32 checksum                                      │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ ACCOUNTS REGION (131,584 bytes)                       │  │
│  │ 1,024 fixed slots × 128 bytes each                    │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ SEGMENT 0 (256-byte header + columnar data)           │  │ 
│  │ SEGMENT 1 ... SEGMENT N                               │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ SPARSE TIMESTAMP INDEX                                │  │
│  │ One entry per 64 rows                                 │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  ledger.wal  (Write-Ahead Log)                              │
│  - Atomic journal entry records with CRC32                  │
│  - Truncated after each successful flush to .ldg            │
└─────────────────────────────────────────────────────────────┘
```

### 1.3 Data Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Application │────▶│     WAL      │────▶│   MemTable   │
└──────────────┘     └──────────────┘     └──────────────┘
                                               │ flush
                                               ▼
                                          ┌──────────────┐
                                          │   Storage    │
                                          │  (Segments)  │
                                          └──────────────┘
```

---

## 2. Data Model

### 2.1 Core Entities

#### Account

Represents a ledger account (e.g., Cash, Revenue, Accounts Payable).

| Field       | Type     | Description                                    |
|-------------|----------|------------------------------------------------|
| id          | u64      | Unique identifier (1-based)                    |
| name        | String   | Account name (up to 64 bytes UTF-8)           |
| kind        | AccountType | Type: Asset, Liability, Equity, Revenue, Expense |
| created_at  | u64      | Unix timestamp of creation                    |
| balance     | i64      | Running signed balance in cents               |

#### AccountType Enum

| Value | Name      | Debit Effect | Credit Effect |
|-------|-----------|---------------|---------------|
| 0     | Asset     | Increase      | Decrease      |
| 1     | Liability | Decrease      | Increase      |
| 2     | Equity    | Decrease      | Increase      |
| 3     | Revenue   | Decrease      | Increase      |
| 4     | Expense   | Increase      | Decrease      |

#### Direction (TransactionType)

| Value | Name   | Sign | Description           |
|-------|--------|------|-----------------------|
| 0     | Debit  | -1   | Debit leg (negative) |
| 1     | Credit | +1   | Credit leg (positive) |

#### Leg

A single component of a journal entry.

| Field       | Type     | Description                          |
|-------------|----------|--------------------------------------|
| account_id  | u64      | Target account                      |
| amount      | u64      | Positive magnitude in cents         |
| direction   | Direction | Debit or Credit                     |

#### JournalEntry

A complete double-entry transaction containing multiple legs.

| Field       | Type     | Description                                    |
|-------------|----------|------------------------------------------------|
| description | String   | Human-readable memo                           |
| legs        | Vec<Leg> | Must contain at least 2 legs                  |

**Invariant**: `∑ leg.signed_amount() == 0` (debits == credits)

#### Transaction

Immutable storage row representing one leg on disk.

| Field            | Type     | Description                                              |
|------------------|----------|----------------------------------------------------------|
| id               | u64      | Globally unique leg ID (monotonically increasing)        |
| journal_entry_id | u64      | Groups all legs of the same journal entry               |
| account_id       | u64      | Target account                                           |
| amount           | i64      | Signed cents: negative for debits, positive for credits |
| transaction_type | Direction | Debit or Credit                                         |
| timestamp        | u64      | Unix timestamp                                          |
| description      | String   | Copy of journal entry description                      |
| tx_hash          | [u8; 32] | SHA-256 hash chaining to previous transaction          |

---

## 3. File Format Specification

All multi-byte integers use **little-endian** byte order.

### 3.1 File Header (512 bytes at offset 0)

| Offset | Size | Field                        | Type   |
|--------|------|------------------------------|--------|
| 0x000  | 4    | Magic bytes                 | [u8;4] "LDGR" |
| 0x004  | 1    | Version                     | u8     (0x01) |
| 0x005  | 3    | Reserved (zero)              | [u8;3] |
| 0x008  | 8    | accounts_count              | u64    |
| 0x010  | 8    | segment_count               | u64    |
| 0x018  | 8    | segments_end_offset         | u64    |
| 0x020  | 8    | sparse_index_count          | u64    |
| 0x028  | 8    | total_tx_count              | u64    |
| 0x030  | 32   | genesis_hash                | [u8;32] |
| 0x050  | 32   | last_tx_hash                | [u8;32] |
| 0x070  | 4    | header_crc32                | u32    |
| 0x074  | 8    | sparse_checkpoint_offset    | u64    |
| 0x07C  | 8    | sparse_checkpoint_seg_count | u64    |
| 0x084  | 376  | Padding (zeroes)            | [u8;376] |

**Total**: 512 bytes (0x200)

### 3.2 Accounts Region (offsets 512 - 131,583)

1,024 fixed slots × 128 bytes each.

| Offset | Size | Field        | Type    |
|--------|------|--------------|---------|
| 0x00   | 1    | is_active   | u8      (0 = empty, 1 = occupied) |
| 0x01   | 8    | id           | u64     |
| 0x09   | 1    | kind         | u8      (AccountType) |
| 0x0A   | 8    | created_at   | u64     |
| 0x12   | 8    | balance      | i64     (cents) |
| 0x1A   | 2    | name_len     | u16     |
| 0x1C   | 64   | name         | [u8;64] UTF-8, null-padded |
| 0x5C   | 36   | Padding      | [u8;36] |

**Total**: 128 bytes per slot

### 3.3 Segment Header (256 bytes, precedes column data)

| Offset | Size | Field                  | Type              |
|--------|------|------------------------|-------------------|
| 0x00   | 4    | Magic                  | [u8;4] "SEGM"    |
| 0x04   | 8    | row_count              | u64               |
| 0x0C   | 8    | min_ts                 | u64 (zone map lo) |
| 0x14   | 8    | max_ts                 | u64 (zone map hi) |
| 0x1C   | 8    | first_row_global_idx   | u64               |
| 0x24   | 64   | col_offsets[8]         | [u64;8]           |
| 0x64   | 64   | col_lengths[8]         | [u64;8]           |
| 0xA4   | 8    | col_encodings[8]       | [u8;8]            |
| 0xAC   | 4    | data_crc32             | u32               |
| 0xB0   | 80   | Padding                | [u8;80]           |

**Total**: 256 bytes

### 3.4 Column Layout (per segment)

| Index | Name             | Encoding     | Element Size         |
|-------|------------------|--------------|---------------------|
| 0     | id               | None         | 8 bytes             |
| 1     | account_id       | None         | 8 bytes             |
| 2     | amount           | None         | 8 bytes (signed)   |
| 3     | transaction_type | Dictionary   | 1 byte code        |
| 4     | timestamp        | None         | 8 bytes             |
| 5     | description      | None         | 4+N bytes (length-prefixed) |
| 6     | tx_hash          | None         | 32 bytes            |
| 7     | journal_entry_id | None         | 8 bytes             |

### 3.5 Sparse Timestamp Index

Stored at `segments_end_offset` after all segments.

| Field            | Size    | Type |
|------------------|---------|------|
| entry_count      | 8 bytes | u64  |
| entries[]        | 16 bytes each | (timestamp: u64, global_row_idx: u64) |

**SPARSE_FACTOR**: One entry per 64 rows

### 3.6 WAL Record Format

| Offset | Size   | Field           |
|--------|--------|-----------------|
| 0x00   | 1      | Record type (0x02 = JournalEntry) |
| 0x01   | 4      | CRC32 over payload |
| 0x05   | 4      | Payload length (bytes) |
| 0x09   | N      | Payload |

**Payload Layout**:
- 8 bytes: journal_entry_id (u64)
- 8 bytes: timestamp (u64)
- 4 bytes: description length (u32)
- M bytes: description (UTF-8)
- 2 bytes: leg_count (u16)
- Per leg (repeated):
  - 8 bytes: leg_id (u64)
  - 8 bytes: account_id (u64)
  - 8 bytes: amount (i64, signed)
  - 1 byte: direction (u8)
  - 32 bytes: tx_hash ([u8;32])

---

## 4. Core Components

### 4.1 Write-Ahead Log (WAL)

**File**: `ledger.wal`

**Purpose**: Ensures durability of uncommitted transactions. All legs of a journal entry are written as a single atomic record.

**Key Properties**:
- Atomic journal entry records (all legs succeed or all fail)
- CRC32 checksum for corruption detection
- Truncated after successful flush to .ldg file

**Replay**: On startup, the WAL is replayed to recover any uncommitted transactions.

### 4.2 MemTable

**Purpose**: In-memory buffer accumulating transactions before flushing to disk.

**Properties**:
- Sorted by (timestamp, id) before flush
- Auto-flush when size exceeds 8 MB threshold
- Holds both flushed and unflushed data during queries

### 4.3 Storage Manager

**File**: `src/storage.rs`

**Responsibilities**:
- Manages the single `.ldg` file
- Handles segment flushing from MemTable
- Maintains sparse index with checkpointing every 100 segments
- Provides columnar read operations

**Flush Sequence** (atomic from reader's perspective):
1. Write new segment header + column data at `segments_end_offset`
2. Compute new sparse index entries
3. Write sparse index after new segment
4. Truncate file to remove any orphaned bytes
5. Rewrite file header with updated pointers
6. fsync

### 4.4 Sparse Index

**Purpose**: Enables O(log n) timestamp-based range queries.

**Implementation**:
- One entry per 64 rows (SPARSE_FACTOR)
- Binary search to find starting row for a timestamp range
- Checkpointed to disk every 100 segment flushes

### 4.5 Hash Chain

**Purpose**: Tamper-evident transaction history.

**Algorithm**:
```
tx_hash[n] = SHA-256(
    id(8) || account_id(8) || amount(8) || tx_type(1)
    || timestamp(8) || desc_len(4) || desc_bytes
    || tx_hash[n-1]    // chain link
)
```

**Properties**:
- First transaction uses genesis_hash = [0u8; 32]
- Modifying any historical row breaks all subsequent hashes
- File header stores both genesis_hash and last_tx_hash for verification

### 4.6 Double-Entry Enforcement

The engine enforces the fundamental accounting equation at write time:

```
∑ leg.signed_amount() == 0
```

Where `signed_amount() = direction.sign() * amount` (debits negative, credits positive).

**Validation**: `JournalEntry::validate()` is called before any I/O. If validation fails, nothing is written.

---

## 5. Query Operations

### 5.1 Expense Summary Query

Retrieves aggregate debits/credits for a timestamp range.

**Optimization layers**:
1. **Sparse index** binary search → O(log n) first-candidate row
2. **Zone-map** per-segment (min_ts, max_ts) → skip non-overlapping segments
3. **Columnar read** of only timestamp, amount, tx_type columns
4. **SIMD split-sum** for debit/credit totals

### 5.2 Ledger Validation

Two-phase integrity verification:

**Phase 1 - SIMD Balance Scan**:
- Loads amount column from all segments + MemTable
- Computes ∑ all amounts (must equal 0)
- Cross-checks against ∑ account.balance

**Phase 2 - Hash Chain Walk**:
- Replays SHA-256 for every flushed leg in global order
- Verifies stored tx_hash matches computed hash
- Any mismatch indicates tampering

---

## 6. Concurrency Model

- Uses `parking_lot::RwLock` for thread-safe access
- Single writer at a time (write lock)
- Multiple readers allowed (read lock)

---

## 7. Dependencies

| Crate       | Version | Purpose                           |
|-------------|---------|-----------------------------------|
| crc32fast   | 1.4     | CRC32 checksums                  |
| byteorder   | 1.5     | Little-endian byte serialization |
| sha2        | 0.10    | SHA-256 for hash chain           |
| hex         | 0.4     | Debug display of hashes          |
| thiserror   | 1.0     | Error types                      |
| parking_lot | 0.12    | Fast RwLock/Mutex                |

---

## 8. Constants

| Constant                | Value                    |
|-------------------------|--------------------------|
| FILE_HEADER_SIZE        | 512 bytes                |
| ACCOUNT_RECORD_SIZE    | 128 bytes                |
| MAX_ACCOUNTS           | 1,024                    |
| ACCOUNTS_REGION_SIZE   | 131,072 bytes           |
| SEGMENTS_BASE_OFFSET   | 131,584 bytes            |
| SEGMENT_HEADER_SIZE    | 256 bytes                |
| NUM_TX_COLUMNS         | 8                        |
| SPARSE_FACTOR          | 64                       |
| FLUSH_THRESHOLD        | 8 MB                     |
| CHECKPOINT_INTERVAL    | 100 segments             |

---

## 9. Design Principles

1. **Single File Storage**: All data (except WAL) in one `.ldg` file for simplicity
2. **Columnar Layout**: Enables efficient analytical queries and SIMD operations
3. **Append-Only Segments**: Never modified after creation - enables easy backup/integrity checking
4. **Tamper Evidence**: Hash chain makes any data modification immediately detectable
5. **Accounting Correctness**: Double-entry invariant enforced at write time
6. **Crash Recovery**: WAL ensures no data loss; sparse index checkpointing speeds up recovery

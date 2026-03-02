//! # Tamper-Evident Hash Chain
//!
//! Every transaction carries a `tx_hash` field computed as:
//!
//! ```text
//! tx_hash[n] = SHA-256(
//!     id(8)  ‖  account_id(8)  ‖  amount(8)  ‖  tx_type(1)
//!   ‖ timestamp(8)  ‖  desc_len(4)  ‖  desc_bytes
//!   ‖ tx_hash[n-1]                               ← chain link
//! )
//! ```
//!
//! The very first transaction uses `genesis_hash = [0u8; 32]` as its
//! previous-hash input.  The file header stores both `genesis_hash` and
//! `last_tx_hash` so that:
//!
//! - **Appends** know which previous hash to chain from.
//! - **Integrity verification** can walk the entire chain and confirm that
//!   no historical row was modified, reordered, or deleted.
//!
//! Modifying any field of any past transaction changes its hash, which
//! cascades to break every subsequent hash in the chain — making tampering
//! immediately detectable.

use crate::models::Transaction;
use sha2::{Digest, Sha256};

/// Compute the SHA-256 hash for a transaction given the previous hash.
///
/// The input is a deterministic serialisation of all immutable fields
/// followed by the 32-byte previous hash.
pub fn compute_tx_hash(tx: &Transaction, prev_hash: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();

    // Immutable transaction fields (big-endian would also work; LE is consistent
    // with the rest of the file format).
    h.update(tx.id.to_le_bytes());
    h.update(tx.account_id.to_le_bytes());
    h.update(tx.amount.to_le_bytes());
    h.update([tx.transaction_type as u8]);
    h.update(tx.timestamp.to_le_bytes());

    let desc_bytes = tx.description.as_bytes();
    h.update((desc_bytes.len() as u32).to_le_bytes());
    h.update(desc_bytes);

    // Chain link – binds this hash to the full history before it.
    h.update(prev_hash);

    h.finalize().into()
}

/// Verify that a stored hash matches what we would compute.
///
/// Returns `Ok(())` on match, `Err(HashChainViolation)` otherwise.
pub fn verify_tx_hash(
    tx: &Transaction,
    prev_hash: &[u8; 32],
    global_row: u64,
) -> crate::error::Result<()> {
    let expected = compute_tx_hash(tx, prev_hash);
    if expected != tx.tx_hash {
        return Err(crate::error::LedgerError::HashChainViolation {
            row: global_row,
            expected: hex::encode(expected),
            actual: hex::encode(tx.tx_hash),
        });
    }
    Ok(())
}

/// Convenience: compute genesis hash (the artificial "prev" for row 0).
/// We use the SHA-256 of a fixed domain-separation string so the genesis
/// is deterministic and meaningful rather than just all-zeros.
pub fn genesis_hash() -> [u8; 32] {
    // Returns [0u8; 32] — kept explicit so callers understand the convention.
    [0u8; 32]
}

// ──────────────────────────────────────────────────────────────────────────────
// Chain state tracker
// ──────────────────────────────────────────────────────────────────────────────

/// Maintains the "tip" of the hash chain so that successive `append` calls
/// each receive the correct `prev_hash` without re-reading the file.
#[derive(Debug, Clone)]
pub struct ChainTip {
    pub last_hash: [u8; 32],
}

impl ChainTip {
    pub fn new(initial: [u8; 32]) -> Self {
        Self { last_hash: initial }
    }

    /// Compute `tx_hash` for `tx`, advance the tip, and return the hash.
    pub fn advance(&mut self, tx: &Transaction) -> [u8; 32] {
        let h = compute_tx_hash(tx, &self.last_hash);
        self.last_hash = h;
        h
    }
}

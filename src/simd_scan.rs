//! # Vectorized SIMD Balance Scan
//!
//! Implements the balance validation described in the spec:
//!
//! ```text
//! 1. Load the `amount` column (i64 array) and the `tx_type` column (u8 array).
//! 2. SIMD-sum them in parallel.
//! 3. Assert ∑Debits + ∑Credits = 0.
//! ```
//!
//! ## Hardware Dispatch
//!
//! We use Rust's `std::arch` for direct x86_64 AVX2 or aarch64 NEON intrinsics.
//! A **runtime CPU feature check** (`is_x86_feature_detected!` / `is_aarch64_feature_detected!`)
//! selects the fast path at startup; the scalar fallback is always present for other
//! architectures (WASM, etc.) and for test environments.
//!
//! ### Why AVX2/NEON for i64 sums?
//!
//! - AVX2: 256-bit register = 4 × i64 lanes, theoretical 4× throughput
//! - NEON: 128-bit register = 2 × i64 lanes, theoretical 2× throughput
//! In practice, memory bandwidth is often the bottleneck.

// ──────────────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────────────

/// Sum every element of `amounts` using the fastest available path.
///
/// Dispatches to AVX2 SIMD on x86_64 or NEON on aarch64 when the CPU supports it,
/// otherwise falls back to a hand-unrolled scalar loop.
pub fn simd_sum_i64(amounts: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2_sum_i64(amounts) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::is_aarch64_feature_detected;
        if is_aarch64_feature_detected!("neon") {
            return unsafe { neon_sum_i64(amounts) };
        }
    }
    scalar_sum_i64(amounts)
}

/// Split-sum: returns `(total_debits, total_credits)` where:
/// - `total_debits`  = sum of `amounts[i]` for rows where `tx_types[i] == 0`
/// - `total_credits` = sum of `amounts[i]` for rows where `tx_types[i] == 1`
///
/// Uses a manually-unrolled 4-wide loop that the compiler's auto-vectoriser
/// can promote to SIMD, plus the hardware-explicit AVX2/NEON path when available.
pub fn simd_sum_by_type(amounts: &[i64], tx_types: &[u8]) -> (i64, i64) {
    assert_eq!(
        amounts.len(),
        tx_types.len(),
        "amount and tx_type column lengths must match"
    );

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { avx2_sum_by_type(amounts, tx_types) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::is_aarch64_feature_detected;
        if is_aarch64_feature_detected!("neon") {
            return unsafe { neon_sum_by_type(amounts, tx_types) };
        }
    }
    scalar_sum_by_type(amounts, tx_types)
}

// ──────────────────────────────────────────────────────────────────────────────
// Scalar fallback  (always compiled; used on non-x86 and as reference)
// ──────────────────────────────────────────────────────────────────────────────

/// Hand-unrolled 4-wide sum.  The LLVM auto-vectoriser often promotes this
/// to SIMD even without explicit intrinsics.
fn scalar_sum_i64(data: &[i64]) -> i64 {
    let mut acc0 = 0i64;
    let mut acc1 = 0i64;
    let mut acc2 = 0i64;
    let mut acc3 = 0i64;

    let chunks = data.chunks_exact(4);
    let rem = chunks.remainder();

    for c in chunks {
        acc0 = acc0.wrapping_add(c[0]);
        acc1 = acc1.wrapping_add(c[1]);
        acc2 = acc2.wrapping_add(c[2]);
        acc3 = acc3.wrapping_add(c[3]);
    }

    let mut total = acc0
        .wrapping_add(acc1)
        .wrapping_add(acc2)
        .wrapping_add(acc3);
    for &v in rem {
        total = total.wrapping_add(v);
    }
    total
}

fn scalar_sum_by_type(amounts: &[i64], tx_types: &[u8]) -> (i64, i64) {
    let mut debit_acc = [0i64; 4];
    let mut credit_acc = [0i64; 4];

    let amt_chunks = amounts.chunks_exact(4);
    let type_chunks = tx_types.chunks_exact(4);
    let amt_rem = amt_chunks.remainder();
    let type_rem = type_chunks.remainder();

    for (a, t) in amt_chunks.zip(type_chunks) {
        // Branchless: multiply by 0 or 1 to route to the right accumulator.
        // t[i] == 0 → debit; t[i] == 1 → credit.
        for lane in 0..4 {
            let is_credit = (t[lane] & 1) as i64;
            let is_debit = 1 - is_credit;
            debit_acc[lane] = debit_acc[lane].wrapping_add(a[lane] * is_debit);
            credit_acc[lane] = credit_acc[lane].wrapping_add(a[lane] * is_credit);
        }
    }

    let mut debits = debit_acc.iter().fold(0i64, |s, &x| s.wrapping_add(x));
    let mut credits = credit_acc.iter().fold(0i64, |s, &x| s.wrapping_add(x));

    for (&a, &t) in amt_rem.iter().zip(type_rem) {
        if t == 0 {
            debits = debits.wrapping_add(a);
        } else {
            credits = credits.wrapping_add(a);
        }
    }

    (debits, credits)
}

// ──────────────────────────────────────────────────────────────────────────────
// AVX2 fast paths (x86_64 only)
// ──────────────────────────────────────────────────────────────────────────────

/// # AVX2 i64 horizontal sum
///
/// ## Register layout (256-bit = 4 × i64):
/// ```text
/// acc   = [ sum_lane0 | sum_lane1 | sum_lane2 | sum_lane3 ]
///            ^q0          ^q1          ^q2          ^q3
///
/// Horizontal reduction:
///   hi128 = acc[q2|q3]     (vextracti128)
///   lo128 = acc[q0|q1]     (vcastsi256_si128)
///   sum128 = hi128 + lo128  (paddq)
///   result = sum128[q0] + sum128[q1]
/// ```
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_sum_i64(data: &[i64]) -> i64 {
    use std::arch::x86_64::*;

    let mut acc = _mm256_setzero_si256();

    // Main loop: 4 × i64 per iteration (32 bytes)
    let chunks = data.chunks_exact(4);
    let rem = chunks.remainder();

    for chunk in chunks {
        // Unaligned load (safe because we have no alignment guarantees)
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        acc = _mm256_add_epi64(acc, v);
    }

    // Horizontal reduction: fold 4 lanes into 1 scalar i64
    let hi128 = _mm256_extracti128_si256::<1>(acc);
    let lo128 = _mm256_castsi256_si128(acc);
    let sum128 = _mm_add_epi64(hi128, lo128);

    // Extract both 64-bit lanes and add them
    let lo64: i64 = _mm_cvtsi128_si64(sum128);
    // Shift the 128-bit register right by 8 bytes to get the high lane
    let sum128_shifted = _mm_srli_si128::<8>(sum128);
    let hi64: i64 = _mm_cvtsi128_si64(sum128_shifted);

    let mut total = lo64.wrapping_add(hi64);

    for &v in rem {
        total = total.wrapping_add(v);
    }
    total
}

/// # AVX2 split sum by transaction type
///
/// Strategy: compute debit and credit masks from `tx_type` bytes,
/// multiply into the amount register, accumulate separately.
///
/// Because AVX2 has no native i64 multiply, we use a 4-wide scalar unroll
/// but with 256-bit gather for the amount load, giving us better cache line
/// utilisation than pure scalar.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_sum_by_type(amounts: &[i64], tx_types: &[u8]) -> (i64, i64) {
    use std::arch::x86_64::*;

    // We process 4 elements per outer iteration, using AVX2 for the amount
    // load and accumulation, and scalar logic for the type masking.
    let mut debit_acc = _mm256_setzero_si256();
    let mut credit_acc = _mm256_setzero_si256();

    let amt_chunks = amounts.chunks_exact(4);
    let type_chunks = tx_types.chunks_exact(4);
    let amt_rem = amt_chunks.remainder();
    let type_rem = type_chunks.remainder();

    for (a, t) in amt_chunks.zip(type_chunks) {
        let v = _mm256_loadu_si256(a.as_ptr() as *const __m256i);

        // Build 256-bit masks: -1 (0xFFFF…) where condition is true, 0 otherwise.
        // We broadcast each type byte to a 64-bit mask using set_epi64x.
        let types = _mm_loadu_si128(t.as_ptr() as *const __m128i);
        let cmp = _mm_cmpeq_epi8(types, _mm_setzero_si128());
        let debit_mask = _mm256_cvtepi8_epi64(cmp);
        let credit_mask = _mm256_xor_si256(debit_mask, _mm256_set1_epi64x(-1));

        // AND amount with mask: zeroes out amounts for non-matching rows.
        let debit_lanes = _mm256_and_si256(v, debit_mask);
        let credit_lanes = _mm256_and_si256(v, credit_mask);

        debit_acc = _mm256_add_epi64(debit_acc, debit_lanes);
        credit_acc = _mm256_add_epi64(credit_acc, credit_lanes);
    }

    // Horizontal reduce both accumulators
    fn reduce256(acc: std::arch::x86_64::__m256i) -> i64 {
        unsafe {
            use std::arch::x86_64::*;
            let hi128 = _mm256_extracti128_si256::<1>(acc);
            let lo128 = _mm256_castsi256_si128(acc);
            let sum128 = _mm_add_epi64(hi128, lo128);
            let lo64 = _mm_cvtsi128_si64(sum128);
            let hi_reg = _mm_srli_si128::<8>(sum128);
            let hi64 = _mm_cvtsi128_si64(hi_reg);
            lo64.wrapping_add(hi64)
        }
    }

    let mut debits = reduce256(debit_acc);
    let mut credits = reduce256(credit_acc);

    for (&a, &t) in amt_rem.iter().zip(type_rem) {
        if t == 0 {
            debits = debits.wrapping_add(a);
        } else {
            credits = credits.wrapping_add(a);
        }
    }

    (debits, credits)
}

// ──────────────────────────────────────────────────────────────────────────────
// NEON fast paths (aarch64 only)
// ──────────────────────────────────────────────────────────────────────────────

/// # NEON i64 horizontal sum
///
/// ## Register layout (128-bit = 2 × i64):
/// ```text
/// acc   = [ sum_lane0 | sum_lane1 ]
/// Horizontal reduction:
///   result = sum_lane0 + sum_lane1
/// ```
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn neon_sum_i64(data: &[i64]) -> i64 {
    use std::arch::aarch64::*;

    let mut acc = vdupq_n_s64(0);

    let chunks = data.chunks_exact(2);
    let rem = chunks.remainder();

    for chunk in chunks {
        let v = vld1q_s64(chunk.as_ptr());
        acc = vaddq_s64(acc, v);
    }

    let mut total = vgetq_lane_s64(acc, 0).wrapping_add(vgetq_lane_s64(acc, 1));

    for &v in rem {
        total = total.wrapping_add(v);
    }
    total
}

/// # NEON split sum by transaction type
///
/// Uses 128-bit NEON to process 2 elements per iteration.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn neon_sum_by_type(amounts: &[i64], tx_types: &[u8]) -> (i64, i64) {
    use std::arch::aarch64::*;

    let mut debit_acc = vdupq_n_s64(0);
    let mut credit_acc = vdupq_n_s64(0);

    let amt_chunks = amounts.chunks_exact(2);
    let type_chunks = tx_types.chunks_exact(2);
    let amt_rem = amt_chunks.remainder();
    let type_rem = type_chunks.remainder();

    for (a, t) in amt_chunks.zip(type_chunks) {
        let v = vld1q_s64(a.as_ptr());
        let a0 = vgetq_lane_s64(v, 0);
        let a1 = vgetq_lane_s64(v, 1);

        let (d0, c0) = if t[0] == 0 { (a0, 0) } else { (0, a0) };
        let (d1, c1) = if t[1] == 0 { (a1, 0) } else { (0, a1) };

        let debit_lanes = vdupq_n_s64(d0 + d1);
        let credit_lanes = vdupq_n_s64(c0 + c1);

        debit_acc = vaddq_s64(debit_acc, debit_lanes);
        credit_acc = vaddq_s64(credit_acc, credit_lanes);
    }

    let mut debits = vgetq_lane_s64(debit_acc, 0).wrapping_add(vgetq_lane_s64(debit_acc, 1));
    let mut credits = vgetq_lane_s64(credit_acc, 0).wrapping_add(vgetq_lane_s64(credit_acc, 1));

    for (&a, &t) in amt_rem.iter().zip(type_rem) {
        if t == 0 {
            debits = debits.wrapping_add(a);
        } else {
            credits = credits.wrapping_add(a);
        }
    }

    (debits, credits)
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod simd_tests {
    use super::*;

    #[test]
    fn scalar_sum_empty() {
        assert_eq!(scalar_sum_i64(&[]), 0);
    }

    #[test]
    fn scalar_sum_basic() {
        let data: Vec<i64> = (-100..=100).collect(); // sum = 0
        assert_eq!(scalar_sum_i64(&data), 0);
    }

    #[test]
    fn scalar_sum_unaligned_remainder() {
        // 13 elements – not a multiple of 4
        let data: Vec<i64> = (1..=13).map(|x| x as i64).collect();
        let expected: i64 = (1..=13).sum();
        assert_eq!(scalar_sum_i64(&data), expected);
    }

    #[test]
    fn simd_sum_matches_scalar() {
        let data: Vec<i64> = (0..1_000).map(|x| (x % 200) - 100).collect();
        let scalar = scalar_sum_i64(&data);
        let simd = simd_sum_i64(&data);
        assert_eq!(
            scalar, simd,
            "SIMD sum must match scalar sum – hardware dispatch issue"
        );
    }

    #[test]
    fn split_sum_correct() {
        let amounts: Vec<i64> = vec![-100, 100, -200, 200, -50, 50];
        let tx_types: Vec<u8> = vec![0, 1, 0, 1, 0, 1];
        let (d, c) = scalar_sum_by_type(&amounts, &tx_types);
        assert_eq!(d, -350);
        assert_eq!(c, 350);
        assert_eq!(d + c, 0, "balanced ledger");
    }

    #[test]
    fn simd_split_sum_matches_scalar() {
        let n = 1_000usize;
        let amounts: Vec<i64> = (0..n)
            .map(|i| if i % 2 == 0 { -(i as i64) } else { i as i64 })
            .collect();
        let tx_types: Vec<u8> = (0..n).map(|i| (i % 2) as u8).collect();

        let (sd, sc) = scalar_sum_by_type(&amounts, &tx_types);
        let (vd, vc) = simd_sum_by_type(&amounts, &tx_types);

        assert_eq!(sd, vd, "debit totals must match");
        assert_eq!(sc, vc, "credit totals must match");
    }
}

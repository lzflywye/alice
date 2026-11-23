pub mod gf256;

use common::DbError;
use gf256::{add, inv, mul, pow};

/// Systematic Reed-Solomon Erasure Coder over GF(2^8).
/// Splits payload into `data_shards` (k) + `parity_shards` (m), tolerating up to `m` lost shards.
#[derive(Debug, Clone)]
pub struct ReedSolomon {
    data_shards: usize,
    parity_shards: usize,
    /// Systematic encoding matrix of size `(k + m) x k` stored row-major.
    /// Top `k x k` is identity; bottom `m x k` is parity generator.
    matrix: Vec<u8>,
}

impl ReedSolomon {
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, DbError> {
        let total = data_shards + parity_shards;
        if data_shards == 0 || parity_shards == 0 || total > 255 {
            return Err(DbError::CorruptedData(
                "Invalid shard counts: require k >= 1, m >= 1, k + m <= 255",
            ));
        }

        // Build Vandermonde matrix V of size (total x k) where V[r][c] = r^c
        let mut vandermonde = vec![0u8; total * data_shards];
        for r in 0..total {
            for c in 0..data_shards {
                vandermonde[r * data_shards + c] = pow(r as u8, c);
            }
        }

        // Convert to systematic form: M = V * (V_top)^(-1)
        let top = &vandermonde[..data_shards * data_shards];
        let top_inv = invert_matrix(top, data_shards)?;
        let matrix = mat_mul(&vandermonde, total, data_shards, &top_inv, data_shards);

        Ok(Self {
            data_shards,
            parity_shards,
            matrix,
        })
    }

    pub fn data_shards(&self) -> usize {
        self.data_shards
    }

    pub fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Encodes `data` into `k + m` equal-sized shards (zero-padded to multiple of `k`).
    /// Returns `(shards, original_byte_len)`.
    pub fn encode(&self, data: &[u8]) -> (Vec<Vec<u8>>, usize) {
        let orig_len = data.len();
        let shard_len = orig_len.div_ceil(self.data_shards).max(1);
        let total = self.total_shards();

        let mut shards = vec![vec![0u8; shard_len]; total];

        // Copy raw data into the first k systematic shards
        for (i, shard) in shards.iter_mut().take(self.data_shards).enumerate() {
            let start = i * shard_len;
            if start < orig_len {
                let end = (start + shard_len).min(orig_len);
                shard[..end - start].copy_from_slice(&data[start..end]);
            }
        }

        // Compute m parity shards via matrix-vector product over GF(2^8)
        for p in 0..self.parity_shards {
            let row_idx = self.data_shards + p;
            let row = &self.matrix[row_idx * self.data_shards..(row_idx + 1) * self.data_shards];
            for c in 0..self.data_shards {
                let coeff = row[c];
                if coeff == 0 {
                    continue;
                }
                for byte_idx in 0..shard_len {
                    let src_byte = shards[c][byte_idx];
                    shards[row_idx][byte_idx] =
                        add(shards[row_idx][byte_idx], mul(coeff, src_byte));
                }
            }
        }

        (shards, orig_len)
    }

    /// Reconstructs missing shards in-place (if at least `k` shards are `Some`) and returns original data.
    pub fn reconstruct(
        &self,
        shards: &mut [Option<Vec<u8>>],
        orig_len: usize,
    ) -> Result<Vec<u8>, DbError> {
        let total = self.total_shards();
        if shards.len() != total {
            return Err(DbError::CorruptedData("Shard vector length mismatch"));
        }

        // Collect indices of available shards (need at least k)
        let mut present_indices = Vec::with_capacity(self.data_shards);
        let mut shard_len: Option<usize> = None;
        for (idx, s) in shards.iter().enumerate() {
            if let Some(buf) = s {
                if let Some(expected) = shard_len {
                    if buf.len() != expected {
                        return Err(DbError::CorruptedData("Inconsistent shard sizes"));
                    }
                } else {
                    shard_len = Some(buf.len());
                }
                if present_indices.len() < self.data_shards {
                    present_indices.push(idx);
                }
            }
        }

        if present_indices.len() < self.data_shards {
            return Err(DbError::CorruptedData(
                "Too many missing shards to reconstruct data",
            ));
        }
        let shard_len = shard_len.unwrap_or(0);

        // If any of the first k data shards is missing, invert submatrix of available rows
        let any_data_missing = (0..self.data_shards).any(|i| shards[i].is_none());
        if any_data_missing {
            let mut sub_matrix = vec![0u8; self.data_shards * self.data_shards];
            for (r, &orig_row) in present_indices.iter().enumerate() {
                let src =
                    &self.matrix[orig_row * self.data_shards..(orig_row + 1) * self.data_shards];
                sub_matrix[r * self.data_shards..(r + 1) * self.data_shards].copy_from_slice(src);
            }
            let decode_matrix = invert_matrix(&sub_matrix, self.data_shards)?;

            // Reconstruct all missing data shards
            let mut recovered_data = vec![vec![0u8; shard_len]; self.data_shards];
            for d in 0..self.data_shards {
                if let Some(ref existing) = shards[d] {
                    recovered_data[d].copy_from_slice(existing);
                    continue;
                }
                let row = &decode_matrix[d * self.data_shards..(d + 1) * self.data_shards];
                for (c, &src_shard_idx) in present_indices.iter().enumerate() {
                    let coeff = row[c];
                    if coeff == 0 {
                        continue;
                    }
                    let src_buf = shards[src_shard_idx].as_ref().unwrap();
                    for byte_idx in 0..shard_len {
                        recovered_data[d][byte_idx] =
                            add(recovered_data[d][byte_idx], mul(coeff, src_buf[byte_idx]));
                    }
                }
                shards[d] = Some(recovered_data[d].clone());
            }
        }

        // Reassemble original payload from the k data shards
        let mut out = Vec::with_capacity(orig_len);
        for shard_opt in shards.iter().take(self.data_shards) {
            let shard = shard_opt.as_ref().unwrap();
            let remaining = orig_len.saturating_sub(out.len());
            if remaining == 0 {
                break;
            }
            let take = remaining.min(shard.len());
            out.extend_from_slice(&shard[..take]);
        }
        Ok(out)
    }
}

fn mat_mul(a: &[u8], a_rows: usize, a_cols: usize, b: &[u8], b_cols: usize) -> Vec<u8> {
    let mut out = vec![0u8; a_rows * b_cols];
    for r in 0..a_rows {
        for c in 0..b_cols {
            let mut sum = 0u8;
            for k in 0..a_cols {
                sum = add(sum, mul(a[r * a_cols + k], b[k * b_cols + c]));
            }
            out[r * b_cols + c] = sum;
        }
    }
    out
}

fn invert_matrix(mat: &[u8], n: usize) -> Result<Vec<u8>, DbError> {
    // Augmented matrix [mat | I] of size n x 2n
    let mut aug = vec![0u8; n * 2 * n];
    for r in 0..n {
        aug[r * 2 * n..r * 2 * n + n].copy_from_slice(&mat[r * n..(r + 1) * n]);
        aug[r * 2 * n + n + r] = 1;
    }

    for col in 0..n {
        // Find non-zero pivot
        let mut pivot_row = None;
        for r in col..n {
            if aug[r * 2 * n + col] != 0 {
                pivot_row = Some(r);
                break;
            }
        }
        let pivot_row =
            pivot_row.ok_or(DbError::CorruptedData("Singular matrix in Reed-Solomon"))?;
        if pivot_row != col {
            for c in 0..(2 * n) {
                aug.swap(col * 2 * n + c, pivot_row * 2 * n + c);
            }
        }

        let pivot_val = aug[col * 2 * n + col];
        let pivot_inv = inv(pivot_val);
        for c in 0..(2 * n) {
            aug[col * 2 * n + c] = mul(aug[col * 2 * n + c], pivot_inv);
        }

        for r in 0..n {
            if r == col {
                continue;
            }
            let factor = aug[r * 2 * n + col];
            if factor == 0 {
                continue;
            }
            for c in 0..(2 * n) {
                let sub = mul(factor, aug[col * 2 * n + c]);
                aug[r * 2 * n + c] = add(aug[r * 2 * n + c], sub);
            }
        }
    }

    let mut inv_out = vec![0u8; n * n];
    for r in 0..n {
        inv_out[r * n..(r + 1) * n].copy_from_slice(&aug[r * 2 * n + n..r * 2 * n + 2 * n]);
    }
    Ok(inv_out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reed_solomon_4_plus_2_recovery() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let original =
            b"AliceDB immutable SSTable segment payload protected by GF(2^8) Reed-Solomon!";

        let (shards, orig_len) = rs.encode(original);
        assert_eq!(shards.len(), 6);

        // Simulate losing 2 data shards (shard 0 and shard 2)
        let mut damaged: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        damaged[0] = None;
        damaged[2] = None;

        let recovered = rs.reconstruct(&mut damaged, orig_len).unwrap();
        assert_eq!(recovered, original);

        // Simulate losing 3 shards (exceeds m=2 parity shards) -> must fail cleanly
        damaged[1] = None;
        assert!(rs.reconstruct(&mut damaged, orig_len).is_err());
    }
}

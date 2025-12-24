use sha2::{Digest, Sha256};

/// Merkle Tree Implementation
pub struct MerkleTree {
    #[allow(dead_code)] // Keep for future use if needed
    root: Vec<u8>,
}

impl MerkleTree {
    /// Compute Merkle Root from leaves
    pub fn compute_root(mut leaves: Vec<Vec<u8>>) -> Vec<u8> {
        if leaves.is_empty() {
            return vec![0u8; 32];
        }

        while leaves.len() > 1 {
            let mut next_level = Vec::with_capacity((leaves.len() + 1) / 2);

            for chunk in leaves.chunks(2) {
                let mut hasher = Sha256::new();
                hasher.update(&chunk[0]);
                if chunk.len() > 1 {
                    hasher.update(&chunk[1]);
                } else {
                    // Duplicate last element if odd number
                    hasher.update(&chunk[0]);
                }
                next_level.push(hasher.finalize().to_vec());
            }
            leaves = next_level;
        }

        leaves[0].clone()
    }
}

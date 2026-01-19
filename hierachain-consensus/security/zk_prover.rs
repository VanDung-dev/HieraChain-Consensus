//! Mock ZK Prover for Pressure Testing
//!
//! Simulates ZK proof generation and verification load without
//! heavy cryptographic computations. Used for testing system resilience.

use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::time::{sleep, Duration};

/// Mock ZK Prover that simulates workload
pub struct MockZKProver;

impl MockZKProver {
    /// Generate dummy proof with simulated latency
    ///
    /// # Arguments
    /// * `input` - Public inputs to generate proof for
    ///
    /// # Returns
    /// Randomized proof bytes (2KB-4KB) after a delay (100-500ms)
    pub async fn generate_proof(input: &[u8]) -> Vec<u8> {
        // Simulate computation time
        let mut rng = rand::thread_rng();
        let delay_ms = rng.gen_range(100..500);
        sleep(Duration::from_millis(delay_ms)).await;

        // Generate mock proof size (2KB - 4KB)
        let proof_size = rng.gen_range(2048..4096);
        let mut proof = vec![0u8; proof_size];
        rng.fill(&mut proof[..]);

        // Embed hash of input at start for verification check (Mock-Hash-Check style)
        // This links proof to input somewhat meaningfully
        let hash = Sha256::digest(input);
        proof[..32].copy_from_slice(&hash);

        proof
    }

    /// Verify proof with simulated latency
    ///
    /// # Arguments
    /// * `proof` - The proof bytes to verify
    /// * `input` - The public inputs the proof claims to verify
    ///
    /// # Returns
    /// true if valid (simulated), false otherwise
    pub async fn verify_proof(proof: &[u8], input: &[u8]) -> bool {
        // Simulate verification time (lighter than generation, e.g. 10-50ms)
        let mut rng = rand::thread_rng();
        let delay_ms = rng.gen_range(10..50);
        sleep(Duration::from_millis(delay_ms)).await;

        if proof.len() < 32 {
            return false;
        }

        // Check if proof starts with hash of input (as generated above)
        // This allows "valid" vs "invalid" proof simulation
        let hash = Sha256::digest(input);
        if &proof[..32] == hash.as_slice() {
            true
        } else {
            // Also accept if it's just random bytes (Mock Accept All behavior for stress test?)
            // But let's enforce the hash check for correctness simulation
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_proof_generation_verification() {
        let input = b"test_transaction_data";

        let start = std::time::Instant::now();
        let proof = MockZKProver::generate_proof(input).await;
        let duration = start.elapsed();

        // Check latency simulation (at least 100ms)
        assert!(duration.as_millis() >= 100);

        // Check contents
        assert!(proof.len() >= 2048 && proof.len() <= 4096);

        // Verify
        let valid = MockZKProver::verify_proof(&proof, input).await;
        assert!(valid);
    }

    #[tokio::test]
    async fn test_invalid_proof() {
        let input = b"real_data";
        let fake_proof = vec![0u8; 3000]; // Just zeros, no hash at start

        let valid = MockZKProver::verify_proof(&fake_proof, input).await;
        assert!(!valid);
    }
}

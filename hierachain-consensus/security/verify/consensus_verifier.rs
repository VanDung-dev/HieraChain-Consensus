//! Consensus Verification Module
//!
//! Provides utilities for verifying consensus messages and quorum signatures.

use super::signature_verifier::SignatureVerifier;
use crate::security::verify::SignatureError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("Verification failed")]
    VerificationFailed(#[from] SignatureError),
    #[error("Insufficient signatures: have {have}, need {need}")]
    InsufficientQuorum { have: usize, need: usize },
}

/// Verifies consensus messages and quorum thresholds
#[derive(Debug, Default)]
pub struct ConsensusVerifier {
    sig_verifier: SignatureVerifier,
}

impl ConsensusVerifier {
    pub fn new() -> Self {
        Self {
            sig_verifier: SignatureVerifier::new(),
        }
    }

    /// Verify a Quorum Certificate (QC) or aggregated signatures
    ///
    /// # Arguments
    /// * `message` - The message content that was signed
    /// * `signatures` - List of signatures
    /// * `public_keys` - List of public keys corresponding to the signatures
    /// * `total_validators` - Total number of validators in the set
    ///
    /// # Returns
    /// Ok if valid valid signatures count > 2/3 of total_validators
    pub fn verify_quorum(
        &self,
        message: &[u8],
        signatures: &[Vec<u8>],
        public_keys: &[Vec<u8>],
        total_validators: usize,
    ) -> Result<(), ConsensusError> {
        let threshold = (total_validators * 2) / 3 + 1;
        let mut valid_count = 0;

        if signatures.len() != public_keys.len() {
            // For simplicity, assume mismatch is bad
            return Err(ConsensusError::InsufficientQuorum {
                have: 0,
                need: threshold,
            });
        }

        // Use batch verify for headers
        let mut batch_items = Vec::new();
        for (sig, pk) in signatures.iter().zip(public_keys.iter()) {
            batch_items.push((message, sig.as_slice(), pk.as_slice()));
        }

        let results = self.sig_verifier.batch_verify(&batch_items);

        for result in results {
            if result.is_ok() {
                valid_count += 1;
            }
        }

        if valid_count < threshold {
            return Err(ConsensusError::InsufficientQuorum {
                have: valid_count,
                need: threshold,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    #[test]
    fn test_quorum_verification() {
        let verifier = ConsensusVerifier::new();
        let msg = b"consensus_proposal_block_1";

        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        let total = 4;
        let threshold = 3; // (4*2)/3 + 1 = 2+1 = 3

        // Sign with 3 validators (meet threshold)
        for _ in 0..3 {
            let mut csprng = OsRng;
            let key = SigningKey::generate(&mut csprng);
            let pk = key.verifying_key().to_bytes().to_vec();
            let sig = key.sign(msg).to_bytes().to_vec();

            signatures.push(sig);
            public_keys.push(pk);
        }

        assert!(verifier
            .verify_quorum(msg, &signatures, &public_keys, total)
            .is_ok());

        // Test failure (only 2 signatures)
        signatures.pop();
        public_keys.pop();
        assert!(matches!(
            verifier.verify_quorum(msg, &signatures, &public_keys, total),
            Err(ConsensusError::InsufficientQuorum { have: 2, need: 3 })
        ));
    }
}

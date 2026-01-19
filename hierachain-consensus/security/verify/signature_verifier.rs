//! Signature Verification Module
//!
//! Provides utilities for verifying signatures on blocks and events.
//! Supports batched verification using Rayon for high performance.

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rayon::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SignatureError {
    #[error("Invalid signature format")]
    InvalidFormat,
    #[error("Signature verification failed")]
    VerificationFailed,
    #[error("Invalid public key")]
    InvalidPublicKey,
}

/// Verifier structure (stateless for now, but extensible)
#[derive(Debug, Default)]
pub struct SignatureVerifier;

impl SignatureVerifier {
    pub fn new() -> Self {
        Self
    }

    /// Verify a single signature
    pub fn verify(
        &self,
        msg: &[u8],
        signature_bytes: &[u8],
        public_key_bytes: &[u8],
    ) -> Result<(), SignatureError> {
        let verifying_key = VerifyingKey::from_bytes(
            public_key_bytes
                .try_into()
                .map_err(|_| SignatureError::InvalidPublicKey)?,
        )
        .map_err(|_| SignatureError::InvalidPublicKey)?;

        let signature = Signature::from_bytes(
            signature_bytes
                .try_into()
                .map_err(|_| SignatureError::InvalidFormat)?,
        );

        verifying_key
            .verify(msg, &signature)
            .map_err(|_| SignatureError::VerificationFailed)
    }

    /// Batch verify signatures in parallel
    ///
    /// Takes a list of (message, signature, public_key) tuples.
    pub fn batch_verify(&self, items: &[(&[u8], &[u8], &[u8])]) -> Vec<Result<(), SignatureError>> {
        items
            .par_iter()
            .map(|(msg, sig, pk)| self.verify(msg, sig, pk))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    #[test]
    fn test_signature_verification() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let msg = b"Hello, HieraChain!";
        let signature = signing_key.sign(msg);

        let verifier = SignatureVerifier::new();
        let result = verifier.verify(msg, &signature.to_bytes(), verifying_key.as_bytes());

        assert!(result.is_ok());
    }

    #[test]
    fn test_batch_verification() {
        let verifier = SignatureVerifier::new();
        let mut keypairs = Vec::new();

        // Generate 100 valid signatures
        for i in 0..100 {
            let mut csprng = OsRng;
            let signing_key = SigningKey::generate(&mut csprng);
            let vk_bytes = signing_key.verifying_key().to_bytes();

            let msg = format!("Message {}", i).into_bytes();
            let signature = signing_key.sign(&msg).to_bytes();

            keypairs.push((msg, signature, vk_bytes));
        }

        // Let's adapt the inputs for batch_verify
        let refs: Vec<(&[u8], &[u8], &[u8])> = keypairs
            .iter()
            .map(|(m, s, k)| (m.as_slice(), s.as_slice(), k.as_slice()))
            .collect();

        let results = verifier.batch_verify(&refs);
        assert_eq!(results.len(), 100);
        assert!(results.iter().all(|r| r.is_ok()));
    }
}

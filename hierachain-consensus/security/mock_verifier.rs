//! Mock Verifier for Development and Testing
//!
//! Provides a simple verifier that accepts proofs based on configurable rules,
//! allowing development without full ZK circuit setup.
//! Matches Python's mock verification behavior.

use super::zk_verifier::{Verifier, ZkVerifyResult};
use log::{debug, warn};
use sha2::{Digest, Sha256};

/// Magic bytes that indicate a mock proof (matches Python implementation)
const MOCK_PROOF_MAGIC: &[u8] = b"mock_proof";

/// Mock verification mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MockMode {
    /// Accept all proofs unconditionally
    AcceptAll,
    /// Reject all proofs unconditionally
    RejectAll,
    /// Accept only if proof starts with magic bytes
    MagicBytes,
    /// Verify using simple SHA-256 hash check (matches Python dev mode)
    HashCheck,
}

/// Mock Verifier for testing and development
///
/// This verifier provides configurable behavior for testing consensus logic
/// without requiring actual ZK proof generation/verification.
pub struct MockVerifier {
    mode: MockMode,
    /// Expected hash for HashCheck mode (optional)
    expected_hash: Option<Vec<u8>>,
}

impl MockVerifier {
    /// Create a new MockVerifier with the specified mode
    pub fn new(mode: MockMode) -> Self {
        Self {
            mode,
            expected_hash: None,
        }
    }

    /// Create a MockVerifier that accepts all proofs
    pub fn accept_all() -> Self {
        Self::new(MockMode::AcceptAll)
    }

    /// Create a MockVerifier that rejects all proofs
    pub fn reject_all() -> Self {
        Self::new(MockMode::RejectAll)
    }

    /// Create a MockVerifier that uses magic bytes check
    pub fn magic_bytes() -> Self {
        Self::new(MockMode::MagicBytes)
    }

    /// Create a MockVerifier that uses hash check mode
    pub fn hash_check() -> Self {
        Self::new(MockMode::HashCheck)
    }

    /// Set expected hash for HashCheck mode
    pub fn with_expected_hash(mut self, hash: Vec<u8>) -> Self {
        self.expected_hash = Some(hash);
        self
    }

    /// Generate a valid mock proof for testing
    ///
    /// # Arguments
    /// * `data` - Data to create proof for (used in HashCheck mode)
    ///
    /// # Returns
    /// A proof that will be accepted by MockVerifier in MagicBytes or HashCheck mode
    pub fn generate_mock_proof(data: &[u8]) -> Vec<u8> {
        let mut proof = MOCK_PROOF_MAGIC.to_vec();
        let hash = Sha256::digest(data);
        proof.extend_from_slice(&hash);
        proof
    }

    /// Check if bytes start with mock magic
    fn has_magic_prefix(proof: &[u8]) -> bool {
        proof.len() >= MOCK_PROOF_MAGIC.len() && proof.starts_with(MOCK_PROOF_MAGIC)
    }

    /// Verify using hash check (matches Python's dev mode behavior)
    fn verify_hash_check(&self, proof: &[u8], public_inputs: &[u8]) -> bool {
        // Proof format: magic_bytes + sha256(public_inputs)
        if !Self::has_magic_prefix(proof) {
            warn!("MockVerifier: Proof missing magic bytes prefix");
            return false;
        }

        let hash_start = MOCK_PROOF_MAGIC.len();
        if proof.len() < hash_start + 32 {
            warn!(
                "MockVerifier: Proof too short (len={}, expected>={})",
                proof.len(),
                hash_start + 32
            );
            return false;
        }

        let proof_hash = &proof[hash_start..hash_start + 32];
        let computed_hash = Sha256::digest(public_inputs);

        if proof_hash != computed_hash.as_slice() {
            debug!(
                "MockVerifier: Hash mismatch. Expected: {:?}, Got: {:?}",
                computed_hash, proof_hash
            );
            return false;
        }

        true
    }
}

impl Default for MockVerifier {
    fn default() -> Self {
        Self::magic_bytes()
    }
}

impl Verifier for MockVerifier {
    fn verify(&self, proof: &[u8], public_inputs: &[u8]) -> ZkVerifyResult<bool> {
        let result = match self.mode {
            MockMode::AcceptAll => true,
            MockMode::RejectAll => false,
            MockMode::MagicBytes => Self::has_magic_prefix(proof),
            MockMode::HashCheck => self.verify_hash_check(proof, public_inputs),
        };

        Ok(result)
    }

    fn is_initialized(&self) -> bool {
        // MockVerifier is always initialized
        true
    }

    fn verifier_type(&self) -> &'static str {
        match self.mode {
            MockMode::AcceptAll => "Mock-AcceptAll",
            MockMode::RejectAll => "Mock-RejectAll",
            MockMode::MagicBytes => "Mock-MagicBytes",
            MockMode::HashCheck => "Mock-HashCheck",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accept_all() {
        let verifier = MockVerifier::accept_all();
        assert!(verifier.verify(&[], &[]).unwrap());
        assert!(verifier.verify(&[1, 2, 3], &[4, 5, 6]).unwrap());
    }

    #[test]
    fn test_reject_all() {
        let verifier = MockVerifier::reject_all();
        assert!(!verifier.verify(&[], &[]).unwrap());
        assert!(!verifier.verify(MOCK_PROOF_MAGIC, &[]).unwrap());
    }

    #[test]
    fn test_magic_bytes() {
        let verifier = MockVerifier::magic_bytes();

        // Valid mock proof
        assert!(verifier.verify(MOCK_PROOF_MAGIC, &[]).unwrap());
        assert!(verifier
            .verify(&[MOCK_PROOF_MAGIC, &[1, 2, 3]].concat(), &[])
            .unwrap());

        // Invalid proof (no magic bytes)
        assert!(!verifier.verify(&[1, 2, 3], &[]).unwrap());
        assert!(!verifier.verify(&[], &[]).unwrap());
    }

    #[test]
    fn test_hash_check() {
        let verifier = MockVerifier::hash_check();
        let public_inputs = b"test_data";

        // Generate valid proof
        let valid_proof = MockVerifier::generate_mock_proof(public_inputs);
        assert!(verifier.verify(&valid_proof, public_inputs).unwrap());

        // Invalid proof (wrong hash)
        let wrong_inputs = b"wrong_data";
        assert!(!verifier.verify(&valid_proof, wrong_inputs).unwrap());

        // Invalid proof (no magic)
        assert!(!verifier.verify(&[0u8; 64], public_inputs).unwrap());
    }

    #[test]
    fn test_generate_mock_proof() {
        let data = b"hello world";
        let proof = MockVerifier::generate_mock_proof(data);

        assert!(proof.starts_with(MOCK_PROOF_MAGIC));
        assert_eq!(proof.len(), MOCK_PROOF_MAGIC.len() + 32); // magic + sha256
    }

    #[test]
    fn test_verifier_type() {
        assert_eq!(MockVerifier::accept_all().verifier_type(), "Mock-AcceptAll");
        assert_eq!(MockVerifier::reject_all().verifier_type(), "Mock-RejectAll");
        assert_eq!(
            MockVerifier::magic_bytes().verifier_type(),
            "Mock-MagicBytes"
        );
        assert_eq!(MockVerifier::hash_check().verifier_type(), "Mock-HashCheck");
    }

    #[test]
    fn test_is_initialized() {
        let verifier = MockVerifier::default();
        assert!(verifier.is_initialized());
    }
}

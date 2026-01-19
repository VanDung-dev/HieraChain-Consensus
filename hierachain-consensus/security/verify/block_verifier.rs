//! Block Verification Module
//!
//! Ensures file integrity and cryptographic consistency of blocks.

use super::signature_verifier::SignatureVerifier;
use crate::core::block::Block;
use crate::core::utils::MerkleTree;
use hex;
use thiserror::Error;

use crate::security::verify::SignatureError;

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
    #[error("Merkle root mismatch")]
    MerkleRootMismatch,
    #[error("Invalid chain link: prev_hash mismatch relative to latest block")]
    InvalidChainLink,
    #[error("Missing signature")]
    MissingSignature,
    #[error("Signature error: {0}")]
    SignatureError(#[from] SignatureError),
    #[error("Timestamp error: {0}")]
    TimestampError(String),
}

pub struct BlockVerifier {
    sig_verifier: SignatureVerifier,
}

impl BlockVerifier {
    pub fn new() -> Self {
        Self {
            sig_verifier: SignatureVerifier::new(),
        }
    }

    /// Verify the block's self-integrity (hash and merkle root)
    pub fn verify_integrity(&self, block: &Block) -> Result<(), VerificationError> {
        // 1. Verify Merkle Root validity (Events -> Merkle Root)
        let tree = MerkleTree::new(&block.events);
        let calculated_root = tree.get_root();

        if calculated_root != block.merkle_root {
            return Err(VerificationError::MerkleRootMismatch);
        }

        // 2. Verify Block Hash (Header fields -> Hash)
        let calculated_hash = block.calculate_hash();
        if calculated_hash != block.hash {
            return Err(VerificationError::HashMismatch {
                expected: block.hash.clone(),
                actual: calculated_hash,
            });
        }

        Ok(())
    }

    /// Verify continuity from previous block
    pub fn verify_chain_link(
        &self,
        block: &Block,
        prev_block: &Block,
    ) -> Result<(), VerificationError> {
        if block.previous_hash != prev_block.hash {
            return Err(VerificationError::InvalidChainLink);
        }

        // Also verify index continuity
        if block.index != prev_block.index + 1 {
            // We could add a specific error for index mismatch, but InvalidChainLink covers structure
            return Err(VerificationError::InvalidChainLink);
        }

        Ok(())
    }

    /// Verify the block signature matches the proposer (creator_id)
    /// Note: This assumes creator_id is the public key, or we have a way to resolve it.
    /// In this mock implementation, we assume creator_id IS the hex-encoded public key.
    pub fn verify_signature(&self, block: &Block) -> Result<(), VerificationError> {
        let signature_hex = match &block.signature {
            Some(s) => s,
            None => return Err(VerificationError::MissingSignature),
        };

        let proposer_pk_hex = match &block.creator_id {
            Some(id) => id,
            None => return Err(VerificationError::MissingSignature), // Require creator_id if signed
        };

        // Decode hex
        let signature_bytes =
            hex::decode(signature_hex).map_err(|_| SignatureError::InvalidFormat)?;
        let pk_bytes =
            hex::decode(proposer_pk_hex).map_err(|_| SignatureError::InvalidPublicKey)?;

        // The message signed is typically the block hash
        let msg = hex::decode(&block.hash).unwrap_or_default();

        self.sig_verifier
            .verify(&msg, &signature_bytes, &pk_bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::block::Block;
    use serde_json::json;

    #[test]
    fn test_valid_block_verification() {
        // Create a valid block using Block's own methods
        let event = json!({
            "event": "test_event",
            "timestamp": 1234567890.0
        });

        // Use pythonize-friendly constructor logic or manual struct init
        // Manual init is safer here to control state
        let mut block = Block {
            index: 1,
            events: vec![],
            arrow_events: None,
            timestamp: 100.0,
            previous_hash: "genesis_hash".to_string(),
            nonce: 0,
            merkle_root: String::new(),
            hash: String::new(),
            creator_id: Some("validator".to_string()),
            signature: None,
            zk_proof: None,
            zk_public_inputs: None,
        };

        block.add_event(event);
        // add_event updates Merkle Root and Hash automatically.

        let verifier = BlockVerifier::new();
        assert!(verifier.verify_integrity(&block).is_ok());
    }

    #[test]
    fn test_tampered_events() {
        let event = json!({
            "event": "test_event",
            "timestamp": 1234567890.0
        });

        let mut block = Block {
            index: 1,
            events: vec![],
            arrow_events: None,
            timestamp: 100.0,
            previous_hash: "genesis_hash".to_string(),
            nonce: 0,
            merkle_root: String::new(),
            hash: String::new(),
            creator_id: None,
            signature: None,
            zk_proof: None,
            zk_public_inputs: None,
        };
        block.add_event(event);

        // Tamper with events BUT NOT merkle_root
        block.events.push(json!({"event": "malicious"}));

        let verifier = BlockVerifier::new();
        assert!(matches!(
            verifier.verify_integrity(&block),
            Err(VerificationError::MerkleRootMismatch)
        ));
    }
}

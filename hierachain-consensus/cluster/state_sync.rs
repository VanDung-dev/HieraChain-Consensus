//! State Synchronization Module
//!
//! Handles "Resurrection" logic: requesting and verifying blocks from peers to fill gaps.

use crate::core::block::Block;
use crate::security::verify::BlockVerifier;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Network request failed: {0}")]
    NetworkError(String),
    #[error("Block verification failed: {0}")]
    VerificationFailed(String),
    #[error("Chain continuity broken at index {0}")]
    ContinuityError(u64),
}

pub struct StateSyncManager {
    verifier: BlockVerifier,
}

impl StateSyncManager {
    pub fn new() -> Self {
        Self {
            verifier: BlockVerifier::new(),
        }
    }

    /// Simulate requesting blocks from peers starting from a specific index
    ///
    /// In a real implementation, this would make async P2P calls.
    pub async fn request_sync(
        &self,
        start_index: u64,
        _peer_id: &str,
    ) -> Result<Vec<Block>, SyncError> {
        println!(
            "P2P REQUEST [SYNC]: Requesting blocks from index {}...",
            start_index
        );

        // Return empty mechanism for now - integration would require mocking a Peer/Network trait
        Ok(vec![])
    }

    /// Verify and prepare a batch of synced blocks for application
    ///
    /// This ensures that "resurrected" data is valid before being added to the local chain.
    pub fn verify_incoming_batch(
        &self,
        blocks: &[Block],
        last_local_block: &Block,
    ) -> Result<(), SyncError> {
        let mut prev_block = last_local_block;

        for block in blocks {
            // 1. Verify Integrity (Hash, Merkle, etc.)
            self.verifier
                .verify_integrity(block)
                .map_err(|e| SyncError::VerificationFailed(e.to_string()))?;

            // 2. Verify Continuity with previous block
            self.verifier
                .verify_chain_link(block, prev_block)
                .map_err(|_e| SyncError::ContinuityError(block.index))?;

            // 3. Verify Signature
            self.verifier
                .verify_signature(block)
                .map_err(|_e| SyncError::VerificationFailed(_e.to_string()))?;

            prev_block = block;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::block::Block;
    use serde_json::json;

    // Helper to create a dummy block (similar to block_verifier tests)
    fn create_valid_block(index: u64, prev_hash: &str) -> Block {
        let mut block = Block {
            index,
            events: vec![],
            arrow_events: None,
            timestamp: 100.0,
            previous_hash: prev_hash.to_string(),
            nonce: 0,
            merkle_root: String::new(),
            hash: String::new(),
            creator_id: Some("validator".to_string()),
            signature: None, // Signature check requires valid sig, mocking verify_signature might be hard without keys
            zk_proof: None,
            zk_public_inputs: None,
        };
        // Add event to calc hash
        block.add_event(json!({"event": "sync_test"}));

        block
    }

    #[test]
    fn test_verify_empty_batch() {
        let manager = StateSyncManager::new();
        let genesis = create_valid_block(0, "0000");

        assert!(manager.verify_incoming_batch(&[], &genesis).is_ok());
    }
}

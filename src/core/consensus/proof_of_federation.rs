//! Proof of Federation (PoF) consensus mechanism for HieraChain Framework.
//!
//! This module implements a Federated consensus mechanism designed for
//! consortium blockchains (e.g., Healthcare, Education, Supply Chain Consortia).
//! It replaces the static authority model with a rotating leader schedule,
//! ensuring fair participation and removing single points of failure.

use crate::core::block::Block;
use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::utils::{validate_event_structure, validate_no_cryptocurrency_terms};
use serde_json::{Map, Value};
use sha2::Digest;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Proof of Federation (PoF) Consensus.
///
/// A Round-Robin based consensus mechanism suitable for semi-trusted consortiums.
///
/// Key Features:
/// - Rotating Leader: Authorities take turns creating blocks based on block height.
/// - Deterministic Schedule: Leader = (BlockHeight) % (TotalAuthorities).
/// - Fault Tolerance: If a leader misses their turn, the protocol can skip to the next.
pub struct ProofOfFederation {
    pub name: String,
    /// Ordered list of validator IDs (sorted for deterministic order across nodes).
    pub validators: Vec<String>,
    /// Metadata for each validator (e.g., organization info).
    pub validator_metadata: HashMap<String, Map<String, Value>>,
    /// Configuration settings (JSON-based, matching Python style).
    pub config: Map<String, Value>,
}

impl ProofOfFederation {
    /// Create a new Proof of Federation consensus instance.
    pub fn new(name: &str) -> Self {
        let mut config = Map::new();
        config.insert("block_interval".to_string(), Value::from(5.0));
        config.insert("min_validators".to_string(), Value::from(3));
        config.insert("enforce_rotation".to_string(), Value::Bool(true));

        ProofOfFederation {
            name: name.to_string(),
            validators: Vec::new(),
            validator_metadata: HashMap::new(),
            config,
        }
    }

    /// Get the number of active validators.
    pub fn get_validator_count(&self) -> usize {
        self.validators.len()
    }

    /// Add a validator to the federation.
    pub fn add_validator(
        &mut self,
        validator_id: String,
        metadata: Option<Map<String, Value>>,
    ) -> bool {
        if self.validators.contains(&validator_id) {
            return false;
        }

        self.validators.push(validator_id.clone());
        // Keep list sorted to ensure deterministic order across all nodes
        self.validators.sort();

        self.validator_metadata
            .insert(validator_id, metadata.unwrap_or_default());
        true
    }

    /// Remove a validator from the federation.
    pub fn remove_validator(&mut self, validator_id: &str) -> bool {
        if let Some(pos) = self.validators.iter().position(|x| x == validator_id) {
            self.validators.remove(pos);
            self.validator_metadata.remove(validator_id);
            return true;
        }
        false
    }

    /// Alias for add_validator for compatibility with PoA API.
    pub fn add_authority(
        &mut self,
        authority_id: String,
        metadata: Option<Map<String, Value>>,
    ) -> bool {
        self.add_validator(authority_id, metadata)
    }

    /// Alias for remove_validator for compatibility with PoA API.
    pub fn remove_authority(&mut self, authority_id: &str) -> bool {
        self.remove_validator(authority_id)
    }

    /// Check if an ID is an active authority/validator.
    pub fn is_authority(&self, authority_id: &str) -> bool {
        self.validators.contains(&authority_id.to_string())
    }

    /// Determine the expected leader for a specific block index.
    /// Algorithm: Leader = Validators[ BlockIndex % ValidatorCount ]
    pub fn get_current_leader(&self, block_index: u64) -> Option<&String> {
        if self.validators.is_empty() {
            return None;
        }
        let leader_idx = (block_index as usize) % self.validators.len();
        Some(&self.validators[leader_idx])
    }

    /// Validate if the proposer is the correct leader for this block height.
    pub fn validate_block_proposer(&self, block_index: u64, proposer_id: &str) -> bool {
        match self.get_current_leader(block_index) {
            Some(expected_leader) => expected_leader == proposer_id,
            None => false,
        }
    }

    /// Extract the signer ID from the block's events.
    fn extract_signer_id(block: &Block) -> Option<String> {
        // Check events in reverse order (end of block first)
        for event in block.events.iter().rev() {
            if let Some(obj) = event.as_object() {
                if let Some(event_type) = obj.get("event").and_then(|v| v.as_str()) {
                    if event_type == "consensus_finalization" {
                        if let Some(details) = obj.get("details").and_then(|v| v.as_object()) {
                            if let Some(leader_id) =
                                details.get("leader_id").and_then(|v| v.as_str())
                            {
                                return Some(leader_id.to_string());
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Estimate the time required to create a new block.
    pub fn estimate_block_time(&self) -> f64 {
        self.config
            .get("block_interval")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0)
    }
}

impl BaseConsensusTrait for ProofOfFederation {
    fn get_validator_count(&self) -> u64 {
        self.validators.len() as u64
    }

    fn can_create_block(&self, authority_id: Option<&str>) -> bool {
        // 1. Check if we have enough validators
        let min_validators = self
            .config
            .get("min_validators")
            .and_then(|v| v.as_u64())
            .unwrap_or(3) as usize;

        if self.validators.len() < min_validators {
            return false;
        }

        // 2. If authority_id provided, check if it's a valid validator
        if let Some(auth_id) = authority_id {
            if !self.validators.contains(&auth_id.to_string()) {
                return false;
            }
        }

        true
    }

    fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        // 1. Basic structure check
        if !block.validate_structure() {
            return false;
        }

        // 2. Timing check (allow 80% leniency for drifting clocks)
        let block_interval = self
            .config
            .get("block_interval")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0);
        let time_diff = block.timestamp - previous_block.timestamp;
        if time_diff < block_interval * 0.8 {
            return false;
        }

        // 3. Leader Check - find who signed the block
        let signer_id = match Self::extract_signer_id(block) {
            Some(id) => id,
            None => return false, // Block must be signed in PoF
        };

        // 4. Enforce rotation if configured
        let enforce_rotation = self
            .config
            .get("enforce_rotation")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        if enforce_rotation && !self.validate_block_proposer(block.index, &signer_id) {
            return false; // "It wasn't your turn!"
        }

        // 5. Validate each event
        for event in &block.events {
            if !self.validate_event_for_consensus(event) {
                return false;
            }
        }

        true
    }

    fn validate_event_for_consensus(&self, event: &Value) -> bool {
        // 1. Basic structure validation
        if !validate_event_structure(event) {
            return false;
        }

        // 2. Forbidden cryptocurrency terms check
        if !validate_no_cryptocurrency_terms(event) {
            return false;
        }

        // 3. PoF specific validation (entity_id used as metadata)
        if let Some(event_obj) = event.as_object() {
            if let Some(eid) = event_obj.get("entity_id") {
                if !eid.is_string() {
                    return false;
                }
            }
        }

        true
    }

    fn finalize_block(&mut self, block: &mut Block) -> bool {
        let creator_id = match &block.creator_id {
            Some(id) => id.clone(),
            None => return false,
        };

        if !self.can_create_block(Some(&creator_id)) {
            return false;
        }

        // Get current timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        // Create signature payload
        let signature_data = format!("{}:{}:{}:{}", block.hash, creator_id, block.index, now);

        // Create signature (SHA-256 hash)
        let mut hasher = sha2::Sha256::new();
        hasher.update(signature_data.as_bytes());
        let signature = format!("{:x}", hasher.finalize());

        // Create consensus event
        let consensus_event = serde_json::json!({
            "event": "consensus_finalization",
            "timestamp": now,
            "details": {
                "consensus_type": "proof_of_federation",
                "leader_id": creator_id,
                "signature": signature,
                "validators_count": self.validators.len(),
                "round": block.index,
                "finalized_at": now
            }
        });

        // Add consensus event to block
        block.add_event(consensus_event);

        // Set top-level signature
        block.signature = Some(signature);

        true
    }

    fn get_consensus_info(&self) -> Map<String, Value> {
        let mut info = Map::new();
        info.insert("name".to_string(), Value::String(self.name.clone()));
        info.insert(
            "type".to_string(),
            Value::String("ProofOfFederation".to_string()),
        );
        info.insert(
            "validator_count".to_string(),
            Value::from(self.validators.len()),
        );

        // Convert validators to JSON array
        let validators_json: Vec<Value> = self
            .validators
            .iter()
            .map(|v| Value::String(v.clone()))
            .collect();
        info.insert("validators".to_string(), Value::Array(validators_json));
        info.insert("config".to_string(), Value::Object(self.config.clone()));

        info
    }

    fn update_config(&mut self, config: Map<String, Value>) {
        for (k, v) in config {
            self.config.insert(k, v);
        }
    }
}

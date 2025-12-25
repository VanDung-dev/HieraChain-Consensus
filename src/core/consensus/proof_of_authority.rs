//! Proof of Authority consensus mechanism for HieraChain Framework.
//!
//! This module implements a Proof of Authority (PoA) consensus mechanism suitable
//! for the HieraChain framework where specific authorities (Main Chain,
//! Sub-Chains) have designated roles and permissions for block creation.

use crate::core::block::Block;
use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::utils::{validate_event_structure, validate_no_cryptocurrency_terms};
use serde_json::{Map, Value};
use sha2::Digest;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Proof of Authority consensus implementation
pub struct ProofOfAuthority {
    pub name: String,
    pub authorities: HashSet<String>,
    pub authority_metadata: HashMap<String, Map<String, Value>>,
    pub config: Map<String, Value>,
}

impl ProofOfAuthority {
    pub fn new(name: &str) -> Self {
        let mut config = Map::new();
        config.insert("block_interval".to_string(), Value::from(10.0));
        config.insert("require_authority_signature".to_string(), Value::Bool(true));
        config.insert("max_authorities".to_string(), Value::from(100));

        ProofOfAuthority {
            name: name.to_string(),
            authorities: HashSet::new(),
            authority_metadata: HashMap::new(),
            config,
        }
    }

    pub fn add_authority(
        &mut self,
        authority_id: String,
        metadata: Option<Map<String, Value>>,
    ) -> bool {
        let max_auths = self
            .config
            .get("max_authorities")
            .and_then(|v| v.as_u64())
            .unwrap_or(100) as usize;
        if self.authorities.len() >= max_auths {
            return false;
        }
        self.authorities.insert(authority_id.clone());
        if let Some(meta) = metadata {
            self.authority_metadata.insert(authority_id, meta);
        } else {
            self.authority_metadata.insert(authority_id, Map::new());
        }
        true
    }

    pub fn remove_authority(&mut self, authority_id: &str) -> bool {
        if self.authorities.contains(authority_id) {
            self.authorities.remove(authority_id);
            self.authority_metadata.remove(authority_id);
            return true;
        }
        false
    }

    pub fn is_authority(&self, authority_id: &str) -> bool {
        self.authorities.contains(authority_id)
    }

    pub fn get_next_authority(&self, current_block_index: u64) -> Option<String> {
        if self.authorities.is_empty() {
            return None;
        }
        let mut sorted_auths: Vec<&String> = self.authorities.iter().collect();
        sorted_auths.sort();

        let idx = (current_block_index + 1) as usize % sorted_auths.len();
        Some(sorted_auths[idx].clone())
    }

    fn has_valid_authority_signature(&self, block: &Block) -> bool {
        // Look for consensus finalization event
        for event in &block.events {
            if let Some(obj) = event.as_object() {
                if let Some(event_type) = obj.get("event").and_then(|v| v.as_str()) {
                    if event_type == "consensus_finalization" {
                        if let Some(details) = obj.get("details").and_then(|v| v.as_object()) {
                            if let Some(auth_id) =
                                details.get("authority_id").and_then(|v| v.as_str())
                            {
                                // Simplify: Check if basic auth ID is recognized.
                                // Real sig check would verify 'authority_signature' field against pubkey.
                                if self.is_authority(auth_id) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        false
    }
}

impl BaseConsensusTrait for ProofOfAuthority {
    fn get_validator_count(&self) -> u64 {
        self.authorities.len() as u64
    }

    fn can_create_block(&self, authority_id: Option<&str>) -> bool {
        if let Some(auth_id) = authority_id {
            self.is_authority(auth_id)
        } else {
            false
        }
    }

    fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        // Basic block structure validation
        if !block.validate_structure() {
            return false;
        }

        // Check block timing
        let block_interval = self
            .config
            .get("block_interval")
            .and_then(|v| v.as_f64())
            .unwrap_or(10.0);
        let time_diff = block.timestamp - previous_block.timestamp;

        if time_diff < (block_interval / 2.0) {
            return false;
        }

        for event in &block.events {
            // Check forbidden terms using our trait method (impl below)
            if !self.validate_event_for_consensus(event) {
                return false;
            }
        }

        // Check authority signature
        let require_sig = self
            .config
            .get("require_authority_signature")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if require_sig && !self.has_valid_authority_signature(block) {
            return false;
        }

        true
    }

    fn validate_event_for_consensus(&self, event: &Value) -> bool {
        // 1. Basic structure
        if !validate_event_structure(event) {
            return false;
        }

        // 2. Forbidden crypto terms
        if !validate_no_cryptocurrency_terms(event) {
            return false;
        }

        // 3. PoA specific validation (entity_id used as metadata)
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
        if let Some(creator_id) = &block.creator_id {
            if self.is_authority(creator_id) {
                // Create signature data
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                let sig_data = format!("{}{}{}", block.hash, creator_id, now);

                // Simple signature (hash) as per Python implementation
                let mut hasher = sha2::Sha256::new();
                hasher.update(sig_data.as_bytes());
                let signature = format!("{:x}", hasher.finalize());

                // Create consensus event
                let consensus_event = serde_json::json!({
                    "event": "consensus_finalization",
                    "entity_id": "system_consensus",
                    "timestamp": now,
                    "details": {
                        "consensus_type": "proof_of_authority",
                        "authority_id": creator_id,
                        "authority_signature": signature,
                        "finalized_at": now
                    }
                });

                block.add_event(consensus_event);
                // Block hash is recalculated in add_event

                // Also set top-level signature if we want
                block.signature = Some(signature);
                return true;
            }
        }
        false
    }

    fn get_consensus_info(&self) -> Map<String, Value> {
        let mut info = Map::new();
        info.insert("name".to_string(), Value::String(self.name.clone()));
        info.insert(
            "type".to_string(),
            Value::String("ProofOfAuthority".to_string()),
        );
        info.insert("config".to_string(), Value::Object(self.config.clone()));
        info.insert(
            "authorities_count".to_string(),
            Value::from(self.authorities.len()),
        );
        info
    }

    fn update_config(&mut self, config: Map<String, Value>) {
        for (k, v) in config {
            self.config.insert(k, v);
        }
    }
}

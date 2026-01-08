//! Proof of Authority consensus mechanism for HieraChain Framework.
//!
//! This module implements a Proof of Authority (PoA) consensus mechanism suitable
//! for the HieraChain framework where specific authorities (Main Chain,
//! Sub-Chains) have designated roles and permissions for block creation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::block::Block;
use crate::core::consensus::base_consensus::{verify_block_zk_proof, BaseConsensusTrait};
use crate::core::utils::{validate_event_structure, validate_no_cryptocurrency_terms};
use crate::security::security_utils::verify_signature;
use crate::security::zk_verifier::Verifier;
use serde_json::{Map, Value};
use sha2::Digest;

/// Proof of Authority consensus implementation
pub struct ProofOfAuthority {
    pub name: String,
    pub authorities: HashSet<String>,
    pub authority_metadata: HashMap<String, Map<String, Value>>,
    /// Public keys for each authority (authority_id -> public_key_hex)
    pub authority_public_keys: HashMap<String, String>,
    pub config: Map<String, Value>,
    /// Optional ZK Verifier
    pub verifier: Option<Arc<dyn Verifier>>,
}

impl ProofOfAuthority {
    pub fn new(name: &str) -> Self {
        let mut config = Map::new();
        config.insert("block_interval".to_string(), Value::from(10.0));
        config.insert("require_authority_signature".to_string(), Value::Bool(true));
        config.insert("max_authorities".to_string(), Value::from(100));
        // New: Enable strict signature verification
        config.insert(
            "strict_signature_verification".to_string(),
            Value::Bool(true),
        );

        ProofOfAuthority {
            name: name.to_string(),
            authorities: HashSet::new(),
            authority_metadata: HashMap::new(),
            authority_public_keys: HashMap::new(),
            config,
            verifier: None,
        }
    }

    /// Set ZK Verifier
    pub fn set_verifier(&mut self, verifier: Arc<dyn Verifier>) {
        self.verifier = Some(verifier);
    }

    /// Add an authority without a public key (backward compatible).
    /// Note: Without public key, signature verification will use legacy mode.
    pub fn add_authority(
        &mut self,
        authority_id: String,
        metadata: Option<Map<String, Value>>,
    ) -> bool {
        self.add_authority_with_key(authority_id, metadata, None)
    }

    /// Add an authority with a public key for signature verification.
    ///
    /// # Arguments
    /// * `authority_id` - Unique identifier for the authority
    /// * `metadata` - Optional metadata for the authority
    /// * `public_key_hex` - Optional Ed25519 public key in hex format
    ///
    /// # Returns
    /// * `true` if authority was added successfully
    pub fn add_authority_with_key(
        &mut self,
        authority_id: String,
        metadata: Option<Map<String, Value>>,
        public_key_hex: Option<String>,
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
            self.authority_metadata.insert(authority_id.clone(), meta);
        } else {
            self.authority_metadata
                .insert(authority_id.clone(), Map::new());
        }
        // Store public key if provided
        if let Some(pk) = public_key_hex {
            self.authority_public_keys.insert(authority_id, pk);
        }
        true
    }

    /// Remove an authority and its associated keys.
    pub fn remove_authority(&mut self, authority_id: &str) -> bool {
        if self.authorities.contains(authority_id) {
            self.authorities.remove(authority_id);
            self.authority_metadata.remove(authority_id);
            self.authority_public_keys.remove(authority_id);
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

    /// Validate authority signature on a block.
    ///
    /// This method performs cryptographic signature verification when public keys
    /// are available. Falls back to membership check if strict verification is disabled.
    fn has_valid_authority_signature(&self, block: &Block) -> bool {
        let strict_verification = self
            .config
            .get("strict_signature_verification")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // Look for consensus finalization event
        for event in &block.events {
            if let Some(obj) = event.as_object() {
                if let Some(event_type) = obj.get("event").and_then(|v| v.as_str()) {
                    if event_type == "consensus_finalization" {
                        if let Some(details) = obj.get("details").and_then(|v| v.as_object()) {
                            if let Some(auth_id) =
                                details.get("authority_id").and_then(|v| v.as_str())
                            {
                                // First, check if authority is recognized
                                if !self.is_authority(auth_id) {
                                    continue;
                                }

                                // Get signature from the event
                                let signature =
                                    details.get("authority_signature").and_then(|v| v.as_str());

                                if let Some(public_key) = self.authority_public_keys.get(auth_id) {
                                    if let Some(sig) = signature {
                                        // Create signable payload from block data
                                        let payload = format!("{}{}", block.hash, auth_id);

                                        // Verify Ed25519 signature
                                        if verify_signature(public_key, payload.as_bytes(), sig) {
                                            return true;
                                        }
                                        // Signature verification failed
                                        continue;
                                    }
                                    // No signature but public key exists - fail if strict
                                    if strict_verification {
                                        continue;
                                    }
                                }

                                if !strict_verification || self.authority_public_keys.is_empty() {
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

        // Verify ZK Proof if configured
        if let Some(verifier) = &self.verifier {
            if !verify_block_zk_proof(block, verifier.as_ref(), false) {
                return false;
            }
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

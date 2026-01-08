//! Main Chain implementation for HieraChain Framework.
//!
//! This module implements the Main Chain class that acts as the root authority
//! in the HieraChain hierarchical structure. The Main Chain only stores proofs
//! from Sub-Chains, never detailed domain data, following framework guidelines.

use crate::core::block::Block;
use crate::core::blockchain::Blockchain;
use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::consensus::proof_of_authority::ProofOfAuthority;
use crate::core::consensus::proof_of_federation::ProofOfFederation;
use crate::security::zk_verifier::Verifier;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Consensus type for MainChain
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusType {
    /// Proof of Authority - for single organization or trusted authority
    ProofOfAuthority,
    /// Proof of Federation - for consortium with rotating leadership
    ProofOfFederation,
}

impl Default for ConsensusType {
    fn default() -> Self {
        ConsensusType::ProofOfAuthority
    }
}

impl ConsensusType {
    /// Parse from string (for Python interop)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "proof_of_federation" | "pof" | "federation" => ConsensusType::ProofOfFederation,
            _ => ConsensusType::ProofOfAuthority,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsensusType::ProofOfAuthority => "proof_of_authority",
            ConsensusType::ProofOfFederation => "proof_of_federation",
        }
    }
}

/// Wrapper enum to hold either PoA or PoF consensus
pub enum ConsensusWrapper {
    PoA(ProofOfAuthority),
    PoF(ProofOfFederation),
}

impl ConsensusWrapper {
    /// Create a new consensus instance based on type
    pub fn new(consensus_type: ConsensusType, name: &str) -> Self {
        match consensus_type {
            ConsensusType::ProofOfAuthority => {
                ConsensusWrapper::PoA(ProofOfAuthority::new(&format!("{}_PoA", name)))
            }
            ConsensusType::ProofOfFederation => {
                ConsensusWrapper::PoF(ProofOfFederation::new(&format!("{}_PoF", name)))
            }
        }
    }

    /// Add an authority/validator
    pub fn add_authority(
        &mut self,
        authority_id: String,
        metadata: Option<Map<String, Value>>,
    ) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.add_authority(authority_id, metadata),
            ConsensusWrapper::PoF(pof) => pof.add_authority(authority_id, metadata),
        }
    }

    /// Check if an entity is an authority
    pub fn is_authority(&self, authority_id: &str) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.is_authority(authority_id),
            ConsensusWrapper::PoF(pof) => pof.is_authority(authority_id),
        }
    }

    /// Finalize a block
    pub fn finalize_block(&mut self, block: &mut Block) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.finalize_block(block),
            ConsensusWrapper::PoF(pof) => pof.finalize_block(block),
        }
    }

    /// Get validator count
    pub fn get_validator_count(&self) -> u64 {
        match self {
            ConsensusWrapper::PoA(poa) => BaseConsensusTrait::get_validator_count(poa),
            ConsensusWrapper::PoF(pof) => BaseConsensusTrait::get_validator_count(pof),
        }
    }

    /// Validate a block
    pub fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.validate_block(block, previous_block),
            ConsensusWrapper::PoF(pof) => pof.validate_block(block, previous_block),
        }
    }

    /// Get consensus name
    pub fn name(&self) -> &str {
        match self {
            ConsensusWrapper::PoA(poa) => &poa.name,
            ConsensusWrapper::PoF(pof) => &pof.name,
        }
    }

    /// Get consensus type as string
    pub fn consensus_type_str(&self) -> &'static str {
        match self {
            ConsensusWrapper::PoA(_) => "proof_of_authority",
            ConsensusWrapper::PoF(_) => "proof_of_federation",
        }
    }

    /// Set ZK Verifier
    pub fn set_verifier(&mut self, verifier: Arc<dyn Verifier>) {
        match self {
            ConsensusWrapper::PoA(poa) => poa.set_verifier(verifier),
            ConsensusWrapper::PoF(pof) => pof.set_verifier(verifier),
        }
    }
}

/// Get current timestamp as f64
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Sanitize metadata to ensure only summary data is stored on Main Chain.
/// Removes any potentially large or detailed domain data.
fn sanitize_metadata_for_main_chain(metadata: &Value) -> Value {
    if let Some(obj) = metadata.as_object() {
        let mut sanitized = Map::new();

        // Allowed summary fields
        let allowed_fields = [
            "block_index",
            "block_hash",
            "events_count",
            "domain_type",
            "timestamp",
            "proof_timestamp",
            "sub_chain_name",
            "submitted_at",
            "merkle_root",
            "previous_hash",
            "summary",
            "status",
        ];

        for (key, value) in obj {
            // Only include allowed fields and non-object/non-array values
            if allowed_fields.contains(&key.as_str()) {
                sanitized.insert(key.clone(), value.clone());
            } else if !value.is_object() && !value.is_array() {
                // Allow simple scalar values
                sanitized.insert(key.clone(), value.clone());
            }
        }

        Value::Object(sanitized)
    } else {
        Value::Object(Map::new())
    }
}

/// Validate that proof metadata doesn't contain detailed domain data.
fn validate_proof_metadata(metadata: &Value) -> bool {
    if let Some(obj) = metadata.as_object() {
        // Reject if metadata contains potential domain-specific data
        let forbidden_fields = [
            "raw_data",
            "detailed_records",
            "transactions",
            "full_events",
            "private_data",
            "confidential",
            "sensitive",
        ];

        for field in &forbidden_fields {
            if obj.contains_key(*field) {
                return false;
            }
        }

        // Check for excessively large values
        let serialized = serde_json::to_string(obj).unwrap_or_default();
        if serialized.len() > 10000 {
            return false; // Metadata too large for Main Chain
        }

        true
    } else {
        true
    }
}

/// Main Chain implementation for the HieraChain framework.
///
/// The Main Chain acts as the root authority and:
/// - Only stores proofs from Sub-Chains, NOT detailed domain data
/// - Maintains the integrity of the entire hierarchical system
/// - Provides proof verification and chain coordination
/// - Supports both Proof of Authority and Proof of Federation consensus
pub struct MainChain {
    /// Base blockchain
    pub blockchain: Blockchain,
    /// Consensus mechanism (PoA or PoF)
    pub consensus: ConsensusWrapper,
    /// Consensus type
    pub consensus_type: ConsensusType,
    /// Registered Sub-Chains
    pub registered_sub_chains: HashSet<String>,
    /// Sub-Chain metadata
    pub sub_chain_metadata: HashMap<String, Value>,
    /// Total proof count
    pub proof_count: u64,
}

impl MainChain {
    /// Create a new Main Chain with default PoA consensus.
    ///
    /// # Arguments
    /// * `name` - Name identifier for the Main Chain
    pub fn new(name: &str) -> Self {
        Self::with_consensus(name, ConsensusType::ProofOfAuthority)
    }

    /// Create a new Main Chain with specified consensus type.
    ///
    /// # Arguments
    /// * `name` - Name identifier for the Main Chain
    /// * `consensus_type` - Type of consensus (PoA or PoF)
    pub fn with_consensus(name: &str, consensus_type: ConsensusType) -> Self {
        let mut consensus = ConsensusWrapper::new(consensus_type, "MainChain");

        // Register Main Chain as the primary authority
        let mut auth_metadata = Map::new();
        auth_metadata.insert(
            "role".to_string(),
            Value::String("root_authority".to_string()),
        );
        auth_metadata.insert(
            "permissions".to_string(),
            Value::Array(vec![
                Value::String("proof_validation".to_string()),
                Value::String("sub_chain_registration".to_string()),
            ]),
        );
        auth_metadata.insert(
            "created_at".to_string(),
            Value::Number(
                serde_json::Number::from_f64(current_timestamp())
                    .unwrap_or(serde_json::Number::from(0)),
            ),
        );

        consensus.add_authority("main_chain".to_string(), Some(auth_metadata));

        MainChain {
            blockchain: Blockchain::new(name),
            consensus,
            consensus_type,
            registered_sub_chains: HashSet::new(),
            sub_chain_metadata: HashMap::new(),
            proof_count: 0,
        }
    }

    /// Set ZK Verifier
    pub fn set_verifier(&mut self, verifier: Arc<dyn Verifier>) {
        self.consensus.set_verifier(verifier);
    }

    /// Get chain name
    pub fn name(&self) -> &str {
        &self.blockchain.name
    }

    /// Check if Sub-Chain is registered
    pub fn is_registered(&self, sub_chain_name: &str) -> bool {
        self.registered_sub_chains.contains(sub_chain_name)
    }

    /// Register a Sub-Chain with the Main Chain.
    ///
    /// # Arguments
    /// * `sub_chain_name` - Name of the Sub-Chain to register
    /// * `metadata` - Metadata about the Sub-Chain
    ///
    /// # Returns
    /// True if Sub-Chain was registered successfully
    pub fn register_sub_chain(&mut self, sub_chain_name: &str, metadata: Option<Value>) -> bool {
        if self.registered_sub_chains.contains(sub_chain_name) {
            return false;
        }

        self.registered_sub_chains
            .insert(sub_chain_name.to_string());

        let meta = metadata.clone().unwrap_or(Value::Object(Map::new()));
        self.sub_chain_metadata
            .insert(sub_chain_name.to_string(), meta.clone());

        // Add Sub-Chain as an authority for proof submission
        let mut auth_metadata = Map::new();
        auth_metadata.insert("role".to_string(), Value::String("sub_chain".to_string()));
        auth_metadata.insert(
            "permissions".to_string(),
            Value::Array(vec![Value::String("proof_submission".to_string())]),
        );
        auth_metadata.insert(
            "registered_at".to_string(),
            Value::Number(
                serde_json::Number::from_f64(current_timestamp())
                    .unwrap_or(serde_json::Number::from(0)),
            ),
        );
        auth_metadata.insert("metadata".to_string(), meta);

        self.consensus
            .add_authority(sub_chain_name.to_string(), Some(auth_metadata));

        // Create registration event
        let sanitized_metadata =
            sanitize_metadata_for_main_chain(&metadata.unwrap_or(Value::Object(Map::new())));

        let registration_event = serde_json::json!({
            "entity_id": sub_chain_name,
            "event": "sub_chain_registration",
            "timestamp": current_timestamp(),
            "details": {
                "sub_chain_name": sub_chain_name,
                "registered_by": "main_chain",
                "metadata": sanitized_metadata
            }
        });

        let _ = self.blockchain.add_event(registration_event);
        true
    }

    /// Add a proof from a Sub-Chain to the Main Chain.
    ///
    /// This is the critical method that follows framework guidelines:
    /// - Only stores proof evidence, NOT domain data
    /// - Metadata contains summary data only
    ///
    /// # Arguments
    /// * `sub_chain_name` - Name of the Sub-Chain submitting the proof
    /// * `proof_hash` - Hash of the block being proven
    /// * `metadata` - Summary metadata (NOT detailed domain data)
    ///
    /// # Returns
    /// True if proof was added successfully
    pub fn add_proof(&mut self, sub_chain_name: &str, proof_hash: &str, metadata: Value) -> bool {
        // Validate Sub-Chain is registered
        if !self.registered_sub_chains.contains(sub_chain_name) {
            return false;
        }

        // Validate metadata is suitable for Main Chain
        if !validate_proof_metadata(&metadata) {
            return false;
        }

        // Sanitize metadata
        let sanitized_metadata = sanitize_metadata_for_main_chain(&metadata);

        self.proof_count += 1;

        // Create proof submission event
        let event = serde_json::json!({
            "type": "sub_chain_proof",
            "sub_chain": sub_chain_name,
            "proof_hash": proof_hash,
            "metadata": sanitized_metadata,
            "entity_id": sub_chain_name,
            "event": "proof_submission",
            "timestamp": current_timestamp(),
            "details": {
                "sub_chain_name": sub_chain_name,
                "proof_hash": proof_hash,
                "proof_id": format!("PROOF-{}", self.proof_count),
                "submitted_at": current_timestamp()
            }
        });

        self.blockchain.add_event(event).is_ok()
    }

    /// Verify a proof exists in the Main Chain.
    ///
    /// # Arguments
    /// * `proof_hash` - Hash of the proof to verify
    /// * `sub_chain_name` - Name of the Sub-Chain that submitted the proof
    ///
    /// # Returns
    /// True if proof exists and is valid
    pub fn verify_proof(&self, proof_hash: &str, sub_chain_name: &str) -> bool {
        // Search in blocks
        for block in &self.blockchain.chain {
            for event in &block.events {
                if let (Some(event_type), Some(details)) =
                    (event.get("event"), event.get("details"))
                {
                    if event_type.as_str() == Some("proof_submission") {
                        if let (Some(hash), Some(name)) = (
                            details.get("proof_hash").and_then(|v| v.as_str()),
                            details.get("sub_chain_name").and_then(|v| v.as_str()),
                        ) {
                            if hash == proof_hash && name == sub_chain_name {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // Search in pending events
        for event in &self.blockchain.pending_events {
            if let (Some(event_type), Some(details)) = (event.get("event"), event.get("details")) {
                if event_type.as_str() == Some("proof_submission") {
                    if let (Some(hash), Some(name)) = (
                        details.get("proof_hash").and_then(|v| v.as_str()),
                        details.get("sub_chain_name").and_then(|v| v.as_str()),
                    ) {
                        if hash == proof_hash && name == sub_chain_name {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// Get all proofs submitted by a specific Sub-Chain.
    pub fn get_proofs_by_sub_chain(&self, sub_chain_name: &str) -> Vec<Value> {
        let mut proofs = Vec::new();

        // Search in blocks
        for block in &self.blockchain.chain {
            for event in &block.events {
                if let (Some(event_type), Some(details)) =
                    (event.get("event"), event.get("details"))
                {
                    if event_type.as_str() == Some("proof_submission") {
                        if let Some(name) = details.get("sub_chain_name").and_then(|v| v.as_str()) {
                            if name == sub_chain_name {
                                proofs.push(event.clone());
                            }
                        }
                    }
                }
            }
        }

        // Search in pending events
        for event in &self.blockchain.pending_events {
            if let (Some(event_type), Some(details)) = (event.get("event"), event.get("details")) {
                if event_type.as_str() == Some("proof_submission") {
                    if let Some(name) = details.get("sub_chain_name").and_then(|v| v.as_str()) {
                        if name == sub_chain_name {
                            proofs.push(event.clone());
                        }
                    }
                }
            }
        }

        proofs
    }

    /// Get summary information about a Sub-Chain.
    pub fn get_sub_chain_summary(&self, sub_chain_name: &str) -> Value {
        if !self.registered_sub_chains.contains(sub_chain_name) {
            return Value::Object(Map::new());
        }

        let proofs = self.get_proofs_by_sub_chain(sub_chain_name);
        let latest_proof = proofs.last().cloned();
        let metadata = self
            .sub_chain_metadata
            .get(sub_chain_name)
            .cloned()
            .unwrap_or(Value::Object(Map::new()));

        serde_json::json!({
            "sub_chain_name": sub_chain_name,
            "registered": true,
            "total_proofs": proofs.len(),
            "metadata": metadata,
            "latest_proof": latest_proof,
            "registration_time": metadata.get("registered_at")
        })
    }

    /// Finalize pending events into a new block using consensus.
    pub fn finalize_block(&mut self) -> Option<Block> {
        if self.blockchain.pending_events.is_empty() {
            return None;
        }

        // Create block
        let mut new_block = match self.blockchain.create_block(None) {
            Ok(block) => block,
            Err(_) => return None,
        };

        // Finalize with PoA consensus
        if !self.consensus.finalize_block(&mut new_block) {
            return None;
        }

        // Add to chain
        let block_clone = new_block.clone();
        if self.blockchain.add_block(new_block) {
            Some(block_clone)
        } else {
            None
        }
    }

    /// Finalize a block and return information about it.
    pub fn finalize_main_chain_block(&mut self) -> Option<Value> {
        let block = self.finalize_block()?;

        Some(serde_json::json!({
            "block_index": block.index,
            "block_hash": block.hash,
            "events_count": block.events.len(),
            "finalized_at": current_timestamp()
        }))
    }

    /// Validate the format of a Sub-Chain proof submission.
    pub fn validate_sub_chain_proof_format(&self, proof_data: &Value) -> bool {
        let required_fields = ["sub_chain_name", "proof_hash", "metadata"];

        if let Some(obj) = proof_data.as_object() {
            for field in &required_fields {
                if !obj.contains_key(*field) {
                    return false;
                }
            }

            // Validate Sub-Chain is registered
            if let Some(name) = obj.get("sub_chain_name").and_then(|v| v.as_str()) {
                if !self.registered_sub_chains.contains(name) {
                    return false;
                }
            } else {
                return false;
            }

            // Validate metadata
            if let Some(metadata) = obj.get("metadata") {
                if !validate_proof_metadata(metadata) {
                    return false;
                }
            }

            true
        } else {
            false
        }
    }

    /// Generate an integrity report for the entire hierarchical system.
    pub fn get_hierarchical_integrity_report(&self) -> Value {
        let mut sub_chains_report = Map::new();

        for sub_chain_name in &self.registered_sub_chains {
            sub_chains_report.insert(
                sub_chain_name.clone(),
                self.get_sub_chain_summary(sub_chain_name),
            );
        }

        serde_json::json!({
            "main_chain": {
                "name": self.blockchain.name,
                "blocks": self.blockchain.chain.len(),
                "valid": self.blockchain.is_chain_valid(),
                "latest_hash": self.blockchain.get_latest_block().hash
            },
            "sub_chains": sub_chains_report,
            "total_proofs": self.proof_count,
            "registered_sub_chains": self.registered_sub_chains.len(),
            "system_integrity": if self.blockchain.is_chain_valid() { "healthy" } else { "compromised" }
        })
    }

    /// Get comprehensive statistics about the Main Chain.
    pub fn get_main_chain_stats(&self) -> Value {
        let base_stats = self.blockchain.get_chain_stats();
        let proof_events = self.blockchain.get_events_by_type("proof_submission");

        let mut stats = base_stats.as_object().cloned().unwrap_or_default();
        stats.insert("role".to_string(), Value::String("main_chain".to_string()));
        stats.insert(
            "registered_sub_chains".to_string(),
            Value::Number((self.registered_sub_chains.len() as u64).into()),
        );
        stats.insert(
            "sub_chains".to_string(),
            Value::Array(
                self.registered_sub_chains
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
        stats.insert(
            "total_proofs".to_string(),
            Value::Number((proof_events.len() as u64).into()),
        );
        stats.insert(
            "consensus_type".to_string(),
            Value::String(self.consensus.name().to_string()),
        );
        stats.insert(
            "authorities".to_string(),
            Value::Number(self.consensus.get_validator_count().into()),
        );

        Value::Object(stats)
    }

    /// Get the latest block
    pub fn get_latest_block(&self) -> &Block {
        self.blockchain.get_latest_block()
    }

    /// Check if chain is valid
    pub fn is_chain_valid(&self) -> bool {
        self.blockchain.is_chain_valid()
    }

    /// Get chain length
    pub fn chain_length(&self) -> usize {
        self.blockchain.chain.len()
    }

    /// Check if authority
    pub fn is_authority(&self, authority_id: &str) -> bool {
        self.consensus.is_authority(authority_id)
    }

    /// Submit proof from a Sub-Chain.
    ///
    /// This method receives cryptographic proofs from Sub-Chains and stores them
    /// as events on the Main Chain.
    ///
    /// # Arguments
    /// * `sub_chain_name` - Name of the Sub-Chain submitting the proof
    /// * `metadata` - Proof metadata (summary data only)
    ///
    /// # Returns
    /// True if proof was accepted and stored
    pub fn submit_proof(&mut self, sub_chain_name: &str, metadata: Value) -> bool {
        // Check if sub-chain is registered
        if !self
            .registered_sub_chains
            .contains(&sub_chain_name.to_string())
        {
            // Auto-register if not present (for ease of use)
            self.register_sub_chain(sub_chain_name, None);
        }

        // Create proof event
        let proof_event = json!({
            "entity_id": sub_chain_name,
            "event": "proof_submission",
            "timestamp": current_timestamp(),
            "details": {
                "sub_chain_name": sub_chain_name,
                "proof_metadata": metadata,
                "received_at": current_timestamp(),
                "proof_index": self.proof_count + 1
            }
        });

        // Add event and try to finalize block
        if self.blockchain.add_event(proof_event).is_ok() {
            self.proof_count += 1;
            true
        } else {
            false
        }
    }

    /// Validate a new block including consensus rules.
    ///
    /// # Arguments
    /// * `block` - Block to validate
    ///
    /// # Returns
    /// True if block is valid
    pub fn is_valid_new_block(&self, block: &Block) -> bool {
        let previous_block = self.blockchain.get_latest_block();

        // Check basic block validity
        if block.index != previous_block.index + 1 {
            return false;
        }

        if block.previous_hash != previous_block.hash {
            return false;
        }

        // Check consensus rules
        self.consensus.validate_block(block, previous_block)
    }
}

impl std::fmt::Display for MainChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MainChain(blocks={}, sub_chains={}, proofs={})",
            self.blockchain.chain.len(),
            self.registered_sub_chains.len(),
            self.proof_count
        )
    }
}

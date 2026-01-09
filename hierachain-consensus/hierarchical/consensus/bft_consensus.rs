//! BFT Consensus Implementation
//!
//! This module implements the Byzantine Fault Tolerance consensus mechanism with
//! 3-phase protocol (pre-prepare, prepare, commit) for enterprise blockchain applications.

use crate::error_mitigation::error_classifier::ErrorClassifier;
use crate::error_mitigation::validator::ConsensusValidator;
use crate::hierarchical::consensus::message::{BFTMessage, MessageType};
use crate::hierarchical::consensus::state::ConsensusState;
use crate::security::key_provider::{KeyProvider, LocalKeyProvider};
use crate::security::security_utils::{verify_signature, KeyPair};
use crate::security::zk_verifier::Verifier;

use log::{debug, info, warn};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Mutex;

/// Consensus error types
#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("BFT requires at least {required} nodes to tolerate {f} faults, but only {actual} nodes provided")]
    InsufficientNodes {
        required: usize,
        f: usize,
        actual: usize,
    },

    #[error("Cryptographic keys are required for BFT consensus")]
    MissingKeys,

    #[error("Cryptographic error: {0}")]
    CryptoError(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Not primary node")]
    NotPrimary,

    #[error("View mismatch: expected {expected}, got {actual}")]
    ViewMismatch { expected: u64, actual: u64 },
}

/// Inner state of BFT consensus (protected by mutex)
#[allow(dead_code)]
struct BFTConsensusInner {
    /// Current view number
    view: u64,

    /// Current sequence number
    sequence_number: u64,

    /// Current consensus state
    state: ConsensusState,

    /// Current request being processed
    current_request: Option<HashMap<String, serde_json::Value>>,

    /// Pre-prepare messages by sequence number
    pre_prepare_messages: HashMap<u64, BFTMessage>,

    /// Prepare messages by sequence number
    prepare_messages: HashMap<u64, Vec<BFTMessage>>,

    /// Commit messages by sequence number
    commit_messages: HashMap<u64, Vec<BFTMessage>>,

    /// View change votes by view number
    view_change_votes: HashMap<u64, Vec<BFTMessage>>,

    /// Last committed sequence number
    committed_sequence: i64,

    /// Pending requests queue
    pending_requests: Vec<HashMap<String, serde_json::Value>>,

    /// Message log for audit
    message_log: Vec<BFTMessage>,

    /// Node response times for monitoring
    node_response_times: HashMap<String, Vec<f64>>,

    /// Node failure counts
    node_failure_counts: HashMap<String, u32>,

    /// Last heartbeat timestamp
    last_heartbeat: f64,

    /// Shutting down flag
    shutting_down: bool,
}

impl BFTConsensusInner {
    fn new() -> Self {
        Self {
            view: 0,
            sequence_number: 0,
            state: ConsensusState::Idle,
            current_request: None,
            pre_prepare_messages: HashMap::new(),
            prepare_messages: HashMap::new(),
            commit_messages: HashMap::new(),
            view_change_votes: HashMap::new(),
            committed_sequence: -1,
            pending_requests: Vec::new(),
            message_log: Vec::new(),
            node_response_times: HashMap::new(),
            node_failure_counts: HashMap::new(),
            last_heartbeat: current_timestamp(),
            shutting_down: false,
        }
    }
}

/// Byzantine Fault Tolerance consensus implementation
#[allow(dead_code)]
pub struct BFTConsensus {
    /// Node ID
    node_id: String,

    /// All validator nodes
    all_nodes: Vec<String>,

    /// Maximum Byzantine faults tolerated
    f: usize,

    /// Total number of nodes
    n: usize,

    /// Key provider for signing
    key_provider: Arc<dyn KeyProvider>,

    /// Public keys of all nodes
    node_public_keys: HashMap<String, String>,

    /// Inner mutable state
    inner: Arc<Mutex<BFTConsensusInner>>,

    /// Consensus validator
    consensus_validator: Option<ConsensusValidator>,

    /// Error classifier
    error_classifier: Arc<Mutex<Option<ErrorClassifier>>>,

    /// Verification strictness level
    verification_strictness: String,

    /// Auto recovery enabled
    auto_recovery_enabled: bool,

    /// View change timeout in seconds
    view_change_timeout: f64,

    /// Maximum failure count before action
    max_failure_count: u32,

    /// ZK Proof Verifier (optional)
    verifier: Option<Arc<dyn Verifier>>,
}

impl BFTConsensus {
    /// Create a new BFT consensus instance
    ///
    /// # Arguments
    /// * `node_id` - Current node ID
    /// * `all_nodes` - All validator nodes in the network
    /// * `f` - Maximum number of Byzantine faults tolerated
    /// * `keypair` - Ed25519 KeyPair for this node
    /// * `node_public_keys` - Map of node_id -> public_key_hex
    ///
    /// # Returns
    /// * `Result<Self, ConsensusError>` - New consensus instance or error
    pub fn new(
        node_id: String,
        all_nodes: Vec<String>,
        f: usize,
        keypair: KeyPair,
        node_public_keys: HashMap<String, String>,
    ) -> Result<Self, ConsensusError> {
        let n = all_nodes.len();

        // Validate BFT requirements (n >= 3f + 1)
        let required = 3 * f + 1;
        if n < required {
            return Err(ConsensusError::InsufficientNodes {
                required,
                f,
                actual: n,
            });
        }

        // Wrap keypair in LocalKeyProvider
        let key_provider: Arc<dyn KeyProvider> = Arc::new(LocalKeyProvider::new(keypair));

        // Initialize consensus validator
        let consensus_validator = Some(ConsensusValidator::new(f));

        // Validate using consensus validator
        if let Some(ref validator) = consensus_validator {
            validator
                .validate_nodes(&all_nodes)
                .map_err(|e| ConsensusError::InvalidMessage(e.to_string()))?;
        }

        if node_public_keys.is_empty() {
            warn!("No public keys provided for validators. Signature verification may fail.");
        }

        info!(
            "BFTConsensus initialized: node_id={}, n={}, f={}, required={}",
            node_id, n, f, required
        );

        Ok(Self {
            node_id,
            all_nodes,
            f,
            n,
            key_provider,
            node_public_keys,
            inner: Arc::new(Mutex::new(BFTConsensusInner::new())),
            consensus_validator,
            error_classifier: Arc::new(Mutex::new(Some(ErrorClassifier::new()))),
            verification_strictness: "high".to_string(),
            auto_recovery_enabled: false,
            view_change_timeout: 30.0,
            max_failure_count: 3,
            verifier: None,
        })
    }

    /// Set the ZK Proof Verifier
    pub fn set_verifier(&mut self, verifier: Arc<dyn Verifier>) {
        info!(
            "BFTConsensus: ZK Verifier enabled: {}",
            verifier.verifier_type()
        );
        self.verifier = Some(verifier);
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get fault tolerance value
    pub fn fault_tolerance(&self) -> usize {
        self.f
    }

    /// Get total node count
    pub fn node_count(&self) -> usize {
        self.n
    }

    /// Determine the primary node for current view
    pub async fn primary(&self) -> String {
        let inner = self.inner.lock().await;
        self.all_nodes[inner.view as usize % self.n].clone()
    }

    /// Check if current node is primary
    pub async fn is_primary(&self) -> bool {
        self.primary().await == self.node_id
    }

    /// Client request to the consensus protocol
    ///
    /// # Arguments
    /// * `operation` - The operation to be consensus on
    ///
    /// # Returns
    /// * `Result<bool, ConsensusError>` - True if request was accepted
    pub async fn request(
        &self,
        operation: HashMap<String, serde_json::Value>,
    ) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;

        if !self.is_primary_inner(&inner) {
            // In production, would forward to primary
            debug!("Not primary, forwarding request");
            return Ok(false);
        }

        // Validate ZK Proof if present in operation
        if !self.validate_zk_proof_inner(&operation) {
            warn!("Invalid ZK Proof in request");
            return Ok(false);
        }

        // Primary node creates pre-prepare message
        inner.sequence_number += 1;
        let seq = inner.sequence_number;

        inner.current_request = Some({
            let mut req = HashMap::new();
            req.insert("operation".to_string(), serde_json::json!(operation));
            req.insert(
                "client_id".to_string(),
                operation
                    .get("client_id")
                    .cloned()
                    .unwrap_or(serde_json::json!("unknown")),
            );
            req.insert(
                "timestamp".to_string(),
                serde_json::json!(current_timestamp()),
            );
            req
        });

        // Create digest
        let digest = Self::hash_request(inner.current_request.as_ref().unwrap());

        // Create pre-prepare message
        let mut data = HashMap::new();
        data.insert(
            "request".to_string(),
            serde_json::json!(inner.current_request),
        );
        data.insert("digest".to_string(), serde_json::Value::String(digest));

        let mut pre_prepare_msg = BFTMessage::with_data(
            MessageType::PrePrepare,
            inner.view,
            seq,
            self.node_id.clone(),
            data,
        );

        // Sign the message
        let signature = self
            .key_provider
            .sign(&pre_prepare_msg.get_signable_payload())
            .map_err(|e| ConsensusError::CryptoError(e.to_string()))?;
        pre_prepare_msg.set_signature(signature);

        // Store and update state
        inner
            .pre_prepare_messages
            .insert(seq, pre_prepare_msg.clone());
        inner.state = ConsensusState::PrePrepared;
        inner.message_log.push(pre_prepare_msg);

        info!(
            "Created pre-prepare for sequence {} in view {}",
            seq, inner.view
        );

        Ok(true)
    }

    /// Handle incoming consensus messages
    ///
    /// # Arguments
    /// * `message` - Message dictionary
    ///
    /// # Returns
    /// * `Result<bool, ConsensusError>` - True if message was processed successfully
    pub async fn handle_message(
        &self,
        message: HashMap<String, serde_json::Value>,
    ) -> Result<bool, ConsensusError> {
        // Parse message type
        let msg_type_str = message
            .get("message_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConsensusError::InvalidMessage("Missing message_type".to_string()))?;

        let msg_type = MessageType::from_str(msg_type_str).ok_or_else(|| {
            ConsensusError::InvalidMessage(format!("Invalid message_type: {}", msg_type_str))
        })?;

        // Convert to BFTMessage
        let bft_message = BFTMessage::from_dict(&message).ok_or_else(|| {
            ConsensusError::InvalidMessage("Failed to parse BFTMessage".to_string())
        })?;

        // Validate message
        if !self.validate_message(&bft_message).await {
            return Ok(false);
        }

        // Handle based on type
        match msg_type {
            MessageType::PrePrepare => self.handle_pre_prepare(bft_message).await,
            MessageType::Prepare => self.handle_prepare(bft_message).await,
            MessageType::Commit => self.handle_commit(bft_message).await,
            MessageType::ViewChange => self.handle_view_change(bft_message).await,
            MessageType::NewView => self.handle_new_view(bft_message).await,
        }
    }

    /// Handle pre-prepare message
    async fn handle_pre_prepare(&self, message: BFTMessage) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;

        // Don't process if we're the primary
        if self.is_primary_inner(&inner) {
            return Ok(false);
        }

        // Verify view and sequence number
        if message.view != inner.view {
            return Ok(false);
        }

        if message.sequence_number as i64 <= inner.committed_sequence {
            return Ok(false);
        }

        // Verify sender is primary
        let primary = self.all_nodes[inner.view as usize % self.n].clone();
        if message.sender_id != primary {
            return Ok(false);
        }

        // Verify signature
        if !self.verify_signature_inner(&message) {
            return Ok(false);
        }

        // Verify ZK Proof in request payload if present
        if let Some(request) = message.data.get("request").and_then(|v| v.as_object()) {
            if let Some(operation) = request.get("operation").and_then(|v| v.as_object()) {
                let mut op_map = HashMap::new();
                for (k, v) in operation {
                    op_map.insert(k.clone(), v.clone());
                }
                if !self.validate_zk_proof_inner(&op_map) {
                    warn!("Invalid ZK Proof in pre-prepare message");
                    return Ok(false);
                }
            }
        }

        // Accept message
        inner
            .pre_prepare_messages
            .insert(message.sequence_number, message.clone());
        inner.state = ConsensusState::PrePrepared;

        // Create prepare message
        let mut data = HashMap::new();
        if let Some(digest) = message.get_digest() {
            data.insert(
                "digest".to_string(),
                serde_json::Value::String(digest.to_string()),
            );
        }

        let mut prepare_msg = BFTMessage::with_data(
            MessageType::Prepare,
            inner.view,
            message.sequence_number,
            self.node_id.clone(),
            data,
        );

        // Sign
        let signature = self
            .key_provider
            .sign(&prepare_msg.get_signable_payload())
            .map_err(|e| ConsensusError::CryptoError(e.to_string()))?;
        prepare_msg.set_signature(signature);

        inner.message_log.push(prepare_msg);
        inner.last_heartbeat = current_timestamp();

        info!(
            "Handled pre-prepare for sequence {} from {}",
            message.sequence_number, message.sender_id
        );

        Ok(true)
    }

    /// Handle prepare message
    async fn handle_prepare(&self, message: BFTMessage) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;
        let seq = message.sequence_number;

        // Verify we have pre-prepare or are in correct state
        if !inner.pre_prepare_messages.contains_key(&seq)
            && inner.state != ConsensusState::PrePrepared
        {
            return Ok(false);
        }

        // Verify signature
        if !self.verify_signature_inner(&message) {
            self.log_node_behavior_inner(&mut inner, &message.sender_id, "invalid_signature");
            return Ok(false);
        }

        // Verify digest matches pre-prepare
        if let Some(pre_prepare) = inner.pre_prepare_messages.get(&seq) {
            if pre_prepare.get_digest() != message.get_digest() {
                self.log_node_behavior_inner(&mut inner, &message.sender_id, "digest_mismatch");
                return Ok(false);
            }
        }

        // Store message (avoid duplicates)
        let prepare_list = inner.prepare_messages.entry(seq).or_insert_with(Vec::new);
        if prepare_list
            .iter()
            .any(|m| m.sender_id == message.sender_id)
        {
            return Ok(false);
        }
        prepare_list.push(message.clone());

        // Check if we have enough prepare messages (2f)
        if prepare_list.len() >= 2 * self.f {
            // Create commit message
            let mut data = HashMap::new();
            if let Some(digest) = message.get_digest() {
                data.insert(
                    "digest".to_string(),
                    serde_json::Value::String(digest.to_string()),
                );
            }

            let mut commit_msg = BFTMessage::with_data(
                MessageType::Commit,
                inner.view,
                seq,
                self.node_id.clone(),
                data,
            );

            let signature = self
                .key_provider
                .sign(&commit_msg.get_signable_payload())
                .map_err(|e| ConsensusError::CryptoError(e.to_string()))?;
            commit_msg.set_signature(signature);

            inner.message_log.push(commit_msg);
            inner.state = ConsensusState::Prepared;

            info!("Prepared sequence {}, sending commit", seq);
        }

        Ok(true)
    }

    /// Handle commit message
    async fn handle_commit(&self, message: BFTMessage) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;
        let seq = message.sequence_number;

        // Verify we have relevant messages
        if !inner.pre_prepare_messages.contains_key(&seq)
            && !inner.prepare_messages.contains_key(&seq)
        {
            return Ok(false);
        }

        // Verify signature
        if !self.verify_signature_inner(&message) {
            self.log_node_behavior_inner(&mut inner, &message.sender_id, "invalid_signature");
            return Ok(false);
        }

        // Store message (avoid duplicates)
        let commit_list = inner.commit_messages.entry(seq).or_insert_with(Vec::new);
        if commit_list.iter().any(|m| m.sender_id == message.sender_id) {
            return Ok(false);
        }
        commit_list.push(message);

        // Check if we have enough commit messages (2f + 1)
        if commit_list.len() >= 2 * self.f + 1 {
            // Execute operation
            if let Some(pre_prepare) = inner.pre_prepare_messages.get(&seq) {
                if let Some(request) = pre_prepare.data.get("request") {
                    self.execute_operation_inner(&inner, request);
                }
            }

            inner.committed_sequence = inner.committed_sequence.max(seq as i64);
            inner.state = ConsensusState::Committed;

            // Cleanup old messages
            self.cleanup_old_messages_inner(&mut inner, seq);

            info!("Committed sequence {}", seq);
            return Ok(true);
        }

        Ok(false)
    }

    /// Handle view change message
    async fn handle_view_change(&self, message: BFTMessage) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;
        let new_view = message.view;

        // Basic validation
        if new_view <= inner.view {
            return Ok(false);
        }

        // Verify signature
        if !self.verify_signature_inner(&message) {
            self.log_node_behavior_inner(&mut inner, &message.sender_id, "invalid_signature");
            return Ok(false);
        }

        // Store vote
        let votes = inner
            .view_change_votes
            .entry(new_view)
            .or_insert_with(Vec::new);
        if !votes.iter().any(|m| m.sender_id == message.sender_id) {
            votes.push(message);
        }

        // Check for quorum (2f + 1)
        if votes.len() >= 2 * self.f + 1 {
            let new_primary = &self.all_nodes[new_view as usize % self.n];

            if &self.node_id == new_primary && inner.state == ConsensusState::ViewChange {
                info!(
                    "Quorum reached for View {}. I am Primary. Broadcasting NEW-VIEW.",
                    new_view
                );
                // In production, would broadcast NEW-VIEW message
            }
        }

        Ok(true)
    }

    /// Handle new view message
    async fn handle_new_view(&self, message: BFTMessage) -> Result<bool, ConsensusError> {
        let mut inner = self.inner.lock().await;
        let new_view = message.view;

        // Validation
        if new_view <= inner.view {
            return Ok(false);
        }

        if !self.verify_signature_inner(&message) {
            self.log_node_behavior_inner(&mut inner, &message.sender_id, "invalid_signature");
            return Ok(false);
        }

        // Validate proof
        if let Some(proof) = message.data.get("proof") {
            if !self.validate_view_change_proof_inner(&inner, new_view, proof) {
                warn!("Invalid NEW-VIEW proof from {}", message.sender_id);
                return Ok(false);
            }
        }

        // Enter new view
        info!("Accepted NEW-VIEW {} from {}", new_view, message.sender_id);
        inner.view = new_view;
        inner.state = ConsensusState::Idle;
        inner.last_heartbeat = current_timestamp();

        Ok(true)
    }

    /// Get current consensus status
    pub async fn get_consensus_status(&self) -> HashMap<String, serde_json::Value> {
        let inner = self.inner.lock().await;

        let mut status = HashMap::new();
        status.insert("node_id".to_string(), serde_json::json!(self.node_id));
        status.insert("view".to_string(), serde_json::json!(inner.view));
        status.insert(
            "sequence_number".to_string(),
            serde_json::json!(inner.sequence_number),
        );
        status.insert("state".to_string(), serde_json::json!(inner.state.as_str()));
        status.insert(
            "is_primary".to_string(),
            serde_json::json!(self.is_primary_inner(&inner)),
        );
        status.insert(
            "primary_node".to_string(),
            serde_json::json!(self.all_nodes[inner.view as usize % self.n]),
        );
        status.insert(
            "committed_sequence".to_string(),
            serde_json::json!(inner.committed_sequence),
        );
        status.insert("fault_tolerance".to_string(), serde_json::json!(self.f));
        status.insert("total_nodes".to_string(), serde_json::json!(self.n));
        status.insert(
            "last_heartbeat".to_string(),
            serde_json::json!(inner.last_heartbeat),
        );

        status
    }

    /// Shutdown consensus mechanism
    pub async fn shutdown(&self) {
        let mut inner = self.inner.lock().await;
        inner.shutting_down = true;
        info!("BFTConsensus shutting down");
    }

    // === Private helper methods ===

    fn is_primary_inner(&self, inner: &BFTConsensusInner) -> bool {
        self.all_nodes[inner.view as usize % self.n] == self.node_id
    }

    fn verify_signature_inner(&self, message: &BFTMessage) -> bool {
        if message.signature.is_empty() {
            return false;
        }

        let public_key = match self.node_public_keys.get(&message.sender_id) {
            Some(pk) => pk,
            None => return false,
        };

        let payload = message.get_signable_payload();
        verify_signature(public_key, &payload, &message.signature)
    }

    async fn validate_message(&self, message: &BFTMessage) -> bool {
        // Basic validation
        if !self.all_nodes.contains(&message.sender_id) {
            return false;
        }

        if message.sequence_number == 0 && message.message_type != MessageType::ViewChange {
            // Sequence 0 is only valid for certain message types
        }

        // Signature validation
        if !self.verify_signature_inner(message) {
            if self.verification_strictness == "high" {
                return false;
            }
            // With lower strictness, log the issue
            let mut inner = self.inner.lock().await;
            self.log_node_behavior_inner(
                &mut inner,
                &message.sender_id,
                "signature_verification_failed",
            );
        }

        true
    }

    fn log_node_behavior_inner(&self, inner: &mut BFTConsensusInner, node_id: &str, issue: &str) {
        let count = inner
            .node_failure_counts
            .entry(node_id.to_string())
            .or_insert(0);
        *count += 1;

        warn!(
            "Node {} exhibited {}: failure_count={}",
            node_id, issue, count
        );

        if *count >= self.max_failure_count && self.auto_recovery_enabled {
            info!(
                "Node {} exceeded failure threshold, may initiate view change",
                node_id
            );
        }
    }

    fn execute_operation_inner(&self, _inner: &BFTConsensusInner, operation: &serde_json::Value) {
        debug!("Executing operation: {:?}", operation);
        // In production, this would add event to blockchain
    }

    fn cleanup_old_messages_inner(&self, inner: &mut BFTConsensusInner, committed_seq: u64) {
        let cutoff = committed_seq.saturating_sub(10);

        inner.pre_prepare_messages.retain(|&k, _| k >= cutoff);
        inner.prepare_messages.retain(|&k, _| k >= cutoff);
        inner.commit_messages.retain(|&k, _| k >= cutoff);

        // Trim message log
        if inner.message_log.len() > 1000 {
            let drain_count = inner.message_log.len() - 500;
            inner.message_log.drain(0..drain_count);
        }
    }

    fn validate_view_change_proof_inner(
        &self,
        _inner: &BFTConsensusInner,
        _view: u64,
        proof: &serde_json::Value,
    ) -> bool {
        let proof_array = match proof.as_array() {
            Some(arr) => arr,
            None => return false,
        };

        if proof_array.len() < 2 * self.f + 1 {
            return false;
        }

        // In production, would validate each message signature
        true
    }

    fn hash_request(request: &HashMap<String, serde_json::Value>) -> String {
        let request_str = serde_json::to_string(request).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(request_str.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn validate_zk_proof_inner(&self, operation: &HashMap<String, serde_json::Value>) -> bool {
        let verifier = match &self.verifier {
            Some(v) => v,
            None => return true,
        };

        // Check for proof strings (hex)
        let zk_proof_hex = match operation.get("zk_proof").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return true,
        };

        let zk_inputs_hex = match operation.get("zk_public_inputs").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return true,
        };

        // Decode hex
        let proof_bytes = match hex::decode(zk_proof_hex) {
            Ok(b) => b,
            Err(_) => {
                warn!("Invalid hex in zk_proof");
                return false;
            }
        };

        let inputs_bytes = match hex::decode(zk_inputs_hex) {
            Ok(b) => b,
            Err(_) => {
                warn!("Invalid hex in zk_public_inputs");
                return false;
            }
        };

        // Verify
        match verifier.verify(&proof_bytes, &inputs_bytes) {
            Ok(valid) => {
                if !valid {
                    warn!("ZK Proof verification failed for operation");
                } else {
                    debug!("ZK Proof verified");
                }
                valid
            }
            Err(e) => {
                warn!("ZK Verifier error: {}", e);
                false
            }
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

/// Factory function to create a BFT consensus network
///
/// # Arguments
/// * `node_configs` - List of (node_id, keypair) configurations
/// * `fault_tolerance` - Number of Byzantine faults to tolerate
///
/// # Returns
/// * `Result<HashMap<String, BFTConsensus>, ConsensusError>` - Map of node_id to consensus instances
pub fn create_bft_network(
    node_ids: &[String],
    fault_tolerance: usize,
) -> Result<HashMap<String, BFTConsensus>, ConsensusError> {
    let required = 3 * fault_tolerance + 1;
    if node_ids.len() < required {
        return Err(ConsensusError::InsufficientNodes {
            required,
            f: fault_tolerance,
            actual: node_ids.len(),
        });
    }

    // Generate keypairs for each node
    let keypairs: HashMap<String, KeyPair> = node_ids
        .iter()
        .map(|id| (id.clone(), KeyPair::generate()))
        .collect();

    // Extract public keys
    let public_keys: HashMap<String, String> = keypairs
        .iter()
        .map(|(id, kp)| (id.clone(), kp.public_key()))
        .collect();

    // Create consensus instances
    let mut consensus_nodes = HashMap::new();

    for node_id in node_ids {
        let keypair = keypairs.get(node_id).unwrap().clone();
        let consensus = BFTConsensus::new(
            node_id.clone(),
            node_ids.to_vec(),
            fault_tolerance,
            KeyPair::from_private_key(&keypair.private_key())
                .map_err(|e| ConsensusError::CryptoError(e.to_string()))?,
            public_keys.clone(),
        )?;
        consensus_nodes.insert(node_id.clone(), consensus);
    }

    Ok(consensus_nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_network() -> (Vec<String>, HashMap<String, String>, Vec<KeyPair>) {
        let node_ids: Vec<String> = (1..=4).map(|i| format!("node{}", i)).collect();
        let keypairs: Vec<KeyPair> = node_ids.iter().map(|_| KeyPair::generate()).collect();
        let public_keys: HashMap<String, String> = node_ids
            .iter()
            .zip(keypairs.iter())
            .map(|(id, kp)| (id.clone(), kp.public_key()))
            .collect();

        (node_ids, public_keys, keypairs)
    }

    #[tokio::test]
    async fn test_consensus_creation() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        let consensus = BFTConsensus::new(
            "node1".to_string(),
            node_ids,
            1, // f=1
            keypairs[0].clone(),
            public_keys,
        )
        .unwrap();

        assert_eq!(consensus.node_id(), "node1");
        assert_eq!(consensus.fault_tolerance(), 1);
        assert_eq!(consensus.node_count(), 4);
    }

    #[tokio::test]
    async fn test_insufficient_nodes() {
        let node_ids = vec!["node1".to_string(), "node2".to_string()];
        let keypair = KeyPair::generate();
        let public_keys = HashMap::new();

        let result = BFTConsensus::new("node1".to_string(), node_ids, 1, keypair, public_keys);

        assert!(result.is_err());
        match result {
            Err(ConsensusError::InsufficientNodes {
                required,
                f,
                actual,
            }) => {
                assert_eq!(required, 4);
                assert_eq!(f, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected InsufficientNodes error"),
        }
    }

    #[tokio::test]
    async fn test_primary_determination() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        let consensus = BFTConsensus::new(
            "node1".to_string(),
            node_ids,
            1,
            keypairs[0].clone(),
            public_keys,
        )
        .unwrap();

        // In view 0, node1 should be primary
        assert!(consensus.is_primary().await);
        assert_eq!(consensus.primary().await, "node1");
    }

    #[tokio::test]
    async fn test_request_as_primary() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        let consensus = BFTConsensus::new(
            "node1".to_string(),
            node_ids,
            1,
            keypairs[0].clone(),
            public_keys,
        )
        .unwrap();

        let mut operation = HashMap::new();
        operation.insert("action".to_string(), serde_json::json!("test"));

        let result = consensus.request(operation).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_request_as_non_primary() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        // node2 is not primary in view 0
        let consensus = BFTConsensus::new(
            "node2".to_string(),
            node_ids,
            1,
            keypairs[1].clone(),
            public_keys,
        )
        .unwrap();

        let mut operation = HashMap::new();
        operation.insert("action".to_string(), serde_json::json!("test"));

        let result = consensus.request(operation).await;
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false (not primary)
    }

    #[tokio::test]
    async fn test_consensus_status() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        let consensus = BFTConsensus::new(
            "node1".to_string(),
            node_ids,
            1,
            keypairs[0].clone(),
            public_keys,
        )
        .unwrap();

        let status = consensus.get_consensus_status().await;

        assert_eq!(status.get("node_id").unwrap(), "node1");
        assert_eq!(status.get("view").unwrap(), 0);
        assert_eq!(status.get("state").unwrap(), "idle");
        assert_eq!(status.get("is_primary").unwrap(), true);
    }

    #[tokio::test]
    async fn test_create_bft_network() {
        let node_ids: Vec<String> = (1..=4).map(|i| format!("node{}", i)).collect();

        let network = create_bft_network(&node_ids, 1).unwrap();

        assert_eq!(network.len(), 4);
        assert!(network.contains_key("node1"));
        assert!(network.contains_key("node4"));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (node_ids, public_keys, keypairs) = create_test_network();

        let consensus = BFTConsensus::new(
            "node1".to_string(),
            node_ids,
            1,
            keypairs[0].clone(),
            public_keys,
        )
        .unwrap();

        consensus.shutdown().await;
        // Should complete without panicking
    }
}

//! Sub-Chain implementation for HieraChain Framework.
//!
//! This module implements the Sub-Chain class that handles domain-specific
//! business operations and submits proofs to the Main Chain, following
//! framework guidelines for HieraChain structure.

use crate::consensus::ordering_service::OrderingService;
use crate::consensus::types::{EventPayload, PendingEvent};
use crate::core::block::Block;
use crate::core::blockchain::Blockchain;
use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::consensus::proof_of_authority::ProofOfAuthority;
use crate::core::consensus::proof_of_federation::ProofOfFederation;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pythonize::{depythonize, pythonize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp as f64
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Consensus type for SubChain (reused from main_chain)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusType {
    ProofOfAuthority,
    ProofOfFederation,
}

impl Default for ConsensusType {
    fn default() -> Self {
        ConsensusType::ProofOfAuthority
    }
}

impl ConsensusType {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "proof_of_federation" | "pof" | "federation" => ConsensusType::ProofOfFederation,
            _ => ConsensusType::ProofOfAuthority,
        }
    }

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

    pub fn finalize_block(&mut self, block: &mut Block) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.finalize_block(block),
            ConsensusWrapper::PoF(pof) => pof.finalize_block(block),
        }
    }

    pub fn validate_block(&self, block: &Block, previous_block: &Block) -> bool {
        match self {
            ConsensusWrapper::PoA(poa) => poa.validate_block(block, previous_block),
            ConsensusWrapper::PoF(pof) => pof.validate_block(block, previous_block),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            ConsensusWrapper::PoA(poa) => &poa.name,
            ConsensusWrapper::PoF(pof) => &pof.name,
        }
    }

    pub fn consensus_type_str(&self) -> &'static str {
        match self {
            ConsensusWrapper::PoA(_) => "proof_of_authority",
            ConsensusWrapper::PoF(_) => "proof_of_federation",
        }
    }
}

/// Sanitize metadata for main chain (only summary data)
fn sanitize_metadata_for_main_chain(metadata: &Value) -> Value {
    if let Some(obj) = metadata.as_object() {
        let mut sanitized = Map::new();

        let allowed_fields = [
            "domain_type",
            "latest_block_index",
            "total_blocks",
            "recent_events",
            "unique_entities",
            "completed_operations",
            "proof_timestamp",
            "sub_chain_name",
        ];

        for (key, value) in obj {
            if allowed_fields.contains(&key.as_str()) {
                sanitized.insert(key.clone(), value.clone());
            } else if !value.is_object() && !value.is_array() {
                sanitized.insert(key.clone(), value.clone());
            }
        }

        Value::Object(sanitized)
    } else {
        Value::Object(Map::new())
    }
}

/// Create a properly structured event
fn create_event(entity_id: &str, event_type: &str, details: Value) -> Value {
    json!({
        "entity_id": entity_id,
        "event": event_type,
        "timestamp": current_timestamp(),
        "details": details
    })
}

/// Sub-Chain implementation for the HieraChain framework.
///
/// Sub-Chains act as domain experts (like department heads) and:
/// - Handle domain-specific business operations
/// - Store detailed domain events and data
/// - Submit cryptographic proofs to Main Chain
/// - Use entity_id as metadata field within events (not as block identifier)
pub struct SubChain {
    /// Base blockchain
    pub blockchain: Blockchain,
    /// Domain type
    pub domain_type: String,
    /// Consensus mechanism
    pub consensus: ConsensusWrapper,
    /// Consensus type
    pub consensus_type: ConsensusType,
    /// Ordering service
    pub ordering_service: Arc<OrderingService>,
    /// Event receiver
    event_receiver: Arc<Mutex<Option<Receiver<PendingEvent>>>>,
    /// Main chain connection (name only for now)
    pub main_chain_connection: Option<String>,
    /// Proof submission interval in seconds
    pub proof_submission_interval: f64,
    /// Last proof submission timestamp
    pub last_proof_submission: f64,
    /// Completed operations count
    pub completed_operations: u64,
    /// Running flag
    pub running: Arc<RwLock<bool>>,
    /// Pending events (for proof check)
    pending_events: Arc<RwLock<Vec<Value>>>,
    /// Block queue from ordering service
    block_queue: Arc<Mutex<Vec<Value>>>,
}

impl SubChain {
    /// Create a new Sub-Chain.
    ///
    /// # Arguments
    /// * `name` - Name identifier for the Sub-Chain
    /// * `domain_type` - Type of domain this Sub-Chain handles
    /// * `consensus_type` - Type of consensus (PoA or PoF)
    pub fn new(name: &str, domain_type: &str, consensus_type: ConsensusType) -> Self {
        // Validate name
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            panic!("Invalid SubChain name. Allowed: alphanumeric, underscore, hyphen.");
        }

        let mut consensus = ConsensusWrapper::new(consensus_type, name);

        // Register Sub-Chain as authority for its own operations
        let mut auth_metadata = Map::new();
        auth_metadata.insert(
            "role".to_string(),
            Value::String("sub_chain_authority".to_string()),
        );
        auth_metadata.insert(
            "domain_type".to_string(),
            Value::String(domain_type.to_string()),
        );
        auth_metadata.insert(
            "permissions".to_string(),
            Value::Array(vec![
                Value::String("domain_operations".to_string()),
                Value::String("event_creation".to_string()),
            ]),
        );
        auth_metadata.insert(
            "created_at".to_string(),
            Value::Number(
                serde_json::Number::from_f64(current_timestamp())
                    .unwrap_or(serde_json::Number::from(0)),
            ),
        );

        consensus.add_authority(name.to_string(), Some(auth_metadata));

        // Initialize ordering service
        let config = json!({
            "storage_dir": format!("data/{}/journal", name),
            "block_size": 50,
            "batch_timeout": 1.0,
            "worker_threads": 2
        });

        let (ordering_service, receiver) = OrderingService::new(vec![], config);

        // Start ordering service
        OrderingService::start(ordering_service.clone(), receiver);

        SubChain {
            blockchain: Blockchain::new(name),
            domain_type: domain_type.to_string(),
            consensus,
            consensus_type,
            ordering_service,
            event_receiver: Arc::new(Mutex::new(None)),
            main_chain_connection: None,
            proof_submission_interval: 60.0,
            last_proof_submission: 0.0,
            completed_operations: 0,
            running: Arc::new(RwLock::new(true)),
            pending_events: Arc::new(RwLock::new(Vec::new())),
            block_queue: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get chain name
    pub fn name(&self) -> &str {
        &self.blockchain.name
    }

    /// Get chain length
    pub fn chain_length(&self) -> usize {
        self.blockchain.chain.len()
    }

    /// Add event to Sub-Chain via OrderingService
    ///
    /// Returns transaction ID
    pub fn add_event(&mut self, mut event: Value) -> String {
        // Add timestamp if missing
        if event.get("timestamp").is_none() {
            if let Some(obj) = event.as_object_mut() {
                obj.insert(
                    "timestamp".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(current_timestamp())
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                );
            }
        }

        // Ensure required fields - extract values first to avoid borrow conflicts
        let needs_entity_id = event.get("entity_id").is_none();
        let needs_event_field = event.get("event").is_none();

        if needs_entity_id {
            let sender = event
                .get("sender")
                .and_then(|v| v.as_str())
                .unwrap_or("system")
                .to_string();
            if let Some(obj) = event.as_object_mut() {
                obj.insert("entity_id".to_string(), Value::String(sender));
            }
        }

        if needs_event_field {
            let event_type = event
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("generic_event")
                .to_string();
            if let Some(obj) = event.as_object_mut() {
                obj.insert("event".to_string(), Value::String(event_type));
            }
        }

        // Track pending event
        if let Ok(mut pending) = self.pending_events.write() {
            pending.push(event.clone());
        }

        // Submit to ordering service
        let payload = EventPayload::Json(event.clone());
        let tx_id = self.ordering_service.receive_event(
            payload,
            self.blockchain.name.clone(),
            self.blockchain.name.clone(),
        );

        tx_id
    }

    /// Start a domain-specific operation for an entity
    pub fn start_operation(
        &mut self,
        entity_id: &str,
        operation_type: &str,
        details: Value,
    ) -> bool {
        let event = create_event(
            entity_id,
            "operation_start",
            json!({
                "operation_type": operation_type,
                "domain_type": self.domain_type,
                "started_by": self.blockchain.name,
                "operation_details": details,
                "started_at": current_timestamp()
            }),
        );

        self.add_event(event);
        true
    }

    /// Complete a domain-specific operation for an entity
    pub fn complete_operation(
        &mut self,
        entity_id: &str,
        operation_type: &str,
        result: Value,
    ) -> bool {
        let event = create_event(
            entity_id,
            "operation_complete",
            json!({
                "operation_type": operation_type,
                "domain_type": self.domain_type,
                "completed_by": self.blockchain.name,
                "result": result,
                "completed_at": current_timestamp()
            }),
        );

        self.add_event(event);
        self.completed_operations += 1;
        true
    }

    /// Update the status of an entity
    pub fn update_entity_status(&mut self, entity_id: &str, status: &str, details: Value) -> bool {
        let event = create_event(
            entity_id,
            "status_update",
            json!({
                "new_status": status,
                "domain_type": self.domain_type,
                "updated_by": self.blockchain.name,
                "status_details": details,
                "updated_at": current_timestamp()
            }),
        );

        self.add_event(event);
        true
    }

    /// Connect this Sub-Chain to a Main Chain
    pub fn connect_to_main_chain(&mut self, main_chain_name: &str) -> bool {
        // Store connection
        self.main_chain_connection = Some(main_chain_name.to_string());

        // Create connection event
        let connection_event = json!({
            "entity_id": self.blockchain.name,
            "event": "main_chain_connection",
            "timestamp": current_timestamp(),
            "details": {
                "main_chain_name": main_chain_name,
                "connected_at": current_timestamp(),
                "status": "connected"
            }
        });

        self.add_event(connection_event);
        true
    }

    /// Generate default proof metadata for Main Chain submission
    fn generate_default_proof_metadata(&self) -> Value {
        let latest_block = self.blockchain.get_latest_block();

        // Count events in recent blocks
        let recent_blocks: Vec<&Block> = self.blockchain.chain.iter().rev().take(5).collect();

        let mut event_count = 0;
        let mut unique_entities: HashSet<String> = HashSet::new();

        for block in &recent_blocks {
            event_count += block.events.len();
            for event in &block.events {
                if let Some(entity_id) = event.get("entity_id").and_then(|v| v.as_str()) {
                    unique_entities.insert(entity_id.to_string());
                }
            }
        }

        let metadata = json!({
            "domain_type": self.domain_type,
            "latest_block_index": latest_block.index,
            "total_blocks": self.blockchain.chain.len(),
            "recent_events": event_count,
            "unique_entities": unique_entities.len(),
            "completed_operations": self.completed_operations,
            "proof_timestamp": current_timestamp(),
            "sub_chain_name": self.blockchain.name
        });

        sanitize_metadata_for_main_chain(&metadata)
    }

    /// Submit cryptographic proof to Main Chain.
    ///
    /// This follows the guidelines pattern for proof submission where
    /// Sub-Chains submit proofs with summary metadata, not detailed data.
    ///
    /// # Arguments
    /// * `main_chain` - Reference to MainChain to submit proof to
    /// * `metadata` - Optional custom metadata (uses default if None)
    ///
    /// # Returns
    /// True if proof was submitted successfully
    pub fn submit_proof_to_main(
        &mut self,
        main_chain: &mut super::main_chain::MainChain,
        metadata: Option<Value>,
    ) -> bool {
        // Generate metadata
        let proof_metadata = match metadata {
            Some(m) => sanitize_metadata_for_main_chain(&m),
            None => self.generate_default_proof_metadata(),
        };

        // Submit proof to main chain
        let result = main_chain.submit_proof(&self.blockchain.name, proof_metadata.clone());

        if result {
            // Update last submission time
            self.last_proof_submission = current_timestamp();

            // Clear pending events after successful submission
            if let Ok(mut pending) = self.pending_events.write() {
                pending.clear();
            }

            // Create proof submission event
            let submission_event = json!({
                "entity_id": self.blockchain.name,
                "event": "proof_submitted",
                "timestamp": current_timestamp(),
                "details": {
                    "main_chain": main_chain.name(),
                    "proof_metadata": proof_metadata,
                    "submitted_at": current_timestamp()
                }
            });

            self.add_event(submission_event);
        }

        result
    }

    /// Automatically submit proof if conditions are met.
    ///
    /// # Returns
    /// True if proof was submitted, False otherwise
    pub fn auto_submit_proof_if_needed(
        &mut self,
        main_chain: Option<&mut super::main_chain::MainChain>,
    ) -> bool {
        if !self.should_submit_proof() {
            return false;
        }

        match main_chain {
            Some(mc) => self.submit_proof_to_main(mc, None),
            None => {
                // No main chain reference available
                false
            }
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

    /// Flush pending events and finalize them into a block with timeout.
    ///
    /// # Arguments
    /// * `timeout_seconds` - Maximum time to wait for events
    ///
    /// # Returns
    /// Finalized block info if successful
    pub fn flush_pending_and_finalize(&mut self, _timeout_seconds: f64) -> Option<Value> {
        // First, try to process any blocks from the ordering service
        self.process_block_queue();

        // Then finalize any pending events
        self.finalize_block()
    }

    /// Process blocks from the block queue (from OrderingService).
    /// This uses the block_queue field.
    pub fn process_block_queue(&mut self) {
        let blocks_to_process = match self.block_queue.lock() {
            Ok(mut queue) => {
                let blocks = queue.clone();
                queue.clear();
                Some(blocks)
            }
            Err(_) => None,
        };

        if let Some(blocks) = blocks_to_process {
            for block_data in blocks {
                // Extract events from block data and add to pending
                if let Some(events) = block_data.get("events").and_then(|e| e.as_array()) {
                    if let Ok(mut pending) = self.pending_events.write() {
                        for event in events {
                            pending.push(event.clone());
                        }
                    }
                }
            }
        }
    }

    /// Synchronize chain from storage/journal.
    /// Rehydrates the chain state from persistent storage.
    pub fn sync_chain(&mut self) {
        // Get any pending events from the event receiver
        if let Ok(receiver_guard) = self.event_receiver.lock() {
            if let Some(ref receiver) = *receiver_guard {
                // Try to receive any pending events (non-blocking)
                while let Ok(pending_event) = receiver.try_recv() {
                    // Add received events to pending (use event_data field)
                    if let Ok(mut pending) = self.pending_events.write() {
                        pending.push(pending_event.event_data.clone());
                    }
                }
            }
        }

        // Process any queued blocks
        self.process_block_queue();
    }

    /// Add block data to the block queue for later processing.
    /// This is called by the background consumer or external sources.
    pub fn queue_block(&self, block_data: Value) {
        if let Ok(mut queue) = self.block_queue.lock() {
            queue.push(block_data);
        }
    }

    /// Get entity history (sorted by timestamp)
    pub fn get_entity_history(&self, entity_id: &str) -> Vec<Value> {
        let mut events = self.blockchain.get_events_by_entity(entity_id);
        events.sort_by(|a, b| {
            let ts_a = a.get("timestamp").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let ts_b = b.get("timestamp").and_then(|v| v.as_f64()).unwrap_or(0.0);
            ts_a.partial_cmp(&ts_b).unwrap_or(std::cmp::Ordering::Equal)
        });
        events
    }

    /// Get comprehensive statistics about this Sub-Chain's domain operations
    pub fn get_domain_statistics(&self) -> Value {
        let base_stats = self.blockchain.get_chain_stats();

        // Count entities and operations
        let mut unique_entities: HashSet<String> = HashSet::new();
        let mut operation_types: HashMap<String, u64> = HashMap::new();

        for block in &self.blockchain.chain {
            for event in &block.events {
                if let Some(entity_id) = event.get("entity_id").and_then(|v| v.as_str()) {
                    unique_entities.insert(entity_id.to_string());
                }

                let event_type = event
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                *operation_types.entry(event_type.to_string()).or_insert(0) += 1;
            }
        }

        let operation_types_value: Map<String, Value> = operation_types
            .into_iter()
            .map(|(k, v)| (k, Value::Number(v.into())))
            .collect();

        json!({
            "name": base_stats.get("name"),
            "total_blocks": base_stats.get("total_blocks"),
            "total_events": base_stats.get("total_events"),
            "domain_type": self.domain_type,
            "unique_entities": unique_entities.len(),
            "completed_operations": self.completed_operations,
            "operation_types": operation_types_value,
            "main_chain_connected": self.main_chain_connection.is_some(),
            "last_proof_submission": self.last_proof_submission,
            "proof_submission_interval": self.proof_submission_interval,
            "consensus_type": self.consensus.consensus_type_str()
        })
    }

    /// Check if it's time to submit a proof to Main Chain
    pub fn should_submit_proof(&self) -> bool {
        let current_time = current_timestamp();
        let time_since_last = current_time - self.last_proof_submission;

        // Check for pending events
        let has_pending = self
            .pending_events
            .read()
            .map(|p| !p.is_empty())
            .unwrap_or(false);

        time_since_last >= self.proof_submission_interval && has_pending
    }

    /// Get the latest block
    pub fn get_latest_block(&self) -> &Block {
        self.blockchain.get_latest_block()
    }

    /// Finalize any pending events into a block
    pub fn finalize_block(&mut self) -> Option<Value> {
        // Get pending events
        let pending = {
            let mut pending_lock = self.pending_events.write().ok()?;
            let events = pending_lock.clone();
            pending_lock.clear();
            events
        };

        if pending.is_empty() {
            return None;
        }

        // Add events to blockchain
        for event in &pending {
            self.blockchain.add_event(event.clone()).ok();
        }

        // Create and finalize block
        let mut block = self.blockchain.create_block(Some(pending.clone())).ok()?;

        // Finalize with consensus
        if !self.consensus.finalize_block(&mut block) {
            return None;
        }

        // Add to chain
        let block_clone = block.clone();
        if self.blockchain.add_block(block) {
            Some(json!({
                "block_index": block_clone.index,
                "block_hash": block_clone.hash,
                "events_count": pending.len(),
                "finalized_at": current_timestamp(),
                "domain_type": self.domain_type
            }))
        } else {
            None
        }
    }

    /// Stop the Sub-Chain
    pub fn stop(&self) {
        if let Ok(mut running) = self.running.write() {
            *running = false;
        }
        self.ordering_service.stop();
    }
}

impl std::fmt::Display for SubChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SubChain(name={}, domain={}, blocks={}, operations={})",
            self.blockchain.name,
            self.domain_type,
            self.blockchain.chain.len(),
            self.completed_operations
        )
    }
}

// ==================== PyO3 Wrapper ====================

/// PyO3 wrapper for SubChain
#[pyclass(name = "SubChain")]
pub struct PySubChain {
    inner: Arc<Mutex<SubChain>>,
}

#[pymethods]
impl PySubChain {
    /// Create a new Sub-Chain.
    ///
    /// Args:
    ///     name: Name of the Sub-Chain
    ///     domain_type: Type of domain (default: "generic")
    ///     consensus_type: Type of consensus (default: "proof_of_authority")
    #[new]
    #[pyo3(signature = (name, domain_type = "generic", consensus_type = None))]
    pub fn new(name: &str, domain_type: &str, consensus_type: Option<&str>) -> Self {
        let ct = match consensus_type {
            Some(ct_str) => ConsensusType::from_str(ct_str),
            None => ConsensusType::ProofOfAuthority,
        };

        PySubChain {
            inner: Arc::new(Mutex::new(SubChain::new(name, domain_type, ct))),
        }
    }

    /// Get the chain name
    #[getter]
    pub fn name(&self) -> String {
        self.inner
            .lock()
            .map(|s| s.name().to_string())
            .unwrap_or_default()
    }

    /// Get the domain type
    #[getter]
    pub fn domain_type(&self) -> String {
        self.inner
            .lock()
            .map(|s| s.domain_type.clone())
            .unwrap_or_default()
    }

    /// Get consensus type
    #[getter]
    pub fn consensus_type(&self) -> String {
        self.inner
            .lock()
            .map(|s| s.consensus_type.as_str().to_string())
            .unwrap_or_default()
    }

    /// Get number of blocks
    #[getter]
    pub fn block_count(&self) -> usize {
        self.inner.lock().map(|s| s.chain_length()).unwrap_or(0)
    }

    /// Get completed operations count
    #[getter]
    pub fn completed_operations(&self) -> u64 {
        self.inner
            .lock()
            .map(|s| s.completed_operations)
            .unwrap_or(0)
    }

    /// Check if connected to main chain
    #[getter]
    pub fn main_chain_connected(&self) -> bool {
        self.inner
            .lock()
            .map(|s| s.main_chain_connection.is_some())
            .unwrap_or(false)
    }

    /// Add event to Sub-Chain
    pub fn add_event(&self, event: &Bound<PyAny>) -> PyResult<String> {
        let event_value: Value = depythonize(event)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.add_event(event_value))
    }

    /// Start a domain-specific operation
    #[pyo3(signature = (entity_id, operation_type, details = None))]
    pub fn start_operation(
        &self,
        entity_id: &str,
        operation_type: &str,
        details: Option<&Bound<PyAny>>,
    ) -> PyResult<bool> {
        let details_value = match details {
            Some(d) => depythonize(d)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            None => Value::Object(Map::new()),
        };

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.start_operation(entity_id, operation_type, details_value))
    }

    /// Complete a domain-specific operation
    #[pyo3(signature = (entity_id, operation_type, result = None))]
    pub fn complete_operation(
        &self,
        entity_id: &str,
        operation_type: &str,
        result: Option<&Bound<PyAny>>,
    ) -> PyResult<bool> {
        let result_value = match result {
            Some(r) => depythonize(r)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            None => Value::Object(Map::new()),
        };

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.complete_operation(entity_id, operation_type, result_value))
    }

    /// Update entity status
    #[pyo3(signature = (entity_id, status, details = None))]
    pub fn update_entity_status(
        &self,
        entity_id: &str,
        status: &str,
        details: Option<&Bound<PyAny>>,
    ) -> PyResult<bool> {
        let details_value = match details {
            Some(d) => depythonize(d)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            None => Value::Object(Map::new()),
        };

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.update_entity_status(entity_id, status, details_value))
    }

    /// Connect to a Main Chain
    pub fn connect_to_main_chain(&self, main_chain_name: &str) -> PyResult<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.connect_to_main_chain(main_chain_name))
    }

    /// Get entity history
    pub fn get_entity_history(&self, entity_id: &str, py: Python) -> PyResult<Py<PyList>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let history = inner.get_entity_history(entity_id);

        let py_list = PyList::empty(py);
        for event in history {
            let py_event = pythonize(py, &event)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            py_list.append(py_event)?;
        }

        Ok(py_list.into())
    }

    /// Get domain statistics
    pub fn get_domain_statistics(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let stats = inner.get_domain_statistics();

        pythonize(py, &stats)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Check if proof should be submitted
    pub fn should_submit_proof(&self) -> PyResult<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(inner.should_submit_proof())
    }

    /// Finalize pending events into a block
    pub fn finalize_block(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        match inner.finalize_block() {
            Some(result) => {
                let py_result = pythonize(py, &result)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                Ok(Some(py_result.unbind()))
            }
            None => Ok(None),
        }
    }

    /// Stop the Sub-Chain
    pub fn stop(&self) -> PyResult<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        inner.stop();
        Ok(())
    }

    fn __str__(&self) -> String {
        self.inner
            .lock()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "SubChain(locked)".to_string())
    }

    fn __repr__(&self) -> String {
        self.inner
            .lock()
            .map(|s| {
                format!(
                    "SubChain(name={}, domain_type={}, blocks={}, operations={}, main_chain_connected={})",
                    s.name(),
                    s.domain_type,
                    s.chain_length(),
                    s.completed_operations,
                    s.main_chain_connection.is_some()
                )
            })
            .unwrap_or_else(|_| "SubChain(locked)".to_string())
    }
}

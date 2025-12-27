//! Base Blockchain implementation for HieraChain Framework.
//!
//! This module implements the base Blockchain struct that serves as the foundation
//! for both MainChain and SubChain implementations, following framework guidelines:
//! - Event-based model (not transactions)
//! - Multiple events per block
//! - Proper chain validation and integrity

use crate::core::block::Block;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pythonize::{depythonize, pythonize};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp as f64 (seconds since UNIX epoch)
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Base blockchain structure for the hierarchical framework.
///
/// This struct provides the fundamental blockchain operations and will be
/// used by MainChain and SubChain implementations. It follows the framework
/// guidelines by using events (not transactions) and supporting multiple
/// events per block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blockchain {
    /// Name identifier for this blockchain
    pub name: String,
    /// Chain of blocks
    pub chain: Vec<Block>,
    /// Pending events waiting to be included in the next block
    pub pending_events: Vec<Value>,
}

impl Blockchain {
    /// Create a new blockchain with a genesis block.
    ///
    /// # Arguments
    /// * `name` - Name identifier for this blockchain
    pub fn new(name: &str) -> Self {
        let mut blockchain = Blockchain {
            name: name.to_string(),
            chain: Vec::new(),
            pending_events: Vec::new(),
        };
        blockchain.create_genesis_block();
        blockchain
    }

    /// Create the genesis (first) block of the blockchain.
    pub fn create_genesis_block(&mut self) {
        let timestamp = current_timestamp();
        let genesis_event = serde_json::json!({
            "entity_id": "SYSTEM",
            "event": "genesis",
            "timestamp": timestamp,
            "details": {
                "chain_name": self.name,
                "created_at": timestamp
            }
        });

        let genesis_block = Block {
            index: 0,
            events: vec![genesis_event],
            arrow_events: None,
            timestamp,
            previous_hash: "0".to_string(),
            nonce: 0,
            merkle_root: String::new(),
            hash: String::new(),
            creator_id: None,
            signature: None,
        };

        // Calculate merkle root and hash
        let mut block = genesis_block;
        let tree = crate::core::utils::MerkleTree::new(&block.events);
        block.merkle_root = tree.get_root();
        block.hash = block.calculate_hash();

        self.chain.push(block);
    }

    /// Get the latest block in the chain.
    ///
    /// # Panics
    /// Panics if the chain is empty (should never happen after init)
    pub fn get_latest_block(&self) -> &Block {
        self.chain.last().expect("Blockchain should never be empty")
    }

    /// Get a mutable reference to the latest block.
    pub fn get_latest_block_mut(&mut self) -> &mut Block {
        self.chain
            .last_mut()
            .expect("Blockchain should never be empty")
    }

    /// Add an event to the pending events list.
    ///
    /// # Arguments
    /// * `event` - Event as a JSON Value
    ///
    /// # Returns
    /// Result indicating success or error message
    pub fn add_event(&mut self, mut event: Value) -> Result<(), String> {
        // Validate event is an object
        if !event.is_object() {
            return Err("Event must be a JSON object".to_string());
        }

        // Add timestamp if not present
        if event.get("timestamp").is_none() {
            if let Some(obj) = event.as_object_mut() {
                obj.insert(
                    "timestamp".to_string(),
                    serde_json::json!(current_timestamp()),
                );
            }
        }

        self.pending_events.push(event);
        Ok(())
    }

    /// Create a new block with the given events or pending events.
    ///
    /// # Arguments
    /// * `events` - Optional list of events. If None, uses pending_events.
    ///
    /// # Returns
    /// The newly created block or an error if no events provided
    pub fn create_block(&mut self, events: Option<Vec<Value>>) -> Result<Block, String> {
        let block_events = match events {
            Some(e) => e,
            None => {
                let e = std::mem::take(&mut self.pending_events);
                e
            }
        };

        if block_events.is_empty() {
            return Err("Cannot create block without events".to_string());
        }

        let latest_block = self.get_latest_block();
        let new_index = latest_block.index + 1;
        let previous_hash = latest_block.hash.clone();
        let timestamp = current_timestamp();

        // Create the block
        let tree = crate::core::utils::MerkleTree::new(&block_events);
        let merkle_root = tree.get_root();

        let mut new_block = Block {
            index: new_index,
            events: block_events,
            arrow_events: None,
            timestamp,
            previous_hash,
            nonce: 0,
            merkle_root,
            hash: String::new(),
            creator_id: None,
            signature: None,
        };

        new_block.hash = new_block.calculate_hash();
        Ok(new_block)
    }

    /// Add a block to the blockchain after validation.
    ///
    /// # Arguments
    /// * `block` - Block to add to the chain
    ///
    /// # Returns
    /// True if block was added successfully, false otherwise
    pub fn add_block(&mut self, block: Block) -> bool {
        if self.is_valid_new_block(&block) {
            self.chain.push(block);
            true
        } else {
            false
        }
    }

    /// Finalize pending events into a new block and add it to the chain.
    ///
    /// # Returns
    /// The newly created and added block, or None if no pending events
    pub fn finalize_block(&mut self) -> Option<Block> {
        if self.pending_events.is_empty() {
            return None;
        }

        match self.create_block(None) {
            Ok(new_block) => {
                let block_clone = new_block.clone();
                if self.add_block(new_block) {
                    Some(block_clone)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    /// Validate a new block before adding it to the chain.
    ///
    /// # Arguments
    /// * `block` - Block to validate
    ///
    /// # Returns
    /// True if block is valid, false otherwise
    pub fn is_valid_new_block(&self, block: &Block) -> bool {
        let latest_block = self.get_latest_block();

        // Check block index
        if block.index != latest_block.index + 1 {
            return false;
        }

        // Check previous hash
        if block.previous_hash != latest_block.hash {
            return false;
        }

        // Check block structure
        if !block.validate_structure() {
            return false;
        }

        // Verify hash calculation
        if block.hash != block.calculate_hash() {
            return false;
        }

        true
    }

    /// Validate the entire blockchain.
    ///
    /// # Returns
    /// True if the entire chain is valid, false otherwise
    pub fn is_chain_valid(&self) -> bool {
        for i in 1..self.chain.len() {
            let current_block = &self.chain[i];
            let previous_block = &self.chain[i - 1];

            // Check if current block structure is valid
            if !current_block.validate_structure() {
                return false;
            }

            // Check if hash is correct
            if current_block.hash != current_block.calculate_hash() {
                return false;
            }

            // Check if previous hash matches
            if current_block.previous_hash != previous_block.hash {
                return false;
            }

            // Check block index
            if current_block.index != previous_block.index + 1 {
                return false;
            }
        }

        true
    }

    /// Get all events for a specific entity across the entire chain.
    ///
    /// # Arguments
    /// * `entity_id` - The entity identifier to search for
    ///
    /// # Returns
    /// List of events for the specified entity
    pub fn get_events_by_entity(&self, entity_id: &str) -> Vec<Value> {
        let mut events = Vec::new();

        for block in &self.chain {
            for event in &block.events {
                if let Some(eid) = event.get("entity_id") {
                    if eid.as_str() == Some(entity_id) {
                        events.push(event.clone());
                    }
                }
            }
        }

        events
    }

    /// Get all events of a specific type across the entire chain.
    ///
    /// # Arguments
    /// * `event_type` - The event type to search for
    ///
    /// # Returns
    /// List of events of the specified type
    pub fn get_events_by_type(&self, event_type: &str) -> Vec<Value> {
        let mut events = Vec::new();

        for block in &self.chain {
            for event in &block.events {
                if let Some(etype) = event.get("event") {
                    if etype.as_str() == Some(event_type) {
                        events.push(event.clone());
                    }
                }
            }
        }

        events
    }

    /// Get all events that match a custom filter function.
    ///
    /// # Arguments
    /// * `filter_fn` - Function that takes an event and returns true if it matches
    ///
    /// # Returns
    /// List of events that match the filter
    pub fn get_events_by_filter<F>(&self, filter_fn: F) -> Vec<Value>
    where
        F: Fn(&Value) -> bool,
    {
        let mut events = Vec::new();

        for block in &self.chain {
            for event in &block.events {
                if filter_fn(event) {
                    events.push(event.clone());
                }
            }
        }

        events
    }

    /// Get statistics about the blockchain.
    ///
    /// # Returns
    /// JSON object containing chain statistics
    pub fn get_chain_stats(&self) -> Value {
        let total_events: usize = self.chain.iter().map(|b| b.events.len()).sum();

        serde_json::json!({
            "name": self.name,
            "total_blocks": self.chain.len(),
            "total_events": total_events,
            "pending_events": self.pending_events.len(),
            "latest_block_hash": self.get_latest_block().hash,
            "chain_valid": self.is_chain_valid()
        })
    }

    /// Convert blockchain to dictionary representation.
    ///
    /// # Returns
    /// JSON object representation of the blockchain
    pub fn to_dict(&self) -> Value {
        let chain_dicts: Vec<Value> = self
            .chain
            .iter()
            .map(|block| {
                serde_json::json!({
                    "index": block.index,
                    "events": block.events,
                    "timestamp": block.timestamp,
                    "previous_hash": block.previous_hash,
                    "nonce": block.nonce,
                    "merkle_root": block.merkle_root,
                    "hash": block.hash,
                    "creator_id": block.creator_id,
                    "signature": block.signature
                })
            })
            .collect();

        serde_json::json!({
            "name": self.name,
            "chain": chain_dicts,
            "pending_events": self.pending_events
        })
    }

    /// Create a Blockchain instance from dictionary data.
    ///
    /// # Arguments
    /// * `data` - JSON object containing blockchain data
    ///
    /// # Returns
    /// Result with Blockchain instance or error
    pub fn from_dict(data: &Value) -> Result<Self, String> {
        let name = data
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'name' field")?;

        let mut blockchain = Blockchain {
            name: name.to_string(),
            chain: Vec::new(),
            pending_events: Vec::new(),
        };

        // Rebuild chain from data
        if let Some(chain_data) = data.get("chain").and_then(|v| v.as_array()) {
            for block_data in chain_data {
                let block = Self::block_from_value(block_data)?;
                blockchain.chain.push(block);
            }
        }

        // Restore pending events
        if let Some(pending) = data.get("pending_events").and_then(|v| v.as_array()) {
            blockchain.pending_events = pending.clone();
        }

        Ok(blockchain)
    }

    /// Helper to create a Block from a JSON Value
    fn block_from_value(data: &Value) -> Result<Block, String> {
        let index = data
            .get("index")
            .and_then(|v| v.as_u64())
            .ok_or("Missing 'index' field")?;

        let events = data
            .get("events")
            .and_then(|v| v.as_array())
            .ok_or("Missing 'events' field")?
            .clone();

        let timestamp = data
            .get("timestamp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let previous_hash = data
            .get("previous_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let nonce = data.get("nonce").and_then(|v| v.as_u64()).unwrap_or(0);

        let merkle_root = data
            .get("merkle_root")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let hash = data
            .get("hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let creator_id = data
            .get("creator_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let signature = data
            .get("signature")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Block {
            index,
            events,
            arrow_events: None,
            timestamp,
            previous_hash,
            nonce,
            merkle_root,
            hash,
            creator_id,
            signature,
        })
    }

    /// Get block by index
    pub fn get_block(&self, index: u64) -> Option<&Block> {
        self.chain.iter().find(|b| b.index == index)
    }

    /// Get total number of blocks
    pub fn len(&self) -> usize {
        self.chain.len()
    }

    /// Check if blockchain is empty (only has genesis)
    pub fn is_empty(&self) -> bool {
        self.chain.len() <= 1
    }

    /// Get total number of events across all blocks
    pub fn total_events(&self) -> usize {
        self.chain.iter().map(|b| b.events.len()).sum()
    }
}

impl std::fmt::Display for Blockchain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Blockchain(name={}, blocks={}, pending={})",
            self.name,
            self.chain.len(),
            self.pending_events.len()
        )
    }
}

// ==================== PyO3 Wrapper ====================

/// PyO3 wrapper for Blockchain - exposes Blockchain to Python
#[pyclass(name = "Blockchain")]
pub struct PyBlockchain {
    inner: Blockchain,
}

#[pymethods]
impl PyBlockchain {
    /// Create a new blockchain with a genesis block.
    ///
    /// # Arguments
    /// * `name` - Name identifier for this blockchain (default: "Blockchain")
    #[new]
    #[pyo3(signature = (name = "Blockchain"))]
    pub fn new(name: &str) -> Self {
        PyBlockchain {
            inner: Blockchain::new(name),
        }
    }

    /// Get the blockchain name
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Set the blockchain name
    #[setter]
    pub fn set_name(&mut self, name: String) {
        self.inner.name = name;
    }

    /// Get the number of blocks in the chain
    #[getter]
    pub fn chain_length(&self) -> usize {
        self.inner.chain.len()
    }

    /// Get the number of pending events
    #[getter]
    pub fn pending_events_count(&self) -> usize {
        self.inner.pending_events.len()
    }

    /// Get the latest block in the chain.
    pub fn get_latest_block(&self, py: Python) -> PyResult<Py<PyAny>> {
        let block = self.inner.get_latest_block();
        block.to_dict(py)
    }

    /// Add an event to the pending events list.
    pub fn add_event(&mut self, event: &Bound<PyAny>) -> PyResult<()> {
        let event_value: Value = depythonize(event)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        self.inner
            .add_event(event_value)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    /// Create a new block with the given events or pending events.
    #[pyo3(signature = (events = None))]
    pub fn create_block(
        &mut self,
        events: Option<&Bound<PyAny>>,
        py: Python,
    ) -> PyResult<Py<PyAny>> {
        let events_vec = match events {
            Some(e) => {
                let list = e
                    .downcast::<PyList>()
                    .map_err(|_| pyo3::exceptions::PyTypeError::new_err("events must be a list"))?;
                let mut vec = Vec::new();
                for item in list.iter() {
                    let val: Value = depythonize(&item)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                    vec.push(val);
                }
                Some(vec)
            }
            None => None,
        };

        let block = self
            .inner
            .create_block(events_vec)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        block.to_dict(py)
    }

    /// Add a block to the blockchain after validation.
    pub fn add_block(&mut self, block: &Bound<PyDict>) -> PyResult<bool> {
        let rust_block = Block::from_dict(block)?;
        Ok(self.inner.add_block(rust_block))
    }

    /// Finalize pending events into a new block and add it to the chain.
    pub fn finalize_block(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.finalize_block() {
            Some(block) => Ok(Some(block.to_dict(py)?)),
            None => Ok(None),
        }
    }

    /// Validate a new block before adding it to the chain.
    pub fn is_valid_new_block(&self, block: &Bound<PyDict>) -> PyResult<bool> {
        let rust_block = Block::from_dict(block)?;
        Ok(self.inner.is_valid_new_block(&rust_block))
    }

    /// Validate the entire blockchain.
    pub fn is_chain_valid(&self) -> bool {
        self.inner.is_chain_valid()
    }

    /// Get all events for a specific entity across the entire chain.
    pub fn get_events_by_entity(&self, entity_id: &str, py: Python) -> PyResult<Py<PyAny>> {
        let events = self.inner.get_events_by_entity(entity_id);
        let py_list = PyList::empty(py);
        for event in events {
            let py_event = pythonize(py, &event)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            py_list.append(py_event)?;
        }
        Ok(py_list.into())
    }

    /// Get all events of a specific type across the entire chain.
    pub fn get_events_by_type(&self, event_type: &str, py: Python) -> PyResult<Py<PyAny>> {
        let events = self.inner.get_events_by_type(event_type);
        let py_list = PyList::empty(py);
        for event in events {
            let py_event = pythonize(py, &event)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            py_list.append(py_event)?;
        }
        Ok(py_list.into())
    }

    /// Get statistics about the blockchain.
    pub fn get_chain_stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        let stats = self.inner.get_chain_stats();
        pythonize(py, &stats)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Convert blockchain to dictionary representation.
    pub fn to_dict(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = self.inner.to_dict();
        pythonize(py, &dict)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Create a Blockchain instance from dictionary data.
    #[staticmethod]
    pub fn from_dict(data: &Bound<PyAny>) -> PyResult<Self> {
        let value: Value = depythonize(data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let blockchain = Blockchain::from_dict(&value)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        Ok(PyBlockchain { inner: blockchain })
    }

    /// Get chain as list of block dicts
    pub fn get_chain(&self, py: Python) -> PyResult<Py<PyAny>> {
        let py_list = PyList::empty(py);
        for block in &self.inner.chain {
            let py_block = block.to_dict(py)?;
            py_list.append(py_block)?;
        }
        Ok(py_list.into())
    }

    /// Get pending events as list
    pub fn get_pending_events(&self, py: Python) -> PyResult<Py<PyAny>> {
        let py_list = PyList::empty(py);
        for event in &self.inner.pending_events {
            let py_event = pythonize(py, event)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            py_list.append(py_event)?;
        }
        Ok(py_list.into())
    }

    fn __str__(&self) -> String {
        format!(
            "Blockchain(name={}, blocks={}, pending={})",
            self.inner.name,
            self.inner.chain.len(),
            self.inner.pending_events.len()
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "Blockchain(name={}, blocks={}, pending_events={}, valid={})",
            self.inner.name,
            self.inner.chain.len(),
            self.inner.pending_events.len(),
            self.inner.is_chain_valid()
        )
    }

    fn __len__(&self) -> usize {
        self.inner.chain.len()
    }
}

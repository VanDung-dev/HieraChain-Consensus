//! PyO3 Wrappers for Hierarchical Module
//!
//! This module provides Python bindings for hierarchical components:
//! - `PyBFTConsensus` - Python wrapper for BFT consensus mechanism
//! - `PyMainChain` - Python wrapper for Main Chain
//! - `PySubChain` - Python wrapper for Sub Chain
//! - `PyHierarchyManager` - Python wrapper for Hierarchy Manager

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pythonize::{depythonize, pythonize};
use serde_json::{Map, Value};
use std::collections::HashMap as StdHashMap;
use std::sync::{Arc, Mutex};

use crate::core::block::Block;
use crate::hierarchical::consensus::bft_consensus::BFTConsensus;
use crate::hierarchical::main_chain::ConsensusType;
use crate::hierarchical::{HierarchyManager, MainChain, SubChain};
use crate::security::mock_verifier::{MockMode, MockVerifier};
use crate::security::security_utils::KeyPair;
use crate::security::zk_verifier::{Groth16Verifier, Verifier};
use crate::security::PyKeyPair;
use crate::utils::pyo3_helpers::{json_to_py, py_to_json};
use crate::utils::{dict_to_map, map_to_py_dict};

// ==================== PyBFTConsensus ====================

/// PyO3 wrapper for BFT Consensus mechanism.
/// Provides Python access to the Rust BFT implementation.
///
/// Byzantine Fault Tolerant consensus for distributed systems.
/// Requires n >= 3f + 1 nodes to tolerate f Byzantine faults.
#[pyclass(name = "BFTConsensus")]
pub struct PyBFTConsensus {
    inner: Arc<tokio::sync::Mutex<BFTConsensus>>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyBFTConsensus {
    /// Create a new BFT consensus instance.
    ///
    /// Args:
    ///     node_id: Current node ID
    ///     all_nodes: List of all validator node IDs
    ///     f: Maximum Byzantine faults to tolerate (n >= 3f + 1)
    ///     keypair: PyKeyPair for signing
    ///     node_public_keys: Dict of node_id -> public_key_hex
    ///
    /// Returns:
    ///     BFTConsensus: New consensus instance
    ///
    /// Raises:
    ///     ValueError: If parameters are invalid
    ///     RuntimeError: If runtime creation fails
    #[new]
    fn new(
        node_id: String,
        all_nodes: Vec<String>,
        f: usize,
        keypair: &PyKeyPair,
        node_public_keys: &Bound<PyDict>,
    ) -> PyResult<Self> {
        // Convert PyDict to HashMap
        let mut public_keys = StdHashMap::new();
        for (key, value) in node_public_keys.iter() {
            let key_str: &str = key.cast::<PyString>()?.to_str()?;
            let value_str: &str = value.cast::<PyString>()?.to_str()?;
            public_keys.insert(key_str.to_string(), value_str.to_string());
        }

        // Create runtime for async operations
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        // Create the inner Rust KeyPair from the Python wrapper
        let rust_keypair = KeyPair::from_private_key(&keypair.inner.private_key())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        // Create BFTConsensus
        let consensus = BFTConsensus::new(node_id, all_nodes, f, rust_keypair, public_keys)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        Ok(PyBFTConsensus {
            inner: Arc::new(tokio::sync::Mutex::new(consensus)),
            runtime: Arc::new(runtime),
        })
    }

    /// Get the node ID.
    fn node_id(&self) -> String {
        let inner = self.runtime.block_on(self.inner.lock());
        inner.node_id().to_string()
    }

    /// Get the fault tolerance value (f).
    fn fault_tolerance(&self) -> usize {
        let inner = self.runtime.block_on(self.inner.lock());
        inner.fault_tolerance()
    }

    /// Get total node count.
    fn node_count(&self) -> usize {
        let inner = self.runtime.block_on(self.inner.lock());
        inner.node_count()
    }

    /// Get the current primary node ID.
    fn primary(&self) -> String {
        let inner = self.runtime.block_on(self.inner.lock());
        self.runtime.block_on(inner.primary())
    }

    /// Check if this node is the primary.
    fn is_primary(&self) -> bool {
        let inner = self.runtime.block_on(self.inner.lock());
        self.runtime.block_on(inner.is_primary())
    }

    /// Submit a client request to consensus.
    ///
    /// Args:
    ///     operation: Dictionary containing the operation data
    ///
    /// Returns:
    ///     bool: True if request was accepted
    ///
    /// Raises:
    ///     RuntimeError: If consensus fails
    fn request(&self, operation: &Bound<PyDict>) -> PyResult<bool> {
        let mut op_map = StdHashMap::new();
        for (key, value) in operation.iter() {
            let key_str: &str = key.cast::<PyString>()?.to_str()?;
            let value_json = py_to_json(&value)?;
            op_map.insert(key_str.to_string(), value_json);
        }

        let inner = self.runtime.block_on(self.inner.lock());
        self.runtime
            .block_on(inner.request(op_map))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Handle an incoming consensus message.
    ///
    /// Args:
    ///     message: Dictionary containing the message data
    ///
    /// Returns:
    ///     bool: True if message was handled successfully
    ///
    /// Raises:
    ///     RuntimeError: If message handling fails
    fn handle_message(&self, message: &Bound<PyDict>) -> PyResult<bool> {
        let mut msg_map = StdHashMap::new();
        for (key, value) in message.iter() {
            let key_str: &str = key.cast::<PyString>()?.to_str()?;
            let value_json = py_to_json(&value)?;
            msg_map.insert(key_str.to_string(), value_json);
        }

        let inner = self.runtime.block_on(self.inner.lock());
        self.runtime
            .block_on(inner.handle_message(msg_map))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get current consensus status.
    ///
    /// Returns:
    ///     dict: Current status including view, phase, etc.
    fn get_consensus_status(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self.runtime.block_on(self.inner.lock());
        let status = self.runtime.block_on(inner.get_consensus_status());

        let dict = PyDict::new(py);
        for (key, value) in status {
            let py_value = json_to_py(py, &value)?;
            dict.set_item(key, py_value)?;
        }
        Ok(dict.into())
    }

    /// Shutdown the consensus mechanism.
    fn shutdown(&self) {
        let inner = self.runtime.block_on(self.inner.lock());
        self.runtime.block_on(inner.shutdown());
    }

    /// Initialize ZK Verifier
    ///
    /// Args:
    ///     mode: "mock" or "groth16"
    ///     verifying_key: Optional bytes for verifying key (required for groth16)
    #[pyo3(signature = (mode, verifying_key=None))]
    fn init_zk_verifier(&self, mode: &str, verifying_key: Option<&[u8]>) -> PyResult<()> {
        let verifier: Arc<dyn Verifier> = match mode {
            "mock" | "test" => Arc::new(MockVerifier::new(MockMode::AcceptAll)),
            "groth16" | "real" => {
                let mut v = Groth16Verifier::new();
                if let Some(vk) = verifying_key {
                    v.init_with_key(vk)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                }
                Arc::new(v)
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid ZK verifier mode. Use 'mock' or 'groth16'",
                ))
            }
        };

        let mut inner = self.runtime.block_on(self.inner.lock());
        inner.set_verifier(verifier);
        Ok(())
    }

    fn __str__(&self) -> String {
        let inner = self.runtime.block_on(self.inner.lock());
        format!(
            "BFTConsensus(node_id='{}', f={}, n={})",
            inner.node_id(),
            inner.fault_tolerance(),
            inner.node_count()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ==================== PyMainChain ====================

/// PyO3 wrapper for MainChain.
/// Provides Python access to the Rust MainChain implementation.
#[pyclass(name = "MainChain")]
pub struct PyMainChain {
    inner: MainChain,
}

#[pymethods]
impl PyMainChain {
    /// Create a new Main Chain.
    ///
    /// Args:
    ///     name: Name of the Main Chain (default: "MainChain")
    ///     consensus_type: Type of consensus - "proof_of_authority" or "proof_of_federation" (default: "proof_of_authority")
    #[new]
    #[pyo3(signature = (name = "MainChain", consensus_type = None))]
    pub fn new(name: &str, consensus_type: Option<&str>) -> Self {
        let ct = match consensus_type {
            Some(ct_str) => ConsensusType::from_str(ct_str),
            None => ConsensusType::ProofOfAuthority,
        };

        PyMainChain {
            inner: MainChain::with_consensus(name, ct),
        }
    }

    /// Create a new Main Chain with Proof of Federation consensus.
    #[staticmethod]
    #[pyo3(signature = (name = "MainChain"))]
    pub fn with_pof(name: &str) -> Self {
        PyMainChain {
            inner: MainChain::with_consensus(name, ConsensusType::ProofOfFederation),
        }
    }

    /// Get the chain name
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Get the consensus type
    #[getter]
    pub fn consensus_type(&self) -> &'static str {
        self.inner.consensus_type.as_str()
    }

    /// Get number of blocks
    #[getter]
    pub fn chain_length(&self) -> usize {
        self.inner.chain_length()
    }

    /// Get number of registered sub-chains
    #[getter]
    pub fn registered_sub_chains_count(&self) -> usize {
        self.inner.registered_sub_chains.len()
    }

    /// Get proof count
    #[getter]
    pub fn proof_count(&self) -> u64 {
        self.inner.proof_count
    }

    /// Check if sub-chain is registered
    pub fn is_registered(&self, sub_chain_name: &str) -> bool {
        self.inner.is_registered(sub_chain_name)
    }

    /// Register a Sub-Chain with the Main Chain.
    #[pyo3(signature = (sub_chain_name, metadata = None))]
    pub fn register_sub_chain(
        &mut self,
        sub_chain_name: &str,
        metadata: Option<&Bound<PyAny>>,
    ) -> PyResult<bool> {
        let meta = match metadata {
            Some(m) => Some(
                depythonize(m)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            ),
            None => None,
        };
        Ok(self.inner.register_sub_chain(sub_chain_name, meta))
    }

    /// Add a proof from a Sub-Chain.
    pub fn add_proof(
        &mut self,
        sub_chain_name: &str,
        proof_hash: &str,
        metadata: &Bound<PyAny>,
    ) -> PyResult<bool> {
        let meta: Value = depythonize(metadata)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.inner.add_proof(sub_chain_name, proof_hash, meta))
    }

    /// Verify a proof exists.
    pub fn verify_proof(&self, proof_hash: &str, sub_chain_name: &str) -> bool {
        self.inner.verify_proof(proof_hash, sub_chain_name)
    }

    /// Get proofs by Sub-Chain.
    pub fn get_proofs_by_sub_chain(&self, sub_chain_name: &str, py: Python) -> PyResult<Py<PyAny>> {
        let proofs = self.inner.get_proofs_by_sub_chain(sub_chain_name);
        let py_list = PyList::empty(py);
        for proof in proofs {
            let py_proof = pythonize(py, &proof)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            py_list.append(py_proof)?;
        }
        Ok(py_list.into())
    }

    /// Get Sub-Chain summary.
    pub fn get_sub_chain_summary(&self, sub_chain_name: &str, py: Python) -> PyResult<Py<PyAny>> {
        let summary = self.inner.get_sub_chain_summary(sub_chain_name);
        pythonize(py, &summary)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Finalize pending events into a block.
    pub fn finalize_block(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.finalize_block() {
            Some(block) => Ok(Some(block.to_dict(py)?)),
            None => Ok(None),
        }
    }

    /// Finalize a block and return info.
    pub fn finalize_main_chain_block(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.finalize_main_chain_block() {
            Some(info) => {
                let py_info = pythonize(py, &info)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                Ok(Some(py_info.unbind()))
            }
            None => Ok(None),
        }
    }

    /// Validate Sub-Chain proof format.
    pub fn validate_sub_chain_proof_format(&self, proof_data: &Bound<PyAny>) -> PyResult<bool> {
        let data: Value = depythonize(proof_data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.inner.validate_sub_chain_proof_format(&data))
    }

    /// Get hierarchical integrity report.
    pub fn get_hierarchical_integrity_report(&self, py: Python) -> PyResult<Py<PyAny>> {
        let report = self.inner.get_hierarchical_integrity_report();
        pythonize(py, &report)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Get Main Chain statistics.
    pub fn get_main_chain_stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        let stats = self.inner.get_main_chain_stats();
        pythonize(py, &stats)
            .map(|v| v.unbind())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Check if chain is valid.
    pub fn is_chain_valid(&self) -> bool {
        self.inner.is_chain_valid()
    }

    /// Check if entity is an authority.
    pub fn is_authority(&self, authority_id: &str) -> bool {
        self.inner.is_authority(authority_id)
    }

    /// Get registered sub-chains list.
    pub fn get_registered_sub_chains(&self) -> Vec<String> {
        self.inner.registered_sub_chains.iter().cloned().collect()
    }

    /// Add event to pending (for testing).
    pub fn add_event(&mut self, event: &Bound<PyAny>) -> PyResult<()> {
        let event_value: Value = depythonize(event)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.inner
            .blockchain
            .add_event(event_value)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    fn __str__(&self) -> String {
        format!(
            "MainChain(blocks={}, sub_chains={}, proofs={})",
            self.inner.blockchain.chain.len(),
            self.inner.registered_sub_chains.len(),
            self.inner.proof_count
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "MainChain(name={}, blocks={}, sub_chains={}, proofs={}, valid={})",
            self.inner.blockchain.name,
            self.inner.blockchain.chain.len(),
            self.inner.registered_sub_chains.len(),
            self.inner.proof_count,
            self.inner.blockchain.is_chain_valid()
        )
    }

    /// Initialize ZK Verifier for Main Chain
    #[pyo3(signature = (mode, verifying_key=None))]
    pub fn init_zk_verifier(&mut self, mode: &str, verifying_key: Option<&[u8]>) -> PyResult<()> {
        let verifier: Arc<dyn Verifier> = match mode {
            "mock" | "test" => Arc::new(MockVerifier::new(MockMode::AcceptAll)),
            "groth16" | "real" => {
                let mut v = Groth16Verifier::new();
                if let Some(vk) = verifying_key {
                    v.init_with_key(vk)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                }
                Arc::new(v)
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid ZK verifier mode. Use 'mock' or 'groth16'",
                ))
            }
        };

        self.inner.set_verifier(verifier);
        Ok(())
    }

    // --- Added for Hybrid Python Compatibility ---

    /// Get current state root (latest block hash)
    pub fn get_state_root(&self) -> String {
        self.inner.blockchain.get_latest_block().hash.clone()
    }

    /// Get blocks in range
    pub fn get_blocks(
        &self,
        py: Python,
        from_index: usize,
        to_index: usize,
    ) -> PyResult<Py<PyList>> {
        let blocks = &self.inner.blockchain.chain;
        let end = std::cmp::min(to_index, blocks.len());
        let start = std::cmp::min(from_index, end);

        let py_list = PyList::empty(py);
        for block in &blocks[start..end] {
            // Convert Block to Python object (Py<Block> would be better but simple dict is safer for managers)
            // Using to_dict(py)
            py_list.append(block.to_dict(py)?)?;
        }
        Ok(py_list.into())
    }

    /// Alias for add_proof to satisfy CrossLevelSyncManager
    pub fn receive_proof(&mut self, anchor_data: &Bound<PyAny>) -> PyResult<bool> {
        let data: Value = depythonize(anchor_data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        // Extract fields expected by add_proof
        let sub_chain_name = data
            .get("sub_chain_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Missing sub_chain_id"))?
            .to_string();

        let proof_hash = data
            .get("proof_hash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Missing proof_hash"))?
            .to_string();

        Ok(self.inner.add_proof(&sub_chain_name, &proof_hash, data))
    }

    fn __len__(&self) -> usize {
        self.inner.blockchain.chain.len()
    }
}

// ==================== PySubChain ====================

/// PyO3 wrapper for SubChain.
/// Provides Python access to the Rust SubChain implementation.
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
            Some(ct_str) => crate::hierarchical::sub_chain::ConsensusType::from_str(ct_str),
            None => crate::hierarchical::sub_chain::ConsensusType::ProofOfAuthority,
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

    /// Flush pending events and finalize them into a block with timeout.
    #[pyo3(signature = (timeout_seconds = 3.0))]
    pub fn flush_pending_and_finalize(
        &self,
        py: Python,
        timeout_seconds: f64,
    ) -> PyResult<Option<Py<PyAny>>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        match inner.flush_pending_and_finalize(timeout_seconds) {
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

    /// Initialize ZK Verifier for Sub Chain
    #[pyo3(signature = (mode, verifying_key=None))]
    pub fn init_zk_verifier(&self, mode: &str, verifying_key: Option<&[u8]>) -> PyResult<()> {
        let verifier: Arc<dyn Verifier> = match mode {
            "mock" | "test" => Arc::new(MockVerifier::new(MockMode::AcceptAll)),
            "groth16" | "real" => {
                let mut v = Groth16Verifier::new();
                if let Some(vk) = verifying_key {
                    v.init_with_key(vk)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                }
                Arc::new(v)
            }
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid ZK verifier mode. Use 'mock' or 'groth16'",
                ))
            }
        };

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        inner.set_verifier(verifier);
        Ok(())
    }

    // --- Added for Hybrid Python Compatibility ---

    /// Get current state root (latest block hash)
    pub fn get_state_root(&self) -> PyResult<String> {
        self.inner
            .lock()
            .map(|s| s.blockchain.get_latest_block().hash.clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Get blocks in range
    pub fn get_blocks(
        &self,
        py: Python,
        from_index: usize,
        to_index: usize,
    ) -> PyResult<Py<PyList>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        let blocks = &inner.blockchain.chain;
        let end = std::cmp::min(to_index, blocks.len());
        let start = std::cmp::min(from_index, end);

        let py_list = PyList::empty(py);
        for block in &blocks[start..end] {
            py_list.append(block.to_dict(py)?)?;
        }
        Ok(py_list.into())
    }

    /// Add a block (used by sync)
    pub fn add_block(&self, block_data: &Bound<PyAny>) -> PyResult<bool> {
        let block_val: Value = depythonize(block_data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        // Deserialize block
        let block: Block = serde_json::from_value(block_val).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid block data: {}", e))
        })?;

        Ok(inner.blockchain.add_block(block))
    }

    /// Add a synced block from another chain (bypasses chain link validation).
    ///
    /// This method is used for cross-chain sync where blocks maintain their
    /// original chain links from the source chain.
    pub fn add_synced_block(&self, block_data: &Bound<PyAny>) -> PyResult<bool> {
        let block_val: Value = depythonize(block_data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        // Deserialize block
        let block: Block = serde_json::from_value(block_val).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid block data: {}", e))
        })?;

        Ok(inner.blockchain.add_synced_block(block))
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

// ==================== PyHierarchyManager ====================

/// PyO3 wrapper for HierarchyManager.
/// Provides Python access to the Rust HierarchyManager implementation.
///
/// Manages the hierarchical structure including:
/// - MainChain and SubChains coordination
/// - Organization and Channel management
/// - Cross-chain proof submission
#[pyclass(name = "HierarchyManager")]
pub struct PyHierarchyManager {
    inner: Arc<Mutex<HierarchyManager>>,
}

#[pymethods]
impl PyHierarchyManager {
    /// Create a new HierarchyManager.
    ///
    /// Args:
    ///     main_chain_name: Name of the main chain (default: "MainChain")
    #[new]
    #[pyo3(signature = (main_chain_name = "MainChain"))]
    fn new(main_chain_name: &str) -> Self {
        PyHierarchyManager {
            inner: Arc::new(Mutex::new(HierarchyManager::new(main_chain_name))),
        }
    }

    /// Create and register a new sub-chain.
    #[pyo3(signature = (name, domain_type, metadata = None))]
    fn create_sub_chain(
        &self,
        name: &str,
        domain_type: &str,
        metadata: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let meta = if let Some(dict) = metadata {
            Some(dict_to_map(dict)?)
        } else {
            None
        };

        let mut manager = self.inner.lock().unwrap();
        match manager.create_sub_chain(name, domain_type, meta) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get a sub-chain by name (returns the sub-chain name if exists).
    fn get_sub_chain(&self, name: &str) -> Option<String> {
        let manager = self.inner.lock().unwrap();
        if manager
            .get_all_sub_chain_names()
            .contains(&name.to_string())
        {
            Some(name.to_string())
        } else {
            None
        }
    }

    /// Get all sub-chain names.
    fn get_all_sub_chains(&self) -> Vec<String> {
        let manager = self.inner.lock().unwrap();
        manager.get_all_sub_chain_names()
    }

    /// Start an operation on a specific sub-chain.
    #[pyo3(signature = (sub_chain_name, entity_id, operation_type, details = None))]
    fn start_operation(
        &self,
        sub_chain_name: &str,
        entity_id: &str,
        operation_type: &str,
        details: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let detail_map = if let Some(dict) = details {
            Some(dict_to_map(dict)?)
        } else {
            None
        };

        let manager = self.inner.lock().unwrap();
        match manager.start_operation(sub_chain_name, entity_id, operation_type, detail_map) {
            Ok(result) => Ok(result),
            Err(_) => Ok(false),
        }
    }

    /// Complete an operation on a specific sub-chain.
    #[pyo3(signature = (sub_chain_name, entity_id, operation_type, result = None))]
    fn complete_operation(
        &self,
        sub_chain_name: &str,
        entity_id: &str,
        operation_type: &str,
        result: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let result_map = if let Some(dict) = result {
            Some(dict_to_map(dict)?)
        } else {
            None
        };

        let manager = self.inner.lock().unwrap();
        match manager.complete_operation(sub_chain_name, entity_id, operation_type, result_map) {
            Ok(res) => Ok(res),
            Err(_) => Ok(false),
        }
    }

    /// Submit a proof from a sub-chain to the main chain.
    fn submit_proof_to_main_chain(&self, sub_chain_name: &str) -> bool {
        let mut manager = self.inner.lock().unwrap();
        manager
            .submit_proof_to_main_chain(sub_chain_name)
            .unwrap_or(false)
    }

    /// Submit proofs for all sub-chains.
    fn submit_all_proofs(&self, py: Python) -> PyResult<Py<PyDict>> {
        let mut manager = self.inner.lock().unwrap();
        let results = manager.submit_all_proofs();

        let dict = PyDict::new(py);
        for (name, success) in results {
            dict.set_item(name, success)?;
        }
        Ok(dict.into())
    }

    /// Get system overview.
    fn get_system_overview(&self, py: Python) -> PyResult<Py<PyDict>> {
        let manager = self.inner.lock().unwrap();
        let overview = manager.get_system_overview();
        map_to_py_dict(py, &overview)
    }

    /// Configure automatic proof submission.
    fn configure_auto_proof_submission(&self, enabled: bool, interval: u64) {
        let mut manager = self.inner.lock().unwrap();
        manager.configure_auto_proof_submission(enabled, interval);
    }

    /// Create an organization.
    #[pyo3(signature = (org_id, name, admin_users = None))]
    fn create_organization(
        &self,
        org_id: &str,
        name: &str,
        admin_users: Option<Vec<String>>,
    ) -> PyResult<bool> {
        let mut manager = self.inner.lock().unwrap();
        match manager.create_organization(org_id, name, admin_users) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get organization by ID.
    fn get_organization(&self, py: Python, org_id: &str) -> PyResult<Option<Py<PyDict>>> {
        let manager = self.inner.lock().unwrap();
        if let Some(org) = manager.get_organization(org_id) {
            let dict = PyDict::new(py);
            dict.set_item("org_id", &org.org_id)?;
            dict.set_item("name", &org.name)?;
            dict.set_item("msp_id", &org.msp_id)?;
            dict.set_item("admin_users", &org.admin_users)?;
            dict.set_item("created_at", org.created_at)?;
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }

    /// Create a channel.
    #[pyo3(signature = (channel_id, org_ids, policy_config = None))]
    fn create_channel(
        &self,
        channel_id: &str,
        org_ids: Vec<String>,
        policy_config: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let policy = if let Some(dict) = policy_config {
            Some(dict_to_map(dict)?)
        } else {
            None
        };

        let mut manager = self.inner.lock().unwrap();
        match manager.create_channel(channel_id, org_ids, policy) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Get channel by ID.
    fn get_channel(&self, py: Python, channel_id: &str) -> PyResult<Option<Py<PyDict>>> {
        let manager = self.inner.lock().unwrap();
        if let Some(channel) = manager.get_channel(channel_id) {
            let dict = PyDict::new(py);
            dict.set_item("channel_id", &channel.channel_id)?;
            dict.set_item("org_ids", channel.get_org_ids())?;
            dict.set_item("status", channel.status.as_str())?;
            dict.set_item("created_at", channel.created_at)?;
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }

    /// Create a private data collection.
    #[pyo3(signature = (name, org_ids, config = None))]
    fn create_private_collection(
        &self,
        name: &str,
        org_ids: Vec<String>,
        config: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let cfg = if let Some(dict) = config {
            Some(dict_to_map(dict)?)
        } else {
            None
        };

        let mut manager = self.inner.lock().unwrap();
        match manager.create_private_collection(name, org_ids, cfg) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Alias for create_private_collection.
    #[pyo3(signature = (name, org_ids, config = None))]
    fn create_private_data_collection(
        &self,
        name: &str,
        org_ids: Vec<String>,
        config: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        self.create_private_collection(name, org_ids, config)
    }

    /// Get private collection by name.
    fn get_private_collection(&self, py: Python, name: &str) -> PyResult<Option<Py<PyDict>>> {
        let manager = self.inner.lock().unwrap();
        if let Some(collection) = manager.get_private_collection(name) {
            let dict = PyDict::new(py);
            dict.set_item("name", &collection.name)?;
            dict.set_item("member_org_ids", &collection.member_org_ids)?;
            dict.set_item("created_at", collection.created_at)?;
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }

    /// Assign an organization to a chain.
    fn assign_organization_to_chain(&self, org_id: &str, chain_name: &str) -> bool {
        let mut manager = self.inner.lock().unwrap();
        manager
            .assign_organization_to_chain(org_id, chain_name)
            .unwrap_or(false)
    }

    /// Validate cross-chain consistency.
    fn validate_cross_chain_consistency(&self, py: Python) -> PyResult<Py<PyDict>> {
        let manager = self.inner.lock().unwrap();
        let result = manager.validate_cross_chain_consistency();
        map_to_py_dict(py, &result)
    }

    /// Execute system maintenance.
    fn execute_system_maintenance(&self, py: Python) -> PyResult<Py<PyDict>> {
        let mut manager = self.inner.lock().unwrap();
        let result = manager.execute_system_maintenance();
        map_to_py_dict(py, &result)
    }

    /// Finalize main chain block.
    fn finalize_main_chain_block(&self) -> Option<String> {
        let mut manager = self.inner.lock().unwrap();
        manager.finalize_main_chain_block().map(|b| b.hash)
    }

    /// Get uptime.
    fn get_uptime(&self) -> f64 {
        let manager = self.inner.lock().unwrap();
        manager.get_uptime()
    }

    fn __str__(&self) -> String {
        let manager = self.inner.lock().unwrap();
        format!("{}", manager)
    }

    fn __repr__(&self) -> String {
        let manager = self.inner.lock().unwrap();
        format!("{:?}", manager)
    }
}

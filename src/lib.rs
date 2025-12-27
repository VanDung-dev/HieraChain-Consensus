//! HieraChain Consensus Library
//!
//! This library provides the consensus mechanisms for the HieraChain blockchain platform.
//! It includes implementations of consensus algorithms, node management, and message handling.
//! The library is designed to be used with Python through PyO3 bindings.

use crate::consensus::types::{ArrowEventData, EventPayload};
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use crossbeam_channel::Receiver;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::IntoPyObjectExt;
use serde_json::{Map, Value};
use std::sync::Arc;

// Import modules
pub mod consensus;
pub mod core;
pub mod error_mitigation;
pub mod hierarchical;
pub mod security;

use crate::consensus::{OrderingNode, OrderingService, OrderingStatus, PendingEvent};
use crate::core::consensus::base_consensus::BaseConsensusTrait;

/// Convert Python object to serde_json::Value
fn py_to_json(obj: &Bound<PyAny>) -> PyResult<Value> {
    if let Ok(val) = obj.cast::<PyString>() {
        Ok(Value::String(val.to_str()?.to_string()))
    } else if let Ok(val) = obj.cast::<PyFloat>() {
        Ok(Value::Number(
            serde_json::Number::from_f64(val.value()).unwrap_or(serde_json::Number::from(0)),
        ))
    } else if let Ok(val) = obj.cast::<PyInt>() {
        // Try to get as i64 first, if that fails, get as u64
        if let Ok(v) = val.extract::<i64>() {
            Ok(Value::Number(v.into()))
        } else if let Ok(v) = val.extract::<u64>() {
            Ok(Value::Number(v.into()))
        } else {
            Ok(Value::Number(0.into()))
        }
    } else if let Ok(val) = obj.cast::<PyBool>() {
        Ok(Value::Bool(val.is_true()))
    } else if let Ok(val) = obj.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in val.iter() {
            vec.push(py_to_json(&item)?);
        }
        Ok(Value::Array(vec))
    } else if let Ok(val) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (key, value) in val.iter() {
            let key_str: &str = key.cast::<PyString>()?.to_str()?;
            map.insert(key_str.to_string(), py_to_json(&value)?);
        }
        Ok(Value::Object(map))
    } else if obj.is_none() {
        Ok(Value::Null)
    } else {
        // Fallback - try to convert to string
        Ok(Value::String(obj.str()?.to_str()?.to_string()))
    }
}

/// Convert serde_json::Value to Python object
fn json_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None().into()),
        Value::Bool(b) => {
            let obj = PyBool::new(py, *b).into_py_any(py)?;
            Ok(obj.into())
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let obj = PyInt::new(py, i).into_py_any(py)?;
                Ok(obj.into())
            } else if let Some(u) = n.as_u64() {
                let obj = PyInt::new(py, u).into_py_any(py)?;
                Ok(obj.into())
            } else if let Some(f) = n.as_f64() {
                let py_float = PyFloat::new(py, f);
                Ok(py_float.into())
            } else {
                let obj = PyInt::new(py, 0i64).into_py_any(py)?;
                Ok(obj.into())
            }
        }
        Value::String(s) => {
            let py_string = PyString::new(py, s);
            Ok(py_string.into())
        }
        Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (key, value) in obj {
                let py_value = json_to_py(py, value)?;
                dict.set_item(key, py_value)?;
            }
            Ok(dict.into())
        }
    }
}

/// Convert Python dict to Rust Map
#[allow(dead_code)]
fn dict_to_map(dict: &Bound<PyDict>) -> PyResult<Map<String, Value>> {
    let mut map = Map::new();
    for (key, value) in dict.iter() {
        let key_str: &str = key.cast::<PyString>()?.to_str()?;
        let value_json = py_to_json(&value)?;
        map.insert(key_str.to_string(), value_json);
    }
    Ok(map)
}

/// Convert Rust Map to Python dict
#[allow(dead_code)]
fn map_to_dict(py: Python, map: &Map<String, Value>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        let py_value = json_to_py(py, value)?;
        dict.set_item(key, py_value)?;
    }
    Ok(dict.into())
}

// ==================== PyO3 Wrapper Classes ====================

/// PyO3 wrapper for OrderingNode
#[pyclass]
#[derive(Clone)]
pub struct PyOrderingNode {
    #[pyo3(get, set)]
    pub node_id: String,
    #[pyo3(get, set)]
    pub endpoint: String,
    #[pyo3(get, set)]
    pub is_leader: bool,
    #[pyo3(get, set)]
    pub weight: f64,
    #[pyo3(get, set)]
    pub status: String,
    #[pyo3(get, set)]
    pub last_heartbeat: f64,
}

#[pymethods]
impl PyOrderingNode {
    #[new]
    fn new(
        node_id: String,
        endpoint: String,
        is_leader: bool,
        weight: f64,
        status: String,
        last_heartbeat: f64,
    ) -> Self {
        PyOrderingNode {
            node_id,
            endpoint,
            is_leader,
            weight,
            status,
            last_heartbeat,
        }
    }

    fn is_healthy(&self, timeout: f64) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        (current_time - self.last_heartbeat) < timeout
    }

    fn __str__(&self) -> String {
        format!(
            "OrderingNode(node_id='{}', endpoint='{}', is_leader={}, status='{}')",
            self.node_id, self.endpoint, self.is_leader, self.status
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

/// PyO3 wrapper for OrderingService
#[pyclass]
pub struct PyOrderingService {
    inner: Arc<OrderingService>,
}

// Helper function to start the processing thread
fn start_ordering_service_processing(
    service: Arc<OrderingService>,
    receiver: Receiver<PendingEvent>,
) {
    OrderingService::start(service, receiver);
}

#[pymethods]
impl PyOrderingService {
    #[new]
    fn new(nodes: Vec<PyOrderingNode>, config: &Bound<PyDict>) -> PyResult<Self> {
        let rust_nodes: Vec<OrderingNode> = nodes
            .into_iter()
            .map(|n| OrderingNode {
                node_id: n.node_id,
                endpoint: n.endpoint,
                is_leader: n.is_leader,
                weight: n.weight,
                status: match n.status.as_str() {
                    "active" => OrderingStatus::Active,
                    "maintenance" => OrderingStatus::Maintenance,
                    "stopped" => OrderingStatus::Stopped,
                    "error" => OrderingStatus::Error,
                    _ => OrderingStatus::Active,
                },
                last_heartbeat: n.last_heartbeat,
            })
            .collect();

        let config_json = dict_to_json(config)?;
        let (service_arc, receiver) = OrderingService::new(rust_nodes, config_json);

        // Start the processing thread immediately
        start_ordering_service_processing(Arc::clone(&service_arc), receiver);

        Ok(PyOrderingService { inner: service_arc })
    }

    fn receive_event(
        &self,
        event_data: &Bound<PyAny>,
        channel_id: String,
        submitter_org: String,
    ) -> PyResult<String> {
        // Try to convert to Arrow RecordBatch first
        let payload = if let Ok(batch) = RecordBatch::from_pyarrow_bound(event_data) {
            EventPayload::Arrow(ArrowEventData {
                batch: Arc::new(batch),
                schema_digest: "digest_placeholder".to_string(),
            })
        } else {
            // Fallback to JSON
            // We need to cast to PyDict if we expect a dict for JSON conversion
            if let Ok(dict) = event_data.downcast::<PyDict>() {
                let json = dict_to_json(dict)?;
                EventPayload::Json(json)
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Expected PyArrow Table or dict",
                ));
            }
        };

        Ok(self.inner.receive_event(payload, channel_id, submitter_org))
    }

    fn get_event_status(&self, event_id: String, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.get_event_status(&event_id) {
            Some(status) => Ok(Some(json_to_py(py, &status)?)),
            None => Ok(None),
        }
    }

    fn get_next_block(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.get_next_block() {
            Some(block) => Ok(Some(json_to_py(py, &block)?)),
            None => Ok(None),
        }
    }

    fn get_service_status(&self, py: Python) -> PyResult<Py<PyAny>> {
        let status = self.inner.get_service_status();
        json_to_py(py, &status)
    }

    fn add_validation_rule(&self, _rule: Py<PyAny>, _py: Python) -> PyResult<()> {
        Ok(())
    }

    // The start method is now handled internally during new()
    fn start(&self) {}

    fn stop(&self) {
        self.inner.stop();
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        self.inner.to_repr()
    }
}

// ==================== Helper Functions for PyO3 ====================

/// Convert Python dict to serde_json::Value
fn dict_to_json(dict: &Bound<PyDict>) -> PyResult<Value> {
    let mut map = Map::new();
    for (key, value) in dict.iter() {
        let key_str: &str = key.cast::<PyString>()?.to_str()?;
        let value_json = py_to_json(&value)?;
        map.insert(key_str.to_string(), value_json);
    }
    Ok(Value::Object(map))
}

// ==================== PyO3 Functions ====================

/// Validate a block using Proof of Authority
#[pyfunction]
fn validate_poa_block(block_data: &Bound<PyDict>, authority_id: &str) -> PyResult<bool> {
    let _block_json = dict_to_json(block_data)?;
    // Implement actual POA validation logic
    Ok(!authority_id.is_empty())
}

#[pyfunction]
fn calculate_block_hash(block_data: &Bound<PyDict>, py: Python) -> PyResult<Py<PyAny>> {
    use crate::core::utils::generate_hash;
    let block_json = dict_to_json(block_data)?;
    let hash = generate_hash(&block_json);
    Ok(PyString::new(py, &hash).into())
}

/// Bulk validate transactions
#[pyfunction]
fn bulk_validate_transactions(transactions: &Bound<PyList>) -> PyResult<bool> {
    for item in transactions.iter() {
        let tx_dict = item.cast::<PyDict>()?;
        let _tx_json = dict_to_json(&tx_dict)?;
        // Implement actual transaction validation logic
    }
    Ok(true)
}

/// Batch create multiple blocks at once - reduces FFI overhead
/// Returns a list of Block objects
#[pyfunction]
fn batch_create_blocks(
    py: Python,
    events_list: &Bound<PyList>,
    start_index: u64,
    previous_hash: &str,
) -> PyResult<Vec<Py<crate::core::block::Block>>> {
    let mut blocks = Vec::with_capacity(events_list.len());
    let mut prev_hash = previous_hash.to_string();

    for (i, events) in events_list.iter().enumerate() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("previous_hash", &prev_hash)?;

        let block = crate::core::block::Block::new(
            start_index + i as u64,
            &events,
            Some(&kwargs.as_borrowed()),
        )?;

        prev_hash = block.hash.clone();
        blocks.push(Py::new(py, block)?);
    }

    Ok(blocks)
}

/// Batch calculate hashes for multiple data items - reduces FFI overhead
/// Accepts a list of dicts and returns a list of hash strings
#[pyfunction]
fn batch_calculate_hashes(data_list: &Bound<PyList>, py: Python) -> PyResult<Vec<Py<PyAny>>> {
    use crate::core::utils::generate_hash;

    let mut results = Vec::with_capacity(data_list.len());

    for item in data_list.iter() {
        let dict = item
            .downcast::<PyDict>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("Expected dict"))?;
        let json_value = dict_to_json(&dict)?;
        let hash = generate_hash(&json_value);
        results.push(PyString::new(py, &hash).into());
    }

    Ok(results)
}

/// Generate Merkle root from a list of events - optimized Rust implementation
#[pyfunction]
fn calculate_merkle_root(events: &Bound<PyList>) -> PyResult<String> {
    use crate::core::utils::{generate_hash, MerkleTree};
    use pythonize::depythonize;

    // Convert Python list to Vec<Value>
    let values: Vec<Value> =
        depythonize(events).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    // Generate leaves (hashes of each event)
    let leaves: Vec<String> = values.iter().map(generate_hash).collect();

    // Build tree from pre-computed leaves
    let tree = MerkleTree::from_leaves(leaves);
    Ok(tree.get_root())
}

// ==================== PyO3 Wrapper for ProofOfFederation ====================

use crate::core::consensus::proof_of_federation::ProofOfFederation;

/// PyO3 wrapper for Proof of Federation consensus mechanism.
/// Provides Python access to the Rust PoF implementation.
#[pyclass(name = "ProofOfFederation")]
pub struct PyProofOfFederation {
    inner: ProofOfFederation,
}

#[pymethods]
impl PyProofOfFederation {
    /// Create a new Proof of Federation consensus instance.
    #[new]
    #[pyo3(signature = (name=None))]
    fn new(name: Option<&str>) -> Self {
        PyProofOfFederation {
            inner: ProofOfFederation::new(name.unwrap_or("ProofOfFederation")),
        }
    }

    /// Get the consensus name.
    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    /// Get the list of validators.
    #[getter]
    fn validators(&self) -> Vec<String> {
        self.inner.validators.clone()
    }

    /// Get the number of active validators.
    fn get_validator_count(&self) -> usize {
        self.inner.get_validator_count()
    }

    /// Add a validator to the federation.
    /// Returns True if added successfully, False if already exists.
    #[pyo3(signature = (validator_id, metadata=None))]
    fn add_validator(
        &mut self,
        validator_id: String,
        metadata: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let meta = if let Some(dict) = metadata {
            Some(dict_to_map(dict)?)
        } else {
            None
        };
        Ok(self.inner.add_validator(validator_id, meta))
    }

    /// Remove a validator from the federation.
    /// Returns True if removed successfully, False if not found.
    fn remove_validator(&mut self, validator_id: &str) -> bool {
        self.inner.remove_validator(validator_id)
    }

    /// Check if an ID is an active validator.
    fn is_authority(&self, authority_id: &str) -> bool {
        self.inner.is_authority(authority_id)
    }

    /// Determine the expected leader for a specific block index.
    /// Returns the validator ID or None if no validators.
    fn get_current_leader(&self, block_index: u64) -> Option<String> {
        self.inner.get_current_leader(block_index).cloned()
    }

    /// Validate if the proposer is the correct leader for this block height.
    fn validate_block_proposer(&self, block_index: u64, proposer_id: &str) -> bool {
        self.inner.validate_block_proposer(block_index, proposer_id)
    }

    /// Check if a block can be created given the current state.
    #[pyo3(signature = (authority_id=None))]
    fn can_create_block(&self, authority_id: Option<&str>) -> bool {
        self.inner.can_create_block(authority_id)
    }

    /// Estimate the time required to create a new block.
    fn estimate_block_time(&self) -> f64 {
        self.inner.estimate_block_time()
    }

    /// Get consensus information as a dictionary.
    fn get_consensus_info(&self, py: Python) -> PyResult<Py<PyAny>> {
        use crate::core::consensus::base_consensus::BaseConsensusTrait;
        let info = self.inner.get_consensus_info();
        let dict = PyDict::new(py);
        for (key, value) in info {
            let py_value = json_to_py(py, &value)?;
            dict.set_item(key, py_value)?;
        }
        Ok(dict.into())
    }

    fn __str__(&self) -> String {
        format!(
            "ProofOfFederation(name='{}', validators={})",
            self.inner.name,
            self.inner.validators.len()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ==================== PyO3 Wrapper for KeyPair ====================

use crate::security::security_utils::{verify_signature as rs_verify_signature, KeyPair};

/// PyO3 wrapper for Ed25519 KeyPair.
/// Provides Python access to cryptographic signing and verification.
#[pyclass(name = "KeyPair")]
#[derive(Clone)]
pub struct PyKeyPair {
    inner: KeyPair,
}

#[pymethods]
impl PyKeyPair {
    /// Generate a new random Ed25519 key pair.
    #[staticmethod]
    fn generate() -> Self {
        PyKeyPair {
            inner: KeyPair::generate(),
        }
    }

    /// Create a key pair from a hex-encoded private key.
    #[staticmethod]
    fn from_private_key(private_key_hex: &str) -> PyResult<Self> {
        KeyPair::from_private_key(private_key_hex)
            .map(|kp| PyKeyPair { inner: kp })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Get the public key as a hex string.
    #[getter]
    fn public_key(&self) -> String {
        self.inner.public_key()
    }

    /// Get the private key as a hex string.
    /// Warning: This exposes sensitive data!
    #[getter]
    fn private_key(&self) -> String {
        self.inner.private_key()
    }

    /// Sign a message and return the signature as a hex string.
    fn sign(&self, message: &[u8]) -> PyResult<String> {
        self.inner
            .sign(message)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __str__(&self) -> String {
        format!("KeyPair(public_key='{}')", self.inner.public_key())
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

/// Verify an Ed25519 signature.
#[pyfunction]
fn verify_signature(public_key_hex: &str, message: &[u8], signature_hex: &str) -> bool {
    rs_verify_signature(public_key_hex, message, signature_hex)
}

// ==================== PyO3 Wrapper for BFTConsensus ====================

use crate::hierarchical::consensus::bft_consensus::BFTConsensus;
use std::collections::HashMap as StdHashMap;

/// PyO3 wrapper for BFT Consensus mechanism.
/// Provides Python access to the Rust BFT implementation.
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

/// Python module
#[pymodule]
fn hierachain_consensus(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Add consensus functions
    m.add_function(wrap_pyfunction!(validate_poa_block, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_block_hash, m)?)?;
    m.add_function(wrap_pyfunction!(bulk_validate_transactions, m)?)?;

    // Add optimized batch functions
    m.add_function(wrap_pyfunction!(batch_create_blocks, m)?)?;
    m.add_function(wrap_pyfunction!(batch_calculate_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_merkle_root, m)?)?;

    // Add PyO3 classes
    m.add_class::<PyOrderingNode>()?;
    m.add_class::<PyOrderingService>()?;
    m.add_class::<crate::core::block::Block>()?;
    m.add_class::<crate::core::blockchain::PyBlockchain>()?;
    m.add_class::<crate::hierarchical::main_chain::PyMainChain>()?;
    m.add_class::<PyProofOfFederation>()?;

    // Add BFT consensus classes
    m.add_class::<PyKeyPair>()?;
    m.add_class::<PyBFTConsensus>()?;

    // Add signature verification function
    m.add_function(wrap_pyfunction!(verify_signature, m)?)?;

    Ok(())
}

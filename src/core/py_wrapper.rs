//! PyO3 Wrappers for Core Module
//!
//! This module provides Python bindings for core blockchain components:
//! - `PyBlockchain` - Python wrapper for Blockchain management  
//! - `PyProofOfAuthority` - Python wrapper for Proof of Authority consensus
//! - `PyProofOfFederation` - Python wrapper for Proof of Federation consensus
//!
//! Note: `Block` is exposed directly via #[pyclass] in block.rs

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use crate::core::block::Block;
use crate::core::blockchain::Blockchain;
use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::consensus::proof_of_authority::ProofOfAuthority;
use crate::core::consensus::proof_of_federation::ProofOfFederation;
use crate::utils::pyo3_helpers::{dict_to_map, json_to_py};

// ==================== PyBlockchain ====================

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
    pub fn add_block(&mut self, block: &Bound<PyAny>) -> PyResult<bool> {
        let rust_block: Block = depythonize(block)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
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
    pub fn is_valid_new_block(&self, block: &Bound<PyAny>) -> PyResult<bool> {
        let rust_block: Block = depythonize(block)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
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

// ==================== PyProofOfAuthority ====================

/// PyO3 wrapper for Proof of Authority consensus mechanism.
/// Provides Python access to the Rust PoA implementation.
#[pyclass(name = "ProofOfAuthority")]
pub struct PyProofOfAuthority {
    inner: ProofOfAuthority,
}

#[pymethods]
impl PyProofOfAuthority {
    /// Create a new Proof of Authority consensus instance.
    ///
    /// Args:
    ///     name: Optional name for the consensus instance
    #[new]
    #[pyo3(signature = (name=None))]
    fn new(name: Option<&str>) -> Self {
        PyProofOfAuthority {
            inner: ProofOfAuthority::new(name.unwrap_or("ProofOfAuthority")),
        }
    }

    /// Get the consensus name.
    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    /// Get the list of authorities.
    #[getter]
    fn authorities(&self) -> Vec<String> {
        self.inner.authorities.iter().cloned().collect()
    }

    /// Get the number of active authorities.
    fn get_authority_count(&self) -> usize {
        self.inner.authorities.len()
    }

    /// Add an authority to the PoA consensus.
    /// Returns True if added successfully, False if already exists.
    ///
    /// Args:
    ///     authority_id: ID of the authority to add
    ///     metadata: Optional metadata dictionary for the authority
    #[pyo3(signature = (authority_id, metadata=None))]
    fn add_authority(
        &mut self,
        authority_id: String,
        metadata: Option<&Bound<PyDict>>,
    ) -> PyResult<bool> {
        let meta = if let Some(dict) = metadata {
            Some(dict_to_map(dict)?)
        } else {
            None
        };
        Ok(self.inner.add_authority(authority_id, meta))
    }

    /// Remove an authority from the PoA consensus.
    /// Returns True if removed successfully, False if not found.
    fn remove_authority(&mut self, authority_id: &str) -> bool {
        self.inner.remove_authority(authority_id)
    }

    /// Check if an ID is an active authority.
    fn is_authority(&self, authority_id: &str) -> bool {
        self.inner.is_authority(authority_id)
    }

    /// Get the next authority in round-robin order.
    ///
    /// Args:
    ///     current_block_index: Current block index to determine next authority
    fn get_next_authority(&self, current_block_index: u64) -> Option<String> {
        self.inner.get_next_authority(current_block_index)
    }

    /// Check if a block can be created given the current state.
    #[pyo3(signature = (authority_id=None))]
    fn can_create_block(&self, authority_id: Option<&str>) -> bool {
        self.inner.can_create_block(authority_id)
    }

    /// Get consensus information as a dictionary.
    fn get_consensus_info(&self, py: Python) -> PyResult<Py<PyAny>> {
        let info = self.inner.get_consensus_info();
        let dict = PyDict::new(py);
        for (key, value) in info {
            let py_value = json_to_py(py, &value)?;
            dict.set_item(key, py_value)?;
        }
        Ok(dict.into())
    }

    /// Get the number of validators (alias for get_authority_count).
    fn get_validator_count(&self) -> u64 {
        BaseConsensusTrait::get_validator_count(&self.inner)
    }

    fn __str__(&self) -> String {
        format!(
            "ProofOfAuthority(name='{}', authorities={})",
            self.inner.name,
            self.inner.authorities.len()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ==================== PyProofOfFederation ====================

/// PyO3 wrapper for Proof of Federation consensus mechanism.
/// Provides Python access to the Rust PoF implementation.
///
/// A Round-Robin based consensus mechanism suitable for semi-trusted consortiums.
#[pyclass(name = "ProofOfFederation")]
pub struct PyProofOfFederation {
    inner: ProofOfFederation,
}

#[pymethods]
impl PyProofOfFederation {
    /// Create a new Proof of Federation consensus instance.
    ///
    /// Args:
    ///     name: Optional name for the consensus instance
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
    ///
    /// Args:
    ///     validator_id: ID of the validator to add
    ///     metadata: Optional metadata dictionary for the validator
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
    ///
    /// Args:
    ///     block_index: Block index to determine leader for
    fn get_current_leader(&self, block_index: u64) -> Option<String> {
        self.inner.get_current_leader(block_index).cloned()
    }

    /// Validate if the proposer is the correct leader for this block height.
    ///
    /// Args:
    ///     block_index: Block index to check
    ///     proposer_id: ID of the proposer to validate
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
        let info = BaseConsensusTrait::get_consensus_info(&self.inner);
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

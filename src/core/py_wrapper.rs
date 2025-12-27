//! PyO3 Wrappers for Core Consensus Module
//!
//! This module provides Python bindings for consensus mechanisms:
//! - `PyProofOfAuthority` - Python wrapper for Proof of Authority consensus
//! - `PyProofOfFederation` - Python wrapper for Proof of Federation consensus

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::core::consensus::base_consensus::BaseConsensusTrait;
use crate::core::consensus::proof_of_authority::ProofOfAuthority;
use crate::core::consensus::proof_of_federation::ProofOfFederation;
use crate::utils::pyo3_helpers::{dict_to_map, json_to_py};

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

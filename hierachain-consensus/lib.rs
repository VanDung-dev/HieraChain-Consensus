//! HieraChain Consensus Library
//!
//! This library provides the consensus mechanisms for the HieraChain blockchain platform.
//! It includes implementations of consensus algorithms, node management, and message handling.
//! The library is designed to be used with Python through PyO3 bindings.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use serde_json::Value;

// Import modules
pub mod consensus;
pub mod core;
pub mod error_mitigation;
pub mod ffi;
pub mod hierarchical;
pub mod security;
pub mod utils;

// Re-export PyO3 wrappers from their respective modules
use crate::consensus::{PyOrderingNode, PyOrderingService};
use crate::core::{Block, PyBlockchain, PyProofOfAuthority, PyProofOfFederation};
use crate::hierarchical::{PyBFTConsensus, PyHierarchyManager, PyMainChain, PySubChain};
use crate::security::{py_verify_signature, PyKeyPair};

// ==================== Helper Functions for PyO3 ====================

/// Convert Python dict to serde_json::Value
fn dict_to_json(dict: &Bound<PyDict>) -> PyResult<Value> {
    crate::utils::pyo3_helpers::dict_to_json(dict)
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

    // Add Ordering Service classes (from consensus/py_wrapper.rs)
    m.add_class::<PyOrderingNode>()?;
    m.add_class::<PyOrderingService>()?;

    // Add Core classes (from core/block.rs)
    m.add_class::<Block>()?;

    // Add Blockchain core classes (from core/py_wrapper.rs)
    m.add_class::<PyProofOfAuthority>()?;
    m.add_class::<PyProofOfFederation>()?;
    m.add_class::<PyBlockchain>()?;

    // Add Hierarchical chain classes (from hierarchical/py_wrapper.rs)
    m.add_class::<PyBFTConsensus>()?;
    m.add_class::<PyMainChain>()?;
    m.add_class::<PySubChain>()?;
    m.add_class::<PyHierarchyManager>()?;

    // Add Security classes (from security/py_wrapper.rs)
    m.add_class::<PyKeyPair>()?;

    // Add signature verification function
    m.add_function(wrap_pyfunction!(py_verify_signature, m)?)?;

    Ok(())
}

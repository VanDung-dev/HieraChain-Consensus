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
    utils::pyo3_helpers::dict_to_json(dict)
}

// ==================== PyO3 Functions ====================

/// Validate a block using Proof of Authority consensus rules.
///
/// Checks:
/// - Block has required fields (index, timestamp, events, hash)
/// - Authority ID is valid (non-empty)
/// - Block structure validation
///
/// # Arguments
/// * `block_data` - Dictionary containing block data
/// * `authority_id` - ID of the authority that created the block
///
/// # Returns
/// * `true` if block is valid, `false` otherwise
#[pyfunction]
fn validate_poa_block(block_data: &Bound<PyDict>, authority_id: &str) -> PyResult<bool> {
    use crate::core::utils::validate_event_structure;

    // Authority ID must be non-empty
    if authority_id.is_empty() {
        return Ok(false);
    }

    let block_json = dict_to_json(block_data)?;

    // Block must be an object
    let block_obj = match block_json.as_object() {
        Some(obj) => obj,
        None => return Ok(false),
    };

    // Check required fields exist
    let required_fields = ["index", "timestamp", "hash"];
    for field in required_fields {
        if !block_obj.contains_key(field) {
            return Ok(false);
        }
    }

    // Validate timestamp is a number
    if let Some(ts) = block_obj.get("timestamp") {
        if !ts.is_number() {
            return Ok(false);
        }
    }

    // Validate index is a number
    if let Some(idx) = block_obj.get("index") {
        if !idx.is_number() {
            return Ok(false);
        }
    }

    // Validate events if present
    if let Some(events) = block_obj.get("events") {
        if let Some(events_arr) = events.as_array() {
            for event in events_arr {
                if !validate_event_structure(event) {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

#[pyfunction]
fn calculate_block_hash(block_data: &Bound<PyDict>, py: Python) -> PyResult<Py<PyAny>> {
    use crate::core::utils::generate_hash;
    let block_json = dict_to_json(block_data)?;
    let hash = generate_hash(&block_json);
    Ok(PyString::new(py, &hash).into())
}

/// Bulk validate transactions with actual validation logic.
///
/// Validates each transaction for:
/// - Required fields (entity_id, event, timestamp)
/// - Proper field types
/// - No cryptocurrency terminology
///
/// # Arguments
/// * `transactions` - List of transaction dictionaries
///
/// # Returns
/// * `true` if all transactions are valid, `false` if any fails
#[pyfunction]
fn bulk_validate_transactions(transactions: &Bound<PyList>) -> PyResult<bool> {
    use crate::core::utils::{validate_event_structure, validate_no_cryptocurrency_terms};

    for item in transactions.iter() {
        let tx_dict = item.cast::<PyDict>()?;
        let tx_json = dict_to_json(&tx_dict)?;

        // Transaction must be an object
        let tx_obj = match tx_json.as_object() {
            Some(obj) => obj,
            None => return Ok(false),
        };

        // Check required fields
        if !tx_obj.contains_key("entity_id") || !tx_obj.contains_key("event") {
            return Ok(false);
        }

        // Validate event structure
        if !validate_event_structure(&tx_json) {
            return Ok(false);
        }

        // Check for forbidden cryptocurrency terms
        if !validate_no_cryptocurrency_terms(&tx_json) {
            return Ok(false);
        }
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
) -> PyResult<Vec<Py<Block>>> {
    let mut blocks = Vec::with_capacity(events_list.len());
    let mut prev_hash = previous_hash.to_string();

    for (i, events) in events_list.iter().enumerate() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("previous_hash", &prev_hash)?;

        let block = Block::new(start_index + i as u64, &events, Some(&kwargs.as_borrowed()))?;

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
    // Add module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

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

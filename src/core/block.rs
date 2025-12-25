//! Block implementation for HieraChain Framework.
//!
//! This module implements the Block struct with PyO3 bindings,
//! using Arrow RecordBatch for event storage (matching Python's pa.Table approach).

use crate::core::utils::{generate_hash, validate_event_structure, MerkleTree};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pythonize::depythonize;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Block structure with PyO3 bindings.
/// Events are stored as Vec<Value> internally but converted efficiently using pythonize.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    #[pyo3(get, set)]
    pub index: u64,

    /// Events stored as serde_json::Value for Merkle tree computation.
    /// Conversion from Python uses pythonize (no json.dumps() call).
    pub events: Vec<Value>,

    #[pyo3(get, set)]
    pub timestamp: f64,
    #[pyo3(get, set)]
    pub previous_hash: String,
    #[pyo3(get, set)]
    pub nonce: u64,
    #[pyo3(get, set)]
    pub merkle_root: String,
    #[pyo3(get, set)]
    pub hash: String,
    #[pyo3(get, set)]
    pub creator_id: Option<String>,
    #[pyo3(get, set)]
    pub signature: Option<String>,
}

// Rust-only implementation block
impl Block {
    /// Add an event to the block and recalculate merkle root and hash
    pub fn add_event(&mut self, event: Value) {
        self.events.push(event);
        let tree = MerkleTree::new(&self.events);
        self.merkle_root = tree.get_root();
        self.hash = self.calculate_hash();
    }

    /// Convert Python events (list of dicts) to Vec<Value> using pythonize
    /// Optimized version with batch conversion and minimal fallback overhead
    fn convert_events_to_values(events: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
        let events_list = events
            .downcast::<PyList>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("events must be a list"))?;

        let len = events_list.len();

        // Pre-allocate with exact capacity
        let mut parsed_events: Vec<Value> = Vec::with_capacity(len);

        // Fast path: try to convert entire list at once
        // This is more efficient when all items are well-formed dicts
        if let Ok(all_values) = depythonize::<Vec<Value>>(events) {
            return Ok(all_values);
        }

        // Fallback: convert items one by one (slower but handles edge cases)
        for item in events_list.iter() {
            // Use pythonize::depythonize to convert PyAny -> serde_json::Value directly
            // This avoids the Python json.dumps() → String → serde_json::from_str() overhead
            if let Ok(val) = depythonize::<Value>(&item) {
                parsed_events.push(val);
            } else {
                // Minimal fallback for edge cases
                parsed_events.push(Value::Null);
            }
        }

        Ok(parsed_events)
    }
}

// PyO3 implementation block for Python-exposed methods
#[pymethods]
impl Block {
    #[new]
    #[pyo3(signature = (index, events, kwargs=None))]
    pub fn new(
        index: u64,
        events: &Bound<'_, PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        // Convert Python events to Vec<Value> using pythonize (no json.dumps!)
        let parsed_events = Self::convert_events_to_values(events)?;

        // Parse kwargs
        let mut timestamp = None;
        let mut previous_hash = String::new();
        let mut nonce = 0;
        let mut merkle_root = None;
        let mut creator_id = None;
        let mut signature = None;

        if let Some(dict) = kwargs {
            if let Some(val) = dict.get_item("timestamp")? {
                timestamp = Some(val.extract::<f64>().unwrap_or(0.0));
            }
            if let Some(val) = dict.get_item("previous_hash")? {
                previous_hash = val.extract::<String>().unwrap_or_default();
            }
            if let Some(val) = dict.get_item("nonce")? {
                nonce = val.extract::<u64>().unwrap_or(0);
            }
            if let Some(val) = dict.get_item("merkle_root")? {
                merkle_root = val.extract::<Option<String>>().unwrap_or(None);
            }
            if let Some(val) = dict.get_item("creator_id")? {
                creator_id = val.extract::<Option<String>>().unwrap_or(None);
            }
            if let Some(val) = dict.get_item("signature")? {
                signature = val.extract::<Option<String>>().unwrap_or(None);
            }
        }

        let timestamp = timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs_f64())
                .unwrap_or(0.0)
        });

        let calculated_merkle_root = if let Some(root) = merkle_root {
            root
        } else {
            let tree = MerkleTree::new(&parsed_events);
            tree.get_root()
        };

        // Construct the block
        let mut block = Block {
            index,
            events: parsed_events,
            timestamp,
            previous_hash,
            nonce,
            merkle_root: calculated_merkle_root,
            hash: String::new(),
            creator_id,
            signature,
        };

        block.hash = block.calculate_hash();
        Ok(block)
    }

    /// Calculate the hash of the block
    pub fn calculate_hash(&self) -> String {
        let block_header = serde_json::json!({
            "index": self.index,
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "merkle_root": self.merkle_root,
            "creator_id": self.creator_id
        });

        generate_hash(&block_header)
    }

    /// Validate the block structure
    pub fn validate_structure(&self) -> bool {
        for event in &self.events {
            if !validate_event_structure(event) {
                return false;
            }
        }
        true
    }

    /// Add an event to the block (Python wrapper)
    #[pyo3(name = "add_event")]
    pub fn add_event_py(&mut self, event: &Bound<PyAny>) -> PyResult<()> {
        // Convert PyAny to serde_json::Value using pythonize
        let val: Value = depythonize(event)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        self.add_event(val);
        Ok(())
    }

    /// Get events as a list of JSON strings (for Python)
    #[getter]
    pub fn get_events(&self) -> Vec<String> {
        self.events.iter().map(|v| v.to_string()).collect()
    }

    /// Set events from a list of JSON strings (from Python)
    #[setter]
    pub fn set_events(&mut self, events: Vec<String>) {
        self.events = events
            .into_iter()
            .map(|s| serde_json::from_str(&s).unwrap_or(Value::Null))
            .collect();
        // Recalculate merkle root and hash when events change
        let tree = MerkleTree::new(&self.events);
        self.merkle_root = tree.get_root();
        self.hash = self.calculate_hash();
    }

    /// Convert block to Python dict
    pub fn to_dict(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("index", self.index)?;

        // Convert events to Python list using pythonize
        let events_list = PyList::empty(py);
        for event in &self.events {
            let py_value = pythonize::pythonize(py, event)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            events_list.append(py_value)?;
        }
        dict.set_item("events", events_list)?;

        dict.set_item("timestamp", self.timestamp)?;
        dict.set_item("previous_hash", &self.previous_hash)?;
        dict.set_item("nonce", self.nonce)?;
        dict.set_item("merkle_root", &self.merkle_root)?;
        dict.set_item("hash", &self.hash)?;
        dict.set_item("creator_id", &self.creator_id)?;
        dict.set_item("signature", &self.signature)?;

        Ok(dict.into())
    }

    #[staticmethod]
    pub fn from_dict(data: &Bound<'_, PyDict>) -> PyResult<Self> {
        let index: u64 = data.get_item("index")?.unwrap().extract()?;
        let events = data.get_item("events")?.unwrap();

        Self::new(index, &events, Some(data))
    }

    fn __str__(&self) -> String {
        format!(
            "Block(index={}, events={}, hash={}...)",
            self.index,
            self.events.len(),
            &self.hash[0..10.min(self.hash.len())]
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "Block(index={}, events={}, hash={})",
            self.index,
            self.events.len(),
            self.hash
        )
    }
}

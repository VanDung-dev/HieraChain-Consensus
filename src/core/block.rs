//! Block implementation for HieraChain Framework.
//!
//! This module implements the Block struct with PyO3 bindings,
//! optimizing performance by using Rust for hashing and Merkle tree calculations.

use crate::core::utils::{generate_hash, validate_event_structure, MerkleTree};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Block structure with PyO3 bindings.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    #[pyo3(get, set)]
    pub index: u64,

    /// Events are stored as JSON Values in Rust for flexibility,
    /// mirroring the Python list[dict] structure before Arrow conversion.
    /// In a full Arrow integration, this might change to use Arrow arrays directly.
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

// Rust-only implementation block for methods using types incompatible with PyO3 (like serde_json::Value)
impl Block {
    /// Add an event to the block and recalculate merkle root and hash
    /// This method is internal to Rust usage or wrapped for Python.
    pub fn add_event(&mut self, event: Value) {
        self.events.push(event);
        let tree = MerkleTree::new(&self.events);
        self.merkle_root = tree.get_root();
        self.hash = self.calculate_hash();
    }
}

// PyO3 implementation block for Python-exposed methods
#[pymethods]
impl Block {
    #[new]
    #[allow(deprecated)]
    pub fn new(
        index: u64,
        events: Py<PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut parsed_events: Vec<Value> = Vec::new();

        Python::with_gil(|py| -> PyResult<()> {
            let json_mod = py.import("json")?;

            // Validate that events is a list
            let events_bound = events.bind(py);
            let events_list = events_bound
                .downcast::<PyList>()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("events must be a list"))?;

            for item in events_list.iter() {
                if let Ok(json_str) = json_mod.call_method1("dumps", (&item,)) {
                    let s: String = json_str.extract()?;
                    if let Ok(val) = serde_json::from_str(&s) {
                        parsed_events.push(val);
                    } else {
                        parsed_events.push(Value::Null);
                    }
                } else {
                    if let Ok(s) = item.extract::<String>() {
                        if let Ok(val) = serde_json::from_str(&s) {
                            parsed_events.push(val);
                        } else {
                            parsed_events.push(Value::String(s));
                        }
                    } else {
                        parsed_events.push(Value::Null);
                    }
                }
            }
            Ok(())
        })?;

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
        // Validate events structure
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
        // Convert PyAny to serde_json::Value
        let py = event.py();
        let json_mod = py.import("json")?;
        let json_str: String = json_mod.call_method1("dumps", (event,))?.extract()?;
        let val: Value = serde_json::from_str(&json_str)
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

    pub fn to_dict(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("index", self.index)?;
        let events_list = PyList::empty(py);
        for event in &self.events {
            let json_str = event.to_string();
            // This is a bit inefficient (Rust Value -> String -> Python String -> Python Dict)
            // But simplest for now without writing full converter
            let py_json = py.import("json")?;
            let py_dict = py_json.call_method1("loads", (json_str,))?;
            events_list.append(py_dict)?;
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

        // Pass data as kwargs.
        let events_list = data.get_item("events")?.unwrap();
        let events = events_list.unbind();

        Self::new(index, events, Some(data))
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

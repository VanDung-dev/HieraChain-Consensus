//! Block implementation for HieraChain Framework.
//!
//! This module implements the Block struct with PyO3 bindings,
//! using Arrow RecordBatch for event storage (matching Python's pa.Table approach).

use crate::core::utils::{generate_hash, validate_event_structure, MerkleTree};
use arrow::array::{Array, AsArray};
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pythonize::depythonize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Maximum JSON input size for deserialization (10 MB)
pub const MAX_JSON_INPUT_SIZE: usize = 10 * 1024 * 1024;

/// Maximum number of events per block
pub const MAX_EVENTS_PER_BLOCK: usize = 10_000;

/// Maximum clock drift allowed for timestamps (5 minutes into future)
pub const MAX_FUTURE_TIMESTAMP_SECONDS: f64 = 300.0;

/// Block structure with PyO3 bindings.
/// Events are stored as Vec<Value> internally but converted efficiently using pythonize.
/// Optionally stores Arrow RecordBatch for zero-copy interop with Python.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Block {
    #[pyo3(get, set)]
    pub index: u64,

    /// Events stored as serde_json::Value for Merkle tree computation.
    /// Conversion from Python uses pythonize (no json.dumps() call).
    pub events: Vec<Value>,

    /// Optional Arrow RecordBatch for zero-copy storage
    #[serde(skip)]
    pub arrow_events: Option<Arc<RecordBatch>>,

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
    #[pyo3(get, set)]
    pub zk_proof: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub zk_public_inputs: Option<Vec<u8>>,
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

    /// Convert Arrow RecordBatch to Vec<Value> for Merkle tree computation
    /// This extracts data from Arrow format to JSON for hashing
    fn arrow_to_values(batch: &RecordBatch) -> Vec<Value> {
        let mut values = Vec::with_capacity(batch.num_rows());

        // Get column names
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        for row_idx in 0..batch.num_rows() {
            let mut row_map = serde_json::Map::new();

            for (col_idx, field_name) in field_names.iter().enumerate() {
                let column = batch.column(col_idx);

                // Handle different Arrow types - simplified for common cases
                if let Some(arr) = column.as_string_opt::<i32>() {
                    if arr.is_valid(row_idx) {
                        row_map.insert(
                            field_name.to_string(),
                            Value::String(arr.value(row_idx).to_string()),
                        );
                    }
                } else if let Some(arr) = column.as_primitive_opt::<arrow::datatypes::Float64Type>()
                {
                    if arr.is_valid(row_idx) {
                        if let Some(n) = serde_json::Number::from_f64(arr.value(row_idx)) {
                            row_map.insert(field_name.to_string(), Value::Number(n));
                        }
                    }
                } else if let Some(arr) = column.as_primitive_opt::<arrow::datatypes::Int64Type>() {
                    if arr.is_valid(row_idx) {
                        row_map.insert(
                            field_name.to_string(),
                            Value::Number(arr.value(row_idx).into()),
                        );
                    }
                }
                // Add more types as needed
            }

            values.push(Value::Object(row_map));
        }

        values
    }

    /// Convert Python events (list of dicts) to Vec<Value> using pythonize
    /// Optimized version with batch conversion and minimal fallback overhead
    ///
    /// # Security
    /// - Enforces MAX_EVENTS_PER_BLOCK limit to prevent DoS via large lists
    fn convert_events_to_values(events: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
        let events_list = events
            .downcast::<PyList>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("events must be a list"))?;

        let len = events_list.len();

        // Security: Limit number of events to prevent DoS
        if len > MAX_EVENTS_PER_BLOCK {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Too many events: {} exceeds maximum of {}",
                len, MAX_EVENTS_PER_BLOCK
            )));
        }

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
        // Try to convert from Arrow RecordBatch first (zero-copy preferred)
        let (parsed_events, arrow_batch) =
            if let Ok(batch) = RecordBatch::from_pyarrow_bound(events) {
                // Fast path: Arrow data - extract values for Merkle computation
                let values = Self::arrow_to_values(&batch);
                (values, Some(Arc::new(batch)))
            } else {
                // Fallback: Convert Python list to Vec<Value>
                let values = Self::convert_events_to_values(events)?;
                (values, None)
            };

        // Parse kwargs
        let mut timestamp = None;
        let mut previous_hash = String::new();
        let mut nonce = 0;
        let mut merkle_root = None;
        let mut creator_id = None;
        let mut signature = None;
        let mut zk_proof = None;
        let mut zk_public_inputs = None;

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
            if let Some(val) = dict.get_item("zk_proof")? {
                zk_proof = val.extract::<Option<Vec<u8>>>().unwrap_or(None);
            }
            if let Some(val) = dict.get_item("zk_public_inputs")? {
                zk_public_inputs = val.extract::<Option<Vec<u8>>>().unwrap_or(None);
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
            arrow_events: arrow_batch,
            timestamp,
            previous_hash,
            nonce,
            merkle_root: calculated_merkle_root,
            hash: String::new(),
            creator_id,
            signature,
            zk_proof,
            zk_public_inputs,
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
    ///
    /// Checks:
    /// - All events have valid structure
    /// - Event count does not exceed maximum
    pub fn validate_structure(&self) -> bool {
        // Check event count limit
        if self.events.len() > MAX_EVENTS_PER_BLOCK {
            return false;
        }

        for event in &self.events {
            if !validate_event_structure(event) {
                return false;
            }
        }
        true
    }

    /// Validate block timestamp is not too far in the future.
    ///
    /// # Arguments
    /// * `current_time` - Optional current time for testing, uses system time if None
    ///
    /// # Returns
    /// * True if timestamp is valid (not in far future)
    pub fn validate_timestamp(&self, current_time: Option<f64>) -> bool {
        let now = current_time.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0)
        });

        // Timestamp must not be more than MAX_FUTURE_TIMESTAMP_SECONDS in the future
        self.timestamp <= now + MAX_FUTURE_TIMESTAMP_SECONDS
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
        dict.set_item("zk_proof", &self.zk_proof)?;
        dict.set_item("zk_public_inputs", &self.zk_public_inputs)?;

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

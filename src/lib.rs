//! HieraChain Consensus Library
//! 
//! This library provides the consensus mechanisms for the HieraChain blockchain platform.
//! It includes implementations of consensus algorithms, node management, and message handling.
//! The library is designed to be used with Python through PyO3 bindings.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyFloat, PyInt, PyBool};
use pyo3::IntoPyObjectExt;
use serde_json::{Map, Value};

// Import modules
pub mod consensus {
    pub mod ordering_service;
}

pub mod core;
pub mod error_mitigation;
pub mod hierarchical;
pub mod storage;

// Re-export items for easier access
use crate::consensus::ordering_service::*;

/// Convert Python object to serde_json::Value
fn py_to_json(obj: &Bound<PyAny>) -> PyResult<Value> {
    if let Ok(val) = obj.cast::<PyString>() {
        Ok(Value::String(val.to_str()?.to_string()))
    } else if let Ok(val) = obj.cast::<PyFloat>() {
        Ok(Value::Number(serde_json::Number::from_f64(val.value()).unwrap_or(serde_json::Number::from(0))))
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
        },
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
        },
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
    fn new(node_id: String, endpoint: String, is_leader: bool, weight: f64, status: String, last_heartbeat: f64) -> Self {
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
    inner: std::sync::Arc<OrderingService>,
}

#[pymethods]
impl PyOrderingService {
    #[new]
    fn new(nodes: Vec<PyOrderingNode>, config: &Bound<PyDict>) -> PyResult<Self> {
        let rust_nodes: Vec<OrderingNode> = nodes.into_iter().map(|n| OrderingNode {
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
        }).collect();

        let config_json = dict_to_json(config)?;
        let service = OrderingService::new(rust_nodes, config_json);
        Ok(PyOrderingService { inner: service })
    }

    fn receive_event(&self, event_data: &Bound<PyDict>, channel_id: String, submitter_org: String) -> PyResult<String> {
        let event_json = dict_to_json(event_data)?;
        Ok(self.inner.receive_event(event_json, channel_id, submitter_org))
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
        // Create a wrapper closure that calls the Python function
        // Note: This would require storing the Python function and calling it from Rust
        // For now, this is a placeholder that can be implemented with more complex binding logic
        Ok(())
    }

    fn start(&self) {
        // Already started in new(), but can be called to restart
    }

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

/// Calculate block hash
#[pyfunction]
fn calculate_block_hash(block_data: &Bound<PyDict>, py: Python) -> PyResult<Py<PyAny>> {
    use sha2::{Sha256, Digest};

    let block_json = dict_to_json(block_data)?;
    let data = serde_json::to_string(&block_json).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    let hash = format!("{:x}", hasher.finalize());

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

/// Python module
#[pymodule]
fn hierachain_consensus(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Add consensus functions
    m.add_function(wrap_pyfunction!(validate_poa_block, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_block_hash, m)?)?;
    m.add_function(wrap_pyfunction!(bulk_validate_transactions, m)?)?;

    // Add PyO3 classes
    m.add_class::<PyOrderingNode>()?;
    m.add_class::<PyOrderingService>()?;

    Ok(())
}

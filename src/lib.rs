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
mod core {
    pub mod consensus {
        pub mod base_consensus;
    }
}

// Re-export items for easier access
use crate::core::consensus::base_consensus::*;

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
fn map_to_dict(py: Python, map: &Map<String, Value>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        let py_value = json_to_py(py, value)?;
        dict.set_item(key, py_value)?;
    }
    Ok(dict.into())
}

/// Python module
#[pymodule]
fn hierachain_consensus(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_poa_block, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_block_hash, m)?)?;
    m.add_function(wrap_pyfunction!(bulk_validate_transactions, m)?)?;
    Ok(())
}

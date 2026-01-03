//! PyO3 Helper Functions for Python ↔ Rust Interop
//!
//! This module provides common conversion utilities for working with PyO3:
//! - `py_to_json`: Convert Python objects to `serde_json::Value`
//! - `json_to_py`: Convert `serde_json::Value` to Python objects
//! - `dict_to_map`: Convert Python dict to Rust `Map<String, Value>`
//! - `map_to_dict`: Convert Rust `Map<String, Value>` to Python dict

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::IntoPyObjectExt;
use serde_json::{Map, Value};

/// Convert Python object to serde_json::Value
///
/// Supports:
/// - String, Int, Float, Bool
/// - List, Dict
/// - None
///
/// # Arguments
/// * `obj` - Reference to a Python object
///
/// # Returns
/// `serde_json::Value` representation of the Python object
/// Convert Python object to serde_json::Value with recursion limit
///
/// # Arguments
/// * `obj` - Reference to a Python object
///
/// # Returns
/// `serde_json::Value` representation of the Python object
pub fn py_to_json(obj: &Bound<PyAny>) -> PyResult<Value> {
    py_to_json_recursive(obj, 0)
}

fn py_to_json_recursive(obj: &Bound<PyAny>, depth: usize) -> PyResult<Value> {
    // Prevent stack overflow
    if depth > 50 {
        return Err(pyo3::exceptions::PyRecursionError::new_err(
            "Recursion limit exceeded during JSON conversion",
        ));
    }

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
            // Error on overflow instead of returning 0
            Err(pyo3::exceptions::PyOverflowError::new_err(
                "Integer too large for Rust i64/u64",
            ))
        }
    } else if let Ok(val) = obj.cast::<PyBool>() {
        Ok(Value::Bool(val.is_true()))
    } else if let Ok(val) = obj.cast::<PyBytes>() {
        // Convert bytes to hex string for JSON compatibility
        Ok(Value::String(hex::encode(val.as_bytes())))
    } else if let Ok(val) = obj.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in val.iter() {
            vec.push(py_to_json_recursive(&item, depth + 1)?);
        }
        Ok(Value::Array(vec))
    } else if let Ok(val) = obj.cast::<PyDict>() {
        let mut map = Map::new();
        for (key, value) in val.iter() {
            let key_str: &str = key.cast::<PyString>()?.to_str()?;
            map.insert(
                key_str.to_string(),
                py_to_json_recursive(&value, depth + 1)?,
            );
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
///
/// # Arguments
/// * `py` - Python GIL token
/// * `value` - Reference to serde_json::Value
///
/// # Returns
/// Python object equivalent
pub fn json_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
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

/// Convert Python dict to Rust Map<String, Value>
///
/// # Arguments
/// * `dict` - Reference to Python dict
///
/// # Returns
/// Rust `Map<String, Value>`
pub fn dict_to_map(dict: &Bound<PyDict>) -> PyResult<Map<String, Value>> {
    let mut map = Map::new();
    for (key, value) in dict.iter() {
        let key_str: &str = key.cast::<PyString>()?.to_str()?;
        let value_json = py_to_json(&value)?;
        map.insert(key_str.to_string(), value_json);
    }
    Ok(map)
}

/// Convert Rust Map<String, Value> to Python dict (returns Py<PyAny>)
///
/// # Arguments
/// * `py` - Python GIL token
/// * `map` - Reference to Rust Map
///
/// # Returns
/// Python dict as `Py<PyAny>`
pub fn map_to_dict(py: Python, map: &Map<String, Value>) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        let py_value = json_to_py(py, value)?;
        dict.set_item(key, py_value)?;
    }
    Ok(dict.into())
}

/// Convert Rust Map<String, Value> to Python dict (returns Py<PyDict>)
///
/// # Arguments
/// * `py` - Python GIL token
/// * `map` - Reference to Rust Map
///
/// # Returns
/// Python dict as `Py<PyDict>`
pub fn map_to_py_dict(py: Python, map: &Map<String, Value>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in map {
        let py_value = json_to_py(py, value)?;
        dict.set_item(key, py_value)?;
    }
    Ok(dict.into())
}

/// Convert Python dict to serde_json::Value
///
/// # Arguments
/// * `dict` - Reference to Python dict
///
/// # Returns
/// `serde_json::Value` (Object)
pub fn dict_to_json(dict: &Bound<PyDict>) -> PyResult<Value> {
    let map = dict_to_map(dict)?;
    Ok(Value::Object(map))
}

//! PyO3 Wrappers for Security Module
//!
//! This module provides Python bindings for security-related types:
//! - `PyKeyPair` - Python wrapper for Ed25519 key pair
//! - `verify_signature` - Python function for signature verification

use pyo3::prelude::*;

use crate::security::security_utils::{verify_signature as rs_verify_signature, KeyPair};

// ==================== PyKeyPair ====================

/// PyO3 wrapper for Ed25519 KeyPair.
/// Provides Python access to cryptographic signing and verification.
#[pyclass(name = "KeyPair")]
#[derive(Clone)]
pub struct PyKeyPair {
    pub(crate) inner: KeyPair,
}

#[pymethods]
impl PyKeyPair {
    /// Generate a new random Ed25519 key pair.
    ///
    /// Returns:
    ///     KeyPair: A new randomly generated key pair
    #[staticmethod]
    fn generate() -> Self {
        PyKeyPair {
            inner: KeyPair::generate(),
        }
    }

    /// Create a key pair from a hex-encoded private key.
    ///
    /// Args:
    ///     private_key_hex: Hex-encoded private key string
    ///
    /// Returns:
    ///     KeyPair: Key pair derived from the private key
    ///
    /// Raises:
    ///     ValueError: If the private key is invalid
    #[staticmethod]
    fn from_private_key(private_key_hex: &str) -> PyResult<Self> {
        KeyPair::from_private_key(private_key_hex)
            .map(|kp| PyKeyPair { inner: kp })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Get the public key as a hex string.
    #[getter]
    fn public_key(&self) -> String {
        self.inner.public_key()
    }

    /// Get the private key as a hex string.
    /// Warning: This exposes sensitive data!
    #[getter]
    fn private_key(&self) -> String {
        self.inner.private_key()
    }

    /// Sign a message and return the signature as a hex string.
    ///
    /// Args:
    ///     message: Bytes to sign
    ///
    /// Returns:
    ///     str: Hex-encoded signature
    ///
    /// Raises:
    ///     ValueError: If signing fails
    fn sign(&self, message: &[u8]) -> PyResult<String> {
        self.inner
            .sign(message)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn __str__(&self) -> String {
        format!("KeyPair(public_key='{}')", self.inner.public_key())
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ==================== verify_signature function ====================

/// Verify an Ed25519 signature.
///
/// Args:
///     public_key_hex: Hex-encoded public key
///     message: Original message bytes
///     signature_hex: Hex-encoded signature to verify
///
/// Returns:
///     bool: True if signature is valid, False otherwise
#[pyfunction]
pub fn verify_signature(public_key_hex: &str, message: &[u8], signature_hex: &str) -> bool {
    rs_verify_signature(public_key_hex, message, signature_hex)
}

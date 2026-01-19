//! Performance Optimization Helpers
//!
//! Provides utilities for high-performance data handling, including
//! binary serialization for efficient thread-pool communication.

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

/// Performance related errors
#[derive(Debug, Error)]
pub enum PerformanceError {
    #[error("Serialization failed: {0}")]
    Serialization(#[from] bincode::Error),
}

/// Serialize data to binary for rapid thread pool exchange
///
/// Uses bincode for compact and fast serialization, suitable for
/// passing data between threads or processes where schema is known.
pub fn serialize_for_pool<T: Serialize>(data: &T) -> Result<Vec<u8>, PerformanceError> {
    bincode::serialize(data).map_err(PerformanceError::Serialization)
}

/// Deserialize data from pool
///
/// Reconstructs objects from binary format used in thread pool exchange.
pub fn deserialize_from_pool<T: DeserializeOwned>(data: &[u8]) -> Result<T, PerformanceError> {
    bincode::deserialize(data).map_err(PerformanceError::Serialization)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestData {
        id: u32,
        payload: String,
        values: Vec<f64>,
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = TestData {
            id: 42,
            payload: "performant".to_string(),
            values: vec![1.0, 2.0, 3.14],
        };

        let serialized = serialize_for_pool(&original).expect("Serialization failed");
        assert!(!serialized.is_empty());

        let deserialized: TestData =
            deserialize_from_pool(&serialized).expect("Deserialization failed");

        assert_eq!(original, deserialized);
    }
}

//! Utility functions for HieraChain Consensus.
//!
//! This module provides common utility functions used throughout the framework,
//! including cryptographic utilities (`MerkleTree`, `generate_hash`) to optimize performance.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Generate SHA-256 hash for given data.
/// Uses strict JSON canonicalization to match Python implementation.
pub fn generate_hash(data: &Value) -> String {
    let canonical_json = to_canonical_json(data);
    let mut hasher = Sha256::new();
    hasher.update(canonical_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Helper to produce canonical JSON string (sorted keys, no spaces)
/// Matches Python's `json.dumps(data, sort_keys=True, separators=(',', ':'))`
fn to_canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            // BTreeMap ensures keys are sorted
            let sorted_map: BTreeMap<_, _> = map.iter().collect();
            let mut result = String::from("{");
            for (i, (k, v)) in sorted_map.iter().enumerate() {
                if i > 0 {
                    result.push(',');
                }
                result.push_str(&format!("\"{}\":{}", k, to_canonical_json(v)));
            }
            result.push('}');
            result
        }
        Value::Array(vec) => {
            let mut result = String::from("[");
            for (i, v) in vec.iter().enumerate() {
                if i > 0 {
                    result.push(',');
                }
                result.push_str(&to_canonical_json(v));
            }
            result.push(']');
            result
        }
        // Primitives should be standard JSON representation
        _ => value.to_string(),
    }
}

/// Merkle Tree implementation for efficient data verification and hashing.
pub struct MerkleTree {
    pub leaves: Vec<String>,
    pub root: String,
}

impl MerkleTree {
    /// Create a new Merkle Tree from a list of data items (as JSON Values)
    pub fn new(data_list: &[Value]) -> Self {
        let leaves: Vec<String> = data_list.iter().map(generate_hash).collect();
        let root = Self::build_tree(&leaves);
        MerkleTree { leaves, root }
    }

    /// Recursively build the Merkle Tree
    fn build_tree(nodes: &[String]) -> String {
        if nodes.is_empty() {
            // Empty tree hash - SHA256("")
            let mut hasher = Sha256::new();
            hasher.update(b"");
            return format!("{:x}", hasher.finalize());
        }

        if nodes.len() == 1 {
            return nodes[0].clone();
        }

        let mut new_level = Vec::new();
        for chunk in nodes.chunks(2) {
            let left = &chunk[0];
            let right = if chunk.len() > 1 { &chunk[1] } else { left };

            let combined = format!("{}{}", left, right);
            let mut hasher = Sha256::new();
            hasher.update(combined.as_bytes());
            new_level.push(format!("{:x}", hasher.finalize()));
        }

        Self::build_tree(&new_level)
    }

    /// Get the Merkle Root hash
    pub fn get_root(&self) -> String {
        self.root.clone()
    }
}

/// Validate event structure according to framework guidelines.
pub fn validate_event_structure(event: &Value) -> bool {
    // Must be an object
    let event_obj = match event.as_object() {
        Some(obj) => obj,
        None => return false,
    };

    // Required fields
    let required_fields = ["event", "timestamp"];
    for field in required_fields {
        if !event_obj.contains_key(field) {
            return false;
        }
    }

    // Event type must be string
    if let Some(event_type) = event_obj.get("event") {
        if !event_type.is_string() {
            return false;
        }
    }

    // Timestamp must be number
    if let Some(timestamp) = event_obj.get("timestamp") {
        if !timestamp.is_number() {
            return false;
        }
    }

    // entity_id (optional) must be string
    if let Some(entity_id) = event_obj.get("entity_id") {
        if !entity_id.is_string() {
            return false;
        }
    }

    true
}

/// Validate that data doesn't contain cryptocurrency terminology.
/// This implementation scans all string values in the JSON object (recursively or generally).
pub fn validate_no_cryptocurrency_terms(event: &Value) -> bool {
    let forbidden_terms = [
        "transaction",
        "mining",
        "coin",
        "token",
        "wallet",
        "address",
        "sender",
        "receiver",
        "amount",
        "fee",
        "reward",
        "coinbase",
    ];

    let check_str = |s: &str| -> bool {
        let lower = s.to_lowercase();
        forbidden_terms.iter().any(|term| lower.contains(term))
    };

    // Helper to traverse JSON
    fn traverse(value: &Value, checker: &impl Fn(&str) -> bool) -> bool {
        match value {
            Value::String(s) => {
                if checker(s) {
                    return false;
                }
            }
            Value::Array(arr) => {
                for item in arr {
                    if !traverse(item, checker) {
                        return false;
                    }
                }
            }
            Value::Object(map) => {
                for (k, v) in map {
                    // Start checking keys as well if needed, but Python version checked values mostly?
                    // Python: `json.dumps(data).lower()` -> checks everything including keys.
                    if checker(k) {
                        return false;
                    }
                    if !traverse(v, checker) {
                        return false;
                    }
                }
            }
            _ => {} // Numbers, bools, nulls are safe
        }
        true
    }

    traverse(event, &check_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generate_hash_consistency() {
        let data1 = json!({"a": 1, "b": 2});
        let data2 = json!({"b": 2, "a": 1});
        assert_eq!(generate_hash(&data1), generate_hash(&data2));
    }

    #[test]
    fn test_merkle_tree() {
        // Matches Python implementation expectation
        // Test with empty list
        let empty_tree = MerkleTree::new(&[]);
        assert!(!empty_tree.get_root().is_empty());

        let events = vec![json!({"event": "e1"}), json!({"event": "e2"})];
        let tree = MerkleTree::new(&events);
        assert!(!tree.get_root().is_empty());
    }

    #[test]
    fn test_validate_crypto_terms() {
        let good_event = json!({
            "event": "user_login",
            "timestamp": 1234567890
        });
        assert!(validate_no_cryptocurrency_terms(&good_event));

        let bad_event = json!({
            "event": "send_coin", // "coin" is forbidden
            "timestamp": 1234567890
        });
        assert!(!validate_no_cryptocurrency_terms(&bad_event));

        let bad_nested = json!({
            "event": "update",
            "details": {
                "balance": "10 tokens" // "token" is forbidden
            }
        });
        assert!(!validate_no_cryptocurrency_terms(&bad_nested));
    }
}

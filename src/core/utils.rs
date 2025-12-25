//! Utility functions for HieraChain Consensus.
//!
//! This module provides common utility functions used throughout the framework,
//! including cryptographic utilities (`MerkleTree`, `generate_hash`) to optimize performance.

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write;

// Reusable hasher for better performance - avoids repeated allocations
// Thread-local to avoid synchronization overhead
thread_local! {
    static HASH_BUFFER: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(4096));
    static MERKLE_BUFFER: std::cell::RefCell<String> = std::cell::RefCell::new(String::with_capacity(256));
}

/// Generate SHA-256 hash for given data.
/// Uses strict JSON canonicalization to match Python implementation.
/// Optimized version with pre-allocated buffer and direct hashing.
#[inline]
pub fn generate_hash(data: &Value) -> String {
    HASH_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();

        // Write canonical JSON directly to buffer
        write_canonical_json(data, &mut *buf);

        // Hash the buffer
        let hash = Sha256::digest(buf.as_bytes());

        // Use faster hex encoding
        faster_hex_encode(&hash)
    })
}

/// Fast hex encoding without format! overhead
#[inline]
fn faster_hex_encode(bytes: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX_CHARS[(byte >> 4) as usize] as char);
        result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    result
}

/// Write canonical JSON directly to a buffer (avoids String allocations)
/// Matches Python's `json.dumps(data, sort_keys=True, separators=(',', ':'))`
#[inline]
fn write_canonical_json(value: &Value, buf: &mut String) {
    match value {
        Value::Object(map) => {
            // BTreeMap ensures keys are sorted - collect into sorted order
            let sorted_map: BTreeMap<_, _> = map.iter().collect();
            buf.push('{');
            for (i, (k, v)) in sorted_map.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                // Write key with quotes
                buf.push('"');
                escape_json_string(k, buf);
                buf.push_str("\":");
                // Recursively write value
                write_canonical_json(v, buf);
            }
            buf.push('}');
        }
        Value::Array(vec) => {
            buf.push('[');
            for (i, v) in vec.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_canonical_json(v, buf);
            }
            buf.push(']');
        }
        Value::String(s) => {
            buf.push('"');
            escape_json_string(s, buf);
            buf.push('"');
        }
        Value::Number(n) => {
            let _ = write!(buf, "{}", n);
        }
        Value::Bool(b) => {
            buf.push_str(if *b { "true" } else { "false" });
        }
        Value::Null => {
            buf.push_str("null");
        }
    }
}

/// Escape special JSON characters in strings
#[inline]
fn escape_json_string(s: &str, buf: &mut String) {
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
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

    /// Create a new Merkle Tree from pre-computed leaf hashes
    /// This is more efficient when you already have the hashes
    pub fn from_leaves(leaves: Vec<String>) -> Self {
        let root = Self::build_tree(&leaves);
        MerkleTree { leaves, root }
    }

    /// Recursively build the Merkle Tree - optimized with pre-allocated buffers
    fn build_tree(nodes: &[String]) -> String {
        if nodes.is_empty() {
            // Empty tree hash - SHA256("")
            let hash = Sha256::digest(b"");
            return faster_hex_encode(&hash);
        }

        if nodes.len() == 1 {
            return nodes[0].clone();
        }

        // Pre-allocate with expected capacity
        let mut new_level = Vec::with_capacity((nodes.len() + 1) / 2);

        MERKLE_BUFFER.with(|buffer| {
            let mut buf = buffer.borrow_mut();

            for chunk in nodes.chunks(2) {
                let left = &chunk[0];
                let right = if chunk.len() > 1 { &chunk[1] } else { left };

                // Reuse buffer instead of format!()
                buf.clear();
                buf.push_str(left);
                buf.push_str(right);

                let hash = Sha256::digest(buf.as_bytes());
                new_level.push(faster_hex_encode(&hash));
            }
        });

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

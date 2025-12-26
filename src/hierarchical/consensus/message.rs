//! BFT Message Types
//!
//! Defines message types and structures for BFT consensus protocol communication.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// BFT message types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    /// Pre-prepare message from primary
    PrePrepare,

    /// Prepare message from replicas
    Prepare,

    /// Commit message from replicas
    Commit,

    /// View change request
    ViewChange,

    /// New view announcement from new primary
    NewView,
}

impl MessageType {
    /// Get the string value of the message type
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::PrePrepare => "pre_prepare",
            MessageType::Prepare => "prepare",
            MessageType::Commit => "commit",
            MessageType::ViewChange => "view_change",
            MessageType::NewView => "new_view",
        }
    }

    /// Parse message type from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pre_prepare" => Some(MessageType::PrePrepare),
            "prepare" => Some(MessageType::Prepare),
            "commit" => Some(MessageType::Commit),
            "view_change" => Some(MessageType::ViewChange),
            "new_view" => Some(MessageType::NewView),
            _ => None,
        }
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// BFT consensus message
///
/// Contains all information needed for BFT protocol communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BFTMessage {
    /// Type of the message
    pub message_type: MessageType,

    /// Current view number
    pub view: u64,

    /// Sequence number for ordering
    pub sequence_number: u64,

    /// ID of the sending node
    pub sender_id: String,

    /// Unix timestamp of message creation
    pub timestamp: f64,

    /// Ed25519 signature of the message (hex encoded)
    pub signature: String,

    /// Additional data payload (request, digest, proof, etc.)
    #[serde(default)]
    pub data: HashMap<String, serde_json::Value>,

    /// Unique nonce to prevent replay attacks
    #[serde(default = "generate_nonce")]
    pub nonce: String,
}

/// Generate a unique nonce
fn generate_nonce() -> String {
    Uuid::new_v4().to_string()
}

/// Get current timestamp as f64
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

impl BFTMessage {
    /// Create a new BFT message
    pub fn new(
        message_type: MessageType,
        view: u64,
        sequence_number: u64,
        sender_id: String,
    ) -> Self {
        Self {
            message_type,
            view,
            sequence_number,
            sender_id,
            timestamp: current_timestamp(),
            signature: String::new(),
            data: HashMap::new(),
            nonce: generate_nonce(),
        }
    }

    /// Create a new message with data
    pub fn with_data(
        message_type: MessageType,
        view: u64,
        sequence_number: u64,
        sender_id: String,
        data: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            message_type,
            view,
            sequence_number,
            sender_id,
            timestamp: current_timestamp(),
            signature: String::new(),
            data,
            nonce: generate_nonce(),
        }
    }

    /// Get the payload bytes to be signed
    ///
    /// The signable payload includes critical fields that must be verified:
    /// - message_type
    /// - view
    /// - sequence_number
    /// - nonce
    /// - digest (if present in data)
    pub fn get_signable_payload(&self) -> Vec<u8> {
        let digest = self
            .data
            .get("digest")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let payload = if digest.is_empty() {
            format!(
                "{}:{}:{}:{}",
                self.message_type.as_str(),
                self.view,
                self.sequence_number,
                self.nonce
            )
        } else {
            format!(
                "{}:{}:{}:{}:{}",
                self.message_type.as_str(),
                self.view,
                self.sequence_number,
                self.nonce,
                digest
            )
        };

        payload.into_bytes()
    }

    /// Convert to dictionary for serialization (Python compatibility)
    pub fn to_dict(&self) -> HashMap<String, serde_json::Value> {
        let mut dict = HashMap::new();

        dict.insert(
            "message_type".to_string(),
            serde_json::Value::String(self.message_type.as_str().to_string()),
        );
        dict.insert(
            "view".to_string(),
            serde_json::Value::Number(self.view.into()),
        );
        dict.insert(
            "sequence_number".to_string(),
            serde_json::Value::Number(self.sequence_number.into()),
        );
        dict.insert(
            "sender_id".to_string(),
            serde_json::Value::String(self.sender_id.clone()),
        );
        dict.insert("timestamp".to_string(), serde_json::json!(self.timestamp));
        dict.insert(
            "signature".to_string(),
            serde_json::Value::String(self.signature.clone()),
        );
        dict.insert("data".to_string(), serde_json::json!(self.data));
        dict.insert(
            "nonce".to_string(),
            serde_json::Value::String(self.nonce.clone()),
        );

        dict
    }

    /// Create BFTMessage from a dictionary (Python compatibility)
    pub fn from_dict(dict: &HashMap<String, serde_json::Value>) -> Option<Self> {
        let message_type_str = dict.get("message_type")?.as_str()?;
        let message_type = MessageType::from_str(message_type_str)?;

        Some(Self {
            message_type,
            view: dict.get("view")?.as_u64()?,
            sequence_number: dict.get("sequence_number")?.as_u64()?,
            sender_id: dict.get("sender_id")?.as_str()?.to_string(),
            timestamp: dict.get("timestamp")?.as_f64()?,
            signature: dict.get("signature")?.as_str()?.to_string(),
            data: dict
                .get("data")
                .and_then(|v| v.as_object())
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
            nonce: dict
                .get("nonce")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(generate_nonce),
        })
    }

    /// Get the digest from data if present
    pub fn get_digest(&self) -> Option<&str> {
        self.data.get("digest").and_then(|v| v.as_str())
    }

    /// Set the digest in data
    pub fn set_digest(&mut self, digest: String) {
        self.data
            .insert("digest".to_string(), serde_json::Value::String(digest));
    }

    /// Set the signature
    pub fn set_signature(&mut self, signature: String) {
        self.signature = signature;
    }

    /// Check if this message is from the expected view
    pub fn is_from_view(&self, expected_view: u64) -> bool {
        self.view == expected_view
    }

    /// Check if the message has a valid (non-empty) signature
    pub fn has_signature(&self) -> bool {
        !self.signature.is_empty()
    }
}

impl Default for BFTMessage {
    fn default() -> Self {
        Self::new(MessageType::PrePrepare, 0, 0, String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_type_conversion() {
        assert_eq!(MessageType::PrePrepare.as_str(), "pre_prepare");
        assert_eq!(MessageType::from_str("commit"), Some(MessageType::Commit));
        assert_eq!(MessageType::from_str("invalid"), None);
    }

    #[test]
    fn test_message_creation() {
        let msg = BFTMessage::new(MessageType::Prepare, 1, 42, "node1".to_string());

        assert_eq!(msg.message_type, MessageType::Prepare);
        assert_eq!(msg.view, 1);
        assert_eq!(msg.sequence_number, 42);
        assert_eq!(msg.sender_id, "node1");
        assert!(msg.timestamp > 0.0);
        assert!(!msg.nonce.is_empty());
    }

    #[test]
    fn test_signable_payload_without_digest() {
        let msg = BFTMessage::new(MessageType::Prepare, 1, 42, "node1".to_string());

        let payload = msg.get_signable_payload();
        let payload_str = String::from_utf8(payload).unwrap();

        assert!(payload_str.starts_with("prepare:1:42:"));
        assert!(payload_str.contains(&msg.nonce));
    }

    #[test]
    fn test_signable_payload_with_digest() {
        let mut msg = BFTMessage::new(MessageType::Prepare, 1, 42, "node1".to_string());
        msg.set_digest("abc123".to_string());

        let payload = msg.get_signable_payload();
        let payload_str = String::from_utf8(payload).unwrap();

        assert!(payload_str.ends_with(":abc123"));
    }

    #[test]
    fn test_serialization() {
        let msg = BFTMessage::new(MessageType::Commit, 2, 10, "node2".to_string());

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: BFTMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.message_type, MessageType::Commit);
        assert_eq!(deserialized.view, 2);
        assert_eq!(deserialized.sequence_number, 10);
        assert_eq!(deserialized.sender_id, "node2");
    }

    #[test]
    fn test_to_dict_and_back() {
        let mut msg = BFTMessage::new(MessageType::ViewChange, 3, 100, "node3".to_string());
        msg.set_digest("test_digest".to_string());
        msg.set_signature("test_signature".to_string());

        let dict = msg.to_dict();
        let recovered = BFTMessage::from_dict(&dict).unwrap();

        assert_eq!(recovered.message_type, msg.message_type);
        assert_eq!(recovered.view, msg.view);
        assert_eq!(recovered.sequence_number, msg.sequence_number);
        assert_eq!(recovered.sender_id, msg.sender_id);
        assert_eq!(recovered.signature, msg.signature);
        assert_eq!(recovered.get_digest(), msg.get_digest());
    }

    #[test]
    fn test_message_with_data() {
        let mut data = HashMap::new();
        data.insert(
            "request".to_string(),
            serde_json::json!({"operation": "test"}),
        );
        data.insert(
            "digest".to_string(),
            serde_json::Value::String("abc".to_string()),
        );

        let msg = BFTMessage::with_data(MessageType::PrePrepare, 0, 1, "primary".to_string(), data);

        assert_eq!(msg.get_digest(), Some("abc"));
        assert!(msg.data.contains_key("request"));
    }
}

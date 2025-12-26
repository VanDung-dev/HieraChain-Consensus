//! Consensus State Management
//!
//! Defines the possible states of a BFT consensus node.

use serde::{Deserialize, Serialize};

/// Consensus node states
///
/// Represents the current phase of the BFT consensus protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusState {
    /// Node is idle, waiting for requests
    #[default]
    Idle,

    /// Node has received pre-prepare message
    PrePrepared,

    /// Node has received enough prepare messages (2f)
    Prepared,

    /// Node has received enough commit messages (2f+1) and executed
    Committed,

    /// Node is in view change process
    ViewChange,
}

impl ConsensusState {
    /// Get the string value of the state (for compatibility with Python)
    pub fn as_str(&self) -> &'static str {
        match self {
            ConsensusState::Idle => "idle",
            ConsensusState::PrePrepared => "pre_prepared",
            ConsensusState::Prepared => "prepared",
            ConsensusState::Committed => "committed",
            ConsensusState::ViewChange => "view_change",
        }
    }

    /// Parse state from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "idle" => Some(ConsensusState::Idle),
            "pre_prepared" => Some(ConsensusState::PrePrepared),
            "prepared" => Some(ConsensusState::Prepared),
            "committed" => Some(ConsensusState::Committed),
            "view_change" => Some(ConsensusState::ViewChange),
            _ => None,
        }
    }

    /// Check if this state allows processing new requests
    pub fn can_process_request(&self) -> bool {
        matches!(self, ConsensusState::Idle | ConsensusState::Committed)
    }

    /// Check if this state is a final state for a round
    pub fn is_terminal(&self) -> bool {
        matches!(self, ConsensusState::Committed)
    }
}

impl std::fmt::Display for ConsensusState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_state() {
        let state = ConsensusState::default();
        assert_eq!(state, ConsensusState::Idle);
    }

    #[test]
    fn test_state_string_conversion() {
        assert_eq!(ConsensusState::Idle.as_str(), "idle");
        assert_eq!(ConsensusState::PrePrepared.as_str(), "pre_prepared");
        assert_eq!(ConsensusState::Prepared.as_str(), "prepared");
        assert_eq!(ConsensusState::Committed.as_str(), "committed");
        assert_eq!(ConsensusState::ViewChange.as_str(), "view_change");
    }

    #[test]
    fn test_state_from_string() {
        assert_eq!(ConsensusState::from_str("idle"), Some(ConsensusState::Idle));
        assert_eq!(
            ConsensusState::from_str("committed"),
            Some(ConsensusState::Committed)
        );
        assert_eq!(ConsensusState::from_str("invalid"), None);
    }

    #[test]
    fn test_can_process_request() {
        assert!(ConsensusState::Idle.can_process_request());
        assert!(ConsensusState::Committed.can_process_request());
        assert!(!ConsensusState::PrePrepared.can_process_request());
        assert!(!ConsensusState::Prepared.can_process_request());
        assert!(!ConsensusState::ViewChange.can_process_request());
    }

    #[test]
    fn test_serialization() {
        let state = ConsensusState::PrePrepared;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"pre_prepared\"");

        let deserialized: ConsensusState = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, state);
    }
}

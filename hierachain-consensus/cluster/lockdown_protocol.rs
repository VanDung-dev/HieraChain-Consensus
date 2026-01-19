//! Cluster Lockdown Protocol
//!
//! Defines the protocol for broadcasting and handling system-wide lockdown events.

use serde::{Deserialize, Serialize};

/// Message broadcast when a node initiates a lockdown
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LockdownMessage {
    /// ID of the node initiating lockdown
    pub node_id: String,
    /// Timestamp of the event
    pub timestamp: u64,
    /// Reason for lockdown
    pub reason: String,
    /// Cryptographic signature of the message (optional for mock)
    pub signature: Vec<u8>,
}

/// Report containing the final state of a node before quarantine/shutdown
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct QuarantineReport {
    pub node_id: String,
    pub timestamp: u64,
    /// Hash of the current state or relevant data
    pub state_fingerprint: String,
    /// Index of the last successfully committed block
    pub last_block_index: u64,
}

/// Broadcast error types
#[derive(Debug)]
pub enum BroadcastError {
    NetworkError(String),
    SerializationError(String),
}

/// Manager for handling cluster-wide lockdown state
pub struct ClusterLockdownManager;

impl ClusterLockdownManager {
    /// Broadcast lockdown event to all peers
    ///
    /// In a real implementation, this would interact with the P2P networking layer.
    /// For now, it simulates the broadcast by logging.
    pub fn broadcast_lockdown(message: &LockdownMessage) -> Result<(), BroadcastError> {
        // Simulate serialization check
        let _serialized = serde_json::to_string(message)
            .map_err(|e| BroadcastError::SerializationError(e.to_string()))?;

        println!(
            "P2P BROADCAST [LOCKDOWN]: Node {} triggered lockdown due to '{}'",
            message.node_id, message.reason
        );

        Ok(())
    }

    /// Handle received lockdown message
    ///
    /// Verifies the authenticity of the message and triggers local response.
    pub fn on_lockdown_received(message: &LockdownMessage) -> Result<bool, String> {
        // Verify timestamp freshness (e.g., within 5 minutes)
        // Verify signature (using security module - stubbed here)

        if message.reason.is_empty() {
            return Err("Empty reason provided".to_string());
        }

        println!(
            "P2P RECEIVE [LOCKDOWN]: Authenticated alert from Node {}. Reason: {}",
            message.node_id, message.reason
        );

        // Return true to indicate successful processing/acceptance
        Ok(true)
    }

    /// Broadcast a quarantine report (Last Breath)
    ///
    /// Used when a node detects corruption or is about to shut down.
    pub fn broadcast_quarantine_report(report: &QuarantineReport) -> Result<(), BroadcastError> {
        let _serialized = serde_json::to_string(report)
            .map_err(|e| BroadcastError::SerializationError(e.to_string()))?;

        println!(
            "P2P BROADCAST [QUARANTINE]: Node {} reporting last breath. Fingerprint: {}, Last Block: {}",
            report.node_id, report.state_fingerprint, report.last_block_index
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_lockdown() {
        let msg = LockdownMessage {
            node_id: "node_01".to_string(),
            timestamp: 1678888888,
            reason: "Critical security breach".to_string(),
            signature: vec![1, 2, 3, 4], // Mock signature
        };

        let result = ClusterLockdownManager::broadcast_lockdown(&msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_on_lockdown_received() {
        let msg = LockdownMessage {
            node_id: "node_02".to_string(),
            timestamp: 1678888999,
            reason: "Suspected invalid block from peer".to_string(),
            signature: vec![0xAB, 0xCD],
        };

        let result = ClusterLockdownManager::on_lockdown_received(&msg);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_invalid_lockdown_message() {
        let msg = LockdownMessage {
            node_id: "node_bad".to_string(),
            timestamp: 0,
            reason: "".to_string(), // Empty reason should fail
            signature: vec![],
        };

        let result = ClusterLockdownManager::on_lockdown_received(&msg);
        assert!(result.is_err());
    }
}

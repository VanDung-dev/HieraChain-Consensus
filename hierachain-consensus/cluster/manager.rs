//! Cluster State Manager
//!
//! Manages the health status of nodes in the cluster and makes high-level decisions
//! like triggering system-wide lockdown based on quorum consensus.

use dashmap::DashMap;

/// Status of a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Normal operation
    Active,
    /// Suspected of malicious or faulty behavior
    Suspect,
    /// Isolated from the network
    Quarantined,
    /// In emergency lockdown state
    Lockdown,
    /// Offline or unreachable
    Offline,
}

/// Manages cluster-wide state and health
pub struct ClusterManager {
    /// Concurrent map of NodeID -> Status
    pub nodes: DashMap<String, NodeStatus>,
    /// Percentage of nodes required to trigger a quorum decision (0.0 - 1.0)
    pub quorum_threshold: f64,
}

impl ClusterManager {
    /// Create a new ClusterManager
    pub fn new(quorum_threshold: f64) -> Self {
        Self {
            nodes: DashMap::new(),
            quorum_threshold,
        }
    }

    /// Register or update a node's status
    pub fn update_node_status(&self, node_id: &str, status: NodeStatus) {
        self.nodes.insert(node_id.to_string(), status);
    }

    /// Check if the cluster should enter lockdown
    ///
    /// Returns true if the ratio of nodes in 'Lockdown' or 'Quarantined' state
    /// exceeds the quorum threshold.
    pub fn check_quorum_lockdown(&self) -> bool {
        if self.nodes.is_empty() {
            return false;
        }

        let total_nodes = self.nodes.len() as f64;
        let critical_nodes = self
            .nodes
            .iter()
            .filter(|r| matches!(*r.value(), NodeStatus::Lockdown | NodeStatus::Quarantined))
            .count() as f64;

        let ratio = critical_nodes / total_nodes;
        ratio >= self.quorum_threshold
    }

    /// Trigger a broadcast for recovery
    ///
    /// This would notify all nodes to lift lockdown.
    pub fn broadcast_recovery(&self) -> Result<(), String> {
        // In a real implementation, this sends a signed RecoveryMessage via P2P
        println!("BROADCAST [RECOVERY]: Initiating cluster recovery protocol...");

        // Reset local state if needed (optional logic)
        // self.nodes.iter_mut().for_each(|mut r| *r.value_mut() = NodeStatus::Active);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_lockdown() {
        // Threshold 66%
        let manager = ClusterManager::new(0.66);

        // 3 Nodes total
        manager.update_node_status("node1", NodeStatus::Active);
        manager.update_node_status("node2", NodeStatus::Active);
        manager.update_node_status("node3", NodeStatus::Active);

        assert!(!manager.check_quorum_lockdown());

        // 1 Node Lockdown (33%) -> No Quorum
        manager.update_node_status("node1", NodeStatus::Lockdown);
        assert!(!manager.check_quorum_lockdown());

        // 2 Nodes Lockdown (66%) -> Quorum Reached
        manager.update_node_status("node2", NodeStatus::Quarantined);
        assert!(manager.check_quorum_lockdown());
    }

    #[test]
    fn test_node_updates() {
        let manager = ClusterManager::new(0.5);
        manager.update_node_status("nodeA", NodeStatus::Active);

        assert_eq!(*manager.nodes.get("nodeA").unwrap(), NodeStatus::Active);

        manager.update_node_status("nodeA", NodeStatus::Offline);
        assert_eq!(*manager.nodes.get("nodeA").unwrap(), NodeStatus::Offline);
    }
}

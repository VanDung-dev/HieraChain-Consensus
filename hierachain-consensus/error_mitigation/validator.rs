//! Consensus Validator
//!
//! Validates BFT consensus requirements like node count (n >= 3f + 1).

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Validation error types
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("BFT requires at least {required} nodes to tolerate {f} faults, but only {actual} nodes provided")]
    InsufficientNodes {
        required: usize,
        f: usize,
        actual: usize,
    },

    #[error("Duplicate node IDs detected: {0:?}")]
    DuplicateNodes(Vec<String>),

    #[error("Empty node list")]
    EmptyNodeList,

    #[error("Invalid node ID: {0}")]
    InvalidNodeId(String),
}

/// Node health status
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Node is healthy and responsive
    Healthy,
    /// Node is responding slowly
    Degraded,
    /// Node is not responding
    Unhealthy,
    /// Status unknown (no data yet)
    Unknown,
}

/// Node health information
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Node identifier
    pub node_id: String,
    /// Current health status
    pub status: HealthStatus,
    /// Last successful heartbeat timestamp
    pub last_heartbeat: f64,
    /// Response times (recent samples)
    pub response_times: Vec<f64>,
    /// Failure count since last healthy
    pub failure_count: u32,
}

impl NodeHealth {
    /// Create a new node health record
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            status: HealthStatus::Unknown,
            last_heartbeat: 0.0,
            response_times: Vec::with_capacity(10),
            failure_count: 0,
        }
    }

    /// Record a successful heartbeat
    pub fn record_heartbeat(&mut self, response_time: f64) {
        self.last_heartbeat = current_timestamp();
        self.status = HealthStatus::Healthy;
        self.failure_count = 0;

        // Keep last 10 response times
        self.response_times.push(response_time);
        if self.response_times.len() > 10 {
            self.response_times.remove(0);
        }

        // Check for degraded status based on response time
        if let Some(avg) = self.average_response_time() {
            if avg > 1.0 {
                self.status = HealthStatus::Degraded;
            }
        }
    }

    /// Record a failed heartbeat
    pub fn record_failure(&mut self) {
        self.failure_count += 1;

        if self.failure_count >= 3 {
            self.status = HealthStatus::Unhealthy;
        } else {
            self.status = HealthStatus::Degraded;
        }
    }

    /// Get average response time
    pub fn average_response_time(&self) -> Option<f64> {
        if self.response_times.is_empty() {
            return None;
        }

        let sum: f64 = self.response_times.iter().sum();
        Some(sum / self.response_times.len() as f64)
    }

    /// Check if this node is considered active
    pub fn is_active(&self, timeout: f64) -> bool {
        let now = current_timestamp();
        (now - self.last_heartbeat) < timeout && self.status != HealthStatus::Unhealthy
    }
}

/// Consensus validator for BFT requirements
pub struct ConsensusValidator {
    /// Maximum Byzantine faults tolerated
    f: usize,
    /// Node health tracking
    node_health: HashMap<String, NodeHealth>,
    /// Heartbeat timeout in seconds
    heartbeat_timeout: f64,
}

impl ConsensusValidator {
    /// Create a new consensus validator
    ///
    /// # Arguments
    /// * `f` - Maximum Byzantine faults to tolerate
    pub fn new(f: usize) -> Self {
        Self {
            f,
            node_health: HashMap::new(),
            heartbeat_timeout: 30.0,
        }
    }

    /// Get the required number of nodes for BFT (3f + 1)
    pub fn required_nodes(&self) -> usize {
        3 * self.f + 1
    }

    /// Get the quorum size (2f + 1)
    pub fn quorum_size(&self) -> usize {
        2 * self.f + 1
    }

    /// Validate that node list meets BFT requirements
    ///
    /// # Arguments
    /// * `nodes` - List of node IDs
    ///
    /// # Returns
    /// * `Result<(), ValidationError>` - Ok if valid, error otherwise
    pub fn validate_nodes(&self, nodes: &[String]) -> Result<(), ValidationError> {
        // Check for empty list
        if nodes.is_empty() {
            return Err(ValidationError::EmptyNodeList);
        }

        // Check for sufficient nodes
        let required = self.required_nodes();
        if nodes.len() < required {
            return Err(ValidationError::InsufficientNodes {
                required,
                f: self.f,
                actual: nodes.len(),
            });
        }

        // Check for duplicates
        let mut seen = std::collections::HashSet::new();
        let mut duplicates = Vec::new();

        for node in nodes {
            if !seen.insert(node.clone()) {
                duplicates.push(node.clone());
            }
        }

        if !duplicates.is_empty() {
            return Err(ValidationError::DuplicateNodes(duplicates));
        }

        // Check for valid node IDs (non-empty)
        for node in nodes {
            if node.trim().is_empty() {
                return Err(ValidationError::InvalidNodeId(node.clone()));
            }
        }

        Ok(())
    }

    /// Record a heartbeat from a node
    pub fn record_heartbeat(&mut self, node_id: &str, response_time: f64) {
        let health = self
            .node_health
            .entry(node_id.to_string())
            .or_insert_with(|| NodeHealth::new(node_id));

        health.record_heartbeat(response_time);
    }

    /// Record a failed heartbeat
    pub fn record_failure(&mut self, node_id: &str) {
        let health = self
            .node_health
            .entry(node_id.to_string())
            .or_insert_with(|| NodeHealth::new(node_id));

        health.record_failure();
    }

    /// Get health status of a node
    pub fn get_node_health(&self, node_id: &str) -> Option<&NodeHealth> {
        self.node_health.get(node_id)
    }

    /// Get count of healthy nodes
    pub fn healthy_node_count(&self) -> usize {
        self.node_health
            .values()
            .filter(|h| h.is_active(self.heartbeat_timeout))
            .count()
    }

    /// Check if we have enough healthy nodes for consensus
    pub fn has_quorum(&self) -> bool {
        self.healthy_node_count() >= self.quorum_size()
    }

    /// Get list of unhealthy nodes
    pub fn get_unhealthy_nodes(&self) -> Vec<String> {
        self.node_health
            .iter()
            .filter(|(_, h)| h.status == HealthStatus::Unhealthy)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Set heartbeat timeout
    pub fn set_heartbeat_timeout(&mut self, timeout: f64) {
        self.heartbeat_timeout = timeout;
    }
}

/// Get current timestamp as f64
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bft_requirements() {
        let validator = ConsensusValidator::new(1);

        assert_eq!(validator.required_nodes(), 4);
        assert_eq!(validator.quorum_size(), 3);
    }

    #[test]
    fn test_validate_sufficient_nodes() {
        let validator = ConsensusValidator::new(1);

        let nodes: Vec<String> = (1..=4).map(|i| format!("node{}", i)).collect();

        assert!(validator.validate_nodes(&nodes).is_ok());
    }

    #[test]
    fn test_validate_insufficient_nodes() {
        let validator = ConsensusValidator::new(1);

        let nodes = vec!["node1".to_string(), "node2".to_string()];

        let result = validator.validate_nodes(&nodes);
        assert!(result.is_err());

        match result {
            Err(ValidationError::InsufficientNodes {
                required,
                f,
                actual,
            }) => {
                assert_eq!(required, 4);
                assert_eq!(f, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected InsufficientNodes error"),
        }
    }

    #[test]
    fn test_validate_duplicate_nodes() {
        let validator = ConsensusValidator::new(1);

        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node1".to_string(), // duplicate
            "node4".to_string(),
        ];

        let result = validator.validate_nodes(&nodes);
        assert!(matches!(result, Err(ValidationError::DuplicateNodes(_))));
    }

    #[test]
    fn test_validate_empty_nodes() {
        let validator = ConsensusValidator::new(1);

        let result = validator.validate_nodes(&[]);
        assert!(matches!(result, Err(ValidationError::EmptyNodeList)));
    }

    #[test]
    fn test_node_health_tracking() {
        let mut validator = ConsensusValidator::new(1);

        // Record healthy heartbeat
        validator.record_heartbeat("node1", 0.1);

        let health = validator.get_node_health("node1").unwrap();
        assert_eq!(health.status, HealthStatus::Healthy);

        // Record failures
        validator.record_failure("node2");
        validator.record_failure("node2");
        validator.record_failure("node2");

        let health2 = validator.get_node_health("node2").unwrap();
        assert_eq!(health2.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_unhealthy_nodes() {
        let mut validator = ConsensusValidator::new(1);

        // Mark node2 as unhealthy
        for _ in 0..3 {
            validator.record_failure("node2");
        }

        let unhealthy = validator.get_unhealthy_nodes();
        assert!(unhealthy.contains(&"node2".to_string()));
    }
}

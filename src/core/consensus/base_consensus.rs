//! Base consensus implementation for HieraChain Framework.
//!
//! This module implements a high-performance consensus algorithm using Rust,
//! designed to work efficiently with the Python components of HieraChain.

use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Represents a consensus node in the network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusNode {
    /// Unique identifier for the node
    pub id: String,
    /// Public key of the node
    pub public_key: String,
    /// Network address of the node
    pub address: String,
    /// Node reputation score
    pub reputation: f64,
}

/// Represents a consensus message exchanged between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusMessage {
    /// Message type
    pub msg_type: String,
    /// Sender node ID
    pub sender: String,
    /// Message content
    pub content: Map<String, Value>,
    /// Message timestamp
    pub timestamp: f64,
    /// Digital signature
    pub signature: String,
}

/// Main consensus state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseConsensus {
    /// List of nodes participating in consensus
    pub nodes: Vec<ConsensusNode>,
    /// Current round of consensus
    pub round: u64,
    /// Current leader node
    pub leader: Option<String>,
    /// Messages received in current round
    pub messages: Vec<ConsensusMessage>,
    /// Decisions made in previous rounds
    pub decisions: HashMap<u64, String>,
    /// Node reputations
    pub reputations: HashMap<String, f64>,
}

impl BaseConsensus {
    /// Create a new consensus instance
    pub fn new() -> Self {
        BaseConsensus {
            nodes: Vec::new(),
            round: 0,
            leader: None,
            messages: Vec::new(),
            decisions: HashMap::new(),
            reputations: HashMap::new(),
        }
    }

    /// Add a new node to the consensus network
    pub fn add_node(&mut self, node: ConsensusNode) {
        self.reputations.insert(node.id.clone(), node.reputation);
        self.nodes.push(node);
    }

    /// Remove a node from the consensus network
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|node| node.id != node_id);
        self.reputations.remove(node_id);
    }

    /// Select leader based on reputation and round
    pub fn select_leader(&mut self) -> Option<String> {
        if self.nodes.is_empty() {
            return None;
        }

        // Simple leader selection based on reputation and round
        let total_reputation: f64 = self.reputations.values().sum();
        if total_reputation <= 0.0 {
            // If no reputation scores, select based on round-robin
            let index = self.round as usize % self.nodes.len();
            self.leader = Some(self.nodes[index].id.clone());
        } else {
            // Weighted selection based on reputation
            let target = (self.round as f64 * 13.7) % total_reputation; // Simple hash-like function
            let mut cumulative = 0.0;
            
            for node in &self.nodes {
                cumulative += self.reputations.get(&node.id).unwrap_or(&0.0);
                if cumulative >= target {
                    self.leader = Some(node.id.clone());
                    break;
                }
            }
            
            // Fallback in case of floating point errors
            if self.leader.is_none() {
                self.leader = Some(self.nodes.last().unwrap().id.clone());
            }
        }
        
        self.leader.clone()
    }

    /// Process an incoming consensus message
    pub fn process_message(&mut self, message: ConsensusMessage) -> Result<bool, String> {
        // Verify message signature (simplified)
        if message.signature.is_empty() {
            return Err("Message signature missing".to_string());
        }

        // Store message
        self.messages.push(message);
        Ok(true)
    }

    /// Execute one round of consensus
    pub fn execute_round(&mut self) -> Result<String, String> {
        self.round += 1;
        
        // Select leader for this round
        self.select_leader();
        
        // In a real implementation, we would:
        // 1. Have the leader propose a block
        // 2. Collect votes from other nodes
        // 3. Make a decision based on the votes
        // 4. Broadcast the decision
        
        // Simplified decision making for demonstration
        let decision = if let Some(leader_id) = &self.leader {
            format!("Round {} decided by leader {}", self.round, leader_id)
        } else {
            format!("Round {} decided by default", self.round)
        };
        
        // Store decision
        self.decisions.insert(self.round, decision.clone());
        
        // Clear messages for next round
        self.messages.clear();
        
        Ok(decision)
    }

    /// Get statistics about the consensus process
    pub fn get_statistics(&self) -> ConsensusStats {
        ConsensusStats {
            total_nodes: self.nodes.len(),
            current_round: self.round,
            total_decisions: self.decisions.len(),
            leader: self.leader.clone().unwrap_or_default(),
        }
    }

    /// Validate if a node is authorized to participate
    pub fn is_authorized_node(&self, node_id: &str) -> bool {
        self.nodes.iter().any(|node| node.id == node_id)
    }

    /// Update node reputation based on behavior
    pub fn update_reputation(&mut self, node_id: &str, delta: f64) {
        if let Some(reputation) = self.reputations.get_mut(node_id) {
            *reputation += delta;
            // Ensure reputation stays within reasonable bounds
            if *reputation < 0.0 {
                *reputation = 0.0;
            }
            if *reputation > 100.0 {
                *reputation = 100.0;
            }
        }
    }
}

impl Default for BaseConsensus {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the consensus process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusStats {
    pub total_nodes: usize,
    pub current_round: u64,
    pub total_decisions: usize,
    pub leader: String,
}

/// Perform proof of authority consensus validation
#[pyo3::pyfunction]
pub fn validate_poa_block(
    block_data: String,
    validator_signature: String,
    validator_id: String,
) -> bool {
    // In a real implementation, we would:
    // 1. Parse the block data
    // 2. Verify the validator is authorized
    // 3. Verify the signature against the block data and validator's public key
    
    // Simplified validation for demonstration
    !block_data.is_empty() && !validator_signature.is_empty() && !validator_id.is_empty()
}

/// Calculate block hash using SHA-256
#[pyo3::pyfunction]
pub fn calculate_block_hash(block_content: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(block_content.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Bulk validate transactions
#[pyo3::pyfunction]
pub fn bulk_validate_transactions(transactions: Vec<String>) -> Vec<bool> {
    transactions.into_iter().map(|tx| !tx.is_empty()).collect()
}
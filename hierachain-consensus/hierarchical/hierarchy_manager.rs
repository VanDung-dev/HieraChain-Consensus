//! Hierarchy Manager for HieraChain Framework.
//!
//! This module provides the HierarchyManager struct, which is responsible for
//! coordinating the interaction between the Main Chain and multiple Sub-Chains
//! (Domain Chains) in the HieraChain system.
//!
//! # Features
//! - Creation and registration of sub-chains
//! - Routing of inter-chain communication
//! - Aggregation of system-wide statistics
//! - Coordination of cross-chain operations
//! - Organization and channel management

use crate::hierarchical::channel::{Channel, Organization, PrivateCollection};
use crate::hierarchical::main_chain::MainChain;
use crate::hierarchical::sub_chain::SubChain;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ==================== Error Types ====================

/// Error types for HierarchyManager operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HierarchyError {
    /// Sub-chain with the given name already exists
    SubChainAlreadyExists(String),
    /// Sub-chain with the given name not found
    SubChainNotFound(String),
    /// Organization with the given ID already exists
    OrganizationAlreadyExists(String),
    /// Organization with the given ID not found
    OrganizationNotFound(String),
    /// Channel with the given ID already exists
    ChannelAlreadyExists(String),
    /// Channel with the given ID not found
    ChannelNotFound(String),
    /// Invalid operation
    InvalidOperation(String),
    /// Connection failed
    ConnectionFailed(String),
}

impl std::fmt::Display for HierarchyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HierarchyError::SubChainAlreadyExists(name) => {
                write!(f, "Sub-chain '{}' already exists", name)
            }
            HierarchyError::SubChainNotFound(name) => {
                write!(f, "Sub-chain '{}' not found", name)
            }
            HierarchyError::OrganizationAlreadyExists(id) => {
                write!(f, "Organization '{}' already exists", id)
            }
            HierarchyError::OrganizationNotFound(id) => {
                write!(f, "Organization '{}' not found", id)
            }
            HierarchyError::ChannelAlreadyExists(id) => {
                write!(f, "Channel '{}' already exists", id)
            }
            HierarchyError::ChannelNotFound(id) => {
                write!(f, "Channel '{}' not found", id)
            }
            HierarchyError::InvalidOperation(msg) => {
                write!(f, "Invalid operation: {}", msg)
            }
            HierarchyError::ConnectionFailed(msg) => {
                write!(f, "Connection failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for HierarchyError {}

// ==================== Supporting Types ====================

// Organization, Channel, and PrivateCollection are imported from crate::hierarchical::channel

/// System-wide statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SystemStats {
    /// Total number of transactions across all chains
    pub total_transactions: u64,
    /// Total number of blocks across all chains
    pub total_blocks: u64,
    /// Number of active chains
    pub active_chains: u64,
    /// System uptime in seconds
    pub system_uptime: f64,
}

/// Sub-chain metadata stored in HierarchyManager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubChainInfo {
    /// Sub-chain name
    pub name: String,
    /// Domain type
    pub domain_type: String,
    /// Creation timestamp
    pub created_at: f64,
    /// Additional metadata
    pub metadata: Map<String, Value>,
}

// ==================== Hierarchy Manager ====================

/// Manages the hierarchy of chains (Main Chain and Sub-Chains).
///
/// This struct handles:
/// - Creation and registration of sub-chains
/// - Routing of inter-chain communication
/// - Aggregation of system-wide statistics
/// - Coordination of cross-chain operations
/// - Organization and channel management
pub struct HierarchyManager {
    /// Main chain instance
    main_chain: MainChain,
    /// Registered sub-chains (name -> SubChain)
    sub_chains: HashMap<String, Arc<Mutex<SubChain>>>,
    /// Sub-chain metadata
    sub_chain_info: HashMap<String, SubChainInfo>,
    /// System start timestamp
    system_started_at: f64,
    /// Auto proof submission enabled
    auto_proof_submission: bool,
    /// Proof submission interval in seconds
    proof_submission_interval: u64,
    /// System-wide statistics
    system_stats: SystemStats,
    /// Registered organizations
    organizations: HashMap<String, Organization>,
    /// Created channels
    channels: HashMap<String, Channel>,
    /// Private data collections
    private_collections: HashMap<String, PrivateCollection>,
    /// Organization to chain assignments
    org_chain_assignments: HashMap<String, Vec<String>>,
}

impl HierarchyManager {
    /// Create a new HierarchyManager with a MainChain.
    ///
    /// # Arguments
    /// * `main_chain_name` - Name for the main chain
    ///
    /// # Returns
    /// A new HierarchyManager instance
    pub fn new(main_chain_name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        Self {
            main_chain: MainChain::new(main_chain_name),
            sub_chains: HashMap::new(),
            sub_chain_info: HashMap::new(),
            system_started_at: now,
            auto_proof_submission: false,
            proof_submission_interval: 60,
            system_stats: SystemStats::default(),
            organizations: HashMap::new(),
            channels: HashMap::new(),
            private_collections: HashMap::new(),
            org_chain_assignments: HashMap::new(),
        }
    }

    /// Get the main chain reference
    pub fn get_main_chain(&self) -> &MainChain {
        &self.main_chain
    }

    /// Get mutable main chain reference
    pub fn get_main_chain_mut(&mut self) -> &mut MainChain {
        &mut self.main_chain
    }

    /// Set the main chain
    pub fn set_main_chain(&mut self, main_chain: MainChain) {
        self.main_chain = main_chain;
    }

    // ==================== Sub-Chain Management ====================

    /// Create and register a new sub-chain.
    ///
    /// # Arguments
    /// * `name` - Unique name for the sub-chain
    /// * `domain_type` - Type of domain (e.g., "supply_chain", "healthcare")
    /// * `metadata` - Additional metadata for the chain
    ///
    /// # Returns
    /// Ok(()) if created successfully, Err if already exists
    pub fn create_sub_chain(
        &mut self,
        name: &str,
        domain_type: &str,
        metadata: Option<Map<String, Value>>,
    ) -> Result<(), HierarchyError> {
        if self.sub_chains.contains_key(name) {
            return Err(HierarchyError::SubChainAlreadyExists(name.to_string()));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        // Create sub-chain with default consensus type (PoA)
        let sub_chain = SubChain::new(
            name,
            domain_type,
            crate::hierarchical::sub_chain::ConsensusType::default(),
        );

        // Store sub-chain info
        let info = SubChainInfo {
            name: name.to_string(),
            domain_type: domain_type.to_string(),
            created_at: now,
            metadata: metadata.unwrap_or_default(),
        };

        // Register with main chain
        let mut chain_metadata = Map::new();
        chain_metadata.insert(
            "domain_type".to_string(),
            Value::String(domain_type.to_string()),
        );
        chain_metadata.insert(
            "created_at".to_string(),
            Value::Number(serde_json::Number::from_f64(now).unwrap_or(0.into())),
        );
        self.main_chain
            .register_sub_chain(name, Some(Value::Object(chain_metadata)));

        // Store in manager
        self.sub_chains
            .insert(name.to_string(), Arc::new(Mutex::new(sub_chain)));
        self.sub_chain_info.insert(name.to_string(), info);

        // Update stats
        self.system_stats.active_chains = self.sub_chains.len() as u64 + 1; // +1 for main chain

        Ok(())
    }

    /// Add an existing sub-chain to the hierarchy.
    ///
    /// # Arguments
    /// * `name` - Name for the sub-chain
    /// * `sub_chain` - The sub-chain instance to add
    pub fn add_sub_chain(&mut self, name: &str, sub_chain: SubChain) -> Result<(), HierarchyError> {
        if self.sub_chains.contains_key(name) {
            return Err(HierarchyError::SubChainAlreadyExists(name.to_string()));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        // Store sub-chain info
        let info = SubChainInfo {
            name: name.to_string(),
            domain_type: sub_chain.domain_type.clone(),
            created_at: now,
            metadata: Map::new(),
        };

        // Register with main chain
        let mut chain_metadata = Map::new();
        chain_metadata.insert(
            "domain_type".to_string(),
            Value::String(sub_chain.domain_type.clone()),
        );
        self.main_chain
            .register_sub_chain(name, Some(Value::Object(chain_metadata)));

        self.sub_chains
            .insert(name.to_string(), Arc::new(Mutex::new(sub_chain)));
        self.sub_chain_info.insert(name.to_string(), info);
        self.system_stats.active_chains = self.sub_chains.len() as u64 + 1;

        Ok(())
    }

    /// Get a sub-chain by name.
    pub fn get_sub_chain(&self, name: &str) -> Option<Arc<Mutex<SubChain>>> {
        self.sub_chains.get(name).cloned()
    }

    /// Get all sub-chain names.
    pub fn get_all_sub_chain_names(&self) -> Vec<String> {
        self.sub_chains.keys().cloned().collect()
    }

    /// Get sub-chain info by name.
    pub fn get_sub_chain_info(&self, name: &str) -> Option<&SubChainInfo> {
        self.sub_chain_info.get(name)
    }

    /// Remove a sub-chain.
    pub fn remove_sub_chain(&mut self, name: &str) -> Result<(), HierarchyError> {
        if !self.sub_chains.contains_key(name) {
            return Err(HierarchyError::SubChainNotFound(name.to_string()));
        }

        self.sub_chains.remove(name);
        self.sub_chain_info.remove(name);
        self.system_stats.active_chains = self.sub_chains.len() as u64 + 1;

        Ok(())
    }

    // ==================== Domain Operations ====================

    /// Start an operation on a specific sub-chain.
    ///
    /// # Arguments
    /// * `sub_chain_name` - Target sub-chain name
    /// * `entity_id` - Entity identifier
    /// * `operation_type` - Type of operation
    /// * `details` - Operation details
    pub fn start_operation(
        &self,
        sub_chain_name: &str,
        entity_id: &str,
        operation_type: &str,
        details: Option<Map<String, Value>>,
    ) -> Result<bool, HierarchyError> {
        let chain = self
            .sub_chains
            .get(sub_chain_name)
            .ok_or_else(|| HierarchyError::SubChainNotFound(sub_chain_name.to_string()))?;

        let mut chain_guard = chain.lock().unwrap();
        let details_value = details
            .map(Value::Object)
            .unwrap_or(Value::Object(Map::new()));
        Ok(chain_guard.start_operation(entity_id, operation_type, details_value))
    }

    /// Complete an operation on a specific sub-chain.
    pub fn complete_operation(
        &self,
        sub_chain_name: &str,
        entity_id: &str,
        operation_type: &str,
        result: Option<Map<String, Value>>,
    ) -> Result<bool, HierarchyError> {
        let chain = self
            .sub_chains
            .get(sub_chain_name)
            .ok_or_else(|| HierarchyError::SubChainNotFound(sub_chain_name.to_string()))?;

        let mut chain_guard = chain.lock().unwrap();
        let result_value = result
            .map(Value::Object)
            .unwrap_or(Value::Object(Map::new()));
        Ok(chain_guard.complete_operation(entity_id, operation_type, result_value))
    }

    // ==================== Proof Management ====================

    /// Submit a proof from a sub-chain to the main chain.
    pub fn submit_proof_to_main_chain(
        &mut self,
        sub_chain_name: &str,
    ) -> Result<bool, HierarchyError> {
        let chain = self
            .sub_chains
            .get(sub_chain_name)
            .ok_or_else(|| HierarchyError::SubChainNotFound(sub_chain_name.to_string()))?;

        let chain_guard = chain.lock().unwrap();
        let stats = chain_guard.get_domain_statistics();

        // Create a proof from sub-chain state
        let mut proof_details = Map::new();
        if let Some(stats_obj) = stats.as_object() {
            for (key, value) in stats_obj {
                proof_details.insert(key.clone(), value.clone());
            }
        }

        // Get latest block hash if available
        let latest_block = chain_guard.get_latest_block();
        let block_hash = latest_block.hash.clone();

        drop(chain_guard);

        // Submit proof to main chain (metadata includes block_hash)
        proof_details.insert("block_hash".to_string(), Value::String(block_hash));
        self.main_chain
            .submit_proof(sub_chain_name, Value::Object(proof_details));

        Ok(true)
    }

    /// Submit proofs for all sub-chains.
    pub fn submit_all_proofs(&mut self) -> HashMap<String, bool> {
        let chain_names: Vec<String> = self.sub_chains.keys().cloned().collect();
        let mut results = HashMap::new();

        for name in chain_names {
            let success = self.submit_proof_to_main_chain(&name).unwrap_or(false);
            results.insert(name, success);
        }

        results
    }

    /// Configure automatic proof submission.
    pub fn configure_auto_proof_submission(&mut self, enabled: bool, interval: u64) {
        self.auto_proof_submission = enabled;
        self.proof_submission_interval = interval;
    }

    /// Check if auto proof submission is enabled.
    pub fn is_auto_proof_submission_enabled(&self) -> bool {
        self.auto_proof_submission
    }

    // ==================== System Statistics ====================

    /// Get a high-level overview of the entire system state.
    pub fn get_system_overview(&self) -> Map<String, Value> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut total_tx: u64 = 0;
        let mut total_blocks = self.main_chain.chain_length() as u64;
        let mut domain_distribution: HashMap<String, u64> = HashMap::new();

        for (name, chain) in &self.sub_chains {
            if let Ok(chain_guard) = chain.lock() {
                let stats = chain_guard.get_domain_statistics();

                // Count operations/events
                if let Some(Value::Number(n)) = stats.get("total_operations") {
                    total_tx += n.as_u64().unwrap_or(0);
                }
                if let Some(Value::Number(n)) = stats.get("total_events") {
                    total_tx += n.as_u64().unwrap_or(0);
                }
                if let Some(Value::Number(n)) = stats.get("total_blocks") {
                    total_blocks += n.as_u64().unwrap_or(0);
                }

                // Count domain types
                if let Some(info) = self.sub_chain_info.get(name) {
                    *domain_distribution
                        .entry(info.domain_type.clone())
                        .or_insert(0) += 1;
                }
            }
        }

        let mut overview = Map::new();
        overview.insert(
            "uptime".to_string(),
            Value::Number(
                serde_json::Number::from_f64(now - self.system_started_at).unwrap_or(0.into()),
            ),
        );
        overview.insert(
            "total_chains".to_string(),
            Value::Number((self.sub_chains.len() as u64 + 1).into()),
        );
        overview.insert(
            "total_transactions_system_wide".to_string(),
            Value::Number(total_tx.into()),
        );
        overview.insert(
            "total_blocks_system_wide".to_string(),
            Value::Number(total_blocks.into()),
        );
        overview.insert(
            "main_chain_height".to_string(),
            Value::Number((self.main_chain.chain_length() as u64).into()),
        );

        // Domain distribution
        let mut domain_map = Map::new();
        for (dtype, count) in domain_distribution {
            domain_map.insert(dtype, Value::Number(count.into()));
        }
        overview.insert("domain_types".to_string(), Value::Object(domain_map));

        overview
    }

    /// Get system statistics.
    pub fn get_system_stats(&self) -> &SystemStats {
        &self.system_stats
    }

    // ==================== Organization Management ====================

    /// Create an organization.
    pub fn create_organization(
        &mut self,
        org_id: &str,
        name: &str,
        admin_users: Option<Vec<String>>,
    ) -> Result<&Organization, HierarchyError> {
        if self.organizations.contains_key(org_id) {
            return Err(HierarchyError::OrganizationAlreadyExists(
                org_id.to_string(),
            ));
        }

        let mut org = Organization::new(
            org_id.to_string(),
            name.to_string(),
            format!("{}-MSP", org_id),
            Vec::new(),
            HashMap::new(),
            HashSet::new(),
        );

        if let Some(admins) = admin_users {
            org.admin_users = admins;
        }

        self.organizations.insert(org_id.to_string(), org);

        Ok(self.organizations.get(org_id).unwrap())
    }

    /// Get an organization by ID.
    pub fn get_organization(&self, org_id: &str) -> Option<&Organization> {
        self.organizations.get(org_id)
    }

    /// Get mutable organization reference.
    pub fn get_organization_mut(&mut self, org_id: &str) -> Option<&mut Organization> {
        self.organizations.get_mut(org_id)
    }

    /// Get all organization IDs.
    pub fn get_all_organization_ids(&self) -> Vec<String> {
        self.organizations.keys().cloned().collect()
    }

    /// Assign an organization to a chain.
    pub fn assign_organization_to_chain(
        &mut self,
        org_id: &str,
        chain_name: &str,
    ) -> Result<bool, HierarchyError> {
        if !self.organizations.contains_key(org_id) {
            return Err(HierarchyError::OrganizationNotFound(org_id.to_string()));
        }

        if !self.sub_chains.contains_key(chain_name) {
            return Err(HierarchyError::SubChainNotFound(chain_name.to_string()));
        }

        let assignments = self
            .org_chain_assignments
            .entry(org_id.to_string())
            .or_insert_with(Vec::new);

        if !assignments.contains(&chain_name.to_string()) {
            assignments.push(chain_name.to_string());
        }

        Ok(true)
    }

    /// Get chains assigned to an organization.
    pub fn get_org_chains(&self, org_id: &str) -> Vec<String> {
        self.org_chain_assignments
            .get(org_id)
            .cloned()
            .unwrap_or_default()
    }

    // ==================== Channel Management ====================

    /// Create a channel for secure data isolation.
    /// Create a channel for secure data isolation.
    pub fn create_channel(
        &mut self,
        channel_id: &str,
        org_ids: Vec<String>,
        policy_config: Option<Map<String, Value>>,
    ) -> Result<&Channel, HierarchyError> {
        if self.channels.contains_key(channel_id) {
            return Err(HierarchyError::ChannelAlreadyExists(channel_id.to_string()));
        }

        // Validate organizations exist and collect them
        let mut orgs_for_channel = Vec::new();
        for org_id in &org_ids {
            if let Some(org) = self.organizations.get(org_id) {
                orgs_for_channel.push(org.clone());
            } else {
                return Err(HierarchyError::OrganizationNotFound(org_id.clone()));
            }
        }

        // Default policy logic (replicated here or use default)
        let default_policy = {
            let mut map = Map::new();
            map.insert("read".to_string(), Value::String("MEMBER".to_string()));
            map.insert("write".to_string(), Value::String("ADMIN".to_string()));
            map.insert(
                "endorsement".to_string(),
                Value::String("MAJORITY".to_string()),
            );
            map
        };

        let policy_to_use = policy_config.unwrap_or(default_policy);

        // Convert Map to HashMap for Channel::new
        let policy_map: HashMap<String, Value> = policy_to_use.into_iter().collect();

        let channel = Channel::new(channel_id.to_string(), orgs_for_channel, policy_map);
        self.channels.insert(channel_id.to_string(), channel);

        Ok(self.channels.get(channel_id).unwrap())
    }

    /// Get a channel by ID.
    pub fn get_channel(&self, channel_id: &str) -> Option<&Channel> {
        self.channels.get(channel_id)
    }

    /// Get all channel IDs.
    pub fn get_all_channel_ids(&self) -> Vec<String> {
        self.channels.keys().cloned().collect()
    }

    // ==================== Private Collection Management ====================

    /// Create a private data collection.
    pub fn create_private_collection(
        &mut self,
        name: &str,
        org_ids: Vec<String>,
        config: Option<Map<String, Value>>,
    ) -> Result<&PrivateCollection, HierarchyError> {
        if self.private_collections.contains_key(name) {
            // Logic to check existing (omitted in original but good practice)
            // But original just overwrites? No, HashMap `insert` overwrites.
        }

        // Validate organizations exist
        for org_id in &org_ids {
            if !self.organizations.contains_key(org_id) {
                return Err(HierarchyError::OrganizationNotFound(org_id.clone()));
            }
        }

        let default_config = {
            let mut map = Map::new();
            map.insert("block_to_purge".to_string(), Value::Number(1000.into()));
            map.insert(
                "endorsement_policy".to_string(),
                Value::String("MAJORITY".to_string()),
            );
            map.insert("min_endorsements".to_string(), Value::Number(2.into()));
            map
        };

        let config_to_use = config.unwrap_or(default_config);
        let config_map: HashMap<String, Value> = config_to_use.into_iter().collect();

        let collection = PrivateCollection::new(name, org_ids, config_map);
        self.private_collections
            .insert(name.to_string(), collection);

        Ok(self.private_collections.get(name).unwrap())
    }

    /// Get a private collection by name.
    pub fn get_private_collection(&self, name: &str) -> Option<&PrivateCollection> {
        self.private_collections.get(name)
    }

    // ==================== Validation ====================

    /// Validate consistency across the entire hierarchical system.
    pub fn validate_cross_chain_consistency(&self) -> Map<String, Value> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut result = Map::new();
        result.insert(
            "timestamp".to_string(),
            Value::Number(serde_json::Number::from_f64(now).unwrap_or(0.into())),
        );
        result.insert(
            "main_chain_valid".to_string(),
            Value::Bool(self.main_chain.is_chain_valid()),
        );

        let mut sub_chain_validation = Map::new();
        let mut proof_consistency = Map::new();
        let mut overall_consistent = true;

        for (name, chain) in &self.sub_chains {
            if let Ok(chain_guard) = chain.lock() {
                let is_valid = chain_guard.blockchain.is_chain_valid();
                sub_chain_validation.insert(name.clone(), Value::Bool(is_valid));

                if !is_valid {
                    overall_consistent = false;
                }

                // Check proof consistency
                if chain_guard.chain_length() > 1 {
                    let latest_block = chain_guard.get_latest_block();
                    let proof_exists = self.main_chain.verify_proof(&latest_block.hash, name);
                    proof_consistency.insert(name.clone(), Value::Bool(proof_exists));
                }
            }
        }

        result.insert(
            "sub_chain_validation".to_string(),
            Value::Object(sub_chain_validation),
        );
        result.insert(
            "proof_consistency".to_string(),
            Value::Object(proof_consistency),
        );
        result.insert(
            "overall_consistent".to_string(),
            Value::Bool(overall_consistent),
        );

        result
    }

    /// Finalize the current block on the main chain.
    pub fn finalize_main_chain_block(&mut self) -> Option<crate::core::block::Block> {
        self.main_chain.finalize_block()
    }

    /// Execute system maintenance tasks.
    pub fn execute_system_maintenance(&mut self) -> Map<String, Value> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut result = Map::new();
        result.insert(
            "timestamp".to_string(),
            Value::Number(serde_json::Number::from_f64(now).unwrap_or(0.into())),
        );

        let mut operations = Vec::new();

        // Submit pending proofs
        let proof_results = self.submit_all_proofs();
        let mut proof_map = Map::new();
        for (name, success) in proof_results {
            proof_map.insert(name, Value::Bool(success));
        }

        let mut proof_op = Map::new();
        proof_op.insert(
            "operation".to_string(),
            Value::String("proof_submission".to_string()),
        );
        proof_op.insert("results".to_string(), Value::Object(proof_map));
        operations.push(Value::Object(proof_op));

        // Finalize main chain block if needed
        if let Some(block) = self.finalize_main_chain_block() {
            let mut finalize_op = Map::new();
            finalize_op.insert(
                "operation".to_string(),
                Value::String("main_chain_finalization".to_string()),
            );
            finalize_op.insert("block_hash".to_string(), Value::String(block.hash));
            finalize_op.insert("block_index".to_string(), Value::Number(block.index.into()));
            operations.push(Value::Object(finalize_op));
        }

        result.insert("operations".to_string(), Value::Array(operations));

        // Update system stats
        self.system_stats.system_uptime = now - self.system_started_at;

        result
    }

    /// Get the system uptime in seconds.
    pub fn get_uptime(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        now - self.system_started_at
    }
}

impl std::fmt::Display for HierarchyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HierarchyManager(main_chain={}, sub_chains={})",
            self.main_chain.name(),
            self.sub_chains.len()
        )
    }
}

impl std::fmt::Debug for HierarchyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let uptime = self.get_uptime();
        write!(
            f,
            "HierarchyManager(main_chain={}, sub_chains={:?}, auto_proof={}, uptime={:.2}s)",
            self.main_chain.name(),
            self.get_all_sub_chain_names(),
            self.auto_proof_submission,
            uptime
        )
    }
}

//! Channel-based Data Isolation for HieraChain Framework.
//!
//! This module implements secure data channels that provide complete isolation between
//! organizations in enterprise blockchain applications. Each channel operates as a
//! completely isolated data space with its own governance policies and access controls.

use crate::core::block::Block;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Channel status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChannelStatus {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "suspended")]
    Suspended,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "maintenance")]
    Maintenance,
}

impl ChannelStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelStatus::Active => "active",
            ChannelStatus::Suspended => "suspended",
            ChannelStatus::Closed => "closed",
            ChannelStatus::Maintenance => "maintenance",
        }
    }
}

/// Organization participating in a channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Organization {
    pub org_id: String,
    pub name: String,
    pub msp_id: String,
    pub endpoints: Vec<String>,
    pub certificates: HashMap<String, Value>,
    pub roles: HashSet<String>,

    // Added for compatibility and full metadata support
    pub admin_users: Vec<String>,
    pub metadata: HashMap<String, Value>,
    pub created_at: f64,
}

impl Organization {
    pub fn new(
        org_id: String,
        name: String,
        msp_id: String,
        endpoints: Vec<String>,
        certificates: HashMap<String, Value>,
        roles: HashSet<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        Self {
            org_id,
            name,
            msp_id,
            endpoints,
            certificates,
            roles,
            admin_users: Vec::new(),
            metadata: HashMap::new(),
            created_at: now,
        }
    }

    /// Check if organization has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Add an admin user
    pub fn add_admin(&mut self, user_id: &str) -> bool {
        if !self.admin_users.contains(&user_id.to_string()) {
            self.admin_users.push(user_id.to_string());
            true
        } else {
            false
        }
    }

    /// Remove an admin user
    pub fn remove_admin(&mut self, user_id: &str) -> bool {
        if let Some(pos) = self.admin_users.iter().position(|x| x == user_id) {
            self.admin_users.remove(pos);
            true
        } else {
            false
        }
    }

    /// Check if a user is an admin
    pub fn is_admin(&self, user_id: &str) -> bool {
        self.admin_users.contains(&user_id.to_string())
    }
}

/// Private data collection for confidential data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateCollection {
    pub name: String,
    pub member_org_ids: Vec<String>,
    pub config: HashMap<String, Value>,
    pub created_at: f64,
}

impl PrivateCollection {
    pub fn new(name: &str, member_org_ids: Vec<String>, config: HashMap<String, Value>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        Self {
            name: name.to_string(),
            member_org_ids,
            config,
            created_at: now,
        }
    }

    /// Check if an organization is a member
    pub fn is_member(&self, org_id: &str) -> bool {
        self.member_org_ids.contains(&org_id.to_string())
    }

    pub fn remove_organization(&mut self, org_id: &str) {
        if let Some(pos) = self.member_org_ids.iter().position(|x| x == org_id) {
            self.member_org_ids.remove(pos);
        }
    }
}

/// Channel access and endorsement policies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPolicy {
    pub read_policy: String,
    pub write_policy: String,
    pub endorsement_policy: String,
    pub admin_policy: String,
    pub lifecycle_endorsement: String,
    pub custom_policies: HashMap<String, Value>,
}

impl ChannelPolicy {
    pub fn new(policy_config: &HashMap<String, Value>) -> Self {
        let read_policy = policy_config
            .get("read")
            .and_then(|v| v.as_str())
            .unwrap_or("MEMBER")
            .to_string();

        let write_policy = policy_config
            .get("write")
            .and_then(|v| v.as_str())
            .unwrap_or("ADMIN")
            .to_string();

        let endorsement_policy = policy_config
            .get("endorsement")
            .and_then(|v| v.as_str())
            .unwrap_or("MAJORITY")
            .to_string();

        let admin_policy = policy_config
            .get("admin")
            .and_then(|v| v.as_str())
            .unwrap_or("UNANIMOUS")
            .to_string();

        let lifecycle_endorsement = policy_config
            .get("lifecycle_endorsement")
            .and_then(|v| v.as_str())
            .unwrap_or("MAJORITY")
            .to_string();

        let custom_policies = policy_config
            .get("custom_policies")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        Self {
            read_policy,
            write_policy,
            endorsement_policy,
            admin_policy,
            lifecycle_endorsement,
            custom_policies,
        }
    }

    /// Evaluate if organization has read access
    pub fn evaluate_read_access(&self, organization: &Organization) -> bool {
        self._evaluate_policy(&self.read_policy, organization)
    }

    /// Evaluate if organization has write access
    pub fn evaluate_write_access(&self, organization: &Organization) -> bool {
        self._evaluate_policy(&self.write_policy, organization)
    }

    /// Evaluate if endorsements meet the policy requirements
    pub fn evaluate_endorsement(&self, endorsements: &[String], total_orgs: usize) -> bool {
        match self.endorsement_policy.as_str() {
            "MAJORITY" => endorsements.len() > total_orgs / 2,
            "UNANIMOUS" => endorsements.len() == total_orgs,
            "ANY" => !endorsements.is_empty(),
            _ => endorsements.len() >= 1, // Custom logic fallback
        }
    }

    fn _evaluate_policy(&self, policy: &str, organization: &Organization) -> bool {
        match policy {
            "MEMBER" => true,
            "ADMIN" => organization.has_role("admin"),
            "OPERATOR" => organization.has_role("operator") || organization.has_role("admin"),
            _ => {
                if let Some(custom) = self.custom_policies.get(policy) {
                    if let Some(required_roles) =
                        custom.get("required_roles").and_then(|r| r.as_array())
                    {
                        for role in required_roles {
                            if let Some(role_str) = role.as_str() {
                                if organization.has_role(role_str) {
                                    return true;
                                }
                            }
                        }
                    }
                    false
                } else {
                    false
                }
            }
        }
    }
}

/// Channel-specific ledger for storing channel events
#[derive(Debug, Clone)]
pub struct ChannelLedger {
    pub blocks: Vec<Block>,
    pub current_block_events: Vec<Value>,
    pub height: u64,
    pub last_block_hash: String,
}

impl ChannelLedger {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            current_block_events: Vec::new(),
            height: 0,
            last_block_hash: "0".to_string(),
        }
    }

    /// Add event to current block
    pub fn add_event(&mut self, mut event: Value) {
        if let Some(obj) = event.as_object_mut() {
            if !obj.contains_key("timestamp") {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                obj.insert("timestamp".to_string(), json!(now));
            }
            obj.insert("channel_event".to_string(), json!(true));
        }
        self.current_block_events.push(event);
    }

    /// Finalize current block and add to ledger
    pub fn finalize_block(&mut self) -> Option<Block> {
        if self.current_block_events.is_empty() {
            return None;
        }

        // Create Block (simplified compare to Python's Arrow conversion for now)
        // In Rust Block, events are Vec<Value>
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let mut block = Block {
            index: self.height,
            events: std::mem::take(&mut self.current_block_events),
            arrow_events: None, // Can be populated if needed
            timestamp,
            previous_hash: self.last_block_hash.clone(),
            nonce: 0,
            merkle_root: String::new(), // Will be calculated
            hash: String::new(),        // Will be calculated
            creator_id: None,
            signature: None,
            zk_proof: None,
            zk_public_inputs: None,
        };

        // Recalculate hash and root
        block.hash = block.calculate_hash();
        let mut temp_block = Block {
            index: block.index,
            events: Vec::new(),
            arrow_events: None,
            timestamp: block.timestamp,
            previous_hash: block.previous_hash.clone(),
            nonce: 0,
            merkle_root: String::new(),
            hash: String::new(),
            creator_id: None,
            signature: None,
            zk_proof: None,
            zk_public_inputs: None,
        };
        for evt in block.events {
            temp_block.add_event(evt);
        }
        block = temp_block;

        self.last_block_hash = block.hash.clone();
        self.blocks.push(block.clone());
        self.height += 1;

        Some(block)
    }

    /// Get events matching filter criteria
    pub fn get_events_by_filter<F>(&self, filter_func: F) -> Vec<Value>
    where
        F: Fn(&Value) -> bool,
    {
        let mut events = Vec::new();
        for block in &self.blocks {
            for event in &block.events {
                if filter_func(event) {
                    events.push(event.clone());
                }
            }
        }
        events
    }
}

/// Secure data channel providing complete isolation between organizations
#[derive(Debug)]
pub struct Channel {
    pub channel_id: String,
    pub organizations: HashMap<String, Organization>,
    pub policy: ChannelPolicy,
    pub private_collections: HashMap<String, PrivateCollection>,
    pub ledger: ChannelLedger,
    pub status: ChannelStatus,
    pub created_at: f64,
    pub last_activity: f64,
    pub configuration: HashMap<String, Value>,
    pub event_statistics: HashMap<String, Value>,
}

impl Channel {
    pub fn new(
        channel_id: String,
        organizations: Vec<Organization>,
        policy_config: HashMap<String, Value>,
    ) -> Self {
        let mut org_map = HashMap::new();
        for org in organizations {
            org_map.insert(org.org_id.clone(), org);
        }

        let policy = ChannelPolicy::new(&policy_config);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let mut config = HashMap::new();
        config.insert(
            "block_size".to_string(),
            policy_config
                .get("block_size")
                .cloned()
                .unwrap_or(json!(500)),
        );
        config.insert(
            "batch_timeout".to_string(),
            policy_config
                .get("batch_timeout")
                .cloned()
                .unwrap_or(json!(2.0)),
        );
        config.insert(
            "max_message_size".to_string(),
            policy_config
                .get("max_message_size")
                .cloned()
                .unwrap_or(json!(1048576)),
        );

        let mut stats = HashMap::new();
        stats.insert("total_events".to_string(), json!(0));
        stats.insert("events_by_type".to_string(), json!({}));
        let mut events_by_org = HashMap::new();
        for org_id in org_map.keys() {
            events_by_org.insert(org_id.clone(), 0);
        }
        stats.insert("events_by_org".to_string(), json!(events_by_org));

        Self {
            channel_id,
            organizations: org_map,
            policy,
            private_collections: HashMap::new(),
            ledger: ChannelLedger::new(),
            status: ChannelStatus::Active,
            created_at: now,
            last_activity: now,
            configuration: config,
            event_statistics: stats,
        }
    }

    // Helper to get organization IDs (for compatibility)
    pub fn get_org_ids(&self) -> Vec<String> {
        self.organizations.keys().cloned().collect()
    }

    pub fn is_member(&self, org_id: &str) -> bool {
        self.organizations.contains_key(org_id)
    }

    pub fn add_organization(
        &mut self,
        organization: Organization,
        endorsements: Vec<String>,
    ) -> bool {
        // Check if endorsements meet policy requirements
        if !self
            .policy
            .evaluate_endorsement(&endorsements, self.organizations.len())
        {
            return false;
        }

        // Verify endorsements are from current channel members
        let valid_endorsements: Vec<&String> = endorsements
            .iter()
            .filter(|e| self.organizations.contains_key(*e))
            .collect();

        if valid_endorsements.len() != endorsements.len() {
            return false;
        }

        // Add organization
        let org_id = organization.org_id.clone();
        let org_name = organization.name.clone();
        self.organizations.insert(org_id.clone(), organization);

        if let Some(events_by_org) = self
            .event_statistics
            .get_mut("events_by_org")
            .and_then(|v| v.as_object_mut())
        {
            events_by_org.insert(org_id.clone(), json!(0));
        }

        // Log channel modification event
        self._log_channel_event(
            "organization_added",
            json!({
                "org_id": org_id,
                "org_name": org_name,
                "endorsed_by": valid_endorsements
            }),
        );

        true
    }

    pub fn remove_organization(&mut self, org_id: &str, endorsements: Vec<String>) -> bool {
        if !self.organizations.contains_key(org_id) {
            return false;
        }

        let remaining_orgs = self.organizations.len() - 1;
        if !self
            .policy
            .evaluate_endorsement(&endorsements, remaining_orgs)
        {
            return false;
        }

        let org = self.organizations.remove(org_id).unwrap();

        // Remove from private collections
        for collection in self.private_collections.values_mut() {
            collection.remove_organization(org_id);
        }

        self._log_channel_event(
            "organization_removed",
            json!({
                "org_id": org_id,
                "org_name": org.name,
                "endorsed_by": endorsements
            }),
        );

        true
    }

    pub fn create_private_collection(
        &mut self,
        name: String,
        member_org_ids: Vec<String>,
        config: HashMap<String, Value>,
    ) -> bool {
        for org_id in &member_org_ids {
            if !self.organizations.contains_key(org_id) {
                return false;
            }
        }

        let collection = PrivateCollection::new(&name, member_org_ids.clone(), config.clone());
        self.private_collections.insert(name.clone(), collection);

        self._log_channel_event(
            "private_collection_created",
            json!({
                "collection_name": name,
                "members": member_org_ids,
                "config": config
            }),
        );

        true
    }

    pub fn submit_event(&mut self, event: Value, submitter_org_id: &str) -> bool {
        let submitter_org = match self.organizations.get(submitter_org_id) {
            Some(org) => org,
            None => return false,
        };

        if !self.policy.evaluate_write_access(submitter_org) {
            return false;
        }

        // Add channel and organization metadata
        let mut enriched_event = event.clone();
        if let Some(obj) = enriched_event.as_object_mut() {
            obj.insert("channel_id".to_string(), json!(self.channel_id));
            obj.insert("submitter_org".to_string(), json!(submitter_org_id));
            obj.insert(
                "timestamp".to_string(),
                json!(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()),
            );
        }

        self.ledger.add_event(enriched_event.clone());

        // Update statistics
        let total = self
            .event_statistics
            .get("total_events")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        self.event_statistics
            .insert("total_events".to_string(), json!(total + 1));

        if let Some(events_by_org) = self
            .event_statistics
            .get_mut("events_by_org")
            .and_then(|v| v.as_object_mut())
        {
            let count = events_by_org
                .get(submitter_org_id)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            events_by_org.insert(submitter_org_id.to_string(), json!(count + 1));
        }

        let event_type = enriched_event
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if let Some(events_by_type) = self
            .event_statistics
            .get_mut("events_by_type")
            .and_then(|v| v.as_object_mut())
        {
            let count = events_by_type
                .get(event_type)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            events_by_type.insert(event_type.to_string(), json!(count + 1));
        }

        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        true
    }

    pub fn query_events(
        &self,
        query_params: &HashMap<String, Value>,
        requester_org_id: &str,
    ) -> Option<Vec<Value>> {
        let requester_org = self.organizations.get(requester_org_id)?;

        if !self.policy.evaluate_read_access(requester_org) {
            return None;
        }

        let filter = |event: &Value| -> bool {
            for (key, value) in query_params {
                if key == "event_type" {
                    if event.get("event") != Some(value) {
                        return false;
                    }
                } else if key == "entity_id" {
                    if event.get("entity_id") != Some(value) {
                        return false;
                    }
                } else if key == "start_time" {
                    if let (Some(ts), Some(val)) = (
                        event.get("timestamp").and_then(|v| v.as_f64()),
                        value.as_f64(),
                    ) {
                        if ts < val {
                            return false;
                        }
                    }
                } else if key == "end_time" {
                    if let (Some(ts), Some(val)) = (
                        event.get("timestamp").and_then(|v| v.as_f64()),
                        value.as_f64(),
                    ) {
                        if ts > val {
                            return false;
                        }
                    }
                } else if key == "limit" {
                    continue;
                } else if key.starts_with("details.") {
                    let detail_key = &key[8..];
                    let details = event.get("details");
                    let actual_val = details.and_then(|d| d.get(detail_key));

                    // Simplified comparison for now (string/number exact match)
                    if actual_val != Some(value) {
                        return false;
                    }
                }
            }
            true
        };

        let mut events = self.ledger.get_events_by_filter(filter);

        if let Some(limit) = query_params.get("limit").and_then(|v| v.as_u64()) {
            if events.len() > limit as usize {
                events.truncate(limit as usize);
            }
        }

        Some(events)
    }

    pub fn finalize_block(&mut self) -> Option<Block> {
        self.ledger.finalize_block()
    }

    pub fn get_channel_info(&self) -> Value {
        json!({
            "channel_id": self.channel_id,
            "status": self.status.as_str(),
            "organizations": self.organizations.keys().collect::<Vec<&String>>(),
            "private_collections": self.private_collections.keys().collect::<Vec<&String>>(),
            "created_at": self.created_at,
            "last_activity": self.last_activity,
            "ledger_height": self.ledger.height,
            "configuration": self.configuration,
            "statistics": self.event_statistics
        })
    }

    pub fn get_organization_info(&self, org_id: &str) -> Option<Value> {
        let org = self.organizations.get(org_id)?;
        let events_submitted = self
            .event_statistics
            .get("events_by_org")
            .and_then(|v| v.get(org_id))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Some(json!({
            "org_id": org.org_id,
            "name": org.name,
            "msp_id": org.msp_id,
            "roles": org.roles,
            "events_submitted": events_submitted
        }))
    }

    pub fn update_channel_policy(
        &mut self,
        new_policy_config: HashMap<String, Value>,
        endorsements: Vec<String>,
    ) -> bool {
        if !self
            .policy
            .evaluate_endorsement(&endorsements, self.organizations.len())
        {
            return false;
        }

        let old_policy = json!({
            "read": self.policy.read_policy,
            "write": self.policy.write_policy,
            "endorsement": self.policy.endorsement_policy,
            "admin": self.policy.admin_policy
        });

        self.policy = ChannelPolicy::new(&new_policy_config);

        self._log_channel_event(
            "policy_updated",
            json!({
                "old_policy": old_policy,
                "new_policy": new_policy_config,
                "endorsed_by": endorsements
            }),
        );

        true
    }

    pub fn suspend_channel(&mut self, reason: String, endorsements: Vec<String>) -> bool {
        if !self
            .policy
            .evaluate_endorsement(&endorsements, self.organizations.len())
        {
            return false;
        }

        self.status = ChannelStatus::Suspended;

        self._log_channel_event(
            "channel_suspended",
            json!({
                "reason": reason,
                "endorsed_by": endorsements
            }),
        );

        true
    }

    pub fn resume_channel(&mut self, endorsements: Vec<String>) -> bool {
        if !self
            .policy
            .evaluate_endorsement(&endorsements, self.organizations.len())
        {
            return false;
        }

        self.status = ChannelStatus::Active;

        self._log_channel_event(
            "channel_resumed",
            json!({
                "endorsed_by": endorsements
            }),
        );

        true
    }

    fn _log_channel_event(&mut self, event_type: &str, details: Value) {
        let event = json!({
            "event": "channel_management",
            "event_type": event_type,
            "channel_id": self.channel_id,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64(),
            "details": details
        });

        self.ledger.add_event(event);
    }
}

/// Channel-based Data Isolation for HieraChain Framework
///
/// This module implements secure data channels that provide complete isolation between
/// organizations in enterprise blockchain applications. Each channel operates as a
/// completely isolated data space with its own governance policies and access controls.

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

impl std::fmt::Display for ChannelStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelStatus::Active => write!(f, "active"),
            ChannelStatus::Suspended => write!(f, "suspended"),
            ChannelStatus::Closed => write!(f, "closed"),
            ChannelStatus::Maintenance => write!(f, "maintenance"),
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
    pub certificates: HashMap<String, String>,
    pub roles: HashSet<String>,
}

impl Organization {
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }
}

/// Channel access and endorsement policies
#[derive(Debug, Clone)]
pub struct ChannelPolicy {
    pub read_policy: String,
    pub write_policy: String,
    pub endorsement_policy: String,
    pub admin_policy: String,
    pub lifecycle_endorsement: String,
    pub custom_policies: HashMap<String, HashMap<String, Vec<String>>>,
}

impl ChannelPolicy {
    pub fn new(policy_config: &HashMap<String, String>) -> Self {
        ChannelPolicy {
            read_policy: policy_config.get("read").cloned().unwrap_or_else(|| "MEMBER".to_string()),
            write_policy: policy_config.get("write").cloned().unwrap_or_else(|| "ADMIN".to_string()),
            endorsement_policy: policy_config.get("endorsement").cloned().unwrap_or_else(|| "MAJORITY".to_string()),
            admin_policy: policy_config.get("admin").cloned().unwrap_or_else(|| "UNANIMOUS".to_string()),
            lifecycle_endorsement: policy_config.get("lifecycle_endorsement").cloned().unwrap_or_else(|| "MAJORITY".to_string()),
            custom_policies: HashMap::new(),
        }
    }

    pub fn evaluate_read_access(&self, organization: &Organization) -> bool {
        self.evaluate_policy(&self.read_policy, organization)
    }

    pub fn evaluate_write_access(&self, organization: &Organization) -> bool {
        self.evaluate_policy(&self.write_policy, organization)
    }

    pub fn evaluate_endorsement(&self, endorsements: &[String], total_orgs: usize) -> bool {
        match self.endorsement_policy.as_str() {
            "MAJORITY" => endorsements.len() > total_orgs / 2,
            "UNANIMOUS" => endorsements.len() == total_orgs,
            "ANY" => !endorsements.is_empty(),
            _ => endorsements.len() >= 1,
        }
    }

    fn evaluate_policy(&self, policy: &str, organization: &Organization) -> bool {
        match policy {
            "MEMBER" => true,
            "ADMIN" => organization.has_role("admin"),
            "OPERATOR" => organization.has_role("operator") || organization.has_role("admin"),
            _ => false,
        }
    }
}

/// Channel-specific ledger for storing channel events
#[derive(Debug, Clone)]
pub struct ChannelLedger {
    pub blocks: Vec<HashMap<String, serde_json::Value>>,
    pub current_block_events: Vec<HashMap<String, String>>,
    pub height: u64,
    pub last_block_hash: String,
}

impl ChannelLedger {
    pub fn new() -> Self {
        ChannelLedger {
            blocks: Vec::new(),
            current_block_events: Vec::new(),
            height: 0,
            last_block_hash: "0".to_string(),
        }
    }

    pub fn add_event(&mut self, mut event: HashMap<String, String>) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64().to_string())
            .unwrap_or_default();
        event.insert("timestamp".to_string(), timestamp);
        event.insert("channel_event".to_string(), "true".to_string());
        self.current_block_events.push(event);
    }

    pub fn finalize_block(&mut self) -> Option<HashMap<String, serde_json::Value>> {
        if self.current_block_events.is_empty() {
            return None;
        }

        let hash = self.calculate_block_hash(&self.current_block_events);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut block = HashMap::new();
        block.insert("height".to_string(), serde_json::Value::Number(self.height.into()));
        block.insert(
            "events".to_string(),
            serde_json::Value::Array(
                self.current_block_events
                    .iter()
                    .map(|e| {
                        let mut json_obj = serde_json::Map::new();
                        for (k, v) in e {
                            json_obj.insert(k.clone(), serde_json::Value::String(v.clone()));
                        }
                        serde_json::Value::Object(json_obj)
                    })
                    .collect(),
            ),
        );
        block.insert("timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(timestamp).unwrap()));
        block.insert("previous_hash".to_string(), serde_json::Value::String(self.last_block_hash.clone()));
        block.insert("hash".to_string(), serde_json::Value::String(hash.clone()));

        self.blocks.push(block.clone());
        self.height += 1;
        self.last_block_hash = hash;
        self.current_block_events.clear();

        Some(block)
    }

    fn calculate_block_hash(&self, events: &[HashMap<String, String>]) -> String {
        let json_str = serde_json::to_string(events).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(json_str.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }
}

impl Default for ChannelLedger {
    fn default() -> Self {
        Self::new()
    }
}

/// Secure data channel providing complete isolation between organizations
pub struct Channel {
    pub channel_id: String,
    pub organizations: HashMap<String, Organization>,
    pub policy: ChannelPolicy,
    pub ledger: ChannelLedger,
    pub status: ChannelStatus,
    pub created_at: f64,
    pub last_activity: f64,
    pub configuration: HashMap<String, u64>,
    pub event_statistics: HashMap<String, serde_json::Value>,
}

impl Channel {
    /// Initialize a new channel
    pub fn new(
        channel_id: &str,
        organizations: Vec<Organization>,
        policy_config: &HashMap<String, String>,
    ) -> Self {
        let mut orgs_map = HashMap::new();
        let mut event_stats_by_org = HashMap::new();

        for org in organizations {
            event_stats_by_org.insert(org.org_id.clone(), serde_json::Value::Number(0.into()));
            orgs_map.insert(org.org_id.clone(), org);
        }

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut configuration = HashMap::new();
        configuration.insert("block_size".to_string(), 500);
        configuration.insert("batch_timeout".to_string(), 2);
        configuration.insert("max_message_size".to_string(), 1048576);

        let mut event_statistics = HashMap::new();
        event_statistics.insert("total_events".to_string(), serde_json::Value::Number(0.into()));
        event_statistics.insert("events_by_type".to_string(), serde_json::Value::Object(serde_json::Map::new()));
        event_statistics.insert("events_by_org".to_string(), serde_json::json!(event_stats_by_org));

        Channel {
            channel_id: channel_id.to_string(),
            organizations: orgs_map,
            policy: ChannelPolicy::new(policy_config),
            ledger: ChannelLedger::new(),
            status: ChannelStatus::Active,
            created_at,
            last_activity: created_at,
            configuration,
            event_statistics,
        }
    }

    /// Add a new organization to the channel
    pub fn add_organization(&mut self, organization: Organization, endorsements: &[String]) -> bool {
        if !self.policy.evaluate_endorsement(endorsements, self.organizations.len()) {
            return false;
        }

        let valid_endorsements: Vec<String> = endorsements
            .iter()
            .filter(|e| self.organizations.contains_key(*e))
            .cloned()
            .collect();

        if valid_endorsements.len() != endorsements.len() {
            return false;
        }

        self.organizations.insert(organization.org_id.clone(), organization);
        true
    }

    /// Remove an organization from the channel
    pub fn remove_organization(&mut self, org_id: &str, endorsements: &[String]) -> bool {
        if !self.organizations.contains_key(org_id) {
            return false;
        }

        let remaining_orgs = self.organizations.len() - 1;
        if !self.policy.evaluate_endorsement(endorsements, remaining_orgs) {
            return false;
        }

        self.organizations.remove(org_id);
        true
    }

    /// Submit an event to the channel
    pub fn submit_event(&mut self, mut event: HashMap<String, String>, submitter_org_id: &str) -> bool {
        if !self.organizations.contains_key(submitter_org_id) {
            return false;
        }

        let submitter_org = &self.organizations[submitter_org_id];

        if !self.policy.evaluate_write_access(submitter_org) {
            return false;
        }

        event.insert("channel_id".to_string(), self.channel_id.clone());
        event.insert("submitter_org".to_string(), submitter_org_id.to_string());

        self.ledger.add_event(event.clone());

        // Update statistics
        if let Some(total) = self.event_statistics.get_mut("total_events") {
            if let serde_json::Value::Number(n) = total {
                *total = serde_json::Value::Number((n.as_u64().unwrap_or(0) + 1).into());
            }
        }

        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        true
    }

    /// Get channel information
    pub fn get_channel_info(&self) -> HashMap<String, serde_json::Value> {
        let mut info = HashMap::new();
        info.insert("channel_id".to_string(), serde_json::Value::String(self.channel_id.clone()));
        info.insert("status".to_string(), serde_json::Value::String(self.status.to_string()));
        info.insert(
            "organizations".to_string(),
            serde_json::Value::Array(
                self.organizations
                    .keys()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .collect(),
            ),
        );
        info.insert("ledger_height".to_string(), serde_json::Value::Number(self.ledger.height.into()));
        info.insert("created_at".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(self.created_at).unwrap()));

        info
    }

    /// Finalize current block in the channel ledger
    pub fn finalize_block(&mut self) -> Option<HashMap<String, serde_json::Value>> {
        self.ledger.finalize_block()
    }

    /// Update channel policy
    pub fn update_channel_policy(&mut self, new_policy_config: &HashMap<String, String>, endorsements: &[String]) -> bool {
        if !self.policy.evaluate_endorsement(endorsements, self.organizations.len()) {
            return false;
        }

        self.policy = ChannelPolicy::new(new_policy_config);
        true
    }

    /// Suspend channel operations
    pub fn suspend_channel(&mut self, _reason: &str, endorsements: &[String]) -> bool {
        if !self.policy.evaluate_endorsement(endorsements, self.organizations.len()) {
            return false;
        }

        self.status = ChannelStatus::Suspended;
        true
    }

    /// Resume channel operations
    pub fn resume_channel(&mut self, endorsements: &[String]) -> bool {
        if !self.policy.evaluate_endorsement(endorsements, self.organizations.len()) {
            return false;
        }

        self.status = ChannelStatus::Active;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_creation() {
        let mut orgs = vec![];
        let mut roles = HashSet::new();
        roles.insert("member".to_string());

        let org = Organization {
            org_id: "org1".to_string(),
            name: "Organization 1".to_string(),
            msp_id: "org1-msp".to_string(),
            endpoints: vec![],
            certificates: HashMap::new(),
            roles,
        };
        orgs.push(org);

        let mut policy_config = HashMap::new();
        policy_config.insert("read".to_string(), "MEMBER".to_string());

        let channel = Channel::new("channel1", orgs, &policy_config);
        assert_eq!(channel.channel_id, "channel1");
    }

    #[test]
    fn test_event_submission() {
        let mut orgs = vec![];
        let mut roles = HashSet::new();
        roles.insert("admin".to_string());

        let org = Organization {
            org_id: "org1".to_string(),
            name: "Organization 1".to_string(),
            msp_id: "org1-msp".to_string(),
            endpoints: vec![],
            certificates: HashMap::new(),
            roles,
        };
        orgs.push(org);

        let mut policy_config = HashMap::new();
        policy_config.insert("write".to_string(), "ADMIN".to_string());

        let mut channel = Channel::new("channel1", orgs, &policy_config);

        let mut event = HashMap::new();
        event.insert("type".to_string(), "test".to_string());

        let result = channel.submit_event(event, "org1");
        assert!(result);
    }
}

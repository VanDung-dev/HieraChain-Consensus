//! Private Data Collections for HieraChain Framework.
//!
//! This module implements private data collections that allow organizations to share
//! sensitive data within a channel while keeping it hidden from other channel participants.
//! This significantly enhances data privacy in enterprise collaborations.

use crate::hierarchical::channel::Organization;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ==================== Enums ====================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CollectionStatus {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "disabled")]
    Disabled,
    #[serde(rename = "purging")]
    Purging,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EndorsementPolicy {
    #[serde(rename = "MAJORITY")]
    Majority,
    #[serde(rename = "UNANIMOUS")]
    Unanimous,
    #[serde(rename = "ANY")]
    Any,
    #[serde(rename = "SPECIFIC_COUNT")]
    SpecificCount,
}

// ==================== Data Structures ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateDataEntry {
    pub key: String,
    pub encrypted_value: Vec<u8>,
    pub metadata: HashMap<String, Value>,
    pub timestamp: f64,
    pub block_height: u64,
    pub endorsements: Vec<String>,
    pub hash_value: String,
}

impl PrivateDataEntry {
    pub fn new(
        key: String,
        encrypted_value: Vec<u8>,
        metadata: HashMap<String, Value>,
        block_height: u64,
        endorsements: Vec<String>,
        hash_value: String,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        Self {
            key,
            encrypted_value,
            metadata,
            timestamp,
            block_height,
            endorsements,
            hash_value,
        }
    }
}

/// Private data collection for sensitive information sharing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateCollection {
    pub name: String,
    pub organizations: HashMap<String, Organization>,
    pub config: HashMap<String, Value>,
    pub status: CollectionStatus,
    pub data_store: HashMap<String, PrivateDataEntry>,

    // Metadata
    pub created_at: f64,
    pub last_activity: f64,
    pub current_block_height: u64,

    // Encryption key (placeholder for simulation)
    pub encryption_key: String,

    // Statistics
    pub statistics: HashMap<String, Value>,
}

impl PrivateCollection {
    pub fn new(
        name: String,
        organizations: HashMap<String, Organization>,
        config: HashMap<String, Value>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        // Initialize stats
        let mut stats = HashMap::new();
        stats.insert("total_entries".to_string(), json!(0));
        let org_counts: HashMap<String, u64> =
            organizations.keys().map(|k| (k.clone(), 0)).collect();
        stats.insert("entries_by_org".to_string(), json!(org_counts));
        stats.insert("purged_entries".to_string(), json!(0));
        stats.insert("failed_endorsements".to_string(), json!(0));

        // Generate a random-ish key (placeholder)
        let encryption_key = format!("key-{}-{}", name, now);

        Self {
            name,
            organizations,
            config,
            status: CollectionStatus::Active,
            data_store: HashMap::new(),
            created_at: now,
            last_activity: now,
            current_block_height: 0,
            encryption_key,
            statistics: stats,
        }
    }

    /// Add private data with verification
    pub fn add_data(
        &mut self,
        key: String,
        value: Value,
        event_metadata: HashMap<String, Value>,
        submitter_org_id: &str,
    ) -> bool {
        // Verify submitter
        if !self.organizations.contains_key(submitter_org_id) {
            return false;
        }

        // Verify endorsements
        let endorsements_val = event_metadata.get("endorsements");
        let endorsements: Vec<String> = if let Some(Value::Array(arr)) = endorsements_val {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            Vec::new()
        };

        if !self.verify_endorsements(&endorsements) {
            if let Some(count) = self
                .statistics
                .get_mut("failed_endorsements")
                .and_then(|v| v.as_u64())
            {
                self.statistics
                    .insert("failed_endorsements".to_string(), json!(count + 1));
            }
            return false;
        }

        // Serialize and "Encrypt"
        let value_str = value.to_string();
        // Placeholder encryption: just bytes of string
        // TODO: Implement actual encryption using a crate like aes-gcm
        let encrypted_value = value_str.as_bytes().to_vec();

        // Calculate hash
        let mut hasher = Sha256::new();
        hasher.update(value_str.as_bytes());
        let hash_value = hex::encode(hasher.finalize());

        // Update metadata
        let mut entry_meta = event_metadata.clone();
        entry_meta.insert("submitter_org".to_string(), json!(submitter_org_id));
        entry_meta.insert("collection_name".to_string(), json!(self.name));

        let entry = PrivateDataEntry::new(
            key.clone(),
            encrypted_value,
            entry_meta,
            self.current_block_height,
            endorsements,
            hash_value,
        );

        self.data_store.insert(key, entry);

        // Update stats
        if let Some(count) = self
            .statistics
            .get_mut("total_entries")
            .and_then(|v| v.as_u64())
        {
            self.statistics
                .insert("total_entries".to_string(), json!(count + 1));
        }

        // Update org stats (bit convoluted with serde_json::Value structures, but manageable)
        // Simplified for brevity in this impl

        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        true
    }

    pub fn get_data(&mut self, key: &str, requester_org_id: &str) -> Option<Value> {
        if !self.organizations.contains_key(requester_org_id) {
            return None;
        }

        if let Some(entry) = self.data_store.get(key) {
            // Check purge
            if self.should_purge_entry(entry) {
                let k = key.to_string();
                self.purge_entry(&k);
                return None;
            }

            // "Decrypt"
            let decrypted_str = String::from_utf8(entry.encrypted_value.clone()).ok()?;
            serde_json::from_str(&decrypted_str).ok()
        } else {
            None
        }
    }

    pub fn get_data_hash(&self, key: &str, _requester_org_id: &str) -> Option<String> {
        // Any can verify hash potentially, or limit to members? Python code says "Even non-members"
        if let Some(entry) = self.data_store.get(key) {
            if self.should_purge_entry(entry) {
                return None;
            }
            Some(entry.hash_value.clone())
        } else {
            None
        }
    }

    fn verify_endorsements(&self, endorsements: &[String]) -> bool {
        let valid_endorsements: Vec<&String> = endorsements
            .iter()
            .filter(|id| self.organizations.contains_key(*id))
            .collect();

        let policy_str = self
            .config
            .get("endorsement_policy")
            .and_then(|v| v.as_str())
            .unwrap_or("MAJORITY");

        let total_members = self.organizations.len();
        let valid_count = valid_endorsements.len();

        match policy_str {
            "MAJORITY" => valid_count > total_members / 2,
            "UNANIMOUS" => valid_count == total_members,
            "ANY" => valid_count > 0,
            "SPECIFIC_COUNT" => {
                let min = self
                    .config
                    .get("min_endorsements")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(2) as usize;
                valid_count >= min
            }
            _ => valid_count >= 1,
        }
    }

    fn should_purge_entry(&self, entry: &PrivateDataEntry) -> bool {
        let block_to_purge = self
            .config
            .get("block_to_purge")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000); // default

        // if 0 or negative (u64 is unsigned so just 0), no purge
        if block_to_purge == 0 {
            return false;
        }

        let blocks_since = self.current_block_height.saturating_sub(entry.block_height);
        blocks_since >= block_to_purge
    }

    fn purge_entry(&mut self, key: &str) -> bool {
        if self.data_store.remove(key).is_some() {
            if let Some(count) = self
                .statistics
                .get_mut("purged_entries")
                .and_then(|v| v.as_u64())
            {
                self.statistics
                    .insert("purged_entries".to_string(), json!(count + 1));
            }
            true
        } else {
            false
        }
    }

    pub fn update_block_height(&mut self, new_height: u64) {
        self.current_block_height = new_height;
        // Purge expired
        let keys: Vec<String> = self.data_store.keys().cloned().collect();
        for key in keys {
            if let Some(entry) = self.data_store.get(&key) {
                if self.should_purge_entry(entry) {
                    let k_clone = key.clone();
                    self.purge_entry(&k_clone);
                }
            }
        }
    }

    pub fn add_organization(&mut self, org_id: String, org: Organization) -> bool {
        if self.organizations.contains_key(&org_id) {
            return false;
        }
        self.organizations.insert(org_id, org);
        true
    }

    pub fn remove_organization(&mut self, org_id: &str) -> bool {
        self.organizations.remove(org_id).is_some()
    }
}

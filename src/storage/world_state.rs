/// World State Management Module
///
/// This module implements a simplified world state mechanism adapted for the hierarchical
/// blockchain structure. The world state represents the current values of all ledger states,
/// enabling efficient read/write operations without traversing the entire blockchain.
///
/// The world state is updated through events processed from blocks, maintaining entity states
/// with efficient indexing for common query patterns.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Simple in-memory storage backend
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: HashMap<String, HashMap<String, String>>,
    #[allow(dead_code)]
    indexes: HashMap<String, HashMap<String, Vec<String>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            data: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, _index_name: &str) {
        // Index already created on demand when storing
    }

    pub fn set(&mut self, key: &str, value: HashMap<String, String>) {
        self.data.insert(key.to_string(), value);
    }

    pub fn get(&self, key: &str) -> Option<HashMap<String, String>> {
        self.data.get(key).cloned()
    }

    pub fn query_by_index(&self, _index_name: &str, _value: &str) -> Vec<String> {
        Vec::new()
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Entity state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityState {
    pub entity_id: String,
    pub created_at: f64,
    pub last_updated: f64,
    pub status: String,
    pub data: HashMap<String, String>,
}

/// Event representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event: String,
    pub entity_id: String,
    pub timestamp: f64,
    pub updates: HashMap<String, String>,
    pub new_status: Option<String>,
}

/// Block representation
#[derive(Debug, Clone)]
pub struct Block {
    pub events: Vec<Event>,
    pub height: u64,
    pub timestamp: f64,
}

/// Simplified World State mechanism for HieraChain
pub struct WorldState {
    pub chain_name: String,
    pub storage: MemoryStorage,
    pub state_cache: HashMap<String, HashMap<String, String>>,
}

impl WorldState {
    /// Initialize world state with optional storage backend
    pub fn new(chain_name: &str) -> Self {
        let mut storage = MemoryStorage::new();
        storage.create_index("entity_id");
        storage.create_index("timestamp");

        WorldState {
            chain_name: chain_name.to_string(),
            storage,
            state_cache: HashMap::new(),
        }
    }

    /// Initialize with custom storage backend
    pub fn with_storage(chain_name: &str, storage: MemoryStorage) -> Self {
        let mut storage = storage;
        storage.create_index("entity_id");
        storage.create_index("timestamp");

        WorldState {
            chain_name: chain_name.to_string(),
            storage,
            state_cache: HashMap::new(),
        }
    }

    /// Update world state from new block
    pub fn update_from_block(&mut self, block: &Block) {
        for event in &block.events {
            if !event.entity_id.is_empty() {
                let entity_key = format!("{}:{}", self.chain_name, event.entity_id);
                let mut current_state = self.storage.get(&entity_key).unwrap_or_default();

                // Update state based on event type
                match event.event.as_str() {
                    "creation" => {
                        current_state.insert("created_at".to_string(), event.timestamp.to_string());
                        current_state.insert("status".to_string(), "active".to_string());
                    }
                    "update" => {
                        for (k, v) in &event.updates {
                            current_state.insert(k.clone(), v.clone());
                        }
                    }
                    "status_change" => {
                        if let Some(new_status) = &event.new_status {
                            current_state.insert("status".to_string(), new_status.clone());
                        }
                    }
                    _ => {}
                }

                current_state.insert("last_updated".to_string(), event.timestamp.to_string());
                self.storage.set(&entity_key, current_state.clone());
                self.state_cache.insert(entity_key, current_state);
            }
        }
    }

    /// Get current state of entity
    pub fn get_entity_state(&mut self, entity_id: &str) -> Option<HashMap<String, String>> {
        let entity_key = format!("{}:{}", self.chain_name, entity_id);

        if let Some(cached) = self.state_cache.get(&entity_key) {
            return Some(cached.clone());
        }

        let state = self.storage.get(&entity_key);
        if let Some(ref s) = state {
            self.state_cache.insert(entity_key, s.clone());
        }

        state
    }

    /// Query using index
    pub fn query_by_index(&self, index_name: &str, value: &str) -> Vec<String> {
        self.storage.query_by_index(index_name, value)
    }

    /// Get all entities in world state
    pub fn get_all_entities(&self) -> Vec<String> {
        self.state_cache
            .keys()
            .filter(|k| k.starts_with(&format!("{}:", self.chain_name)))
            .cloned()
            .collect()
    }

    /// Clear cache to save memory
    pub fn clear_cache(&mut self) {
        self.state_cache.clear();
    }

    /// Get cache size
    pub fn get_cache_size(&self) -> usize {
        self.state_cache.len()
    }

    /// Check if entity exists in world state
    pub fn entity_exists(&mut self, entity_id: &str) -> bool {
        self.get_entity_state(entity_id).is_some()
    }

    /// Get entity status
    pub fn get_entity_status(&mut self, entity_id: &str) -> Option<String> {
        self.get_entity_state(entity_id)
            .and_then(|state| state.get("status").cloned())
    }

    /// Count total entities in world state
    pub fn count_entities(&self) -> usize {
        self.state_cache
            .keys()
            .filter(|k| k.starts_with(&format!("{}:", self.chain_name)))
            .count()
    }

    /// Get statistics about world state
    pub fn get_statistics(&self) -> HashMap<String, serde_json::Value> {
        let mut stats = HashMap::new();

        stats.insert(
            "chain_name".to_string(),
            serde_json::Value::String(self.chain_name.clone()),
        );

        stats.insert(
            "entity_count".to_string(),
            serde_json::Value::Number(self.count_entities().into()),
        );

        stats.insert(
            "cache_size".to_string(),
            serde_json::Value::Number(self.get_cache_size().into()),
        );

        let mut status_distribution: HashMap<String, u32> = HashMap::new();
        for state in self.state_cache.values() {
            if let Some(status) = state.get("status") {
                *status_distribution.entry(status.clone()).or_insert(0) += 1;
            }
        }

        let mut status_map = serde_json::Map::new();
        for (status, count) in status_distribution {
            status_map.insert(status, serde_json::Value::Number(count.into()));
        }
        stats.insert("status_distribution".to_string(), serde_json::Value::Object(status_map));

        stats
    }

    /// Bulk update multiple entities
    pub fn bulk_update(&mut self, updates: &[(String, HashMap<String, String>)]) {
        for (entity_id, updates) in updates {
            let entity_key = format!("{}:{}", self.chain_name, entity_id);
            let mut current_state = self.storage.get(&entity_key).unwrap_or_default();

            for (k, v) in updates {
                current_state.insert(k.clone(), v.clone());
            }

            self.storage.set(&entity_key, current_state.clone());
            self.state_cache.insert(entity_key, current_state);
        }
    }

    /// Delete an entity from world state
    pub fn delete_entity(&mut self, entity_id: &str) -> bool {
        let entity_key = format!("{}:{}", self.chain_name, entity_id);
        self.state_cache.remove(&entity_key).is_some()
    }
}


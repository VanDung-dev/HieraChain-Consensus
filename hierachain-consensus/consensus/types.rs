//! The main consensus types used by the blockchain component.
//!
//! This module defines common data structures for ordering nodes, pending events,
//! and status enums used throughout the ordering/consensus subsystem.
//! It provides small helpers (like `current_timestamp`) and lightweight
//! serialization support via `serde` for interoperability (JSON).

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Helper to get current timestamp
pub fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderingStatus {
    Active,
    Maintenance,
    Stopped,
    Error,
    Lockdown,
}

impl ToString for OrderingStatus {
    fn to_string(&self) -> String {
        match self {
            OrderingStatus::Active => "active".to_string(),
            OrderingStatus::Maintenance => "maintenance".to_string(),
            OrderingStatus::Stopped => "stopped".to_string(),
            OrderingStatus::Error => "error".to_string(),
            OrderingStatus::Lockdown => "lockdown".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventStatus {
    Pending,
    Processing,
    Ordered,
    Certified,
    Rejected,
}

impl ToString for EventStatus {
    fn to_string(&self) -> String {
        match self {
            EventStatus::Pending => "pending".to_string(),
            EventStatus::Processing => "processing".to_string(),
            EventStatus::Ordered => "ordered".to_string(),
            EventStatus::Certified => "certified".to_string(),
            EventStatus::Rejected => "rejected".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingNode {
    pub node_id: String,
    pub endpoint: String,
    pub is_leader: bool,
    pub weight: f64,
    pub status: OrderingStatus,
    pub last_heartbeat: f64,
}

impl OrderingNode {
    pub fn is_healthy(&self, timeout: f64) -> bool {
        (current_timestamp() - self.last_heartbeat) < timeout
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEvent {
    pub event_id: String,
    pub event_data: Value, // Legacy JSON
    #[serde(skip, default)]
    pub arrow_data: Option<ArrowEventData>, // New Arrow Support
    pub channel_id: String,
    pub submitter_org: String,
    pub received_at: f64,
    pub status: EventStatus,
    pub certification_result: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ArrowEventData {
    pub batch: Arc<RecordBatch>,
    pub schema_digest: String,
}

#[derive(Debug, Clone)]
pub enum EventPayload {
    Json(Value),
    Arrow(ArrowEventData),
}

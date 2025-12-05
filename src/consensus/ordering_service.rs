/// Independent Ordering Service for HieraChain Framework.
///
/// This module implements a decoupled event ordering service that significantly improves
/// scalability and reduces communication bandwidth. The ordering service separates event
/// ordering from consensus validation, enabling enterprise-scale event volumes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::thread;
use sha2::{Sha256, Digest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Ordering service status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderingStatus {
    Active,
    Maintenance,
    Stopped,
    Error,
}

impl ToString for OrderingStatus {
    fn to_string(&self) -> String {
        match self {
            OrderingStatus::Active => "active".to_string(),
            OrderingStatus::Maintenance => "maintenance".to_string(),
            OrderingStatus::Stopped => "stopped".to_string(),
            OrderingStatus::Error => "error".to_string(),
        }
    }
}

/// Event processing status
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

/// Ordering service node configuration
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
    /// Check if node is healthy based on heartbeat
    pub fn is_healthy(&self, timeout: f64) -> bool {
        let current_time = current_timestamp();
        (current_time - self.last_heartbeat) < timeout
    }
}

/// Event waiting to be ordered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEvent {
    pub event_id: String,
    pub event_data: Value,
    pub channel_id: String,
    pub submitter_org: String,
    pub received_at: f64,
    pub status: EventStatus,
    pub certification_result: Option<Value>,
}

impl PendingEvent {
    /// Convert to dictionary/JSON value
    pub fn to_dict(&self) -> Value {
        json!({
            "event_id": self.event_id,
            "event_data": self.event_data,
            "channel_id": self.channel_id,
            "submitter_org": self.submitter_org,
            "received_at": self.received_at,
            "status": self.status.to_string(),
            "certification_result": self.certification_result
        })
    }
}

/// Event certification and validation
pub struct EventCertifier {
    validation_rules: Arc<Mutex<Vec<fn(&Value) -> bool>>>,
    certified_events: Arc<Mutex<HashMap<String, Value>>>,
}

impl EventCertifier {
    /// Create a new event certifier
    pub fn new() -> Self {
        EventCertifier {
            validation_rules: Arc::new(Mutex::new(Vec::new())),
            certified_events: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a validation rule for events
    pub fn add_validation_rule(&self, rule: fn(&Value) -> bool) {
        if let Ok(mut rules) = self.validation_rules.lock() {
            rules.push(rule);
        }
    }

    /// Validate and certify an event
    pub fn validate(&self, event: &PendingEvent) -> Value {
        let mut certification = json!({
            "event_id": event.event_id,
            "certified_at": current_timestamp(),
            "valid": true,
            "validation_errors": [],
            "metadata": {}
        });

        let mut valid = true;
        let mut errors = Vec::new();

        // Apply validation rules
        if let Ok(rules) = self.validation_rules.lock() {
            for rule in rules.iter() {
                if !rule(&event.event_data) {
                    valid = false;
                    errors.push(format!("Validation rule failed"));
                }
            }
        }

        // Basic structural validation
        if !Self::validate_structure(&event.event_data) {
            valid = false;
            errors.push("Invalid event structure".to_string());
        }

        // Check for required fields
        let required_fields = vec!["entity_id", "event", "timestamp"];
        for field in required_fields {
            if event.event_data.get(field).is_none() {
                valid = false;
                errors.push(format!("Missing required field: {}", field));
            }
        }

        certification["valid"] = json!(valid);
        certification["validation_errors"] = json!(errors);

        // Store certification result
        if let Ok(mut certified) = self.certified_events.lock() {
            certified.insert(event.event_id.clone(), certification.clone());
        }

        certification
    }

    /// Validate basic event structure
    fn validate_structure(event_data: &Value) -> bool {
        if !event_data.is_object() {
            return false;
        }

        // Check timestamp is reasonable
        if let Some(timestamp) = event_data.get("timestamp").and_then(|v| v.as_f64()) {
            let current_time = current_timestamp();
            if (timestamp - current_time).abs() > 3600.0 {
                return false;
            }
        } else {
            return false;
        }

        true
    }

    /// Get certification result for an event
    pub fn get_certification(&self, event_id: &str) -> Option<Value> {
        if let Ok(certified) = self.certified_events.lock() {
            certified.get(event_id).cloned()
        } else {
            None
        }
    }
}

impl Default for EventCertifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds blocks from ordered events
pub struct BlockBuilder {
    #[allow(dead_code)]
    config: Value,
    block_size: usize,
    batch_timeout: f64,
    current_batch: Arc<Mutex<Vec<PendingEvent>>>,
    batch_start_time: Arc<Mutex<f64>>,
}

impl BlockBuilder {
    /// Initialize block builder
    pub fn new(config: Value) -> Self {
        let block_size = config.get("block_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as usize;

        let batch_timeout = config.get("batch_timeout")
            .and_then(|v| v.as_f64())
            .unwrap_or(2.0);

        BlockBuilder {
            config,
            block_size,
            batch_timeout,
            current_batch: Arc::new(Mutex::new(Vec::new())),
            batch_start_time: Arc::new(Mutex::new(current_timestamp())),
        }
    }

    /// Add event to current batch
    pub fn add_event(&self, event: PendingEvent) -> Option<Value> {
        if let Ok(mut batch) = self.current_batch.lock() {
            batch.push(event);
            if self.is_batch_ready() {
                return self.create_block();
            }
        }
        None
    }

    /// Force creation of block from current batch
    pub fn force_create_block(&self) -> Option<Value> {
        if let Ok(batch) = self.current_batch.lock() {
            if batch.is_empty() {
                return None;
            }
        }
        self.create_block()
    }

    /// Check if current batch is ready for block creation
    fn is_batch_ready(&self) -> bool {
        if let Ok(batch) = self.current_batch.lock() {
            if batch.len() >= self.block_size {
                return true;
            }
        }

        if let Ok(start_time) = self.batch_start_time.lock() {
            if (current_timestamp() - *start_time) >= self.batch_timeout {
                return true;
            }
        }

        false
    }

    /// Create block from current batch
    fn create_block(&self) -> Option<Value> {
        let mut batch_guard = self.current_batch.lock().ok()?;

        if batch_guard.is_empty() {
            return None;
        }

        let mut block_events = Vec::new();

        for pending_event in batch_guard.iter() {
            let mut event_obj = pending_event.event_data.clone();

            let ordering_metadata = json!({
                "event_id": pending_event.event_id,
                "ordered_at": current_timestamp(),
                "channel_id": pending_event.channel_id,
                "submitter_org": pending_event.submitter_org
            });

            if let Some(obj) = event_obj.as_object_mut() {
                obj.insert("ordering_metadata".to_string(), ordering_metadata);
            }

            block_events.push(event_obj);
        }

        let batch_start_time = if let Ok(start) = self.batch_start_time.lock() {
            *start
        } else {
            current_timestamp()
        };

        let created_at = current_timestamp();
        let batch_duration = created_at - batch_start_time;

        let block = json!({
            "events": block_events,
            "event_count": batch_guard.len(),
            "created_at": created_at,
            "batch_info": {
                "batch_start": batch_start_time,
                "batch_duration": batch_duration,
                "events_processed": batch_guard.len()
            },
            "hash": calculate_block_hash(&block_events)
        });

        batch_guard.clear();
        drop(batch_guard);

        if let Ok(mut start_time) = self.batch_start_time.lock() {
            *start_time = current_timestamp();
        }

        Some(block)
    }
}

/// Statistics for ordering service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub events_received: u64,
    pub events_certified: u64,
    pub events_rejected: u64,
    pub blocks_created: u64,
    pub average_batch_size: f64,
    pub average_processing_time: f64,
}

impl Default for Statistics {
    fn default() -> Self {
        Statistics {
            events_received: 0,
            events_certified: 0,
            events_rejected: 0,
            blocks_created: 0,
            average_batch_size: 0.0,
            average_processing_time: 0.0,
        }
    }
}

/// Decoupled event ordering service for improved scalability
pub struct OrderingService {
    nodes: Arc<Mutex<HashMap<String, OrderingNode>>>,
    config: Value,
    status: Arc<Mutex<OrderingStatus>>,

    // Event processing components
    event_pool: Arc<Mutex<Vec<PendingEvent>>>,
    block_builder: Arc<BlockBuilder>,
    commit_queue: Arc<Mutex<Vec<Value>>>,
    certifier: Arc<EventCertifier>,

    // Processing state
    pending_events: Arc<Mutex<HashMap<String, PendingEvent>>>,
    processed_events: Arc<Mutex<HashMap<String, PendingEvent>>>,
    blocks_created: Arc<Mutex<u64>>,
    events_processed: Arc<Mutex<u64>>,

    // Statistics
    statistics: Arc<Mutex<Statistics>>,

    // Processing control
    should_stop: Arc<Mutex<bool>>,
    processing_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl OrderingService {
    /// Initialize ordering service
    pub fn new(nodes: Vec<OrderingNode>, config: Value) -> Arc<Self> {
        let nodes_map: HashMap<String, OrderingNode> = nodes.into_iter()
            .map(|node| (node.node_id.clone(), node))
            .collect();

        let service = Arc::new(OrderingService {
            nodes: Arc::new(Mutex::new(nodes_map)),
            config: config.clone(),
            status: Arc::new(Mutex::new(OrderingStatus::Active)),
            event_pool: Arc::new(Mutex::new(Vec::new())),
            block_builder: Arc::new(BlockBuilder::new(config)),
            commit_queue: Arc::new(Mutex::new(Vec::new())),
            certifier: Arc::new(EventCertifier::new()),
            pending_events: Arc::new(Mutex::new(HashMap::new())),
            processed_events: Arc::new(Mutex::new(HashMap::new())),
            blocks_created: Arc::new(Mutex::new(0)),
            events_processed: Arc::new(Mutex::new(0)),
            statistics: Arc::new(Mutex::new(Statistics::default())),
            should_stop: Arc::new(Mutex::new(false)),
            processing_thread: Arc::new(Mutex::new(None)),
        });

        // Setup default validation rules
        service.setup_default_validation_rules();

        // Start processing
        service.clone().start();

        service
    }

    /// Receive event from client or application channel
    pub fn receive_event(&self, event_data: Value, channel_id: String, submitter_org: String) -> String {
        let event_id = Self::generate_event_id(&event_data, &channel_id);

        let pending_event = PendingEvent {
            event_id: event_id.clone(),
            event_data,
            channel_id,
            submitter_org,
            received_at: current_timestamp(),
            status: EventStatus::Pending,
            certification_result: None,
        };

        if let Ok(mut pool) = self.event_pool.lock() {
            pool.push(pending_event.clone());
        }

        if let Ok(mut pending) = self.pending_events.lock() {
            pending.insert(event_id.clone(), pending_event);
        }

        if let Ok(mut stats) = self.statistics.lock() {
            stats.events_received += 1;
        }

        event_id
    }

    /// Get status of a specific event
    pub fn get_event_status(&self, event_id: &str) -> Option<Value> {
        // Check pending events
        if let Ok(pending) = self.pending_events.lock() {
            if let Some(event) = pending.get(event_id) {
                return Some(json!({
                    "event_id": event_id,
                    "status": event.status.to_string(),
                    "received_at": event.received_at,
                    "channel_id": event.channel_id,
                    "certification_result": event.certification_result
                }));
            }
        }

        // Check processed events
        if let Ok(processed) = self.processed_events.lock() {
            if let Some(event) = processed.get(event_id) {
                return Some(json!({
                    "event_id": event_id,
                    "status": event.status.to_string(),
                    "processed_at": event.received_at,
                    "channel_id": event.channel_id,
                    "certification_result": event.certification_result
                }));
            }
        }

        None
    }

    /// Get next completed block from the commit queue
    pub fn get_next_block(&self) -> Option<Value> {
        if let Ok(mut queue) = self.commit_queue.lock() {
            if !queue.is_empty() {
                return Some(queue.remove(0));
            }
        }
        None
    }

    /// Get comprehensive service status
    pub fn get_service_status(&self) -> Value {
        let nodes_map = self.nodes.lock().ok();
        let healthy_count = nodes_map.as_ref()
            .map(|n| n.values().filter(|node| node.is_healthy(30.0)).count())
            .unwrap_or(0);

        let total_nodes = nodes_map.as_ref().map(|n| n.len()).unwrap_or(0);

        let leader = nodes_map.as_ref()
            .and_then(|n| n.values().find(|node| node.is_leader).map(|n| n.node_id.clone()));

        let status = self.status.lock().ok().map(|s| s.to_string()).unwrap_or_default();
        let pending_count = self.pending_events.lock().ok().map(|p| p.len()).unwrap_or(0);
        let commit_count = self.commit_queue.lock().ok().map(|q| q.len()).unwrap_or(0);
        let stats = self.statistics.lock().ok().map(|s| s.clone()).unwrap_or_default();

        json!({
            "status": status,
            "nodes": {
                "total": total_nodes,
                "healthy": healthy_count,
                "leader": leader
            },
            "queues": {
                "pending_events": self.event_pool.lock().ok().map(|p| p.len()).unwrap_or(0),
                "commit_queue": commit_count,
                "processing_events": pending_count
            },
            "statistics": stats,
            "configuration": {
                "block_size": self.block_builder.block_size,
                "batch_timeout": self.block_builder.batch_timeout,
                "worker_threads": self.config.get("worker_threads").and_then(|v| v.as_u64()).unwrap_or(4)
            }
        })
    }

    /// Add custom validation rule for events
    pub fn add_validation_rule(&self, rule: fn(&Value) -> bool) {
        self.certifier.add_validation_rule(rule);
    }

    /// Start the ordering service
    pub fn start(self: Arc<Self>) {
        if let Ok(mut should_stop) = self.should_stop.lock() {
            *should_stop = false;
        }

        if let Ok(mut status) = self.status.lock() {
            *status = OrderingStatus::Active;
        }

        let service_clone = self.clone();
        let thread = thread::spawn(move || {
            service_clone.process_events();
        });

        if let Ok(mut processing_thread) = self.processing_thread.lock() {
            *processing_thread = Some(thread);
        }
    }

    /// Stop the ordering service
    pub fn stop(&self) {
        if let Ok(mut should_stop) = self.should_stop.lock() {
            *should_stop = true;
        }

        if let Ok(mut status) = self.status.lock() {
            *status = OrderingStatus::Stopped;
        }
    }

    /// Main event processing loop
    fn process_events(&self) {
        loop {
            if let Ok(should_stop) = self.should_stop.lock() {
                if *should_stop {
                    break;
                }
            }

            // Get event from pool
            let event = if let Ok(mut pool) = self.event_pool.lock() {
                if !pool.is_empty() {
                    Some(pool.remove(0))
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(pending_event) = event {
                self.process_single_event(pending_event);
            } else {
                // Check for timeout-based block creation
                self.check_timeout_block_creation();
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// Process a single event through certification and ordering
    fn process_single_event(&self, mut pending_event: PendingEvent) {
        pending_event.status = EventStatus::Processing;

        let certification_result = self.certifier.validate(&pending_event);
        pending_event.certification_result = Some(certification_result.clone());

        if let Some(valid) = certification_result.get("valid").and_then(|v| v.as_bool()) {
            if valid {
                pending_event.status = EventStatus::Certified;

                if let Ok(mut stats) = self.statistics.lock() {
                    stats.events_certified += 1;
                }

                // Add to block builder
                if let Some(block) = self.block_builder.add_event(pending_event.clone()) {
                    self.commit_block(block);
                }

                // Move to processed events
                if let Ok(mut processed) = self.processed_events.lock() {
                    processed.insert(pending_event.event_id.clone(), pending_event.clone());
                }

                if let Ok(mut pending) = self.pending_events.lock() {
                    pending.remove(&pending_event.event_id);
                }
            } else {
                pending_event.status = EventStatus::Rejected;

                if let Ok(mut stats) = self.statistics.lock() {
                    stats.events_rejected += 1;
                }
            }
        }
    }

    /// Check if a block should be created due to timeout
    fn check_timeout_block_creation(&self) {
        if let Some(block) = self.block_builder.force_create_block() {
            self.commit_block(block);
        }
    }

    /// Commit a completed block to the commit queue
    fn commit_block(&self, mut block: Value) {
        let block_number = if let Ok(mut created) = self.blocks_created.lock() {
            let num = *created;
            *created += 1;
            num
        } else {
            0
        };

        let first_node_id = self.nodes.lock()
            .ok()
            .and_then(|n| n.keys().next().cloned())
            .unwrap_or_else(|| "unknown".to_string());

        if let Some(obj) = block.as_object_mut() {
            obj.insert("block_number".to_string(), json!(block_number));
            obj.insert("ordering_service_id".to_string(), json!(first_node_id));
            obj.insert("committed_at".to_string(), json!(current_timestamp()));
        }

        if let Ok(mut queue) = self.commit_queue.lock() {
            queue.push(block.clone());
        }

        if let Ok(mut stats) = self.statistics.lock() {
            stats.blocks_created += 1;
            if let Some(event_count) = block.get("event_count").and_then(|v| v.as_u64()) {
                stats.average_batch_size = (stats.average_batch_size * (stats.blocks_created as f64 - 1.0) +
                     event_count as f64) / stats.blocks_created as f64;
            }
        }
    }

    /// Generate unique event ID
    fn generate_event_id(event_data: &Value, channel_id: &str) -> String {
        let data = format!(
            "{}:{}:{}",
            channel_id,
            event_data.to_string(),
            current_timestamp()
        );

        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        // Return first 16 characters
        hash.chars().take(16).collect()
    }

    /// Setup default validation rules
    fn setup_default_validation_rules(&self) {
        fn validate_non_empty_entity_id(event_data: &Value) -> bool {
            event_data
                .get("entity_id")
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
        }

        fn validate_event_type(event_data: &Value) -> bool {
            event_data
                .get("event")
                .and_then(|v| v.as_str())
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
        }

        fn validate_timestamp_format(event_data: &Value) -> bool {
            event_data
                .get("timestamp")
                .and_then(|v| v.as_f64())
                .map(|ts| ts > 0.0)
                .unwrap_or(false)
        }

        self.certifier.add_validation_rule(validate_non_empty_entity_id);
        self.certifier.add_validation_rule(validate_event_type);
        self.certifier.add_validation_rule(validate_timestamp_format);
    }

    /// String representation of ordering service
    pub fn to_string(&self) -> String {
        let nodes_count = self.nodes.lock().ok().map(|n| n.len()).unwrap_or(0);
        let status = self.status.lock().ok().map(|s| s.to_string()).unwrap_or_default();
        format!("OrderingService(nodes={}, status={})", nodes_count, status)
    }

    /// Detailed string representation
    pub fn to_repr(&self) -> String {
        let nodes_count = self.nodes.lock().ok().map(|n| n.len()).unwrap_or(0);
        let status = self.status.lock().ok().map(|s| s.to_string()).unwrap_or_default();
        let events = self.events_processed.lock().ok().map(|e| *e).unwrap_or(0);
        format!(
            "OrderingService(nodes={}, status='{}', events_processed={})",
            nodes_count, status, events
        )
    }
}

impl Clone for OrderingService {
    fn clone(&self) -> Self {
        OrderingService {
            nodes: Arc::clone(&self.nodes),
            config: self.config.clone(),
            status: Arc::clone(&self.status),
            event_pool: Arc::clone(&self.event_pool),
            block_builder: Arc::clone(&self.block_builder),
            commit_queue: Arc::clone(&self.commit_queue),
            certifier: Arc::clone(&self.certifier),
            pending_events: Arc::clone(&self.pending_events),
            processed_events: Arc::clone(&self.processed_events),
            blocks_created: Arc::clone(&self.blocks_created),
            events_processed: Arc::clone(&self.events_processed),
            statistics: Arc::clone(&self.statistics),
            should_stop: Arc::clone(&self.should_stop),
            processing_thread: Arc::clone(&self.processing_thread),
        }
    }
}

// ==================== Helper Functions ====================

/// Get current timestamp in seconds since UNIX_EPOCH
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Calculate block hash from events
fn calculate_block_hash(block_events: &[Value]) -> String {
    let data = serde_json::to_string(block_events).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}


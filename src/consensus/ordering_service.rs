/// Independent Ordering Service for HieraChain Framework.
///
/// This module implements a decoupled event ordering service that significantly improves
/// scalability and reduces communication bandwidth. The ordering service separates event
/// ordering from consensus validation, enabling enterprise-scale event volumes.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;
use sha2::{Sha256, Digest};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crossbeam_channel::{select, unbounded, Receiver, Sender};

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
        let block_size = config
            .get("block_size")
            .and_then(|v| v.as_u64())
            .unwrap_or(500) as usize;

        let batch_timeout = config
            .get("batch_timeout")
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
        let should_create_block = {
            let mut batch = self.current_batch.lock().ok()?;
            batch.push(event);
            self.is_batch_ready_with_len(batch.len())
        };

        if should_create_block {
            return self.create_block();
        }
        None
    }

    /// Force creation of block from current batch
    pub fn force_create_block(&self) -> Option<Value> {
        let is_empty = {
            let batch = self.current_batch.lock().ok()?;
            batch.is_empty()
        };

        if is_empty {
            return None;
        }
        self.create_block()
    }

    /// Check if current batch is ready for block creation (without locking)
    fn is_batch_ready_with_len(&self, batch_len: usize) -> bool {
        // Check batch size first (no lock needed, len passed in)
        if batch_len >= self.block_size {
            return true;
        }

        // Check timeout - only need to lock batch_start_time
        if let Ok(start_time) = self.batch_start_time.lock() {
            let elapsed = current_timestamp() - *start_time;
            // Only trigger timeout if we have at least some events
            if batch_len > 0 && elapsed >= self.batch_timeout {
                return true;
            }
        }

        false
    }

    /// Check if there's a batch that should be created due to timeout
    pub fn has_pending_timeout_batch(&self) -> bool {
        let batch_len = if let Ok(batch) = self.current_batch.lock() {
            batch.len()
        } else {
            return false;
        };

        if batch_len == 0 {
            return false;
        }

        if let Ok(start_time) = self.batch_start_time.lock() {
            let elapsed = current_timestamp() - *start_time;
            return elapsed >= self.batch_timeout;
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
    event_sender: Sender<PendingEvent>,
    block_builder: Arc<BlockBuilder>,
    commit_queue: Arc<Mutex<VecDeque<Value>>>,
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

    // We need to keep track of the number of events sent to the channel
    // to properly calculate the number of pending events.
    events_in_channel: Arc<Mutex<usize>>,
}

impl OrderingService {
    /// Initialize ordering service and return it along with the event receiver
    pub fn new(nodes: Vec<OrderingNode>, config: Value) -> (Arc<Self>, Receiver<PendingEvent>) {
        let nodes_map: HashMap<String, OrderingNode> = nodes
            .into_iter()
            .map(|node| (node.node_id.clone(), node))
            .collect();

        let (tx, rx) = unbounded();

        let service = Arc::new(OrderingService {
            nodes: Arc::new(Mutex::new(nodes_map)),
            config: config.clone(),
            status: Arc::new(Mutex::new(OrderingStatus::Active)),
            event_sender: tx,
            block_builder: Arc::new(BlockBuilder::new(config.clone())),
            commit_queue: Arc::new(Mutex::new(VecDeque::new())),
            certifier: Arc::new(EventCertifier::new()),
            pending_events: Arc::new(Mutex::new(HashMap::new())),
            processed_events: Arc::new(Mutex::new(HashMap::new())),
            blocks_created: Arc::new(Mutex::new(0)),
            events_processed: Arc::new(Mutex::new(0)),
            statistics: Arc::new(Mutex::new(Statistics::default())),
            should_stop: Arc::new(Mutex::new(false)),
            processing_thread: Arc::new(Mutex::new(None)),
            events_in_channel: Arc::new(Mutex::new(0)),
        });

        // Setup default validation rules
        service.setup_default_validation_rules();

        // Start processing
        (service, rx)
    }

    /// Receive event from client or application channel
    pub fn receive_event(
        &self,
        event_data: Value,
        channel_id: String,
        submitter_org: String,
    ) -> String {
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

        if let Ok(mut pending) = self.pending_events.lock() {
            pending.insert(event_id.clone(), pending_event.clone());
        }

        if let Ok(mut stats) = self.statistics.lock() {
            stats.events_received += 1;
        }

        if let Ok(mut count) = self.events_in_channel.lock() {
            *count += 1;
        }

        self.event_sender.send(pending_event).unwrap_or_else(|e| {
            eprintln!("Failed to send event to processing channel: {}", e);
            if let Ok(mut count) = self.events_in_channel.lock() {
                *count -= 1;
            }
        });

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
            queue.pop_front()
        } else {
            None
        }
    }

    /// Get comprehensive service status
    pub fn get_service_status(&self) -> Value {
        let nodes_map = self.nodes.lock().ok();
        let healthy_count = nodes_map
            .as_ref()
            .map(|n| n.values().filter(|node| node.is_healthy(30.0)).count())
            .unwrap_or(0);

        let total_nodes = nodes_map.as_ref().map(|n| n.len()).unwrap_or(0);

        let leader = nodes_map.as_ref().and_then(|n| {
            n.values()
                .find(|node| node.is_leader)
                .map(|n| n.node_id.clone())
        });

        let status = self
            .status
            .lock()
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let pending_count = self
            .pending_events
            .lock()
            .ok()
            .map(|p| p.len())
            .unwrap_or(0);
        let commit_count = self.commit_queue.lock().ok().map(|q| q.len()).unwrap_or(0);
        let stats = self
            .statistics
            .lock()
            .ok()
            .map(|s| s.clone())
            .unwrap_or_default();
        let events_in_channel = self.events_in_channel.lock().ok().map(|c| *c).unwrap_or(0);

        json!({
            "status": status,
            "nodes": {
                "total": total_nodes,
                "healthy": healthy_count,
                "leader": leader
            },
            "queues": {
                "pending_events": events_in_channel,
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

    /// Start the ordering service's processing thread
    pub fn start(service: Arc<Self>, receiver: Receiver<PendingEvent>) {
        if let Ok(mut should_stop) = service.should_stop.lock() {
            *should_stop = false;
        }

        if let Ok(mut status) = service.status.lock() {
            *status = OrderingStatus::Active;
        }

        let service_clone = Arc::clone(&service);
        let thread = thread::spawn(move || {
            service_clone.process_events(receiver);
        });

        if let Ok(mut processing_thread) = service.processing_thread.lock() {
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
    fn process_events(&self, receiver: Receiver<PendingEvent>) {
        let timeout = Duration::from_millis(100);
        loop {
            if *self.should_stop.lock().unwrap() {
                break;
            }

            select! {
                recv(receiver) -> msg => {
                    if let Ok(mut count) = self.events_in_channel.lock() {
                        *count -= 1;
                    }
                    match msg {
                        Ok(event) => self.process_single_event(event),
                        Err(_) => {
                            break;
                        }
                    }
                },
                default(timeout) => {
                    self.check_timeout_block_creation();
                }
            }
        }
    }

    /// Process a single event through certification and ordering
    fn process_single_event(&self, mut pending_event: PendingEvent) {
        pending_event.status = EventStatus::Processing;

        let certification_result = self.certifier.validate(&pending_event);
        let is_valid = certification_result
            .get("valid")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        pending_event.certification_result = Some(certification_result);

        if is_valid {
            pending_event.status = EventStatus::Certified;
            let event_id = pending_event.event_id.clone();

            // Add event to block builder first (this is the critical path)
            if let Some(block) = self.block_builder.add_event(pending_event.clone()) {
                self.commit_block(block);
            }

            // Batch lock operations together to reduce contention
            if let Ok(mut stats) = self.statistics.lock() {
                stats.events_certified += 1;
            }

            // Update processed and pending events
            if let Ok(mut processed) = self.processed_events.lock() {
                processed.insert(event_id.clone(), pending_event);
            }

            if let Ok(mut pending) = self.pending_events.lock() {
                pending.remove(&event_id);
            }
        } else {
            pending_event.status = EventStatus::Rejected;

            if let Ok(mut stats) = self.statistics.lock() {
                stats.events_rejected += 1;
            }
        }
    }

    /// Check if a block should be created due to timeout
    fn check_timeout_block_creation(&self) {
        // Quick check first to avoid unnecessary lock acquisitions
        if !self.block_builder.has_pending_timeout_batch() {
            return;
        }

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

        let first_node_id = self
            .nodes
            .lock()
            .ok()
            .and_then(|n| n.keys().next().cloned())
            .unwrap_or_else(|| "unknown".to_string());

        if let Some(obj) = block.as_object_mut() {
            obj.insert("block_number".to_string(), json!(block_number));
            obj.insert("ordering_service_id".to_string(), json!(first_node_id));
            obj.insert("committed_at".to_string(), json!(current_timestamp()));
        }

        if let Ok(mut queue) = self.commit_queue.lock() {
            queue.push_back(block.clone());
        }

        if let Ok(mut stats) = self.statistics.lock() {
            stats.blocks_created += 1;
            if let Some(event_count) = block.get("event_count").and_then(|v| v.as_u64()) {
                stats.average_batch_size = (stats.average_batch_size
                    * (stats.blocks_created as f64 - 1.0)
                    + event_count as f64)
                    / stats.blocks_created as f64;
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

        self.certifier
            .add_validation_rule(validate_non_empty_entity_id);
        self.certifier.add_validation_rule(validate_event_type);
        self.certifier
            .add_validation_rule(validate_timestamp_format);
    }

    /// String representation of ordering service
    pub fn to_string(&self) -> String {
        let nodes_count = self.nodes.lock().ok().map(|n| n.len()).unwrap_or(0);
        let status = self
            .status
            .lock()
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default();
        format!("OrderingService(nodes={}, status={})", nodes_count, status)
    }

    /// Detailed string representation
    pub fn to_repr(&self) -> String {
        let nodes_count = self.nodes.lock().ok().map(|n| n.len()).unwrap_or(0);
        let status = self
            .status
            .lock()
            .ok()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let events = self.events_processed.lock().ok().map(|e| *e).unwrap_or(0);
        format!(
            "OrderingService(nodes={}, status='{}', events_processed={})",
            nodes_count, status, events
        )
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


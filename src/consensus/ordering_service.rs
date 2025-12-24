use crossbeam_channel::{select, unbounded, Receiver, Sender};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration; // Removed generic SystemTime import
use tokio::runtime::Runtime;

// Import from sibling modules
// Import from sibling modules
use crate::consensus::types::{
    current_timestamp, EventPayload, EventStatus, OrderingNode, OrderingStatus, PendingEvent,
};
use crate::core::utils::MerkleTree;
use crate::error_mitigation::journal::TransactionJournal;
use crate::security::security_utils::verify_signature;

/// Event certification and validation
pub struct EventCertifier {
    validation_rules: Arc<Mutex<Vec<fn(&Value) -> bool>>>,
}

impl EventCertifier {
    pub fn new() -> Self {
        EventCertifier {
            validation_rules: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn add_validation_rule(&self, rule: fn(&Value) -> bool) {
        if let Ok(mut rules) = self.validation_rules.lock() {
            rules.push(rule);
        }
    }

    pub fn validate(&self, event: &mut PendingEvent) -> bool {
        let mut valid = true;
        let mut errors = Vec::new();

        // 1. Basic Structure Check
        if !event.event_data.is_object() {
            valid = false;
            errors.push("Invalid event structure".to_string());
        }

        // 2. Crypto Verification (Signature)
        // Check if event has 'signature' and 'sender' (public key)
        if let Some(obj) = event.event_data.as_object() {
            if let (Some(sig), Some(sender), Some(details)) = (
                obj.get("signature").and_then(|v| v.as_str()),
                obj.get("sender").and_then(|v| v.as_str()),
                obj.get("details"),
            ) {
                // We need to reconstruct the message bytes.
                // In Python: msg = details.get("payload")
                // Here we assume details is the payload or contains it.
                // This is a simplified check.
                let msg_str = details
                    .get("payload")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Convert hex to bytes
                if let (Ok(sig_bytes), Ok(pk_bytes)) = (hex::decode(sig), hex::decode(sender)) {
                    match verify_signature(&pk_bytes, msg_str.as_bytes(), &sig_bytes) {
                        Ok(true) => {} // Valid
                        _ => {
                            valid = false;
                            errors.push("Invalid signature".to_string());
                        }
                    }
                }
            }
        }

        // 3. Custom Rules
        if let Ok(rules) = self.validation_rules.lock() {
            for rule in rules.iter() {
                if !rule(&event.event_data) {
                    valid = false;
                    errors.push("Custom validation rule failed".to_string());
                }
            }
        }

        event.certification_result = Some(json!({
            "valid": valid,
            "errors": errors
        }));

        valid
    }
}

pub struct BlockBuilder {
    config: Value,
    block_size: usize,
    batch_timeout: f64,
    current_batch: Arc<Mutex<Vec<PendingEvent>>>,
    batch_start_time: Arc<Mutex<f64>>,
}

impl BlockBuilder {
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

    pub fn add_event(&self, event: PendingEvent) -> Option<Value> {
        let should_create = {
            let mut batch = self.current_batch.lock().unwrap();
            batch.push(event);
            batch.len() >= self.block_size
        };

        if should_create {
            self.create_block()
        } else {
            None
        }
    }

    pub fn check_timeout(&self) -> Option<Value> {
        let (len, elapsed) = {
            let batch = self.current_batch.lock().unwrap();
            let start = self.batch_start_time.lock().unwrap();
            (batch.len(), current_timestamp() - *start)
        };

        if len > 0 && elapsed > self.batch_timeout {
            self.create_block()
        } else {
            None
        }
    }

    fn create_block(&self) -> Option<Value> {
        let mut batch = self.current_batch.lock().unwrap();
        if batch.is_empty() {
            return None;
        }

        // 1. Extract events
        let events: Vec<Value> = batch.iter().map(|e| e.event_data.clone()).collect();

        // 2. Compute Merkle Root (Using our new Crypto impl)
        // Convert events to bytes for hashing (simplified canonicalization)
        let leaves: Vec<Vec<u8>> = events
            .iter()
            .map(|e| serde_json::to_vec(e).unwrap_or_default())
            .collect();
        let merkle_root = MerkleTree::compute_root(leaves);

        let now = current_timestamp();

        let block = json!({
            "events": events,
            "event_count": batch.len(),
            "created_at": now,
            "merkle_root": hex::encode(merkle_root),
            // "hash": ... // Block hash usually computed on the whole structure
        });

        // Reset
        batch.clear();
        if let Ok(mut t) = self.batch_start_time.lock() {
            *t = now;
        }

        Some(block)
    }
}

pub struct OrderingService {
    nodes: Arc<Mutex<HashMap<String, OrderingNode>>>,
    config: Value,
    status: Arc<Mutex<OrderingStatus>>,

    event_sender: Sender<PendingEvent>,

    // Components
    certifier: Arc<EventCertifier>,
    block_builder: Arc<BlockBuilder>,
    journal: Arc<TransactionJournal>, // New persistence

    // State
    pending_events: Arc<Mutex<HashMap<String, PendingEvent>>>,
    commit_queue: Arc<Mutex<VecDeque<Value>>>,

    // Control
    should_stop: Arc<Mutex<bool>>,
    processing_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl OrderingService {
    pub fn new(nodes: Vec<OrderingNode>, config: Value) -> (Arc<Self>, Receiver<PendingEvent>) {
        let nodes_map: HashMap<String, OrderingNode> =
            nodes.into_iter().map(|n| (n.node_id.clone(), n)).collect();

        let (tx, rx) = unbounded();

        let storage_dir = config
            .get("storage_dir")
            .and_then(|s| s.as_str())
            .unwrap_or("data/journal");
        let journal =
            TransactionJournal::new(storage_dir, "ordering.log").expect("Failed to init journal");

        let service = Arc::new(OrderingService {
            nodes: Arc::new(Mutex::new(nodes_map)),
            config: config.clone(),
            status: Arc::new(Mutex::new(OrderingStatus::Active)),
            event_sender: tx,
            certifier: Arc::new(EventCertifier::new()),
            block_builder: Arc::new(BlockBuilder::new(config.clone())),
            journal: Arc::new(journal),
            pending_events: Arc::new(Mutex::new(HashMap::new())),
            commit_queue: Arc::new(Mutex::new(VecDeque::new())),
            should_stop: Arc::new(Mutex::new(false)),
            processing_thread: Arc::new(Mutex::new(None)),
        });

        (service, rx)
    }

    pub fn receive_event(
        &self,
        payload: EventPayload,
        channel_id: String,
        submitter_org: String,
    ) -> String {
        let event_id = format!("{}-{}", channel_id, current_timestamp());

        let (event_data, arrow_data) = match payload {
            EventPayload::Json(v) => (v, None),
            EventPayload::Arrow(a) => {
                (json!({"type": "arrow", "digest": a.schema_digest}), Some(a))
            }
        };

        // 1. Log to Journal (WAL)
        if let Err(e) = self.journal.log_event(&event_data) {
            eprintln!("Failed to write to journal: {}", e);
        }

        let event = PendingEvent {
            event_id: event_id.clone(),
            event_data,
            arrow_data,
            channel_id,
            submitter_org,
            received_at: current_timestamp(),
            status: EventStatus::Pending,
            certification_result: None,
        };

        if let Ok(mut guard) = self.pending_events.lock() {
            guard.insert(event_id.clone(), event.clone());
        }

        let _ = self.event_sender.send(event);
        event_id
    }

    pub fn get_next_block(&self) -> Option<Value> {
        self.commit_queue.lock().unwrap().pop_front()
    }

    pub fn get_event_status(&self, event_id: &str) -> Option<Value> {
        self.pending_events
            .lock()
            .unwrap()
            .get(event_id)
            .map(|e| json!(e)) // Simplified
    }

    pub fn get_service_status(&self) -> Value {
        json!({
            "status": self.status.lock().unwrap().to_string(),
            // detailed stats omitted for brevity in this refactor
        })
    }

    pub fn stop(&self) {
        *self.should_stop.lock().unwrap() = true;
    }

    pub fn start(service: Arc<Self>, receiver: Receiver<PendingEvent>) {
        let service_clone = service.clone();

        let handle = thread::spawn(move || {
            // Optional: Start Tokio runtime here if we need async tasks
            let _rt = Runtime::new().unwrap();

            loop {
                if *service_clone.should_stop.lock().unwrap() {
                    break;
                }

                select! {
                    recv(receiver) -> msg => {
                        if let Ok(mut event) = msg {
                             // Process Event
                            event.status = EventStatus::Processing;

                             // Certify
                            if service_clone.certifier.validate(&mut event) {
                                event.status = EventStatus::Certified;
                                if let Some(block) = service_clone.block_builder.add_event(event.clone()) {
                                    service_clone.commit_queue.lock().unwrap().push_back(block);
                                }
                            } else {
                                event.status = EventStatus::Rejected;
                            }

                             // Update map
                            service_clone.pending_events.lock().unwrap().insert(event.event_id.clone(), event);
                        }
                    },
                    default(Duration::from_millis(100)) => {
                         // Check timeout
                        if let Some(block) = service_clone.block_builder.check_timeout() {
                            service_clone.commit_queue.lock().unwrap().push_back(block);
                        }
                    }
                }
            }
        });

        *service.processing_thread.lock().unwrap() = Some(handle);
    }
}

impl fmt::Display for OrderingService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.status.lock().unwrap().to_string();
        let node_count = self.nodes.lock().unwrap().len();
        write!(
            f,
            "OrderingService(status='{}', nodes={})",
            status, node_count
        )
    }
}

impl OrderingService {
    pub fn to_repr(&self) -> String {
        self.to_string()
    }
}

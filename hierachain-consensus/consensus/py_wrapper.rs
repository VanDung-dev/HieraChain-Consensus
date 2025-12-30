//! PyO3 Wrappers for Consensus Module
//!
//! This module provides Python bindings for consensus-related types:
//! - `PyOrderingNode` - Python wrapper for ordering node
//! - `PyOrderingService` - Python wrapper for ordering service

use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use crossbeam_channel::Receiver;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::consensus::types::{
    ArrowEventData, EventPayload, OrderingNode, OrderingStatus, PendingEvent,
};
use crate::consensus::OrderingService;
use crate::utils::pyo3_helpers::{dict_to_json, json_to_py};

// ==================== PyOrderingNode ====================

/// PyO3 wrapper for OrderingNode
#[pyclass(name = "OrderingNode")]
#[derive(Clone)]
pub struct PyOrderingNode {
    #[pyo3(get, set)]
    pub node_id: String,
    #[pyo3(get, set)]
    pub endpoint: String,
    #[pyo3(get, set)]
    pub is_leader: bool,
    #[pyo3(get, set)]
    pub weight: f64,
    #[pyo3(get, set)]
    pub status: String,
    #[pyo3(get, set)]
    pub last_heartbeat: f64,
}

#[pymethods]
impl PyOrderingNode {
    #[new]
    #[pyo3(signature = (node_id, endpoint, is_leader = false, weight = 1.0, status = "active".to_string(), last_heartbeat = 0.0))]
    fn new(
        node_id: String,
        endpoint: String,
        is_leader: bool,
        weight: f64,
        status: String,
        last_heartbeat: f64,
    ) -> Self {
        PyOrderingNode {
            node_id,
            endpoint,
            is_leader,
            weight,
            status,
            last_heartbeat,
        }
    }

    /// Check if the node is healthy based on heartbeat timeout
    fn is_healthy(&self, timeout: f64) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        (current_time - self.last_heartbeat) < timeout
    }

    fn __str__(&self) -> String {
        format!(
            "OrderingNode(node_id='{}', endpoint='{}', is_leader={}, status='{}')",
            self.node_id, self.endpoint, self.is_leader, self.status
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// Non-PyO3 methods for PyOrderingNode (Rust-only)
impl PyOrderingNode {
    /// Convert to Rust OrderingNode
    pub fn to_rust(&self) -> OrderingNode {
        OrderingNode {
            node_id: self.node_id.clone(),
            endpoint: self.endpoint.clone(),
            is_leader: self.is_leader,
            weight: self.weight,
            status: match self.status.as_str() {
                "active" => OrderingStatus::Active,
                "maintenance" => OrderingStatus::Maintenance,
                "stopped" => OrderingStatus::Stopped,
                "error" => OrderingStatus::Error,
                _ => OrderingStatus::Active,
            },
            last_heartbeat: self.last_heartbeat,
        }
    }
}

// ==================== PyOrderingService ====================

/// PyO3 wrapper for OrderingService
#[pyclass(name = "OrderingService")]
pub struct PyOrderingService {
    inner: Arc<OrderingService>,
}

// Helper function to start the processing thread
fn start_ordering_service_processing(
    service: Arc<OrderingService>,
    receiver: Receiver<PendingEvent>,
) {
    OrderingService::start(service, receiver);
}

#[pymethods]
impl PyOrderingService {
    /// Create a new OrderingService
    ///
    /// Args:
    ///     nodes: List of PyOrderingNode instances
    ///     config: Configuration dictionary
    #[new]
    fn new(nodes: Vec<PyOrderingNode>, config: &Bound<PyDict>) -> PyResult<Self> {
        let rust_nodes: Vec<OrderingNode> = nodes.into_iter().map(|n| n.to_rust()).collect();

        let config_json = dict_to_json(config)?;
        let (service_arc, receiver) = OrderingService::new(rust_nodes, config_json);

        // Start the processing thread immediately
        start_ordering_service_processing(Arc::clone(&service_arc), receiver);

        Ok(PyOrderingService { inner: service_arc })
    }

    /// Receive an event for processing
    ///
    /// Args:
    ///     event_data: Event data (PyArrow Table or dict)
    ///     channel_id: Channel identifier
    ///     submitter_org: Submitter organization
    ///
    /// Returns:
    ///     Transaction ID
    fn receive_event(
        &self,
        event_data: &Bound<PyAny>,
        channel_id: String,
        submitter_org: String,
    ) -> PyResult<String> {
        // Try to convert to Arrow RecordBatch first
        let payload = if let Ok(batch) = RecordBatch::from_pyarrow_bound(event_data) {
            EventPayload::Arrow(ArrowEventData {
                batch: Arc::new(batch),
                schema_digest: "digest_placeholder".to_string(),
            })
        } else {
            // Fallback to JSON
            if let Ok(dict) = event_data.downcast::<PyDict>() {
                let json = dict_to_json(dict)?;
                EventPayload::Json(json)
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Expected PyArrow Table or dict",
                ));
            }
        };

        Ok(self.inner.receive_event(payload, channel_id, submitter_org))
    }

    /// Get the status of an event by ID
    fn get_event_status(&self, event_id: String, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.get_event_status(&event_id) {
            Some(status) => Ok(Some(json_to_py(py, &status)?)),
            None => Ok(None),
        }
    }

    /// Get the next block from the service
    fn get_next_block(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.get_next_block() {
            Some(block) => Ok(Some(json_to_py(py, &block)?)),
            None => Ok(None),
        }
    }

    /// Get the current service status
    fn get_service_status(&self, py: Python) -> PyResult<Py<PyAny>> {
        let status = self.inner.get_service_status();
        json_to_py(py, &status)
    }

    /// Add a validation rule (placeholder)
    fn add_validation_rule(&self, _rule: Py<PyAny>, _py: Python) -> PyResult<()> {
        Ok(())
    }

    /// Start the service (handled internally during new())
    fn start(&self) {}

    /// Stop the service
    fn stop(&self) {
        self.inner.stop();
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        self.inner.to_repr()
    }
}

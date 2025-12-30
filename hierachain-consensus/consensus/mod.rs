//! Consensus module for the HieraChain framework.
//!
//! This module provides the core functionality for implementing hierarchical consensus
//! in the HieraChain framework. It includes the definition of the `Consensus` trait,
//! which defines the methods required for consensus implementations.

pub mod ordering_service;
pub mod py_wrapper;
pub mod types;

pub use ordering_service::OrderingService;
pub use py_wrapper::{PyOrderingNode, PyOrderingService};
pub use types::{EventStatus, OrderingNode, OrderingStatus, PendingEvent};

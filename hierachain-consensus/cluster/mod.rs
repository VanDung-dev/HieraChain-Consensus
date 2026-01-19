//! Cluster Management Module
//!
//! Handles inter-node communication, gossiping, and cluster state management.

pub mod lockdown_protocol;
pub use lockdown_protocol::{ClusterLockdownManager, LockdownMessage};

pub mod manager;
pub use manager::{ClusterManager, NodeStatus};

//! Core module for HieraChain Framework.
//!
//! This module provides the core functionality for HieraChain, including
//! block management, blockchain management, consensus algorithms, and utility functions.

pub mod block;
pub mod blockchain;
pub mod consensus;
pub mod py_wrapper;
pub mod schemas;
pub mod utils;

// Re-export core types
pub use block::Block;

// Re-export PyO3 wrappers
pub use py_wrapper::{PyBlockchain, PyProofOfAuthority, PyProofOfFederation};

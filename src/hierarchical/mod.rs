//! Hierarchical Module
//!
//! Contains hierarchical consensus implementations for enterprise blockchain.

pub mod consensus;

pub use consensus::{BFTConsensus, BFTMessage, ConsensusError, ConsensusState, MessageType};

//! BFT Consensus Module
//!
//! Provides Byzantine Fault Tolerance consensus implementation including
//! message types, state management, and the main consensus engine.

pub mod bft_consensus;
pub mod message;
pub mod state;

pub use bft_consensus::{BFTConsensus, ConsensusError};
pub use message::{BFTMessage, MessageType};
pub use state::ConsensusState;

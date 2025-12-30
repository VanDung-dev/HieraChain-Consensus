//! Hierarchical Module
//!
//! Contains hierarchical consensus implementations for enterprise blockchain.
//! This includes Main Chain, Sub-Chain, Hierarchy Manager, and supporting components.

pub mod channel;
pub mod consensus;
pub mod hierarchy_manager;
pub mod main_chain;
pub mod multi_org;
pub mod private_data;
pub mod py_wrapper;
pub mod sub_chain;

pub use channel::{Channel, ChannelStatus, Organization, PrivateCollection};
pub use consensus::{BFTConsensus, BFTMessage, ConsensusError, ConsensusState, MessageType};
pub use hierarchy_manager::{HierarchyError, HierarchyManager, SubChainInfo, SystemStats};
pub use main_chain::MainChain;
pub use sub_chain::SubChain;

pub use py_wrapper::{PyBFTConsensus, PyHierarchyManager, PyMainChain, PySubChain};

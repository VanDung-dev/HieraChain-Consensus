//! Error Mitigation Module
//!
//! Provides error classification, validation, and recovery mechanisms
//! for the HieraChain consensus framework.

pub mod error_classifier;
pub mod journal;
pub mod types;
pub mod validator;

pub use error_classifier::ErrorClassifier;
pub use types::{ErrorCategory, ImpactLevel, LikelihoodLevel, PriorityLevel};
pub use validator::ConsensusValidator;

//! Client SDK Module
//!
//! Provides tools for external applications to interact with HieraChain.

pub mod client;
pub use client::{ClientConfig, ClientError, HieraChainClient};

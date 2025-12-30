//! Security Module
//!
//! Provides cryptographic primitives for the HieraChain consensus framework:
//! - Ed25519 key pair generation and signing
//! - Signature verification
//! - Key provider abstractions

pub mod key_provider;
pub mod py_wrapper;
pub mod security_utils;

pub use key_provider::{KeyProvider, LocalKeyProvider};
pub use py_wrapper::{verify_signature as py_verify_signature, PyKeyPair};
pub use security_utils::{verify_signature, verify_signature_bytes, CryptoError, KeyPair};

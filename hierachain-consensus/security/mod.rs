//! Security Module
//!
//! Provides cryptographic primitives for the HieraChain consensus framework:
//! - Ed25519 key pair generation and signing
//! - Signature verification
//! - Key provider abstractions
//! - Zero Knowledge Proof verification

pub mod key_provider;
pub mod mock_verifier;
pub mod py_wrapper;
pub mod security_utils;
pub mod zk_verifier;

pub use key_provider::{KeyProvider, LocalKeyProvider};
pub use mock_verifier::{MockMode, MockVerifier};
pub use py_wrapper::{verify_signature as py_verify_signature, PyKeyPair};
pub use security_utils::{verify_signature, verify_signature_bytes, CryptoError, KeyPair};
pub use zk_verifier::{Groth16Verifier, Verifier, ZkVerifyError, ZkVerifyResult};

pub mod integrity;
pub use integrity::{ChecksumValidator, IntegrityError};

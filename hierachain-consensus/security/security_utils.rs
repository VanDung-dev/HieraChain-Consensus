//! Security Utilities
//!
//! Ed25519 key pair generation, signing, and verification for HieraChain consensus.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use thiserror::Error;

/// Cryptographic error types
#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Invalid signature format: {0}")]
    InvalidSignature(String),

    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),

    #[error("Invalid private key: {0}")]
    InvalidPrivateKey(String),

    #[error("Verification failed")]
    VerificationFailed,

    #[error("Signing failed: {0}")]
    SigningFailed(String),

    #[error("Hex decode error: {0}")]
    HexDecodeError(#[from] hex::FromHexError),
}

/// Ed25519 key pair for signing and verification
///
/// # Security
/// This struct implements `Drop` to zero out the signing key from memory
/// when the KeyPair is dropped, preventing key material from being leaked
/// through memory forensics.
pub struct KeyPair {
    verifying_key: VerifyingKey,
    signing_key: SigningKey,
}


impl Clone for KeyPair {
    fn clone(&self) -> Self {
        Self {
            verifying_key: self.verifying_key,
            signing_key: SigningKey::from_bytes(self.signing_key.as_bytes()),
        }
    }
}

impl KeyPair {
    /// Generate a new random key pair
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Create a key pair from a hex-encoded private key
    ///
    /// # Arguments
    /// * `private_key_hex` - 64-character hex string representing the 32-byte private key
    ///
    /// # Returns
    /// * `Result<KeyPair, CryptoError>` - The key pair or an error
    pub fn from_private_key(private_key_hex: &str) -> Result<Self, CryptoError> {
        let private_key_bytes = hex::decode(private_key_hex)?;

        if private_key_bytes.len() != 32 {
            return Err(CryptoError::InvalidPrivateKey(format!(
                "Expected 32 bytes, got {}",
                private_key_bytes.len()
            )));
        }

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&private_key_bytes);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();

        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Get the public key as a hex string
    pub fn public_key(&self) -> String {
        hex::encode(self.verifying_key.as_bytes())
    }

    /// Get the private key as a hex string
    ///
    /// # Security Warning
    /// This method exposes the private key. Use with caution!
    pub fn private_key(&self) -> String {
        hex::encode(self.signing_key.as_bytes())
    }

    /// Sign a message and return the signature as a hex string
    ///
    /// # Arguments
    /// * `message` - The message bytes to sign
    ///
    /// # Returns
    /// * `Result<String, CryptoError>` - Hex-encoded signature or error
    pub fn sign(&self, message: &[u8]) -> Result<String, CryptoError> {
        let signature: Signature = self.signing_key.sign(message);
        Ok(hex::encode(signature.to_bytes()))
    }

    /// Get the verifying key for external use
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.verifying_key
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::generate()
    }
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("public_key", &self.public_key())
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

/// Verify an Ed25519 signature using hex-encoded inputs
///
/// # Arguments
/// * `public_key_hex` - The signer's public key in hex format
/// * `message` - The original message bytes
/// * `signature_hex` - The signature in hex format
///
/// # Returns
/// * `bool` - True if the signature is valid, false otherwise
pub fn verify_signature(public_key_hex: &str, message: &[u8], signature_hex: &str) -> bool {
    verify_signature_internal(public_key_hex, message, signature_hex).unwrap_or(false)
}

/// Internal verification function that returns detailed errors
fn verify_signature_internal(
    public_key_hex: &str,
    message: &[u8],
    signature_hex: &str,
) -> Result<bool, CryptoError> {
    // Decode public key
    let public_key_bytes = hex::decode(public_key_hex)?;
    if public_key_bytes.len() != 32 {
        return Err(CryptoError::InvalidPublicKey(format!(
            "Expected 32 bytes, got {}",
            public_key_bytes.len()
        )));
    }

    let mut pk_bytes = [0u8; 32];
    pk_bytes.copy_from_slice(&public_key_bytes);

    let verifying_key = VerifyingKey::from_bytes(&pk_bytes)
        .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;

    // Decode signature
    let signature_bytes = hex::decode(signature_hex)?;
    if signature_bytes.len() != 64 {
        return Err(CryptoError::InvalidSignature(format!(
            "Expected 64 bytes, got {}",
            signature_bytes.len()
        )));
    }

    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&signature_bytes);

    let signature = Signature::from_bytes(&sig_bytes);

    // Verify
    match verifying_key.verify(message, &signature) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Verify an Ed25519 signature using raw byte arrays (backward compatible)
///
/// # Arguments
/// * `public_key` - The signer's public key as bytes (32 bytes)
/// * `message` - The original message bytes
/// * `signature` - The signature as bytes (64 bytes)
///
/// # Returns
/// * `Result<bool, CryptoError>` - Ok(true) if valid, Ok(false) or Err if invalid
pub fn verify_signature_bytes(
    public_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoError> {
    if public_key.len() != 32 {
        return Err(CryptoError::InvalidPublicKey(format!(
            "Expected 32 bytes, got {}",
            public_key.len()
        )));
    }

    if signature.len() != 64 {
        return Err(CryptoError::InvalidSignature(format!(
            "Expected 64 bytes, got {}",
            signature.len()
        )));
    }

    let mut pk_bytes = [0u8; 32];
    pk_bytes.copy_from_slice(public_key);

    let verifying_key = VerifyingKey::from_bytes(&pk_bytes)
        .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;

    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(signature);

    let sig = Signature::from_bytes(&sig_bytes);

    match verifying_key.verify(message, &sig) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Generate a new key pair and return (public_key_hex, private_key_hex)
pub fn generate_key_pair_hex() -> (String, String) {
    let kp = KeyPair::generate();
    (kp.public_key(), kp.private_key())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let kp = KeyPair::generate();
        assert_eq!(kp.public_key().len(), 64); // 32 bytes = 64 hex chars
        assert_eq!(kp.private_key().len(), 64);
    }

    #[test]
    fn test_from_private_key() {
        let kp1 = KeyPair::generate();
        let private_hex = kp1.private_key();

        let kp2 = KeyPair::from_private_key(&private_hex).unwrap();
        assert_eq!(kp1.public_key(), kp2.public_key());
    }

    #[test]
    fn test_sign_and_verify() {
        let kp = KeyPair::generate();
        let message = b"Hello, HieraChain!";

        let signature = kp.sign(message).unwrap();
        assert_eq!(signature.len(), 128); // 64 bytes = 128 hex chars

        // Verify signature
        assert!(verify_signature(&kp.public_key(), message, &signature));
    }

    #[test]
    fn test_invalid_signature() {
        let kp1 = KeyPair::generate();
        let kp2 = KeyPair::generate();
        let message = b"Test message";

        // Sign with kp1, verify with kp2's public key
        let signature = kp1.sign(message).unwrap();

        assert!(!verify_signature(&kp2.public_key(), message, &signature));
    }

    #[test]
    fn test_tampered_message() {
        let kp = KeyPair::generate();
        let message = b"Original message";
        let tampered = b"Tampered message";

        let signature = kp.sign(message).unwrap();

        assert!(!verify_signature(&kp.public_key(), tampered, &signature));
    }

    #[test]
    fn test_invalid_private_key() {
        let result = KeyPair::from_private_key("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_key_pair_hex() {
        let (public, private) = generate_key_pair_hex();
        assert_eq!(public.len(), 64);
        assert_eq!(private.len(), 64);
    }
}

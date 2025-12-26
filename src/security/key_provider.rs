//! Key Provider Abstraction
//!
//! Defines the interface for cryptographic key operations, allowing for
//! different storage backends without changing the core consensus logic.

use crate::security::security_utils::{CryptoError, KeyPair};

/// Abstract interface for key operations
///
/// Implementations handle how the private key is stored and accessed.
pub trait KeyProvider: Send + Sync {
    /// Return the public key in hex format
    fn public_key_hex(&self) -> &str;

    /// Sign data and return hex signature
    ///
    /// # Arguments
    /// * `data` - Bytes to sign
    ///
    /// # Returns
    /// * `Result<String, CryptoError>` - Hex-encoded signature
    fn sign(&self, data: &[u8]) -> Result<String, CryptoError>;
}

/// Standard provider that holds the KeyPair in memory
///
/// Used for development and backward compatibility.
pub struct LocalKeyProvider {
    keypair: KeyPair,
    public_key_cache: String,
}

impl LocalKeyProvider {
    /// Create a new LocalKeyProvider from an existing KeyPair
    pub fn new(keypair: KeyPair) -> Self {
        let public_key_cache = keypair.public_key();
        Self {
            keypair,
            public_key_cache,
        }
    }

    /// Generate a new random key provider
    pub fn generate() -> Self {
        Self::new(KeyPair::generate())
    }

    /// Get the underlying KeyPair (for testing purposes)
    pub fn keypair(&self) -> &KeyPair {
        &self.keypair
    }
}

impl KeyProvider for LocalKeyProvider {
    fn public_key_hex(&self) -> &str {
        &self.public_key_cache
    }

    fn sign(&self, data: &[u8]) -> Result<String, CryptoError> {
        self.keypair.sign(data)
    }
}

impl Default for LocalKeyProvider {
    fn default() -> Self {
        Self::generate()
    }
}

impl std::fmt::Debug for LocalKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalKeyProvider")
            .field("public_key", &self.public_key_cache)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_key_provider() {
        let provider = LocalKeyProvider::generate();

        assert_eq!(provider.public_key_hex().len(), 64);

        let message = b"Test message";
        let signature = provider.sign(message).unwrap();

        assert_eq!(signature.len(), 128);
    }

    #[test]
    fn test_local_key_provider_from_keypair() {
        let kp = KeyPair::generate();
        let public_key = kp.public_key();

        let provider = LocalKeyProvider::new(kp);

        assert_eq!(provider.public_key_hex(), public_key);
    }

    #[test]
    fn test_sign_and_verify() {
        use crate::security::security_utils::verify_signature;

        let provider = LocalKeyProvider::generate();
        let message = b"Consensus message";

        let signature = provider.sign(message).unwrap();

        assert!(verify_signature(
            provider.public_key_hex(),
            message,
            &signature
        ));
    }
}

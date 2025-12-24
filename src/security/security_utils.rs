use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Invalid signature format")]
    InvalidSignature,
    #[error("Invalid public key")]
    InvalidPublicKey,
    #[error("Verification failed")]
    VerificationFailed,
}

/// Verify an Ed25519 signature
pub fn verify_signature(
    public_key: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoError> {
    let verify_key = VerifyingKey::from_bytes(
        public_key
            .try_into()
            .map_err(|_| CryptoError::InvalidPublicKey)?,
    )
    .map_err(|_| CryptoError::InvalidPublicKey)?;

    let signature = Signature::from_bytes(
        signature
            .try_into()
            .map_err(|_| CryptoError::InvalidSignature)?,
    );

    verify_key
        .verify(message, &signature)
        .map(|_| true)
        .map_err(|_| CryptoError::VerificationFailed)
}

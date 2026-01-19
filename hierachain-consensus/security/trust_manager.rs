//! Peer Trust Manager
//!
//! Manages the reputation and authorization status of network peers.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrustStatus {
    Trusted,
    Suspicious,
    Blacklisted,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub address: String,
    pub status: TrustStatus,
    pub reputation_score: i32,
    pub violations: u32,
}

#[derive(Debug, Error)]
pub enum TrustError {
    #[error("Peer not found")]
    PeerNotFound,
    #[error("Peer already blacklisted")]
    AlreadyBlacklisted,
}

#[derive(Debug, Clone)]
pub struct PeerTrustManager {
    peers: Arc<DashMap<String, PeerInfo>>,
}

impl PeerTrustManager {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
        }
    }

    /// Authorize a new peer or update existing one to Trusted
    pub fn authorize_peer(&self, peer_id: &str, address: &str) {
        self.peers.insert(
            peer_id.to_string(),
            PeerInfo {
                id: peer_id.to_string(),
                address: address.to_string(),
                status: TrustStatus::Trusted,
                reputation_score: 100,
                violations: 0,
            },
        );
    }

    /// Check if a peer is authorized
    pub fn is_authorized(&self, peer_id: &str) -> bool {
        match self.peers.get(peer_id) {
            Some(peer) => peer.status == TrustStatus::Trusted,
            None => false,
        }
    }

    /// Report misbehavior by a peer
    pub fn report_misbehavior(&self, peer_id: &str, severity: i32) -> Result<(), TrustError> {
        let mut peer = self
            .peers
            .get_mut(peer_id)
            .ok_or(TrustError::PeerNotFound)?;

        if peer.status == TrustStatus::Blacklisted {
            return Err(TrustError::AlreadyBlacklisted);
        }

        peer.reputation_score -= severity;
        peer.violations += 1;

        // Auto-downgrade status based on reputation
        if peer.reputation_score <= 0 {
            peer.status = TrustStatus::Blacklisted;
        } else if peer.reputation_score < 50 {
            peer.status = TrustStatus::Suspicious;
        }

        Ok(())
    }

    /// Get peer info
    pub fn get_peer_info(&self, peer_id: &str) -> Option<PeerInfo> {
        self.peers.get(peer_id).map(|p| p.clone())
    }

    /// Revoke authorization (Blacklist)
    pub fn blacklist_peer(&self, peer_id: &str) -> Result<(), TrustError> {
        let mut peer = self
            .peers
            .get_mut(peer_id)
            .ok_or(TrustError::PeerNotFound)?;
        peer.status = TrustStatus::Blacklisted;
        peer.reputation_score = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_lifecycle() {
        let manager = PeerTrustManager::new();
        let peer_id = "node_123";

        // Authorize
        manager.authorize_peer(peer_id, "127.0.0.1:8000");
        assert!(manager.is_authorized(peer_id));

        // Report minor violation
        let _ = manager.report_misbehavior(peer_id, 10);
        let info = manager.get_peer_info(peer_id).unwrap();
        assert_eq!(info.reputation_score, 90);
        assert_eq!(info.status, TrustStatus::Trusted);

        // Report major violation
        let _ = manager.report_misbehavior(peer_id, 80);
        let info = manager.get_peer_info(peer_id).unwrap();
        assert_eq!(info.reputation_score, 10);
        assert_eq!(info.status, TrustStatus::Suspicious);

        // Blacklist
        let _ = manager.report_misbehavior(peer_id, 20);
        let info = manager.get_peer_info(peer_id).unwrap();
        assert_eq!(info.status, TrustStatus::Blacklisted);
        assert!(!manager.is_authorized(peer_id));
    }
}

"""
BFT Consensus Data Transmission Tests.

Tests data integrity for Byzantine Fault Tolerant consensus:
- KeyPair generation and signing
- Signature verification
- BFT consensus creation
"""

import pytest
import time
from typing import Any

# Rust imports
RUST_AVAILABLE = False
RustBFT = None
RustKeyPair = None
rust_verify_signature = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "BFTConsensus"):
        RustBFT = hierachain_consensus.BFTConsensus
    if hasattr(hierachain_consensus, "KeyPair"):
        RustKeyPair = hierachain_consensus.KeyPair
    if hasattr(hierachain_consensus, "verify_signature"):
        rust_verify_signature = hierachain_consensus.verify_signature
    RUST_AVAILABLE = True
except ImportError:
    pass


def create_test_message() -> dict[str, Any]:
    """Create test BFT message."""
    return {
        "type": "prepare",
        "view": 1,
        "sequence": 100,
        "digest": "a" * 64,
        "timestamp": time.time(),
    }


@pytest.mark.skipif(not RUST_AVAILABLE or RustKeyPair is None,
                    reason="Rust KeyPair not available")
class TestKeyPairOperations:
    """Test KeyPair cryptographic operations."""

    def test_keypair_generate(self):
        """Test key pair generation using static method."""
        keypair = RustKeyPair.generate()
        assert keypair is not None
        assert keypair.public_key is not None
        assert len(keypair.public_key) == 64  # Hex-encoded Ed25519 public key

    def test_keypair_has_private_key(self):
        """Test keypair has private key."""
        keypair = RustKeyPair.generate()
        assert keypair.private_key is not None
        assert len(keypair.private_key) == 64  # Hex-encoded

    def test_message_signing(self):
        """Test message signing."""
        keypair = RustKeyPair.generate()
        message = b"test message for signing"

        signature = keypair.sign(message)
        assert signature is not None
        assert len(signature) == 128  # Ed25519 signature hex = 64 bytes * 2

    def test_signature_verification(self):
        """Test signature verification."""
        if rust_verify_signature is None:
            pytest.skip("verify_signature not available")

        keypair = RustKeyPair.generate()
        message = b"test message for verification"

        signature = keypair.sign(message)
        public_key = keypair.public_key

        is_valid = rust_verify_signature(public_key, message, signature)
        assert is_valid is True

    def test_invalid_signature_rejected(self):
        """Test that invalid signatures are rejected."""
        if rust_verify_signature is None:
            pytest.skip("verify_signature not available")

        keypair1 = RustKeyPair.generate()
        keypair2 = RustKeyPair.generate()
        message = b"test message"

        # Sign with keypair1
        signature = keypair1.sign(message)

        # Verify with keypair2's public key should fail
        is_valid = rust_verify_signature(keypair2.public_key, message, signature)
        assert is_valid is False

    def test_signature_deterministic(self):
        """Test signature for same message with same key."""
        keypair = RustKeyPair.generate()
        message = b"deterministic test"

        sig1 = keypair.sign(message)
        sig2 = keypair.sign(message)

        # Ed25519 signatures are deterministic
        assert sig1 == sig2

    def test_different_messages_different_signatures(self):
        """Test different messages produce different signatures."""
        keypair = RustKeyPair.generate()

        sig1 = keypair.sign(b"message 1")
        sig2 = keypair.sign(b"message 2")

        assert sig1 != sig2


@pytest.mark.skipif(not RUST_AVAILABLE or RustBFT is None or RustKeyPair is None,
                    reason="Rust BFT or KeyPair not available")
class TestBFTConsensus:
    """Test BFT consensus operations."""

    def test_bft_creation(self):
        """Test BFT consensus creation with correct API."""
        # Create keypairs for all nodes
        keypair1 = RustKeyPair.generate()
        keypair2 = RustKeyPair.generate()
        keypair3 = RustKeyPair.generate()
        keypair4 = RustKeyPair.generate()

        all_nodes = ["node1", "node2", "node3", "node4"]
        node_public_keys = {
            "node1": keypair1.public_key,
            "node2": keypair2.public_key,
            "node3": keypair3.public_key,
            "node4": keypair4.public_key,
        }

        # f=1 for 4 nodes (3f+1 = 4)
        bft = RustBFT(
            node_id="node1",
            all_nodes=all_nodes,
            f=1,
            keypair=keypair1,
            node_public_keys=node_public_keys
        )

        assert bft is not None

    def test_bft_node_id(self):
        """Test BFT node ID property."""
        keypair = RustKeyPair.generate()
        all_nodes = ["node1", "node2", "node3", "node4"]
        node_public_keys = {n: RustKeyPair.generate().public_key for n in all_nodes}
        node_public_keys["node1"] = keypair.public_key

        bft = RustBFT(
            node_id="node1",
            all_nodes=all_nodes,
            f=1,
            keypair=keypair,
            node_public_keys=node_public_keys
        )

        assert bft.node_id() == "node1"

    def test_bft_fault_tolerance(self):
        """Test BFT fault tolerance property."""
        keypair = RustKeyPair.generate()
        all_nodes = ["node1", "node2", "node3", "node4"]
        node_public_keys = {n: RustKeyPair.generate().public_key for n in all_nodes}
        node_public_keys["node1"] = keypair.public_key

        bft = RustBFT(
            node_id="node1",
            all_nodes=all_nodes,
            f=1,
            keypair=keypair,
            node_public_keys=node_public_keys
        )

        assert bft.fault_tolerance() == 1


class TestBFTMessageFormat:
    """Test BFT message format consistency."""

    def test_message_has_required_fields(self):
        """Test message has all required fields."""
        msg = create_test_message()

        required = ["type", "view", "sequence", "digest", "timestamp"]
        for field in required:
            assert field in msg

    def test_digest_format(self):
        """Test digest is valid hex."""
        msg = create_test_message()
        digest = msg["digest"]

        assert len(digest) == 64
        int(digest, 16)  # Should not raise


class TestBFTPerformance:
    """Performance sanity checks for BFT."""

    @pytest.mark.skipif(not RUST_AVAILABLE or RustKeyPair is None,
                        reason="Rust KeyPair needed")
    def test_keypair_generation_speed(self):
        """Test keypair generation speed."""
        start = time.perf_counter()
        for _ in range(100):
            RustKeyPair.generate()
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Too slow: {elapsed:.2f}s for 100 keypairs"

    @pytest.mark.skipif(not RUST_AVAILABLE or RustKeyPair is None,
                        reason="Rust KeyPair needed")
    def test_signature_speed(self):
        """Test signing speed."""
        keypair = RustKeyPair.generate()
        message = b"performance test message"

        start = time.perf_counter()
        for _ in range(100):
            keypair.sign(message)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 signatures"

    @pytest.mark.skipif(
        not RUST_AVAILABLE or RustKeyPair is None or rust_verify_signature is None,
        reason="Rust signature verification needed"
    )
    def test_verification_speed(self):
        """Test verification speed."""
        keypair = RustKeyPair.generate()
        message = b"verification test message"
        signature = keypair.sign(message)
        public_key = keypair.public_key

        start = time.perf_counter()
        for _ in range(100):
            rust_verify_signature(public_key, message, signature)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 verifications"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

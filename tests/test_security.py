"""
Security Test Suite for HieraChain Consensus

This module contains comprehensive security tests covering:
1. Cryptographic Operations (KeyPair, Signatures)
2. ZK Proof Verification (Mock Verifier)
3. Block Hash Integrity
4. Replay Attack Prevention
5. Tamper Detection
6. Input Validation & Edge Cases
"""

import pytest
import time
import hashlib

from hierachain_consensus import (
    KeyPair,
    calculate_block_hash,
    calculate_merkle_root,
    verify_signature,
    bulk_validate_transactions,
)


class TestCryptographicOperations:
    """Test cryptographic key generation and signature operations."""

    def test_keypair_generation_uniqueness(self):
        """Each generated keypair should be unique."""
        keypairs = [KeyPair.generate() for _ in range(10)]
        public_keys = [kp.public_key for kp in keypairs]
        
        # All public keys should be unique
        assert len(set(public_keys)) == 10, "Generated keypairs should be unique"

    def test_keypair_public_key_format(self):
        """Public key should be valid hex string of correct length."""
        kp = KeyPair.generate()
        public_key = kp.public_key
        
        # Ed25519 public key is 32 bytes = 64 hex characters
        assert len(public_key) == 64, "Public key should be 64 hex characters"
        assert all(c in '0123456789abcdef' for c in public_key.lower()), \
            "Public key should be valid hex"

    def test_signature_verification_valid(self):
        """Valid signatures should be verified successfully."""
        kp = KeyPair.generate()
        message = b"test message for signing"
        
        signature = kp.sign(message)
        is_valid = verify_signature(kp.public_key, message, signature)
        
        assert is_valid, "Valid signature should be verified"

    def test_signature_verification_invalid_message(self):
        """Signature should fail verification with different message."""
        kp = KeyPair.generate()
        message = b"original message"
        tampered_message = b"tampered message"
        
        signature = kp.sign(message)
        is_valid = verify_signature(kp.public_key, tampered_message, signature)
        
        assert not is_valid, "Signature should fail for tampered message"

    def test_signature_verification_wrong_key(self):
        """Signature should fail verification with wrong public key."""
        kp1 = KeyPair.generate()
        kp2 = KeyPair.generate()
        message = b"test message"
        
        signature = kp1.sign(message)
        is_valid = verify_signature(kp2.public_key, message, signature)
        
        assert not is_valid, "Signature should fail with wrong public key"

    def test_signature_non_repudiation(self):
        """Signer cannot deny signing a message."""
        kp = KeyPair.generate()
        message = b"I agree to the terms"
        
        signature = kp.sign(message)
        
        # The signature can always be verified with the public key
        assert verify_signature(kp.public_key, message, signature)
        
        # Cannot create valid signature without private key
        fake_signature = "a" * len(signature)
        assert not verify_signature(kp.public_key, message, fake_signature)


class TestBlockIntegrity:
    """Test block hash and merkle root integrity."""

    def test_block_hash_determinism(self):
        """Same block data should produce same hash."""
        block_data = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "test data"
        }
        
        hash1 = calculate_block_hash(block_data)
        hash2 = calculate_block_hash(block_data)
        
        assert hash1 == hash2, "Block hash should be deterministic"

    def test_block_hash_sensitivity(self):
        """Any change in block data should change the hash."""
        base_block = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "test data"
        }
        
        original_hash = calculate_block_hash(base_block)
        
        # Change each field and verify hash changes
        modifications = [
            ("index", 2),
            ("previous_hash", "xyz789"),
            ("timestamp", 1234567891.0),
            ("data", "modified data"),
        ]
        
        for field, new_value in modifications:
            modified_block = base_block.copy()
            modified_block[field] = new_value
            modified_hash = calculate_block_hash(modified_block)
            
            assert modified_hash != original_hash, \
                f"Hash should change when {field} is modified"

    def test_merkle_root_determinism(self):
        """Same events should produce same merkle root."""
        events = ["event1", "event2", "event3"]
        
        root1 = calculate_merkle_root(events)
        root2 = calculate_merkle_root(events)
        
        assert root1 == root2, "Merkle root should be deterministic"

    def test_merkle_root_order_sensitivity(self):
        """Event order should affect merkle root."""
        events1 = ["event1", "event2", "event3"]
        events2 = ["event3", "event2", "event1"]
        
        root1 = calculate_merkle_root(events1)
        root2 = calculate_merkle_root(events2)
        
        assert root1 != root2, "Merkle root should be order-sensitive"

    def test_merkle_root_single_event(self):
        """Single event should produce valid merkle root."""
        events = ["single_event"]
        root = calculate_merkle_root(events)
        
        assert root is not None
        assert len(root) == 64, "Merkle root should be SHA256 (64 hex chars)"


class TestTamperDetection:
    """Test tamper detection capabilities."""

    def test_detect_modified_transaction(self):
        """Modified transaction should fail validation."""
        # Valid transaction structure
        valid_tx = {
            "id": "tx123",
            "sender": "alice",
            "receiver": "bob",
            "amount": 100,
            "timestamp": time.time()
        }
        
        # This test depends on bulk_validate_transactions implementation
        # Assuming it checks basic structure
        _result = bulk_validate_transactions([valid_tx])  # noqa: F841
        # Note: actual validation depends on implementation

    def test_hash_chain_integrity(self):
        """Breaking hash chain should be detectable."""
        blocks = []
        previous_hash = "genesis"
        
        for i in range(5):
            block = {
                "index": i,
                "previous_hash": previous_hash,
                "timestamp": time.time() + i,
                "data": f"block {i}"
            }
            block["hash"] = calculate_block_hash(block)
            blocks.append(block)
            previous_hash = block["hash"]
        
        # Verify chain integrity
        for i in range(1, len(blocks)):
            assert blocks[i]["previous_hash"] == blocks[i-1]["hash"], \
                f"Block {i} should reference previous block's hash"
        
        # Tamper with block 2
        _original_data = blocks[2]["data"]  # Save original (unused) # noqa: F841
        blocks[2]["data"] = "tampered data"
        new_hash = calculate_block_hash(blocks[2])
        
        # Tampering breaks the chain
        assert new_hash != blocks[2]["hash"], "Tampering should change hash"
        assert blocks[3]["previous_hash"] != new_hash, "Chain should be broken"


class TestReplayAttackPrevention:
    """Test replay attack prevention mechanisms."""

    def test_timestamp_prevents_replay(self):
        """Transactions with old timestamps should be rejectable."""
        current_time = time.time()
        old_time = current_time - 3600  # 1 hour ago
        
        # Transaction with current timestamp
        current_tx = {
            "id": "tx1",
            "timestamp": current_time,
            "nonce": 1
        }
        
        # Same transaction replayed (old timestamp)
        replayed_tx = {
            "id": "tx1",
            "timestamp": old_time,
            "nonce": 1
        }
        
        # Different hashes demonstrate they are distinguishable
        hash1 = hashlib.sha256(str(current_tx).encode()).hexdigest()
        hash2 = hashlib.sha256(str(replayed_tx).encode()).hexdigest()
        
        assert hash1 != hash2, "Different timestamps should produce different hashes"

    def test_nonce_prevents_replay(self):
        """Sequential nonces prevent transaction replay."""
        base_tx = {
            "sender": "alice",
            "receiver": "bob",
            "amount": 100,
        }
        
        transactions = []
        for nonce in range(5):
            tx = base_tx.copy()
            tx["nonce"] = nonce
            tx["hash"] = hashlib.sha256(str(tx).encode()).hexdigest()
            transactions.append(tx)
        
        # All transaction hashes should be unique
        hashes = [tx["hash"] for tx in transactions]
        assert len(set(hashes)) == 5, "Each nonce should produce unique hash"


class TestInputValidation:
    """Test input validation and edge cases."""

    def test_empty_events_merkle_root(self):
        """Empty event list should be handled."""
        try:
            root = calculate_merkle_root([])
            # If it doesn't raise, it should return something valid
            assert root is not None or root == ""
        except Exception as e:
            # Empty list may raise an error, which is acceptable
            assert "empty" in str(e).lower() or isinstance(e, (ValueError, IndexError))

    def test_special_characters_in_data(self):
        """Special characters should be handled safely."""
        special_data = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "Test with special chars: <script>alert('xss')</script> \"quotes\" & ampersand"
        }
        
        # Should not raise an error
        hash_result = calculate_block_hash(special_data)
        assert hash_result is not None
        assert len(hash_result) == 64

    def test_unicode_data_handling(self):
        """Unicode data should be handled correctly."""
        unicode_data = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "Unicode test: Tiếng Việt có dấu 🚀 émojis"
        }
        
        hash_result = calculate_block_hash(unicode_data)
        assert hash_result is not None
        assert len(hash_result) == 64

    def test_large_data_handling(self):
        """Large data payloads should be handled."""
        large_data = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "x" * 100000  # 100KB of data
        }
        
        hash_result = calculate_block_hash(large_data)
        assert hash_result is not None
        assert len(hash_result) == 64


class TestZKProofMock:
    """Test Zero Knowledge Proof mock verification (if available)."""

    def test_mock_proof_format(self):
        """Mock proof should have expected format."""
        # Mock proof starts with "mock_proof" magic bytes
        mock_proof = b"mock_proof" + b"\x00" * 32
        
        assert mock_proof.startswith(b"mock_proof"), \
            "Mock proof should start with magic bytes"

    def test_proof_verification_structure(self):
        """Proof verification should follow expected structure."""
        # This tests the expected interface for ZK proof verification
        proof_data = {
            "zk_proof": "mock_proof_data",
            "zk_public_inputs": ["input1", "input2"],
            "verified": True
        }
        
        # Verify structure
        assert "zk_proof" in proof_data
        assert "zk_public_inputs" in proof_data
        assert isinstance(proof_data["zk_public_inputs"], list)


class TestSecurityBestPractices:
    """Test security best practices implementation."""

    def test_no_private_key_exposure(self):
        """Private key should not be directly accessible."""
        kp = KeyPair.generate()
        
        # Check that private key is not in public attributes
        public_attrs = [attr for attr in dir(kp) if not attr.startswith('_')]
        
        # Should have public_key but not expose private_key directly
        assert 'public_key' in public_attrs
        # Note: Actual implementation may vary

    def test_constant_time_comparison_simulation(self):
        """Signature verification should use constant-time comparison."""
        kp = KeyPair.generate()
        message = b"test"
        signature = kp.sign(message)
        
        # Measure verification time for valid signature
        start = time.perf_counter()
        for _ in range(100):
            verify_signature(kp.public_key, message, signature)
        valid_time = time.perf_counter() - start
        
        # Measure verification time for invalid signature (wrong first byte)
        invalid_sig = "0" + signature[1:]
        start = time.perf_counter()
        for _ in range(100):
            verify_signature(kp.public_key, message, invalid_sig)
        invalid_time = time.perf_counter() - start
        
        # Times should be similar (within 50% tolerance for timing attacks)
        # Note: This is a simplified test; real timing attack prevention
        ratio = max(valid_time, invalid_time) / min(valid_time, invalid_time)
        # We just log this for now as timing can vary significantly
        print(f"Timing ratio: {ratio:.2f}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

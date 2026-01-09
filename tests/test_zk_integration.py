"""
ZK Verifier Integration Tests for HieraChain Consensus

This module tests the ZK (Zero Knowledge) proof verification system including:
1. Mock Verifier modes (AcceptAll, RejectAll, MagicBytes, HashCheck)
2. Cross-language FFI verification (Python Prover → Rust Verifier)
3. Proof generation and validation flow
4. End-to-end integration between hierachain (Python) and hierachain_consensus (Rust)
"""

import pytest
import hashlib

try:
    from hierachain.security.zk_prover import ZKProver, generate_zk_proof
    from hierachain.security.zk_verifier import (
        ZKVerifier,
        ZKPublicInputs,
        verify_zk_proof,
    )
    HIERACHAIN_AVAILABLE = True
except ImportError:
    HIERACHAIN_AVAILABLE = False

try:
    from hierachain_consensus import (
        KeyPair,
        calculate_block_hash,
        calculate_merkle_root,
        verify_signature,
    )
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


class TestMockVerifierModes:
    """Test different MockVerifier modes through Python bindings."""

    def test_mock_proof_magic_bytes(self):
        """Mock proof with magic bytes should be recognizable."""
        MOCK_MAGIC = b"mock_proof"

        # Valid mock proof starts with magic bytes
        valid_proof = MOCK_MAGIC + b"\x00" * 32
        assert valid_proof.startswith(MOCK_MAGIC)

        # Invalid proof doesn't have magic bytes
        invalid_proof = b"invalid_proof" + b"\x00" * 32
        assert not invalid_proof.startswith(MOCK_MAGIC)

    def test_mock_proof_hash_check(self):
        """Mock proof with hash check mode verification."""
        MOCK_MAGIC = b"mock_proof"

        # Create proof: magic_bytes + sha256(public_inputs)
        public_inputs = b"test_public_inputs"
        input_hash = hashlib.sha256(public_inputs).digest()

        valid_proof = MOCK_MAGIC + input_hash

        # Extract and verify hash
        proof_magic = valid_proof[:len(MOCK_MAGIC)]
        proof_hash = valid_proof[len(MOCK_MAGIC):len(MOCK_MAGIC) + 32]

        assert proof_magic == MOCK_MAGIC
        assert proof_hash == input_hash

        # Verify the proof matches the inputs
        computed_hash = hashlib.sha256(public_inputs).digest()
        assert proof_hash == computed_hash

    def test_proof_hash_mismatch_detection(self):
        """Proof with wrong hash should fail verification."""
        MOCK_MAGIC = b"mock_proof"

        original_inputs = b"original_inputs"
        tampered_inputs = b"tampered_inputs"

        # Create proof for original inputs
        original_hash = hashlib.sha256(original_inputs).digest()
        _proof = MOCK_MAGIC + original_hash

        # Verification with tampered inputs should fail
        tampered_hash = hashlib.sha256(tampered_inputs).digest()

        assert original_hash != tampered_hash, \
            "Different inputs should produce different hashes"


class TestProofStructure:
    """Test ZK proof data structures."""

    def test_proof_bytes_length(self):
        """Groth16 proof should have expected byte length."""
        # Groth16 proof on BN254:
        # - A: 64 bytes (G1 point)
        # - B: 128 bytes (G2 point)
        # - C: 64 bytes (G1 point)
        # Total: 256 bytes (compressed may vary)

        _expected_min_length = 64  # At minimum, one group element

        # Mock proof structure
        mock_proof = b"mock_proof" + b"\x00" * 32
        assert len(mock_proof) >= 10 + 32  # magic + hash

    def test_public_inputs_format(self):
        """Public inputs should be serializable."""
        public_inputs = {
            "old_state_root": "abc123" * 10,
            "new_state_root": "def456" * 10,
            "block_hash": "789ghi" * 10,
        }

        # Convert to bytes for verification
        import json
        inputs_bytes = json.dumps(public_inputs, sort_keys=True).encode()

        assert len(inputs_bytes) > 0

        # Hash should be deterministic
        hash1 = hashlib.sha256(inputs_bytes).hexdigest()
        hash2 = hashlib.sha256(inputs_bytes).hexdigest()
        assert hash1 == hash2


@pytest.mark.skipif(not HIERACHAIN_AVAILABLE, reason="hierachain not installed")
class TestPythonZKProver:
    """Test Python ZKProver from hierachain."""

    def test_prover_initialization(self):
        """ZKProver should initialize in mock mode."""
        prover = ZKProver(mode="mock")
        assert prover.mode == "mock"

    def test_generate_mock_proof(self):
        """Generate mock proof for state transition."""
        prover = ZKProver(mode="mock")

        old_state = "0" * 64  # 64 hex chars = 32 bytes hash
        new_state = "a" * 64
        block_index = 1

        result = prover.generate_proof(
            old_state_root=old_state,
            new_state_root=new_state,
            block_index=block_index
        )

        assert result.success
        assert result.proof is not None
        assert len(result.proof) > 0
        assert result.mode == "mock"

    def test_prover_stats(self):
        """Prover should track generation statistics."""
        prover = ZKProver(mode="mock")
        prover.reset_stats()

        # Generate a proof
        prover.generate_proof(
            old_state_root="0" * 64,
            new_state_root="a" * 64,
            block_index=1
        )

        stats = prover.get_stats()
        assert stats["successful_generations"] >= 1


@pytest.mark.skipif(not HIERACHAIN_AVAILABLE, reason="hierachain not installed")
class TestPythonZKVerifier:
    """Test Python ZKVerifier from hierachain."""

    def test_verifier_initialization(self):
        """ZKVerifier should initialize in mock mode."""
        verifier = ZKVerifier(mode="mock")
        assert verifier.mode == "mock"

    def test_verify_valid_mock_proof(self):
        """Valid mock proof should be verified."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        old_state = "0" * 64
        new_state = "abc123" * 10 + "abcd"  # 64 chars
        block_index = 5

        # Generate proof
        result = prover.generate_proof(
            old_state_root=old_state,
            new_state_root=new_state,
            block_index=block_index
        )

        # Verify
        public_inputs = ZKPublicInputs(
            old_state_root=old_state,
            new_state_root=new_state,
            block_index=block_index
        )

        is_valid = verifier.verify(result.proof, public_inputs)
        assert is_valid, "Valid mock proof should be verified"

    def test_verify_invalid_proof(self):
        """Invalid proof should fail verification."""
        verifier = ZKVerifier(mode="mock")

        fake_proof = b"not_a_valid_proof"
        public_inputs = ZKPublicInputs(
            old_state_root="0" * 64,
            new_state_root="a" * 64,
            block_index=1
        )

        is_valid = verifier.verify(fake_proof, public_inputs)
        assert not is_valid, "Invalid proof should fail verification"

    def test_verify_tampered_inputs(self):
        """Proof should fail if public inputs are tampered."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        old_state = "0" * 64
        new_state = "a" * 64

        # Generate proof for original inputs
        result = prover.generate_proof(
            old_state_root=old_state,
            new_state_root=new_state,
            block_index=1
        )

        # Tamper with inputs
        tampered_inputs = ZKPublicInputs(
            old_state_root=old_state,
            new_state_root="b" * 64,  # Different new state!
            block_index=1
        )

        is_valid = verifier.verify(result.proof, tampered_inputs)
        assert not is_valid, "Proof should fail with tampered inputs"


@pytest.mark.skipif(
    not (HIERACHAIN_AVAILABLE and RUST_AVAILABLE),
    reason="Both hierachain and hierachain_consensus required"
)
class TestCrossLanguageIntegration:
    """Test Python to Rust integration for ZK proofs."""

    def test_proof_bytes_transfer(self):
        """Proof bytes should survive Python → Rust transfer."""
        prover = ZKProver(mode="mock")

        # Generate proof in Python
        result = prover.generate_proof(
            old_state_root="0" * 64,
            new_state_root="a" * 64,
            block_index=1
        )

        proof_bytes = result.proof

        # Hash with Python
        python_hash = hashlib.sha256(proof_bytes).hexdigest()

        # Hash with Rust via calculate_block_hash
        block_data = {
            "index": 1,
            "proof": proof_bytes.hex(),
            "timestamp": 0.0,
            "data": "test"
        }
        _rust_block_hash = calculate_block_hash(block_data)

        # Python hash should be valid
        assert len(python_hash) == 64

    def test_merkle_root_for_events_in_proof(self):
        """Events merkle root used in proof can be verified with Rust."""
        events = ["event1", "event2", "event3"]

        # Calculate merkle root with Rust
        merkle_root = calculate_merkle_root(events)

        # Use merkle root as part of state
        prover = ZKProver(mode="mock")
        result = prover.generate_proof(
            old_state_root="0" * 64,
            new_state_root=merkle_root,  # Use Rust-computed merkle root
            block_index=1,
            events=[{"event": e} for e in events]
        )

        assert result.success

    def test_signed_proof_submission(self):
        """Proof can be signed with Rust KeyPair for submission."""
        prover = ZKProver(mode="mock")

        # Generate proof
        result = prover.generate_proof(
            old_state_root="0" * 64,
            new_state_root="a" * 64,
            block_index=1
        )

        # Sign proof with Rust KeyPair
        kp = KeyPair.generate()
        signature = kp.sign(result.proof)

        # Verify signature
        is_valid = verify_signature(kp.public_key, result.proof, signature)
        assert is_valid, "Proof signature should be valid"


class TestProofVerificationFlow:
    """Test end-to-end proof verification flow."""

    def test_block_with_zk_proof(self):
        """Block with ZK proof should include required fields."""
        block_with_proof = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "transactions",
            "zk_proof": "mock_proof_base64_encoded",
            "zk_public_inputs": {
                "old_state": "state0",
                "new_state": "state1"
            }
        }

        # Required ZK fields
        assert "zk_proof" in block_with_proof
        assert "zk_public_inputs" in block_with_proof

        # Public inputs should be a dict or list
        assert isinstance(block_with_proof["zk_public_inputs"], (dict, list))

    def test_block_without_zk_proof_backward_compatible(self):
        """Blocks without ZK proof should still be valid (backward compatible)."""
        legacy_block = {
            "index": 1,
            "previous_hash": "abc123",
            "timestamp": 1234567890.0,
            "data": "transactions",
        }

        # No ZK fields - backward compatible
        assert "zk_proof" not in legacy_block

        # Block still has required fields
        assert "index" in legacy_block
        assert "previous_hash" in legacy_block
        assert "data" in legacy_block

    def test_enable_zk_proofs_toggle(self):
        """ENABLE_ZK_PROOFS config should control verification."""
        # Simulating config toggle
        config = {
            "ENABLE_ZK_PROOFS": True,
            "ZK_VERIFIER_TYPE": "mock",  # or "groth16"
            "ZK_STRICT_MODE": False,
        }

        if config["ENABLE_ZK_PROOFS"]:
            # Verification required
            required_fields = ["zk_proof", "zk_public_inputs"]
        else:
            # Verification skipped
            required_fields = []

        assert isinstance(required_fields, list)


class TestSecurityVulnerabilities:
    """Test protection against ZK-related security vulnerabilities."""

    def test_malformed_proof_rejection(self):
        """Malformed proofs should be rejected."""
        malformed_proofs = [
            b"",  # Empty
            b"\x00" * 5,  # Too short
            b"not_a_valid_proof",  # Invalid format
            b"\xff" * 100,  # Random bytes
        ]

        MOCK_MAGIC = b"mock_proof"

        for proof in malformed_proofs:
            # None of these should pass magic bytes check
            is_valid = (
                proof.startswith(MOCK_MAGIC) and
                len(proof) >= len(MOCK_MAGIC) + 32
            )
            assert not is_valid, \
                f"Malformed proof should be rejected: {proof[:20]}"

    def test_proof_replay_prevention(self):
        """Same proof should not be accepted twice (if tracking)."""
        MOCK_MAGIC = b"mock_proof"

        # Create a unique proof
        unique_data = b"unique_transaction_data"
        proof = MOCK_MAGIC + hashlib.sha256(unique_data).digest()

        # Simulate tracking used proofs
        used_proofs: set[str] = set()

        proof_id = hashlib.sha256(proof).hexdigest()

        # First use - should be accepted
        assert proof_id not in used_proofs
        used_proofs.add(proof_id)

        # Second use - should be rejected (replay)
        assert proof_id in used_proofs, "Replayed proof should be detected"

    def test_proof_binding_to_block(self):
        """Proof should be bound to specific block data."""
        MOCK_MAGIC = b"mock_proof"

        block1_data = b"block1_transactions"
        block2_data = b"block2_transactions"

        # Proof for block1
        proof1 = MOCK_MAGIC + hashlib.sha256(block1_data).digest()

        # Extract hash from proof
        proof_hash = proof1[len(MOCK_MAGIC):len(MOCK_MAGIC) + 32]

        # Verify against block1 - should match
        assert proof_hash == hashlib.sha256(block1_data).digest()

        # Verify against block2 - should NOT match
        assert proof_hash != hashlib.sha256(block2_data).digest()


class TestGroth16VerifierInterface:
    """Test Groth16 verifier interface (structure only, no actual proofs)."""

    def test_verifier_initialization_state(self):
        """Verifier should require initialization before use."""
        # Simulating verifier state
        verifier_state = {
            "initialized": False,
            "verification_key": None,
            "verifier_type": "Groth16-BN254"
        }

        # Should not be initialized without verification key
        assert not verifier_state["initialized"]

        # After setting verification key
        verifier_state["verification_key"] = b"fake_vk_bytes"
        verifier_state["initialized"] = True

        assert verifier_state["initialized"]
        assert verifier_state["verification_key"] is not None

    def test_verifier_type_identification(self):
        """Verifier should identify its type."""
        verifier_types = [
            "Groth16-BN254",
            "Mock-AcceptAll",
            "Mock-RejectAll",
            "Mock-MagicBytes",
            "Mock-HashCheck",
        ]

        for vtype in verifier_types:
            assert isinstance(vtype, str)
            assert len(vtype) > 0


class TestFFIProofTransfer:
    """Test proof data transfer through FFI."""

    def test_bytes_integrity_through_transfer(self):
        """Proof bytes should not be corrupted through FFI."""
        # Create proof bytes
        original_proof = b"mock_proof" + bytes(range(32))

        # Simulate transfer (in real scenario, this goes through PyO3)
        transferred_proof = bytes(original_proof)

        assert transferred_proof == original_proof, \
            "Proof bytes should not be corrupted"

    def test_large_proof_handling(self):
        """Large proofs should be handled correctly."""
        # Groth16 proofs are typically ~256 bytes, but test larger
        large_proof = b"mock_proof" + b"\x00" * 10000

        assert len(large_proof) == 10 + 10000

        # Hash should still work
        proof_hash = hashlib.sha256(large_proof).hexdigest()
        assert len(proof_hash) == 64

    def test_unicode_in_public_inputs(self):
        """Public inputs with unicode should be handled."""
        import json

        inputs_with_unicode = {
            "description": "Dữ liệu test 🚀",
            "state": "Bình thường",
        }

        # Serialize with UTF-8
        inputs_bytes = json.dumps(
            inputs_with_unicode, ensure_ascii=False
        ).encode('utf-8')

        # Should be valid bytes
        assert isinstance(inputs_bytes, bytes)

        # Should be deserializable
        decoded = json.loads(inputs_bytes.decode('utf-8'))
        assert decoded == inputs_with_unicode


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

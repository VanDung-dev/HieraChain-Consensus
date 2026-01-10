"""
ZK Proof Data Transmission Tests.

Tests data integrity for Zero-Knowledge proofs:
- Mock proof generation
- Proof verification
- Format compatibility between Python and Rust
- Public inputs serialization
"""

import pytest
import json
import hashlib
import time
from typing import Any

# Python imports
from hierachain.security.zk_prover import ZKProver
from hierachain.security.zk_verifier import ZKVerifier

# Rust imports - check for mock verifier exposure
RUST_AVAILABLE = False

try:
    import hierachain_consensus
    RUST_AVAILABLE = True
except ImportError:
    pass


def create_zk_inputs() -> tuple[str, str, int]:
    """Create standard ZK public inputs."""
    old_root = "0" * 64
    new_root = "a" * 64
    block_index = 100
    return old_root, new_root, block_index


class TestZKProverPython:
    """Test ZK Prover Python implementation."""

    def test_prover_creation_mock(self):
        """Test creating prover in mock mode."""
        prover = ZKProver(mode="mock")
        assert prover is not None
        assert prover.mode == "mock"

    def test_proof_generation(self):
        """Test generating a proof."""
        prover = ZKProver(mode="mock")
        old_root, new_root, block_index = create_zk_inputs()

        result = prover.generate_proof(old_root, new_root, block_index)

        assert result.success is True
        assert result.proof is not None
        assert len(result.proof) > 0

    def test_proof_has_magic_bytes(self):
        """Test proof has correct magic bytes prefix."""
        prover = ZKProver(mode="mock")
        old_root, new_root, block_index = create_zk_inputs()

        result = prover.generate_proof(old_root, new_root, block_index)

        assert result.proof.startswith(b"mock_proof")

    def test_proof_structure(self):
        """Test proof has correct structure (magic + 32-byte hash)."""
        prover = ZKProver(mode="mock")
        old_root, new_root, block_index = create_zk_inputs()

        result = prover.generate_proof(old_root, new_root, block_index)

        magic = b"mock_proof"
        expected_length = len(magic) + 32  # SHA256
        assert len(result.proof) == expected_length

    def test_proof_deterministic(self):
        """Test same inputs produce same proof."""
        prover = ZKProver(mode="mock")
        old_root, new_root, block_index = create_zk_inputs()

        result1 = prover.generate_proof(old_root, new_root, block_index)
        result2 = prover.generate_proof(old_root, new_root, block_index)

        assert result1.proof == result2.proof

    def test_different_inputs_different_proofs(self):
        """Test different inputs produce different proofs."""
        prover = ZKProver(mode="mock")

        result1 = prover.generate_proof("0" * 64, "a" * 64, 1)
        result2 = prover.generate_proof("0" * 64, "b" * 64, 1)

        assert result1.proof != result2.proof

    def test_prover_stats(self):
        """Test prover statistics."""
        prover = ZKProver(mode="mock")

        # Generate some proofs
        for i in range(5):
            prover.generate_proof(f"{i:064x}", f"{i+1:064x}", i)

        stats = prover.get_stats()
        # Check stats dict is not empty (keys may vary)
        assert len(stats) > 0


class TestZKVerifierPython:
    """Test ZK Verifier Python implementation."""

    def test_verifier_creation_mock(self):
        """Test creating verifier in mock mode."""
        verifier = ZKVerifier(mode="mock")
        assert verifier is not None
        assert verifier.mode == "mock"

    def test_valid_proof_verification(self):
        """Test verifying a valid proof."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        old_root, new_root, block_index = create_zk_inputs()

        result = prover.generate_proof(old_root, new_root, block_index)

        public_inputs = {
            "old_state_root": old_root,
            "new_state_root": new_root,
            "block_index": block_index,
            "sub_chain_name": ""
        }

        is_valid = verifier.verify(result.proof, public_inputs)
        assert is_valid is True

    def test_invalid_proof_rejected(self):
        """Test that invalid proof is rejected."""
        verifier = ZKVerifier(mode="mock")

        fake_proof = b"fake_proof_data"
        public_inputs = {
            "old_state_root": "0" * 64,
            "new_state_root": "a" * 64,
            "block_index": 1,
            "sub_chain_name": ""
        }

        is_valid = verifier.verify(fake_proof, public_inputs)
        assert is_valid is False

    def test_mismatched_inputs_rejected(self):
        """Test that proof with wrong inputs is rejected."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        # Generate proof with one set of inputs
        result = prover.generate_proof("0" * 64, "a" * 64, 1)

        # Try to verify with different inputs
        wrong_inputs = {
            "old_state_root": "1" * 64,  # Different!
            "new_state_root": "a" * 64,
            "block_index": 1,
            "sub_chain_name": ""
        }

        is_valid = verifier.verify(result.proof, wrong_inputs)
        assert is_valid is False


class TestZKPublicInputs:
    """Test ZK public inputs serialization."""

    def test_json_serialization_format(self):
        """Test public inputs serialize to correct JSON format."""
        old_root = "abc"
        new_root = "def"
        block_index = 123

        public_inputs = {
            "block_index": block_index,
            "new_state_root": new_root,
            "old_state_root": old_root,
            "sub_chain_name": ""
        }

        # Must use sort_keys=True for deterministic output
        json_bytes = json.dumps(public_inputs, sort_keys=True).encode('utf-8')

        expected = b'{"block_index": 123, "new_state_root": "def", "old_state_root": "abc", "sub_chain_name": ""}'
        assert json_bytes == expected

    def test_hash_matches_proof(self):
        """Test that hash in proof matches public inputs."""
        prover = ZKProver(mode="mock")

        old_root = "abc"
        new_root = "def"
        block_index = 123

        result = prover.generate_proof(old_root, new_root, block_index)

        # Compute expected hash
        public_inputs = {
            "block_index": block_index,
            "new_state_root": new_root,
            "old_state_root": old_root,
            "sub_chain_name": ""
        }
        json_bytes = json.dumps(public_inputs, sort_keys=True).encode('utf-8')
        expected_hash = hashlib.sha256(json_bytes).digest()

        # Extract hash from proof
        magic = b"mock_proof"
        actual_hash = result.proof[len(magic):len(magic) + 32]

        assert actual_hash == expected_hash


class TestZKRustCompatibility:
    """Test ZK compatibility with Rust."""

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
    def test_proof_format_rust_compatible(self):
        """Test proof format is compatible with Rust verifier expectations."""
        prover = ZKProver(mode="mock")
        old_root, new_root, block_index = create_zk_inputs()

        result = prover.generate_proof(old_root, new_root, block_index)

        # Rust expects: magic_bytes + sha256(json_public_inputs)
        magic = b"mock_proof"

        assert result.proof.startswith(magic)
        assert len(result.proof) == len(magic) + 32


class TestZKPerformance:
    """Performance sanity checks for ZK operations."""

    def test_proof_generation_speed(self):
        """Test proof generation speed."""
        prover = ZKProver(mode="mock")

        start = time.perf_counter()
        for i in range(100):
            prover.generate_proof(f"{i:064x}", f"{i+1:064x}", i)
        elapsed = time.perf_counter() - start

        # Should generate 100 proofs in less than 0.1 seconds
        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 proofs"

    def test_verification_speed(self):
        """Test verification speed."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        # Pre-generate proofs
        proofs = []
        for i in range(100):
            old_root = f"{i:064x}"
            new_root = f"{i+1:064x}"
            result = prover.generate_proof(old_root, new_root, i)
            proofs.append((result.proof, {
                "old_state_root": old_root,
                "new_state_root": new_root,
                "block_index": i,
                "sub_chain_name": ""
            }))

        start = time.perf_counter()
        for proof, inputs in proofs:
            verifier.verify(proof, inputs)
        elapsed = time.perf_counter() - start

        # Should verify 100 proofs in less than 0.1 seconds
        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 verifications"

    def test_end_to_end_throughput(self):
        """Test end-to-end throughput."""
        prover = ZKProver(mode="mock")
        verifier = ZKVerifier(mode="mock")

        start = time.perf_counter()
        success_count = 0

        for i in range(100):
            old_root = f"{i:064x}"
            new_root = f"{i+1:064x}"

            result = prover.generate_proof(old_root, new_root, i)

            inputs = {
                "old_state_root": old_root,
                "new_state_root": new_root,
                "block_index": i,
                "sub_chain_name": ""
            }

            if verifier.verify(result.proof, inputs):
                success_count += 1

        elapsed = time.perf_counter() - start

        assert success_count == 100
        assert elapsed < 0.2, f"Too slow: {elapsed:.2f}s for 100 E2E flows"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

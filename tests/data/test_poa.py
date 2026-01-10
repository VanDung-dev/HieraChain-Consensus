"""
Proof of Authority (PoA) Data Transmission Tests.

Tests data integrity for PoA consensus:
- Block validation with authority
- Block creation and hashing
- Python vs Rust implementation consistency
"""

import pytest
import time
from typing import Any

# Python imports
from hierachain.core.block import Block as PyBlock

# Rust imports
RUST_AVAILABLE = False
RustBlock = None
RustPoA = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "Block"):
        RustBlock = hierachain_consensus.Block
    if hasattr(hierachain_consensus, "ProofOfAuthority"):
        RustPoA = hierachain_consensus.ProofOfAuthority
    if hasattr(hierachain_consensus, "validate_poa_block"):
        validate_poa_block = hierachain_consensus.validate_poa_block
    else:
        validate_poa_block = None
    RUST_AVAILABLE = True
except ImportError:
    validate_poa_block = None


def create_poa_block_data() -> dict[str, Any]:
    """Create test block data for PoA validation."""
    return {
        "index": 1,
        "timestamp": time.time(),
        "events": [
            {"entity_id": "e1", "event": "test", "timestamp": time.time()},
            {"entity_id": "e2", "event": "test", "timestamp": time.time()},
        ],
        "previous_hash": "0" * 64,
        "hash": "a" * 64,
    }


class TestPoAPython:
    """Test PoA with Python implementation."""

    def test_block_creation(self):
        """Test block creation in Python."""
        events = [
            {"entity_id": "entity1", "event": "created", "timestamp": 1000.0},
            {"entity_id": "entity2", "event": "updated", "timestamp": 1001.0},
        ]
        block = PyBlock(index=1, events=events)

        assert block.index == 1
        assert len(block.events) == 2
        assert block.hash is not None
        assert len(block.hash) == 64

    def test_block_hash_deterministic(self):
        """Test that same events produce same hash."""
        events = [
            {"entity_id": "e1", "event": "test", "timestamp": 1000.0},
        ]
        block1 = PyBlock(index=1, events=events)
        block2 = PyBlock(index=1, events=events)

        # Same input should produce same merkle root
        assert block1.merkle_root == block2.merkle_root

    def test_block_with_previous_hash(self):
        """Test block creation with previous hash."""
        events = [{"entity_id": "e1", "event": "test", "timestamp": 1000.0}]
        prev_hash = "abc123" + "0" * 58

        block = PyBlock(index=2, events=events, previous_hash=prev_hash)

        assert block.previous_hash == prev_hash
        assert block.index == 2


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
class TestPoARust:
    """Test PoA with Rust implementation."""

    def test_rust_block_creation(self):
        """Test block creation in Rust."""
        events = [
            {"entity_id": "entity1", "event": "created", "timestamp": 1000.0},
            {"entity_id": "entity2", "event": "updated", "timestamp": 1001.0},
        ]
        block = RustBlock(index=1, events=events)

        assert block.index == 1
        assert len(block.hash) == 64

    @pytest.mark.skipif(validate_poa_block is None, reason="validate_poa_block not exposed")
    def test_poa_block_validation(self):
        """Test PoA block validation in Rust."""
        block_data = create_poa_block_data()
        authority_id = "authority_node_1"

        result = validate_poa_block(block_data, authority_id)
        assert result is True

    @pytest.mark.skipif(validate_poa_block is None, reason="validate_poa_block not exposed")
    def test_poa_validation_empty_authority_fails(self):
        """Test that empty authority ID fails validation."""
        block_data = create_poa_block_data()

        result = validate_poa_block(block_data, "")
        assert result is False

    def test_merkle_root_consistency(self):
        """Test Merkle root matches between Python and Rust."""
        events = [
            {"entity_id": "e1", "event": "test", "timestamp": 1000.0},
            {"entity_id": "e2", "event": "test", "timestamp": 1001.0},
        ]

        py_block = PyBlock(index=1, events=events)
        rs_block = RustBlock(index=1, events=events)

        assert py_block.merkle_root == rs_block.merkle_root


class TestPoAPerformance:
    """Performance sanity checks for PoA."""

    def test_python_block_creation_speed(self):
        """Test Python block creation is fast."""
        events = [{"entity_id": f"e{i}", "event": "test", "timestamp": 1000.0 + i}
                  for i in range(10)]

        start = time.perf_counter()
        for _ in range(100):
            PyBlock(index=1, events=events)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Too slow: {elapsed:.2f}s for 100 blocks"

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
    def test_rust_block_creation_speed(self):
        """Test Rust block creation is fast."""
        events = [{"entity_id": f"e{i}", "event": "test", "timestamp": 1000.0 + i}
                  for i in range(10)]

        start = time.perf_counter()
        for _ in range(100):
            RustBlock(index=1, events=events)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.2, f"Too slow: {elapsed:.2f}s for 100 blocks"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

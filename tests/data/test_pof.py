"""
Proof of Federation (PoF) Data Transmission Tests.

Tests data integrity for PoF consensus:
- Federation member management
- Weighted voting validation
- Cross-implementation consistency
"""

import pytest
import time
from typing import Any

# Python imports
from hierachain.hierarchical.main_chain import MainChain as PyMainChain

# Rust imports
RUST_AVAILABLE = False
RustMainChain = None
RustPoF = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "MainChain"):
        RustMainChain = hierachain_consensus.MainChain
    if hasattr(hierachain_consensus, "ProofOfFederation"):
        RustPoF = hierachain_consensus.ProofOfFederation
    RUST_AVAILABLE = True
except ImportError:
    pass


def create_pof_metadata() -> dict[str, Any]:
    """Create test metadata for PoF operations."""
    return {
        "block_index": 1,
        "events_count": 5,
        "merkle_root": "f" * 64,
        "federation_weight": 0.75,
    }


class TestPoFPython:
    """Test PoF with Python implementation."""

    def test_mainchain_pof_creation(self):
        """Test creating MainChain with PoF consensus."""
        # Python MainChain uses settings for consensus type
        main = PyMainChain("test_pof_main")
        assert main is not None

    def test_subchain_registration_pof(self):
        """Test sub-chain registration in PoF mode."""
        main = PyMainChain("test_pof_reg")

        success = main.register_sub_chain("FedMember1", {
            "domain": "logistics",
            "weight": 1.0
        })
        assert success is True

        stats = main.get_main_chain_stats()
        assert stats["registered_sub_chains"] >= 1

    def test_multiple_federation_members(self):
        """Test registering multiple federation members."""
        main = PyMainChain("test_pof_multi")

        for i in range(5):
            success = main.register_sub_chain(f"Member_{i}", {
                "domain": f"domain_{i}",
                "weight": 1.0 / (i + 1)
            })
            assert success is True

        stats = main.get_main_chain_stats()
        assert stats["registered_sub_chains"] >= 5

    def test_proof_submission_pof(self):
        """Test proof submission in PoF context."""
        main = PyMainChain("test_pof_proof")
        main.register_sub_chain("FedNode", {"domain": "test"})

        metadata = create_pof_metadata()
        success = main.add_proof("FedNode", "fed_proof_001", metadata)
        assert success is True


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
class TestPoFRust:
    """Test PoF with Rust implementation."""

    def test_rust_mainchain_pof_creation(self):
        """Test creating Rust MainChain with PoF."""
        # Try to create with PoF if supported
        if hasattr(RustMainChain, "with_pof"):
            main = RustMainChain.with_pof("rust_pof_main")
        else:
            main = RustMainChain("rust_pof_main", "proof_of_federation")
        assert main is not None

    def test_rust_subchain_registration(self):
        """Test sub-chain registration in Rust PoF."""
        if hasattr(RustMainChain, "with_pof"):
            main = RustMainChain.with_pof("rust_pof_reg")
        else:
            main = RustMainChain("rust_pof_reg", "proof_of_federation")

        main.register_sub_chain("RustFedMember", {"domain": "test"})

        stats = main.get_main_chain_stats()
        assert stats["registered_sub_chains"] >= 1

    def test_rust_proof_addition(self):
        """Test proof addition in Rust PoF."""
        if hasattr(RustMainChain, "with_pof"):
            main = RustMainChain.with_pof("rust_pof_add")
        else:
            main = RustMainChain("rust_pof_add", "proof_of_federation")

        main.register_sub_chain("RustNode", {"domain": "test"})

        metadata = create_pof_metadata()
        success = main.add_proof("RustNode", "rust_fed_proof", metadata)
        assert success is True


class TestPoFCrossImplementation:
    """Test PoF consistency between Python and Rust."""

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
    def test_registration_data_format(self):
        """Test that registration data format is consistent."""
        py_main = PyMainChain("py_pof_compare")
        if hasattr(RustMainChain, "with_pof"):
            rs_main = RustMainChain.with_pof("rs_pof_compare")
        else:
            rs_main = RustMainChain("rs_pof_compare", "proof_of_federation")

        reg_data = {"domain": "shared", "weight": 0.5}

        py_success = py_main.register_sub_chain("SharedMember", reg_data)
        rs_success = rs_main.register_sub_chain("SharedMember", reg_data)

        assert py_success == rs_success


class TestPoFPerformance:
    """Performance sanity checks for PoF."""

    def test_python_registration_speed(self):
        """Test Python registration speed."""
        main = PyMainChain("perf_pof_py")

        start = time.perf_counter()
        for i in range(50):
            main.register_sub_chain(f"Member_{i}", {"domain": f"d_{i}"})
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Too slow: {elapsed:.2f}s for 50 registrations"

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust not available")
    def test_rust_registration_speed(self):
        """Test Rust registration speed."""
        if hasattr(RustMainChain, "with_pof"):
            main = RustMainChain.with_pof("perf_pof_rs")
        else:
            main = RustMainChain("perf_pof_rs", "proof_of_federation")

        start = time.perf_counter()
        for i in range(50):
            main.register_sub_chain(f"Member_{i}", {"domain": f"d_{i}"})
        elapsed = time.perf_counter() - start

        assert elapsed < 0.2, f"Too slow: {elapsed:.2f}s for 50 registrations"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

"""
Demo: Proof of Federation (PoF) Consensus

This demo showcases the integration of BOTH libraries for PoF consensus:
- hierachain (Python): Rich domain logic and SubChain operations
- hierachain_consensus (Rust): High-performance consensus and crypto

Features demonstrated:
1. Validator key generation (Rust)
2. SubChain creation with PoF (Python + Rust comparison)
3. Federation management and validator setup
4. Cross-chain proof anchoring (MainChain)
5. Hierarchical chain architecture
"""

import sys
import os
import time
import json

# Add parent directory to path for hierachain Python package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
#  Import Both Libraries
# ============================================================

# Python implementation (hierachain)
try:
    from hierachain.core.utils import MerkleTree as PyMerkleTree
    from hierachain.hierarchical.main_chain import MainChain as PyMainChain
    from hierachain.hierarchical.sub_chain import SubChain as PySubChain
    import hierachain
    py_version = hierachain.__version__
    print(f"[OK] Loaded hierachain (Python) v{py_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain: {e}")
    exit(1)

# Rust implementation (hierachain_consensus)
try:
    import hierachain_consensus as hc
    from hierachain_consensus import (
        MainChain as RustMainChain,
        SubChain as RustSubChain,
        KeyPair,
        ProofOfFederation,
        calculate_merkle_root as rust_merkle_root,
    )
    rust_version = hc.__version__
    print(f"[OK] Loaded hierachain_consensus (Rust) v{rust_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain_consensus: {e}")
    exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def demo_validator_setup():
    """Demonstrate validator key pair generation (Rust)."""
    print_section("1. Validator Setup for Federation (Rust)")

    validators = {}
    for i in range(1, 5):  # Create 4 validators
        name = f"validator_{i}"
        validators[name] = KeyPair.generate()
        print(f"  [OK] {name}: {validators[name].public_key[:24]}...")

    print(f"\n  Total validators: {len(validators)}")
    quorum = (len(validators) * 2) // 3 + 1
    print(f"  Quorum requirement (2/3): {quorum} validators")

    return validators


def demo_pof_consensus(validators):
    """Demonstrate PoF consensus mechanism (Rust)."""
    print_section("2. Proof of Federation Consensus (Rust)")

    # Create PoF instance using Rust
    pof = ProofOfFederation()
    print(f"PoF Name: {pof.name}")

    # Add validators with keys (Rust requires dict format)
    for name, keypair in validators.items():
        pof.add_validator(name, {"public_key": keypair.public_key})
        print(f"  [OK] Added validator: {name}")

    print(f"\nValidator count: {pof.get_validator_count()}")

    # Get current leader (round-robin based on block index)
    leader = pof.get_current_leader(block_index=0)
    print(f"Current Leader (block 0): {leader}")

    return pof


def demo_subchain_creation():
    """Demonstrate SubChain creation: Python vs Rust."""
    print_section("3. SubChain Creation with PoF (Python vs Rust)")

    # ===== Python SubChain =====
    print("Python SubChain (hierachain):")
    py_subchain = PySubChain(
        name="PyAcademicRecords",
        domain_type="education"
    )
    print(f"  Name: {py_subchain.name}")
    print(f"  Domain Type: {py_subchain.domain_type}")
    print(f"  Chain Length: {len(py_subchain.chain)}")

    # ===== Rust SubChain =====
    print("\nRust SubChain (hierachain_consensus):")
    rust_subchain = RustSubChain(
        name="RustAcademicRecords",
        domain_type="education",
        consensus_type="proof_of_federation"
    )
    print(f"  Name: {rust_subchain.name}")
    print(f"  Domain Type: {rust_subchain.domain_type}")
    print(f"  Consensus Type: {rust_subchain.consensus_type}")
    print(f"  Block Count: {rust_subchain.block_count}")

    return py_subchain, rust_subchain


def demo_domain_operations(py_subchain, rust_subchain):
    """Demonstrate domain operations on SubChains."""
    print_section("4. Domain Operations (Python + Rust)")

    # Create academic events
    events = [
        {
            "event": "grade_recorded",
            "entity_id": "STUDENT001",
            "course_id": "CS101",
            "grade": "A",
            "timestamp": time.time(),
        },
        {
            "event": "certificate_issued",
            "entity_id": "STUDENT001",
            "certificate_type": "completion",
            "timestamp": time.time(),
        },
    ]

    # Add events to Python SubChain (using add_event - common API)
    print("Python SubChain operations:")
    for event in events:
        py_subchain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # Python SubChain also supports rich domain operations
    result = py_subchain.start_operation(
        entity_id="STUDENT002",
        operation_type="enrollment",
        details={"course_id": "CS102", "semester": "Spring2026"}
    )
    print(f"  [OK] Started operation: enrollment -> {result}")

    # Rust SubChain (uses add_event)
    print("\nRust SubChain operations:")
    for event in events:
        rust_subchain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    return events


def demo_mainchain_anchoring(events):
    """Demonstrate cross-chain proof anchoring."""
    print_section("5. MainChain Anchoring (Hierarchical)")

    # ===== Create MainChains =====
    print("Creating MainChains...")

    # Python MainChain
    py_main = PyMainChain(name="PyHieraChainMain")
    py_main.register_sub_chain("PyFinance", {"domain": "finance"})
    print(f"  Python MainChain: {py_main.name}")
    print(f"    Registered: {list(py_main.registered_sub_chains)}")

    # Rust MainChain
    rust_main = RustMainChain(
        name="RustHieraChainMain",
        consensus_type="proof_of_authority"
    )
    rust_main.register_sub_chain("RustFinance", {"domain": "finance"})
    print(f"  Rust MainChain: {rust_main.name}")
    print(f"    Registered: {rust_main.get_registered_sub_chains()}")

    # ===== Calculate proof using Rust (performance) =====
    print("\nAnchoring proof to Rust MainChain...")
    proof_hash = rust_merkle_root([json.dumps(e) for e in events])
    print(f"  Proof hash (Rust Merkle): {proof_hash[:32]}...")

    success = rust_main.add_proof(
        "RustFinance",
        proof_hash,
        {"event_count": len(events), "timestamp": time.time()}
    )
    print(f"  Proof anchored: {'[OK]' if success else '[FAIL]'}")

    # Verify proof
    verified = rust_main.verify_proof(proof_hash, "RustFinance")
    print(f"  Verification: {'[OK] Valid' if verified else '[FAIL] Invalid'}")

    return py_main, rust_main


def demo_hierarchy_stats(py_main, rust_main):
    """Demonstrate hierarchy statistics."""
    print_section("6. Hierarchy Statistics")

    # Python MainChain stats
    print("Python MainChain:")
    print(f"  Chain Length: {len(py_main.chain)}")
    print(f"  Registered SubChains: {len(py_main.registered_sub_chains)}")

    # Rust MainChain stats
    print("\nRust MainChain:")
    print(f"  Chain Length: {rust_main.chain_length}")
    print(f"  Proof Count: {rust_main.proof_count}")
    print(f"  Registered SubChains: {rust_main.registered_sub_chains_count}")
    is_valid = rust_main.is_chain_valid
    print(f"  Chain Valid: {'[OK]' if is_valid else '[FAIL]'}")

    # Get stats
    stats = rust_main.get_main_chain_stats()
    print(f"\n  Statistics:")
    print(f"    Total Blocks: {stats.get('total_blocks', 0)}")


def main():
    """Run all PoF demo scenarios."""
    print("\n" + "="*60)
    print("   HIERACHAIN - Proof of Federation (PoF) Demo")
    print("   Python (hierachain) + Rust (hierachain_consensus)")
    print("="*60)

    try:
        # Demo 1: Validator setup (Rust)
        validators = demo_validator_setup()

        # Demo 2: PoF consensus setup (Rust)
        pof = demo_pof_consensus(validators)

        # Demo 3: SubChain creation (Python + Rust)
        py_sub, rust_sub = demo_subchain_creation()

        # Demo 4: Domain operations
        events = demo_domain_operations(py_sub, rust_sub)

        # Demo 5: MainChain anchoring
        py_main, rust_main = demo_mainchain_anchoring(events)

        # Demo 6: Statistics
        demo_hierarchy_stats(py_main, rust_main)

        # Avoid unused variable warnings
        _ = pof

        print_section("[OK] ALL POF DEMOS COMPLETED SUCCESSFULLY")

        print("Summary:")
        print("  - hierachain (Python): SubChain domain logic, rich APIs")
        print("  - hierachain_consensus (Rust): Validators, federation, proofs")
        print("  - Best Practice: Use both for optimal performance!")

    except Exception as e:
        print(f"\n[FAIL] Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()

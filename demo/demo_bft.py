"""
Demo: BFT Consensus (Byzantine Fault Tolerant)

This demo showcases BFT consensus mechanism for tolerating Byzantine
(malicious or faulty) nodes. Both Python and Rust implementations are demonstrated.

Features demonstrated:
1. BFT consensus configuration
2. Fault tolerance calculation
3. Consensus round simulation
4. State management
"""

import sys
import os
import time

# Add parent directory to path for hierachain Python package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
#  Import Both Libraries
# ============================================================

# Python implementation (hierachain)
try:
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
        BFTConsensus as RustBFTConsensus,
        KeyPair,
        SubChain as RustSubChain,
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


def demo_bft_basics(validators):
    """Demonstrate BFT consensus basics (Rust)."""
    print_section("1. BFT Consensus Basics (Rust)")

    # Prepare validator info
    validator_names = list(validators.keys())
    node_public_keys = {name: kp.public_key for name, kp in validators.items()}

    # BFT requires 3f+1 nodes to tolerate f faulty nodes
    # With 7 validators, we can tolerate f=2 faulty nodes
    f = 2  # Max faulty nodes

    # Create BFT consensus instance with first validator as current node
    current_node = validator_names[0]
    bft = RustBFTConsensus(
        node_id=current_node,
        all_nodes=validator_names,
        f=f,
        keypair=validators[current_node],
        node_public_keys=node_public_keys
    )

    print(f"BFT Consensus Properties:")
    print(f"  Node ID: {bft.node_id()}")
    print(f"  Fault Tolerance (f): {bft.fault_tolerance()}")
    print(f"  Node Count: {bft.node_count()}")
    print(f"  Primary Node: {bft.primary()}")
    print(f"  Is Primary: {bft.is_primary()}")

    # BFT requires 3f+1 nodes to tolerate f faulty nodes
    print("\nBFT Node Requirements (3f+1 rule):")
    for f_val in range(1, 5):
        min_nodes = 3 * f_val + 1
        print(f"  Tolerate {f_val} faulty nodes -> Need {min_nodes} total nodes")

    return bft


def demo_validator_setup():
    """Set up validators for BFT consensus."""
    print_section("2. Validator Setup for BFT")

    # Generate validator keys (using Rust for crypto)
    validators = {}
    for i in range(7):  # 7 nodes can tolerate 2 faulty (3*2+1 = 7)
        name = f"validator_{i+1}"
        validators[name] = KeyPair.generate()
        print(f"  [OK] {name}: {validators[name].public_key[:24]}...")

    total = len(validators)
    max_faulty = (total - 1) // 3
    quorum = (2 * total) // 3 + 1

    print(f"\n  Total validators: {total}")
    print(f"  Max faulty nodes tolerated: {max_faulty}")
    print(f"  Quorum required (2/3+1): {quorum}")

    return validators


def demo_subchain_with_bft(validators):
    """Demonstrate SubChain with BFT consensus."""
    print_section("3. SubChain with BFT (Python vs Rust)")

    # ===== Python SubChain =====
    print("Python SubChain (hierachain):")
    py_subchain = PySubChain(
        name="PyBFTSubChain",
        domain_type="financial"
    )
    print(f"  Name: {py_subchain.name}")
    print(f"  Domain: {py_subchain.domain_type}")
    print(f"  Chain Length: {len(py_subchain.chain)}")

    # ===== Rust SubChain =====
    print("\nRust SubChain (hierachain_consensus):")
    rust_subchain = RustSubChain(
        name="RustBFTSubChain",
        domain_type="financial",
        consensus_type="bft"
    )
    print(f"  Name: {rust_subchain.name}")
    print(f"  Domain: {rust_subchain.domain_type}")
    print(f"  Consensus: {rust_subchain.consensus_type}")
    print(f"  Block Count: {rust_subchain.block_count}")

    return py_subchain, rust_subchain


def demo_bft_consensus_rounds():
    """Simulate BFT consensus rounds."""
    print_section("4. BFT Consensus Rounds Simulation")

    print("Simulating BFT consensus phases:")
    print("  Phase 1: PRE-PREPARE (Leader proposes block)")
    print("    -> Leader broadcasts block proposal to all validators")

    print("\n  Phase 2: PREPARE (Validators validate)")
    print("    -> Each validator validates and broadcasts PREPARE message")
    print("    -> Wait for 2f+1 PREPARE messages")

    print("\n  Phase 3: COMMIT (Validators commit)")
    print("    -> Validators broadcast COMMIT message")
    print("    -> Wait for 2f+1 COMMIT messages")

    print("\n  Phase 4: FINALIZE (Block added)")
    print("    -> Block is finalized and added to chain")

    # Demonstrate with example
    num_validators = 7
    f = (num_validators - 1) // 3  # max faulty = 2
    quorum = 2 * f + 1  # = 5

    print(f"\nExample with {num_validators} validators:")
    print(f"  Faulty nodes tolerated: {f}")
    print(f"  Messages needed for consensus: {quorum}")

    # Simulate votes
    prepare_votes = 6
    commit_votes = 5

    print(f"\n  PREPARE votes received: {prepare_votes}")
    print(f"    Consensus reached: {'[OK] Yes' if prepare_votes >= quorum else '[FAIL] No'}")

    print(f"\n  COMMIT votes received: {commit_votes}")
    print(f"    Consensus reached: {'[OK] Yes' if commit_votes >= quorum else '[FAIL] No'}")


def demo_byzantine_fault_tolerance():
    """Demonstrate Byzantine fault tolerance scenarios."""
    print_section("5. Byzantine Fault Tolerance Scenarios")

    scenarios = [
        {
            "name": "Normal Operation",
            "total": 7,
            "faulty": 0,
            "responding": 7,
        },
        {
            "name": "1 Faulty Node",
            "total": 7,
            "faulty": 1,
            "responding": 6,
        },
        {
            "name": "2 Faulty Nodes (Max Tolerated)",
            "total": 7,
            "faulty": 2,
            "responding": 5,
        },
        {
            "name": "3 Faulty Nodes (Beyond Tolerance)",
            "total": 7,
            "faulty": 3,
            "responding": 4,
        },
    ]

    for scenario in scenarios:
        total = scenario["total"]
        faulty = scenario["faulty"]
        responding = scenario["responding"]
        max_faulty = (total - 1) // 3
        quorum = 2 * max_faulty + 1

        can_reach_consensus = responding >= quorum

        status = "[OK] Can reach consensus" if can_reach_consensus else "[FAIL] Cannot reach consensus"
        print(f"{scenario['name']}:")
        print(f"  Total nodes: {total}, Faulty: {faulty}, Responding: {responding}")
        print(f"  Quorum needed: {quorum}, {status}")
        print()


def demo_event_processing(py_subchain, rust_subchain):
    """Process events through BFT SubChains."""
    print_section("6. Event Processing with BFT")

    events = [
        {"event": "trade_executed", "entity_id": "TRADE001", "timestamp": time.time()},
        {"event": "settlement_complete", "entity_id": "SETTLE001", "timestamp": time.time()},
        {"event": "audit_logged", "entity_id": "AUDIT001", "timestamp": time.time()},
    ]

    # Python SubChain
    print("Python SubChain - Adding events:")
    for event in events:
        py_subchain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # Rust SubChain
    print("\nRust SubChain - Adding events:")
    for event in events:
        rust_subchain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # Statistics
    print("\nSubChain Statistics:")
    print(f"  Python Chain Length: {len(py_subchain.chain)}")
    print(f"  Rust Block Count: {rust_subchain.block_count}")


def main():
    """Run all BFT consensus demos."""
    print("\n" + "="*60)
    print("   HIERACHAIN - BFT Consensus Demo")
    print("   Python (hierachain) + Rust (hierachain_consensus)")
    print("="*60)

    try:
        # Demo 1: Validator setup (need keys for BFT)
        validators = demo_validator_setup()

        # Demo 2: BFT basics (requires validators)
        bft = demo_bft_basics(validators)

        # Demo 3: SubChain with BFT
        py_sub, rust_sub = demo_subchain_with_bft(validators)

        # Demo 4: Consensus rounds (theoretical explanation)
        demo_bft_consensus_rounds()

        # Demo 5: Fault tolerance scenarios
        demo_byzantine_fault_tolerance()

        # Demo 6: Event processing
        demo_event_processing(py_sub, rust_sub)

        # Avoid unused variable warnings
        _ = bft
        _ = validators

        print_section("[OK] ALL BFT CONSENSUS DEMOS COMPLETED")

        print("Summary:")
        print("  - BFT tolerates f faulty nodes with 3f+1 total nodes")
        print("  - Consensus requires 2f+1 agreeing validators")
        print("  - Used in mission-critical financial/enterprise systems")
        print("  - Both Python and Rust provide BFT SubChain support")

    except Exception as e:
        print(f"\n[FAIL] Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()

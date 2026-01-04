"""
Demo: Integration of hierachain (Python) and hierachain_consensus (Rust)

This demo showcases using BOTH libraries together in a single application:
- hierachain: Pure Python implementation (feature-rich, flexible)
- hierachain_consensus: Rust implementation via PyO3 (high-performance)

Use cases:
1. Use Python for complex business logic and domain operations
2. Use Rust for performance-critical consensus and cryptographic operations
3. Combine both for a hybrid high-performance blockchain application
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

# Python implementation
try:
    from hierachain.core.block import Block as PyBlock
    from hierachain.core.utils import MerkleTree as PyMerkleTree
    from hierachain.hierarchical.main_chain import MainChain as PyMainChain
    from hierachain.hierarchical.sub_chain import SubChain as PySubChain
    import hierachain
    py_version = hierachain.__version__
    print(f"[OK] Loaded hierachain (Python) v{py_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain: {e}")
    exit(1)

# Rust implementation via PyO3
try:
    import hierachain_consensus as hc
    from hierachain_consensus import (
        MainChain as RustMainChain,
        SubChain as RustSubChain,
        KeyPair,
        calculate_merkle_root as rust_merkle_root,
        validate_poa_block,
        verify_signature,
    )
    rust_version = hc.__version__
    print(f"[OK] Loaded hierachain_consensus (Rust) v{rust_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain_consensus: {e}")
    exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def demo_merkle_root_comparison():
    """Compare Merkle root calculation: Python vs Rust."""
    print_section("1. Merkle Root Calculation: Python vs Rust")

    transactions = ["tx1", "tx2", "tx3", "tx4", "tx5", "tx6", "tx7", "tx8"]

    # Python implementation
    start_py = time.perf_counter()
    py_merkle = PyMerkleTree(transactions)
    py_root = py_merkle.get_root()
    time_py = (time.perf_counter() - start_py) * 1000

    # Rust implementation
    start_rust = time.perf_counter()
    rust_root = rust_merkle_root(transactions)
    time_rust = (time.perf_counter() - start_rust) * 1000

    print(f"Transactions: {len(transactions)} items")
    print(f"\nPython Merkle Root:  {py_root[:32]}...")
    print(f"Rust Merkle Root:    {rust_root[:32]}...")
    print(f"Roots match:         {'[OK] Yes' if py_root == rust_root else '[FAIL] No'}")

    print(f"\nPerformance:")
    print(f"  Python: {time_py:.4f} ms")
    print(f"  Rust:   {time_rust:.4f} ms")
    if time_py > 0:
        speedup = time_py / time_rust if time_rust > 0 else float('inf')
        print(f"  Speedup: {speedup:.1f}x faster (Rust)")


def demo_hybrid_workflow():
    """Demonstrate a hybrid workflow using both libraries."""
    print_section("2. Hybrid Workflow: Python Business Logic + Rust Consensus")

    # ===== Step 1: Use Rust for cryptographic operations =====
    print("Step 1: Generate keys using Rust (Ed25519)")
    authority_key = KeyPair.generate()
    print(f"  Authority Public Key: {authority_key.public_key[:32]}...")

    # Sign a message
    message = b"Block data to be signed"
    signature = authority_key.sign(message)
    print(f"  Signature: {signature[:32]}...")

    # Verify with Rust
    is_valid = verify_signature(authority_key.public_key, message, signature)
    print(f"  Rust Verification: {'[OK] Valid' if is_valid else '[FAIL] Invalid'}")

    # ===== Step 2: Use Python for complex domain logic =====
    print("\nStep 2: Create SubChain with Python (rich domain features)")
    py_subchain = PySubChain(
        name="AcademicRecords",
        domain_type="education"
    )
    print(f"  Python SubChain: {py_subchain.name}")

    # Add domain-specific events with Python (more flexible)
    events = [
        {
            "event": "student_enrolled",
            "entity_id": "STU001",
            "course_id": "CS101",
            "timestamp": time.time(),
            "details": {"semester": "Fall2025", "credits": 3}
        },
        {
            "event": "grade_recorded",
            "entity_id": "STU001",
            "course_id": "CS101",
            "grade": "A",
            "timestamp": time.time(),
            "details": {"instructor": "Prof. Smith", "final_score": 95}
        },
    ]

    for event in events:
        py_subchain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # ===== Step 3: Use Rust for performance-critical validation =====
    print("\nStep 3: Validate block with Rust (high-performance)")

    # Create a block structure for Rust validation
    block_data = {
        "index": 1,
        "timestamp": time.time(),
        "hash": "abc123def456",
        "previous_hash": "genesis",
        "events": events,
        "merkle_root": rust_merkle_root([json.dumps(e) for e in events])
    }

    is_valid = validate_poa_block(block_data, authority_key.public_key)
    print(f"  Rust Block Validation: {'[OK] Valid' if is_valid else '[FAIL] Invalid'}")


def demo_mainchain_comparison():
    """Compare MainChain operations: Python vs Rust."""
    print_section("3. MainChain Operations: Python vs Rust")

    # ===== Python MainChain =====
    print("Python MainChain:")
    py_main = PyMainChain(name="PyMainChain")
    py_main.register_sub_chain("PyFinance", {"domain": "finance"})
    py_main.register_sub_chain("PyHR", {"domain": "hr"})
    print(f"  Name: {py_main.name}")
    print(f"  Registered SubChains: {list(py_main.registered_sub_chains)}")
    print(f"  Chain Length: {len(py_main.chain)}")

    # ===== Rust MainChain =====
    print("\nRust MainChain:")
    rust_main = RustMainChain(name="RustMainChain", consensus_type="proof_of_authority")
    rust_main.register_sub_chain("RustFinance", {"domain": "finance"})
    rust_main.register_sub_chain("RustHR", {"domain": "hr"})
    print(f"  Name: {rust_main.name}")
    print(f"  Registered SubChains: {rust_main.get_registered_sub_chains()}")
    print(f"  Chain Length: {rust_main.chain_length}")


def demo_cross_library_proof():
    """Demonstrate cross-library proof anchoring."""
    print_section("4. Cross-Library Proof Anchoring")

    # Create events in Python (rich domain logic)
    events = [
        {"event": "transaction_1", "amount": 1000, "timestamp": time.time()},
        {"event": "transaction_2", "amount": 2000, "timestamp": time.time()},
        {"event": "transaction_3", "amount": 3000, "timestamp": time.time()},
    ]

    # Calculate Merkle root with Rust (performance)
    proof_hash = rust_merkle_root([json.dumps(e) for e in events])
    print(f"Proof hash (Rust Merkle): {proof_hash[:32]}...")

    # Anchor to Rust MainChain
    rust_main = RustMainChain(name="HybridMainChain")
    rust_main.register_sub_chain("FinanceSubChain")

    success = rust_main.add_proof(
        "FinanceSubChain",
        proof_hash,
        {"event_count": len(events), "timestamp": time.time()}
    )
    print(f"Proof anchored to Rust MainChain: {'[OK]' if success else '[FAIL]'}")

    # Verify with Rust
    verified = rust_main.verify_proof(proof_hash, "FinanceSubChain")
    print(f"Proof verified: {'[OK]' if verified else '[FAIL]'}")

    # Get stats
    print(f"\nRust MainChain Stats:")
    print(f"  Proof Count: {rust_main.proof_count}")
    print(f"  Chain Valid: {'[OK]' if rust_main.is_chain_valid else '[FAIL]'}")


def demo_performance_benchmark():
    """Benchmark performance: Python vs Rust for Merkle calculations."""
    print_section("5. Performance Benchmark: Merkle Root (1000 items)")

    # Generate test data
    items = [f"transaction_{i}" for i in range(1000)]

    # Benchmark Python
    start = time.perf_counter()
    for _ in range(10):
        py_tree = PyMerkleTree(items)
        _ = py_tree.get_root()
    py_time = (time.perf_counter() - start) * 100  # ms per iteration

    # Benchmark Rust
    start = time.perf_counter()
    for _ in range(10):
        _ = rust_merkle_root(items)
    rust_time = (time.perf_counter() - start) * 100  # ms per iteration

    print(f"1000 items, 10 iterations each:")
    print(f"  Python: {py_time:.2f} ms avg")
    print(f"  Rust:   {rust_time:.2f} ms avg")

    speedup = py_time / rust_time if rust_time > 0 else float('inf')
    print(f"  Rust is {speedup:.1f}x faster")

    # Recommendation
    print("\nRecommendation:")
    if speedup > 2:
        print("  -> Use Rust for performance-critical operations")
    else:
        print("  -> Performance is similar; choose based on feature needs")


def main():
    """Run all integration demos."""
    print("\n" + "="*70)
    print("   HIERACHAIN INTEGRATION DEMO")
    print("   Python (hierachain) + Rust (hierachain_consensus)")
    print("="*70)

    try:
        demo_merkle_root_comparison()
        demo_hybrid_workflow()
        demo_mainchain_comparison()
        demo_cross_library_proof()
        demo_performance_benchmark()

        print_section("[OK] ALL INTEGRATION DEMOS COMPLETED SUCCESSFULLY")

        print("Summary:")
        print("  - hierachain (Python): Rich domain logic, flexible APIs")
        print("  - hierachain_consensus (Rust): High-performance consensus & crypto")
        print("  - Best Practice: Use both together for optimal results!")

    except Exception as e:
        print(f"\n[FAIL] Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()

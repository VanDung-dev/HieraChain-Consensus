"""
Demo: Proof of Authority (PoA) Consensus

This demo showcases the integration of BOTH libraries for PoA consensus:
- hierachain (Python): Rich domain logic and business operations
- hierachain_consensus (Rust): High-performance consensus and crypto

Features demonstrated:
1. Key pair generation and digital signatures (Rust)
2. MainChain creation with PoA (Python + Rust comparison)
3. Event processing and block creation
4. Block validation (Rust)
5. Merkle root calculation (Python vs Rust)
6. Chain integrity verification
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
        KeyPair,
        validate_poa_block,
        calculate_merkle_root as rust_merkle_root,
        verify_signature,
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


def demo_keypair_and_signing():
    """Demonstrate Ed25519 key pair generation and signing (Rust)."""
    print_section("1. Key Pair Generation & Signing (Rust)")

    # Generate authority key pairs using Rust (high-performance crypto)
    authority1 = KeyPair.generate()
    authority2 = KeyPair.generate()
    authority3 = KeyPair.generate()

    print(f"Authority 1: {authority1.public_key[:32]}...")
    print(f"Authority 2: {authority2.public_key[:32]}...")
    print(f"Authority 3: {authority3.public_key[:32]}...")

    # Sign and verify with Rust
    message = b"PoA block data to be signed by authority"
    signature = authority1.sign(message)
    print(f"\n[Rust] Signed message with Authority 1")
    print(f"   Signature: {signature[:32]}...")

    # Verify signature (Rust)
    is_valid = verify_signature(authority1.public_key, message, signature)
    print(f"   Verification: {'[OK] Valid' if is_valid else '[FAIL] Invalid'}")

    return authority1, authority2, authority3


def demo_mainchain_creation(authorities: tuple):
    """Demonstrate MainChain creation: Python vs Rust."""
    print_section("2. MainChain Creation with PoA (Python vs Rust)")

    authority1, authority2, authority3 = authorities

    # ===== Python MainChain =====
    print("Python MainChain (hierachain):")
    py_chain = PyMainChain(name="PyPoAMainChain")
    print(f"  Name: {py_chain.name}")
    print(f"  Chain Length: {len(py_chain.chain)}")

    # ===== Rust MainChain =====
    print("\nRust MainChain (hierachain_consensus):")
    rust_chain = RustMainChain(
        name="RustPoAMainChain",
        consensus_type="proof_of_authority"
    )
    print(f"  Name: {rust_chain.name}")
    print(f"  Consensus Type: {rust_chain.consensus_type}")
    print(f"  Chain Length: {rust_chain.chain_length}")

    return py_chain, rust_chain


def demo_event_processing(py_chain, rust_chain):
    """Demonstrate event processing on both chains."""
    print_section("3. Event Processing & Block Creation")

    # Create sample events
    events = [
        {
            "event": "user_registered",
            "entity_id": "USER001",
            "timestamp": time.time(),
            "details": {"role": "admin"}
        },
        {
            "event": "course_created",
            "entity_id": "COURSE001",
            "timestamp": time.time(),
            "details": {"instructor": "Prof. Smith"}
        },
        {
            "event": "enrollment",
            "entity_id": "ENROLL001",
            "timestamp": time.time(),
            "details": {"student_id": "STU001", "course_id": "COURSE001"}
        },
    ]

    print(f"Processing {len(events)} events...")

    # Add events to Python MainChain
    print("\nPython MainChain:")
    for event in events:
        py_chain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # Add events to Rust MainChain
    print("\nRust MainChain:")
    for event in events:
        rust_chain.add_event(event)
        print(f"  [OK] Added: {event['event']}")

    # Finalize blocks
    print("\nFinalizing blocks...")
    py_block = py_chain.finalize_block()
    if py_block:
        print(f"  Python: Block finalized (index: {py_block.index})")
    else:
        print("  Python: No events to finalize")

    rust_block = rust_chain.finalize_block()
    if rust_block:
        print(f"  Rust: Block finalized")
    else:
        print("  Rust: No events to finalize")

    return events


def demo_block_validation(events: list, authority):
    """Demonstrate PoA block validation (Rust)."""
    print_section("4. Block Validation (Rust)")

    # Create a valid block structure
    merkle_root = rust_merkle_root([json.dumps(e) for e in events])
    valid_block = {
        "index": 1,
        "timestamp": time.time(),
        "hash": "abc123def456",
        "previous_hash": "genesis",
        "events": events,
        "merkle_root": merkle_root
    }

    # Test with valid authority
    is_valid = validate_poa_block(valid_block, authority.public_key)
    status = "[OK] Passed" if is_valid else "[FAIL] Failed"
    print(f"Valid block + valid authority: {status}")

    # Test with empty authority (should fail)
    is_invalid = validate_poa_block(valid_block, "")
    status = "[OK] Rejected" if not is_invalid else "[FAIL] Accepted"
    print(f"Valid block + empty authority: {status}")

    # Test with missing fields (should fail)
    invalid_block = {"index": 1}  # Missing required fields
    is_invalid = validate_poa_block(invalid_block, authority.public_key)
    status = "[OK] Rejected" if not is_invalid else "[FAIL] Accepted"
    print(f"Invalid block (missing fields): {status}")


def demo_merkle_root():
    """Demonstrate Merkle root calculation: Python vs Rust."""
    print_section("5. Merkle Root Calculation (Python vs Rust)")

    transactions = ["tx1", "tx2", "tx3", "tx4"]
    print(f"Transactions: {transactions}")

    # Python Merkle
    start = time.perf_counter()
    py_tree = PyMerkleTree(transactions)
    py_root = py_tree.get_root()
    py_time = (time.perf_counter() - start) * 1000

    # Rust Merkle
    start = time.perf_counter()
    rust_root = rust_merkle_root(transactions)
    rust_time = (time.perf_counter() - start) * 1000

    print(f"\nPython Merkle Root: {py_root[:32]}...")
    print(f"Rust Merkle Root:   {rust_root[:32]}...")

    roots_match = py_root == rust_root
    print(f"Roots match: {'[OK] Yes' if roots_match else '[FAIL] No'}")

    print(f"\nPerformance:")
    print(f"  Python: {py_time:.4f} ms")
    print(f"  Rust:   {rust_time:.4f} ms")

    # Different transactions produce different roots
    different_txs = ["tx1", "tx2", "tx3", "tx5"]
    different_root = rust_merkle_root(different_txs)
    status = "[OK] Different" if rust_root != different_root else "[FAIL] Same"
    print(f"\nDifferent inputs produce different roots: {status}")


def demo_chain_integrity(py_chain, rust_chain):
    """Demonstrate chain integrity verification."""
    print_section("6. Chain Integrity Verification")

    # Python chain
    print("Python MainChain:")
    print(f"  Chain Length: {len(py_chain.chain)}")
    print(f"  Pending Events: {len(py_chain.pending_events)}")

    # Rust chain
    print("\nRust MainChain:")
    print(f"  Chain Length: {rust_chain.chain_length}")
    print(f"  Proof Count: {rust_chain.proof_count}")
    is_valid = rust_chain.is_chain_valid
    print(f"  Chain Valid: {'[OK]' if is_valid else '[FAIL]'}")

    # Get Rust chain stats
    stats = rust_chain.get_main_chain_stats()
    print(f"\n  Statistics:")
    print(f"    Total Blocks: {stats.get('total_blocks', 0)}")


def main():
    """Run all PoA demo scenarios."""
    print("\n" + "="*60)
    print("   HIERACHAIN - Proof of Authority (PoA) Demo")
    print("   Python (hierachain) + Rust (hierachain_consensus)")
    print("="*60)

    try:
        # Demo 1: Key pair generation (Rust)
        authorities = demo_keypair_and_signing()

        # Demo 2: MainChain creation (Python + Rust)
        py_chain, rust_chain = demo_mainchain_creation(authorities)

        # Demo 3: Event processing
        events = demo_event_processing(py_chain, rust_chain)

        # Demo 4: Block validation (Rust)
        demo_block_validation(events, authorities[0])

        # Demo 5: Merkle root (Python vs Rust)
        demo_merkle_root()

        # Demo 6: Chain integrity
        demo_chain_integrity(py_chain, rust_chain)

        print_section("[OK] ALL POA DEMOS COMPLETED SUCCESSFULLY")

        print("Summary:")
        print("  - hierachain (Python): MainChain, event management")
        print("  - hierachain_consensus (Rust): Crypto, validation, Merkle")
        print("  - Best Practice: Use Rust for crypto, Python for logic!")

    except Exception as e:
        print(f"\n[FAIL] Demo failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()

"""
Benchmark script for comparing Rust and Python implementations of Proof of Federation (PoF).

This script compares Block creation, Hashing, Leader Rotation, and Consensus Validation performance.
"""

import time
import random
import string
import json
import sys
import os
from typing import Any, Callable
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Implementation Imports ---

# 1. Python Implementation
PYTHON_AVAILABLE = False
PyBlock = None
PyPoF = None

try:
    from hierachain.core.block import Block as _PyBlock
    from hierachain.core.consensus.proof_of_federation import ProofOfFederation as _PyPoF
    PyBlock = _PyBlock
    PyPoF = _PyPoF
    PYTHON_AVAILABLE = True
except ImportError:
    pass

# 2. Rust Implementation
RUST_AVAILABLE = False
RsBlock = None
RsPoF = None
rs_validate_poa = None
rs_calculate_block_hash = None

try:
    import hierachain_consensus
    # Check for Block class
    if hasattr(hierachain_consensus, "Block"):
        RsBlock = hierachain_consensus.Block
        RUST_AVAILABLE = True
    
    # Check for ProofOfFederation class
    if hasattr(hierachain_consensus, "ProofOfFederation"):
        RsPoF = hierachain_consensus.ProofOfFederation
    
    # Check for Validation function (reuse PoA validation for now)
    if hasattr(hierachain_consensus, "validate_poa_block"):
        rs_validate_poa = hierachain_consensus.validate_poa_block
        if not RUST_AVAILABLE:
            RUST_AVAILABLE = True

    # Check for Hash function (standalone)
    if hasattr(hierachain_consensus, "calculate_block_hash"):
        rs_calculate_block_hash = hierachain_consensus.calculate_block_hash
        if not RUST_AVAILABLE:
            RUST_AVAILABLE = True
except ImportError:
    pass

# --- Helper Functions ---


def create_test_events(count: int) -> list[dict[str, Any]]:
    """Creates a list of dummy events for benchmarking."""
    events = []
    for i in range(count):
        events.append({
            "entity_id": f"ENTITY-{i}",
            "event": "benchmark_event",
            "timestamp": time.time(),
            "details": {
                "key1": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
                "value1": "some_data",
                "federation_round": i
            }
        })
    return events


def create_test_validators(count: int) -> list[str]:
    """Creates a list of test validator IDs."""
    return [f"validator_{chr(65 + i)}" for i in range(count)]


class PoFImplementation:
    """Wrapper to standardize access to Python and Rust implementations."""
    def __init__(self, name: str, block_cls: Any, pof_validator: Any, hash_fn: Callable | None = None):
        self.name = name
        self.Block = block_cls
        self.pof_validator = pof_validator  # Instance of PoF consensus
        self.hash_fn = hash_fn


# --- Benchmark Functions ---

def benchmark_leader_rotation(impl: PoFImplementation, validator_count: int, iterations: int) -> dict[str, Any]:
    """Benchmarks leader rotation calculation performance."""
    pof_engine = impl.pof_validator
    if pof_engine is None:
        return {
            "implementation": impl.name,
            "validator_count": validator_count,
            "iterations": iterations,
            "note": "PoF validator not available"
        }
    
    # Add validators
    for i in range(validator_count):
        pof_engine.add_validator(f"validator_{i}")
    
    # Benchmark get_current_leader
    start_time = time.perf_counter()
    for block_idx in range(iterations):
        _ = pof_engine.get_current_leader(block_idx)
    rotation_time = time.perf_counter() - start_time
    
    # Benchmark validate_block_proposer
    start_time = time.perf_counter()
    for block_idx in range(iterations):
        leader = pof_engine.get_current_leader(block_idx)
        _ = pof_engine.validate_block_proposer(block_idx, leader)
    proposer_validation_time = time.perf_counter() - start_time
    
    return {
        "implementation": impl.name,
        "validator_count": validator_count,
        "iterations": iterations,
        "total_rotation_time": rotation_time,
        "avg_rotation_time_us": (rotation_time / iterations) * 1_000_000,
        "total_proposer_validation_time": proposer_validation_time,
        "avg_proposer_validation_time_us": (proposer_validation_time / iterations) * 1_000_000,
        "rotations_per_sec": iterations / rotation_time if rotation_time > 0 else 0
    }


def benchmark_pof_run(impl: PoFImplementation, event_count: int, iterations: int) -> dict[str, Any]:
    """Runs one configuration of the benchmark (Block Creation, Hashing, Validation)."""
    events = create_test_events(event_count)
    
    # 1. Block Creation
    start_time = time.perf_counter()
    for i in range(iterations):
        if impl.name == "Python":
            _ = impl.Block(index=i, events=events, previous_hash="0"*64)
        else:  # Rust
            _ = impl.Block(i, events, {"previous_hash": "0"*64})
    creation_time = time.perf_counter() - start_time
    
    # 2. Hashing
    if impl.name == "Python":
        block_for_hash = impl.Block(index=1, events=events, previous_hash="0"*64)
    else:
        block_for_hash = impl.Block(1, events, {"previous_hash": "0"*64})

    start_time = time.perf_counter()
    for _ in range(iterations):
        _ = block_for_hash.calculate_hash()
    hashing_time = time.perf_counter() - start_time

    # 3. Validation
    validation_time = 0.0
    finalization_time = 0.0
    
    if impl.name == "Python" and impl.pof_validator:
        pof_engine = impl.pof_validator
        
        pof_engine.validators = [] 
        pof_engine.validator_metadata = {}
        for vid in create_test_validators(5):
            pof_engine.add_validator(vid)
        
        prev_block = impl.Block(index=9, events=[], previous_hash="0"*64, timestamp=time.time() - 15)
        curr_block = impl.Block(index=10, events=create_test_events(10), previous_hash="0"*64)
        
        expected_leader = pof_engine.get_current_leader(10)
        curr_block.creator_id = expected_leader
        
        start_time = time.perf_counter()
        for _ in range(iterations):
            test_block = impl.Block(index=10, events=create_test_events(10), previous_hash="0"*64)
            test_block.creator_id = expected_leader
            _ = pof_engine.finalize_block(test_block, expected_leader)
        finalization_time = time.perf_counter() - start_time
        
        finalized_block = pof_engine.finalize_block(curr_block, expected_leader)
        
        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = pof_engine.validate_block(finalized_block, prev_block)
        validation_time = time.perf_counter() - start_time

    elif impl.name == "Rust" and impl.pof_validator:
        pof_engine = impl.pof_validator
        
        # Setup validators for Rust instance
        test_validators = create_test_validators(5)
        for vid in test_validators:
            pof_engine.add_validator(vid)
            
        expected_leader = pof_engine.get_current_leader(10)
        
        # Prepare blocks for benchmarks
        # Create lightweight block objects/dicts for Rust
        prev_block_rust = impl.Block(9, [], {"previous_hash": "0"*64}).to_dict()
        
        # For finalization bench
        start_time = time.perf_counter()
        for i in range(iterations):
            temp_block = impl.Block(10, create_test_events(10), {"previous_hash": "0"*64})
            temp_block.creator_id = expected_leader
            temp_block_dict = temp_block.to_dict()
            
            _ = pof_engine.finalize_block(temp_block_dict)
        finalization_time = time.perf_counter() - start_time

        # For validation bench
        # First create a valid finalized block
        valid_block = impl.Block(10, create_test_events(10), {"previous_hash": "0"*64})
        valid_block.creator_id = expected_leader
        valid_block_dict = valid_block.to_dict()
        finalized_block_dict = pof_engine.finalize_block(valid_block_dict)

        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = pof_engine.validate_block(finalized_block_dict, prev_block_rust)
        validation_time = time.perf_counter() - start_time

    return {
        "implementation": impl.name,
        "event_count": event_count,
        "iterations": iterations,
        "total_creation_time": creation_time,
        "avg_creation_time_ms": (creation_time / iterations) * 1000,
        "total_hashing_time": hashing_time,
        "avg_hashing_time_ms": (hashing_time / iterations) * 1000,
        "total_finalization_time": finalization_time,
        "avg_finalization_time_ms": (
            (finalization_time / iterations) * 1000 if finalization_time > 0 else 0
        ),
        "total_validation_time": validation_time,
        "avg_validation_time_ms": (validation_time / iterations) * 1000,
        "blocks_created_per_sec": iterations / creation_time if creation_time > 0 else 0
    }


def run_comprehensive_benchmark():
    """Initializes implementations and runs benchmarks, saving to JSON."""
    if PYTHON_AVAILABLE:
        print("✓ Python implementation available")
    else:
        print("⚠ Warning: Python implementation not available")

    if RUST_AVAILABLE:
        print("✓ Rust implementation available")
    else:
        print("⚠ Warning: Rust implementation not available")

    configs = [
        {"events": 10, "iter": 2000},
        {"events": 100, "iter": 1000},
        {"events": 500, "iter": 500}
    ]
    
    rotation_configs = [
        {"validators": 5, "iter": 10000},
        {"validators": 21, "iter": 10000},
        {"validators": 100, "iter": 10000}
    ]
    
    all_results = []
    rotation_results = []
    implementations = []
    
    if PYTHON_AVAILABLE and PyBlock:
        py_pof_instance = PyPoF() if PyPoF else None
        implementations.append(PoFImplementation("Python", PyBlock, py_pof_instance))
        
    if RUST_AVAILABLE and RsBlock:
        rs_pof_instance = RsPoF() if RsPoF else None
        implementations.append(
            PoFImplementation("Rust", RsBlock, rs_pof_instance, rs_calculate_block_hash)
        )
        
    if not implementations:
        print("❌ No implementations available to benchmark.")
        return

    for config in configs:
        event_count = config["events"]
        iterations = config["iter"]
        for impl in implementations:
            res = benchmark_pof_run(impl, event_count, iterations)
            all_results.append(res)
    
    for config in rotation_configs:
        validator_count = config["validators"]
        iterations = config["iter"]
        for impl in implementations:
            if impl.pof_validator is not None:
                if impl.name == "Python":
                    impl.pof_validator.validators = []
                    impl.pof_validator.validator_metadata = {}
                else:
                    impl.pof_validator = RsPoF() if RsPoF else None
                
                if impl.pof_validator:
                    res = benchmark_leader_rotation(impl, validator_count, iterations)
                    rotation_results.append(res)
            
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'PoF_benchmark.json')
    combined_results = {
        "block_validation_benchmarks": all_results,
        "leader_rotation_benchmarks": rotation_results,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(results_path, 'w') as f:
        json.dump(combined_results, f, indent=2)
        
    print_summary(all_results, rotation_results)
    return combined_results


def print_summary(all_results: list[dict[str, Any]], rotation_results: list[dict[str, Any]]):
    """Print summary comparison tables."""
    w = 100
    m_h = f"{'Events / Metric':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'PROOF OF FEDERATION (POF) BENCHMARK SUMMARY':^100}")
    print("=" * w)
    print(h)
    print("-" * w)

    def get_status_icon(py_v, rs_v, higher_is_better=True):
        if not py_v or not rs_v or py_v == 0 or rs_v == 0:
            return "N/A", ""
        sp = (rs_v / py_v) if higher_is_better else (py_v / rs_v)

        if sp > 1.5:
            return f"{sp:.2f}x", "🚀"
        if sp < 0.8:
            return f"{sp:.2f}x", "⚠️"
        return f"{sp:.2f}x", "➡️"

    py_res = [r for r in all_results if r.get("implementation") == "Python"]
    rs_res = [r for r in all_results if r.get("implementation") == "Rust"]

    counts = sorted(list(set([r["event_count"] for r in all_results])))

    for count in counts:
        p_r = next((r for r in py_res if r.get("event_count") == count), None)
        r_r = next((r for r in rs_res if r.get("event_count") == count), None)

        # 1. Blocks / sec (Higher is better)
        pv = p_r.get("blocks_created_per_sec", 0) if p_r else 0
        rv = r_r.get("blocks_created_per_sec", 0) if r_r else 0
        pt = f"{pv:>12.0f} blk/s" if pv else "N/A"
        rt = f"{rv:>12.0f} blk/s" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, True)
        print(f"{f'{count} Events (Creation)':<30} | {pt:<18} | "f"{rt:<18} | {sp:<8} | {icon:<6}")

        # 2. Hashing (Lower is better)
        pv = p_r.get("avg_hashing_time_ms", 0) if p_r else 0
        rv = r_r.get("avg_hashing_time_ms", 0) if r_r else 0
        pt = f"{pv:>12.4f} ms" if pv else "N/A"
        rt = f"{rv:>12.4f} ms" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, False)
        print(f"{'  - Hashing Avg Time':<30} | {pt:<18} | {rt:<18} | "f"{sp:<8} | {icon:<6}")

        # 3. Validation (Lower is better)
        pv = p_r.get("avg_validation_time_ms", 0) if p_r else 0
        rv = r_r.get("avg_validation_time_ms", 0) if r_r else 0
        pt = f"{pv:>12.4f} ms" if pv else "N/A"
        rt = f"{rv:>12.4f} ms" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, False)
        print(f"{'  - Validation Avg Time':<30} | {pt:<18} | {rt:<18} | "f"{sp:<8} | {icon:<6}")
        print("-" * w)

    if rotation_results:
        print("\n" + "=" * w)
        print(f"{'LEADER ROTATION BENCHMARK SUMMARY':^100}")
        print("=" * w)
        print(f"{'Validators / Metric':<30} | {'Python Result':<18} | "f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}")
        print("-" * w)
        
        py_rot = [r for r in rotation_results if r.get("implementation") == "Python"]
        rs_rot = [r for r in rotation_results if r.get("implementation") == "Rust"]
        
        v_counts = sorted(list(set([r["validator_count"] for r in rotation_results])))
        for vcount in v_counts:
            p_r = next((r for r in py_rot if r.get("validator_count") == vcount), None)
            r_r = next((r for r in rs_rot if r.get("validator_count") == vcount), None)
            
            # Rotations / sec
            pv = p_r.get("rotations_per_sec", 0) if p_r else 0
            rv = r_r.get("rotations_per_sec", 0) if r_r else 0
            pt = f"{pv:>12.0f} rot/s" if pv else "N/A"
            rt = f"{rv:>12.0f} rot/s" if rv else "N/A"
            sp, icon = get_status_icon(pv, rv, True)
            print(f"{f'{vcount} Validators (Rotation)':<30} | {pt:<18} | "f"{rt:<18} | {sp:<8} | {icon:<6}")
            
            # Avg Time (us)
            pv = p_r.get("avg_rotation_time_us", 0) if p_r else 0
            rv = r_r.get("avg_rotation_time_us", 0) if r_r else 0
            pt = f"{pv:>12.2f} us" if pv else "N/A"
            rt = f"{rv:>12.2f} us" if rv else "N/A"
            sp, icon = get_status_icon(pv, rv, False)
            print(f"{'  - Avg Rotation Time':<30} | {pt:<18} | {rt:<18} | "f"{sp:<8} | {icon:<6}")
            print("-" * w)

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

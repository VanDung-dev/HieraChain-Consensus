"""
Benchmark script for comparing Rust and Python implementations of Proof of Authority (PoA).

This script compares Block creation, Hashing, and Consensus Validation performance.
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
PyPoA = None

try:
    from hierachain.core.block import Block as _PyBlock
    from hierachain.core.consensus.proof_of_authority import ProofOfAuthority as _PyPoA
    PyBlock = _PyBlock
    PyPoA = _PyPoA
    PYTHON_AVAILABLE = True
except ImportError:
    pass

# 2. Rust Implementation
RUST_AVAILABLE = False
RsBlock = None
rs_validate_poa = None
rs_calculate_block_hash = None

try:
    import hierachain_consensus
    # Check for Block class
    if hasattr(hierachain_consensus, "Block"):
        RsBlock = hierachain_consensus.Block
        RUST_AVAILABLE = True
    
    # Check for Validation function
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
                "value1": "some_data"
            }
        })
    return events


class PoAImplementation:
    """Wrapper to standardize access to Python and Rust implementations."""
    def __init__(self, name: str, block_cls: Any, poa_validator: Any, hash_fn: Callable | None = None):
        self.name = name
        self.Block = block_cls
        self.poa_validator = poa_validator  # Can be class instance (Py) or function (Rust)
        self.hash_fn = hash_fn

# --- Main Benchmark Logic ---

def benchmark_poa_run(impl: PoAImplementation, event_count: int, iterations: int) -> dict[str, Any]:
    """
    Runs one configuration of the benchmark (Block Creation, Hashing, Validation).
    """
    events = create_test_events(event_count)
    
    # 1. Block Creation
    start_time = time.perf_counter()
    for i in range(iterations):
        if impl.name == "Python":
            _ = impl.Block(index=i, events=events, previous_hash="0"*64)
        else: # Rust
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
    if impl.name == "Python":
        poa_engine = impl.poa_validator # Instance of PyPoA
        prev_block = impl.Block(index=9, events=[], previous_hash="0"*64, timestamp=time.time()-15)
        curr_block = impl.Block(index=10, events=create_test_events(10), previous_hash="0"*64)
        
        poa_engine.add_authority("AUTH001")
        curr_block.creator_id = "AUTH001"
        try:
            curr_block = poa_engine.finalize_block(curr_block, "AUTH001")
        except Exception:
            pass

        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = poa_engine.validate_block(curr_block, prev_block)
        validation_time = time.perf_counter() - start_time

    elif impl.name == "Rust":
        validate_fn = impl.poa_validator
        curr_block_rust = impl.Block(10, create_test_events(10), {"previous_hash": "0"*64})
        
        block_data = {}
        if hasattr(curr_block_rust, 'to_dict'):
            block_data = curr_block_rust.to_dict()
            
        auth_id = "AUTH001"
        
        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = validate_fn(block_data, auth_id)
        validation_time = time.perf_counter() - start_time

    return {
        "implementation": impl.name,
        "event_count": event_count,
        "iterations": iterations,
        "total_creation_time": creation_time,
        "avg_creation_time_ms": (creation_time / iterations) * 1000,
        "total_hashing_time": hashing_time,
        "avg_hashing_time_ms": (hashing_time / iterations) * 1000,
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
    
    all_results = []
    implementations = []
    
    if PYTHON_AVAILABLE and PyBlock:
        py_poa_instance = PyPoA() if PyPoA else None
        implementations.append(PoAImplementation("Python", PyBlock, py_poa_instance))
        
    if RUST_AVAILABLE and RsBlock:
        implementations.append(PoAImplementation("Rust", RsBlock, rs_validate_poa, rs_calculate_block_hash))
        
    if not implementations:
        print("❌ No implementations available to benchmark.")
        return

    for config in configs:
        event_count = config["events"]
        iterations = config["iter"]
        
        for impl in implementations:
            res = benchmark_poa_run(impl, event_count, iterations)
            all_results.append(res)
            
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'PoA_benchmark.json')
    with open(results_path, 'w') as f:
        json.dump(all_results, f, indent=2)
        
    print_summary(all_results)
    return all_results


def print_summary(all_results: list[dict[str, Any]]):
    """Print a summary comparison table."""
    w = 100
    m_h = f"{'Events / Metric':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'PROOF OF AUTHORITY (POA) BENCHMARK SUMMARY':^100}")
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

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

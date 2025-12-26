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
import statistics
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Implementation Imports ---

# 1. Python Implementation
PYTHON_AVAILABLE = False
PyBlock = None
PyPoF = None

try:
    from hierachain.core.block import Block as PyBlock
    from hierachain.core.consensus.proof_of_federation import ProofOfFederation as PyPoF
    PYTHON_AVAILABLE = True
    print("✓ Python implementation available")
except ImportError as e:
    print(f"⚠ Warning: Python implementation not available: {e}")

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
        print("✓ Rust ProofOfFederation available")
    
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
        
    if RUST_AVAILABLE:
        print(f"✓ Rust implementation available ({hierachain_consensus.__file__})")
    else:
        print("⚠ Warning: Rust module loaded but required symbols (Block) not found.")

except ImportError as e:
    print(f"⚠ Warning: Rust implementation not available: {e}")

# --- Helper Functions ---

def create_test_events(count: int) -> List[Dict[str, Any]]:
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


def create_test_validators(count: int) -> List[str]:
    """Creates a list of test validator IDs."""
    return [f"validator_{chr(65 + i)}" for i in range(count)]


class PoFImplementation:
    """Wrapper to standardize access to Python and Rust implementations."""
    def __init__(self, name: str, block_cls: Any, pof_validator: Any, hash_fn: Optional[Callable] = None):
        self.name = name
        self.Block = block_cls
        self.pof_validator = pof_validator  # Instance of PoF consensus
        self.hash_fn = hash_fn


# --- Benchmark Functions ---

def benchmark_leader_rotation(
    impl: PoFImplementation,
    validator_count: int,
    iterations: int
) -> Dict[str, Any]:
    """
    Benchmarks leader rotation calculation performance.
    """
    print(f"\n* Benchmarking {impl.name} Leader Rotation "
          f"(Validators: {validator_count}, Iterations: {iterations})...")
    
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
    
    result = {
        "implementation": impl.name,
        "validator_count": validator_count,
        "iterations": iterations,
        "total_rotation_time": rotation_time,
        "avg_rotation_time_us": (rotation_time / iterations) * 1_000_000,
        "total_proposer_validation_time": proposer_validation_time,
        "avg_proposer_validation_time_us": (
            (proposer_validation_time / iterations) * 1_000_000
        ),
        "rotations_per_sec": iterations / rotation_time if rotation_time > 0 else 0
    }
    
    print(f"  ✅ Leader Rotation: {rotation_time:.4f}s "
          f"({result['rotations_per_sec']:.2f} ops/sec)")
    print(f"  ✅ Proposer Validation: {proposer_validation_time:.4f}s")
    
    return result


def benchmark_pof_run(impl: PoFImplementation, event_count: int, iterations: int) -> Dict[str, Any]:
    """
    Runs one configuration of the benchmark (Block Creation, Hashing, Validation).
    """
    print(f"\n* Benchmarking {impl.name} (Events: {event_count}, Iterations: {iterations})...")
    
    events = create_test_events(event_count)
    
    # 1. Block Creation
    start_time = time.perf_counter()
    for i in range(iterations):
        if impl.name == "Python":
            _ = impl.Block(index=i, events=events, previous_hash="0"*64)
        else:  # Rust
            # Rust Block.new(index, events, kwargs_dict)
            _ = impl.Block(i, events, {"previous_hash": "0"*64})
    creation_time = time.perf_counter() - start_time
    
    # 2. Hashing
    # Create a reference block first
    if impl.name == "Python":
        block_for_hash = impl.Block(index=1, events=events, previous_hash="0"*64)
    else:
        block_for_hash = impl.Block(1, events, {"previous_hash": "0"*64})

    # Fair comparison: Always use calculate_hash() method directly on the block
    start_time = time.perf_counter()
    for _ in range(iterations):
        _ = block_for_hash.calculate_hash()
    hashing_time = time.perf_counter() - start_time

    # 3. Validation (PoF specific)
    validation_time = 0.0
    finalization_time = 0.0
    
    if impl.name == "Python" and impl.pof_validator:
        pof_engine = impl.pof_validator
        
        # Setup validators
        pof_engine.validators = []  # Reset
        pof_engine.validator_metadata = {}
        for vid in create_test_validators(5):
            pof_engine.add_validator(vid)
        
        # Setup blocks for validation
        prev_block = impl.Block(
            index=9, 
            events=[], 
            previous_hash="0"*64, 
            timestamp=time.time() - 15
        )
        
        curr_block = impl.Block(
            index=10, 
            events=create_test_events(10), 
            previous_hash="0"*64
        )
        
        # Finalize the block (sign it)
        expected_leader = pof_engine.get_current_leader(10)
        curr_block.creator_id = expected_leader
        
        # Benchmark finalization
        start_time = time.perf_counter()
        for _ in range(iterations):
            test_block = impl.Block(
                index=10, 
                events=create_test_events(10), 
                previous_hash="0"*64
            )
            test_block.creator_id = expected_leader
            _ = pof_engine.finalize_block(test_block, expected_leader)
        finalization_time = time.perf_counter() - start_time
        
        # Get a finalized block for validation
        finalized_block = pof_engine.finalize_block(curr_block, expected_leader)
        
        # Benchmark validation
        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = pof_engine.validate_block(finalized_block, prev_block)
        validation_time = time.perf_counter() - start_time

    elif impl.name == "Rust" and rs_validate_poa:
        # Use generic PoA validation for Rust benchmark
        validate_fn = rs_validate_poa
        curr_block_rust = impl.Block(10, create_test_events(10), {"previous_hash": "0"*64})
        
        # Convert to dict OUTSIDE the loop to avoid measuring FFI overhead
        if hasattr(curr_block_rust, 'to_dict'):
            block_data = curr_block_rust.to_dict()
        else:
            block_data = {}
            
        auth_id = "validator_A"
        
        start_time = time.perf_counter()
        for _ in range(iterations):
            _ = validate_fn(block_data, auth_id)
        validation_time = time.perf_counter() - start_time

    # Compile results
    result = {
        "implementation": impl.name,
        "event_count": event_count,
        "iterations": iterations,
        "total_creation_time": creation_time,
        "avg_creation_time_ms": (creation_time / iterations) * 1000,
        "total_hashing_time": hashing_time,
        "avg_hashing_time_ms": (hashing_time / iterations) * 1000,
        "total_finalization_time": finalization_time,
        "avg_finalization_time_ms": (finalization_time / iterations) * 1000 if finalization_time > 0 else 0,
        "total_validation_time": validation_time,
        "avg_validation_time_ms": (validation_time / iterations) * 1000,
        "blocks_created_per_sec": iterations / creation_time if creation_time > 0 else 0
    }
    
    print(f"  ✅ Creation: {creation_time:.4f}s ({result['blocks_created_per_sec']:.2f} ops/sec)")
    print(f"  ✅ Hashing: {hashing_time:.4f}s")
    print(f"  ✅ Finalization: {finalization_time:.4f}s")
    print(f"  ✅ Validation: {validation_time:.4f}s")
    
    return result


def run_comprehensive_benchmark():
    """
    Initializes implementations and runs benchmarks, saving to JSON.
    """
    print("🚀 Starting PoF comprehensive benchmark...")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Configuration
    configs = [
        {"events": 10, "iter": 2000},
        {"events": 100, "iter": 1000},
        {"events": 500, "iter": 500}
    ]
    
    # Leader rotation configs
    rotation_configs = [
        {"validators": 5, "iter": 10000},
        {"validators": 21, "iter": 10000},
        {"validators": 100, "iter": 10000}
    ]
    
    all_results = []
    rotation_results = []
    
    # Define Implementations
    implementations = []
    
    if PYTHON_AVAILABLE and PyBlock:
        py_pof_instance = PyPoF() if PyPoF else None
        implementations.append(
            PoFImplementation("Python", PyBlock, py_pof_instance)
        )
        
    if RUST_AVAILABLE and RsBlock:
        # For Rust, use RsPoF if available, otherwise None
        rs_pof_instance = RsPoF() if RsPoF else None
        implementations.append(
            PoFImplementation(
                "Rust", RsBlock, rs_pof_instance, rs_calculate_block_hash
            )
        )
        
    if not implementations:
        print("❌ No implementations available to benchmark.")
        return

    # Run Block/Hash/Validation Benchmarks
    print("\n" + "=" * 60)
    print("📦 BLOCK CREATION & VALIDATION BENCHMARKS")
    print("=" * 60)
    
    for config in configs:
        event_count = config["events"]
        iterations = config["iter"]
        
        for impl in implementations:
            res = benchmark_pof_run(impl, event_count, iterations)
            all_results.append(res)
    
    # Run Leader Rotation Benchmarks
    print("\n" + "=" * 60)
    print("🔄 LEADER ROTATION BENCHMARKS")
    print("=" * 60)
    
    for config in rotation_configs:
        validator_count = config["validators"]
        iterations = config["iter"]
        
        for impl in implementations:
            if impl.pof_validator is not None:
                # Reset validators for each run
                if impl.name == "Python":
                    impl.pof_validator.validators = []
                    impl.pof_validator.validator_metadata = {}
                else:
                    # For Rust, create a fresh instance
                    impl.pof_validator = RsPoF() if RsPoF else None
                
                if impl.pof_validator:
                    res = benchmark_leader_rotation(
                        impl, validator_count, iterations
                    )
                    rotation_results.append(res)
            
    # Save Results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Save main results
    results_path = os.path.join(output_dir, 'PoF_benchmark.json')
    print(f"DEBUG: Saving results to: {results_path}")
    
    combined_results = {
        "block_validation_benchmarks": all_results,
        "leader_rotation_benchmarks": rotation_results,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(results_path, 'w') as f:
        json.dump(combined_results, f, indent=2)
        
    # Print Summary
    print("\n" + "=" * 60)
    print("📈 BENCHMARK SUMMARY")
    print("=" * 60)
    
    # Block/Validation summary
    py_results = [r for r in all_results if r['implementation'] == 'Python']
    rs_results = [r for r in all_results if r['implementation'] == 'Rust']
    
    # Compare for each event count
    if py_results and rs_results:
        print("\n📊 Block Creation/Validation Comparison:\n")
        print(f"{'Events':<10} {'Metric':<20} {'Python':<15} {'Rust':<15} {'Ratio':<10}")
        print("-" * 70)
        
        for py_r in py_results:
            event_count = py_r['event_count']
            rs_r = next((r for r in rs_results if r['event_count'] == event_count), None)
            if not rs_r:
                continue
                
            # Block Creation
            py_blk = py_r['blocks_created_per_sec']
            rs_blk = rs_r['blocks_created_per_sec']
            ratio = rs_blk / py_blk if py_blk > 0 else 0
            print(f"{event_count:<10} {'Blocks/sec':<20} {py_blk:<15.2f} {rs_blk:<15.2f} {ratio:<10.2f}x")
            
            # Hashing
            py_hash = py_r['avg_hashing_time_ms']
            rs_hash = rs_r['avg_hashing_time_ms']
            ratio = py_hash / rs_hash if rs_hash > 0 else 0
            print(f"{'':<10} {'Hashing (ms)':<20} {py_hash:<15.4f} {rs_hash:<15.4f} {ratio:<10.2f}x")
            
            # Validation
            py_val = py_r['avg_validation_time_ms']
            rs_val = rs_r['avg_validation_time_ms']
            ratio = py_val / rs_val if rs_val > 0 else 0
            print(f"{'':<10} {'Validation (ms)':<20} {py_val:<15.4f} {rs_val:<15.4f} {ratio:<10.2f}x")
            print()
    
    # Leader Rotation summary
    if rotation_results:
        print("\n📊 Leader Rotation Performance:\n")
        print(f"{'Validators':<12} {'Rotations/sec':<18} {'Avg Time (μs)':<15}")
        print("-" * 45)
        
        for res in rotation_results:
            if 'note' not in res:
                print(f"{res['validator_count']:<12} {res['rotations_per_sec']:<18.2f} {res['avg_rotation_time_us']:<15.4f}")
    
    # Overall summary
    if py_results:
        print("\n🏆 Python PoF Summary:")
        avg_creation = statistics.mean([r['blocks_created_per_sec'] for r in py_results])
        avg_validation = statistics.mean([r['avg_validation_time_ms'] for r in py_results])
        print(f"  • Avg Block Creation: {avg_creation:.2f} blocks/sec")
        print(f"  • Avg Validation Time: {avg_validation:.4f} ms")
        
        if rotation_results:
            avg_rotation = statistics.mean([r['rotations_per_sec'] for r in rotation_results if 'rotations_per_sec' in r])
            print(f"  • Avg Leader Rotation: {avg_rotation:.2f} ops/sec")

    print("\n" + "=" * 60)
    return combined_results


def analyze_benchmark(file_path):
    """
    Reads the JSON result and generates plots.
    """
    try:
        with open(file_path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Could not find result file: {file_path}")
        return

    block_data = data.get("block_validation_benchmarks", [])
    rotation_data = data.get("leader_rotation_benchmarks", [])
    
    python_data = [d for d in block_data if d['implementation'] == 'Python']
    rust_data = [d for d in block_data if d['implementation'] == 'Rust']
    
    if not python_data and not rust_data:
        print("No block/validation data to plot.")
        return

    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Proof of Federation (PoF) Benchmark Results', fontsize=14, fontweight='bold')
    
    # 1. Block Creation Time
    ax = axes[0, 0]
    if python_data:
        ax.plot([d['event_count'] for d in python_data],
                [d['avg_creation_time_ms'] for d in python_data],
                marker='o', label='Python', linewidth=2)
    if rust_data:
        ax.plot([d['event_count'] for d in rust_data],
                [d['avg_creation_time_ms'] for d in rust_data],
                marker='x', label='Rust', linewidth=2)
    ax.set_title('Block Creation Time vs Event Count')
    ax.set_xlabel('Number of Events')
    ax.set_ylabel('Avg Time (ms)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 2. Hashing Time
    ax = axes[0, 1]
    if python_data:
        ax.plot([d['event_count'] for d in python_data],
                [d['avg_hashing_time_ms'] for d in python_data],
                marker='o', label='Python', linewidth=2)
    if rust_data:
        ax.plot([d['event_count'] for d in rust_data],
                [d['avg_hashing_time_ms'] for d in rust_data],
                marker='x', label='Rust', linewidth=2)
    ax.set_title('Hashing Time vs Event Count')
    ax.set_xlabel('Number of Events')
    ax.set_ylabel('Avg Time (ms)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 3. Validation Time
    ax = axes[1, 0]
    if python_data:
        ax.plot([d['event_count'] for d in python_data],
                [d['avg_validation_time_ms'] for d in python_data],
                marker='o', label='Python', linewidth=2)
    if rust_data:
        ax.plot([d['event_count'] for d in rust_data],
                [d['avg_validation_time_ms'] for d in rust_data],
                marker='x', label='Rust', linewidth=2)
    ax.set_title('Block Validation Time vs Event Count')
    ax.set_xlabel('Number of Events')
    ax.set_ylabel('Avg Time (ms)')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 4. Leader Rotation Performance
    ax = axes[1, 1]
    py_rotation = [
        d for d in rotation_data
        if d.get('implementation') == 'Python' and 'rotations_per_sec' in d
    ]
    rs_rotation = [
        d for d in rotation_data
        if d.get('implementation') == 'Rust' and 'rotations_per_sec' in d
    ]
    
    if py_rotation or rs_rotation:
        import numpy as np
        validator_counts = sorted(set(
            [d['validator_count'] for d in py_rotation] +
            [d['validator_count'] for d in rs_rotation]
        ))
        x = np.arange(len(validator_counts))
        width = 0.35
        
        py_values = []
        rs_values = []
        for vc in validator_counts:
            py_val = next(
                (d['rotations_per_sec'] for d in py_rotation
                 if d['validator_count'] == vc), 0
            )
            rs_val = next(
                (d['rotations_per_sec'] for d in rs_rotation
                 if d['validator_count'] == vc), 0
            )
            py_values.append(py_val)
            rs_values.append(rs_val)
        
        ax.bar(x - width/2, py_values, width,
               label='Python', color='steelblue', alpha=0.8)
        ax.bar(x + width/2, rs_values, width,
               label='Rust', color='darkorange', alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels([str(vc) for vc in validator_counts])
    
    ax.set_title('Leader Rotation Performance')
    ax.set_xlabel('Number of Validators')
    ax.set_ylabel('Rotations/sec')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    
    output_dir = os.path.dirname(file_path)
    chart_path = os.path.join(output_dir, 'PoF_benchmark.png')
    plt.savefig(chart_path, dpi=150)
    print(f"📊 Chart saved to '{chart_path}'")


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")
    
    time.sleep(1)  # Ensure write completion
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_path = os.path.join(script_dir, 'output', 'PoF_benchmark.json')
    
    if os.path.exists(results_path):
        print(f"DEBUG: Reading results from: {results_path}")
        analyze_benchmark(results_path)
    else:
        print("⚠ Result file not found, skipping analysis.")

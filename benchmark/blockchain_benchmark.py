"""
Benchmark script for comparing Rust and Python implementations of Blockchain.

This script measures and compares the performance of:
- Block creation
- Event addition
- Chain validation
- Event filtering/queries
"""

import time
import json
import sys
import statistics
import os
from typing import Any
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Implementation Imports ---
try:
    from hierachain.core.blockchain import Blockchain as PythonBlockchain
    PYTHON_AVAILABLE = True
    print("✓ Python Blockchain implementation available")
except ImportError as e:
    PYTHON_AVAILABLE = False
    print(f"⚠ Warning: Python Blockchain implementation not available: {e}")

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "Blockchain"):
        RustBlockchain = hierachain_consensus.Blockchain
        RUST_AVAILABLE = True
        print("✓ Rust Blockchain implementation available")
    else:
        RUST_AVAILABLE = False
        print("⚠ Warning: Rust module loaded but Blockchain not found.")
        print(f"   Available: {dir(hierachain_consensus)}")
except ImportError as e:
    RUST_AVAILABLE = False
    print(f"⚠ Warning: Rust implementation not available: {e}")


# --- Helper Functions ---

def create_test_events(count: int) -> list[dict[str, Any]]:
    """Creates a list of test events for benchmarking."""
    return [{
        "entity_id": f"entity_{i % 100}",
        "event": f"event_type_{i % 10}",
        "timestamp": time.time(),
        "details": {"source": "benchmark", "index": i},
    } for i in range(count)]


def benchmark_event_addition(blockchain: Any, events: list[dict], name: str) -> dict:
    """Benchmark adding events to blockchain."""
    start = time.perf_counter()
    for event in events:
        blockchain.add_event(event)
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "add_event",
        "count": len(events),
        "time": elapsed,
        "ops_per_second": len(events) / elapsed if elapsed > 0 else 0
    }


def benchmark_block_creation(blockchain: Any, events_per_block: int, num_blocks: int, name: str) -> dict:
    """Benchmark block creation and finalization."""
    times = []
    
    for _ in range(num_blocks):
        # Add events for this block
        for i in range(events_per_block):
            blockchain.add_event({
                "entity_id": f"entity_{i}",
                "event": "benchmark_event",
                "timestamp": time.time(),
            })
        
        # Time the finalization
        start = time.perf_counter()
        blockchain.finalize_block()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    return {
        "operation": "finalize_block",
        "num_blocks": num_blocks,
        "events_per_block": events_per_block,
        "total_time": sum(times),
        "avg_time": statistics.mean(times) if times else 0,
        "min_time": min(times) if times else 0,
        "max_time": max(times) if times else 0,
        "blocks_per_second": num_blocks / sum(times) if sum(times) > 0 else 0
    }


def benchmark_chain_validation(blockchain: Any, name: str) -> dict:
    """Benchmark chain validation."""
    start = time.perf_counter()
    is_valid = blockchain.is_chain_valid()
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "is_chain_valid",
        "time": elapsed,
        "result": is_valid
    }


def benchmark_event_query(blockchain: Any, entity_id: str, name: str) -> dict:
    """Benchmark event querying by entity."""
    start = time.perf_counter()
    events = blockchain.get_events_by_entity(entity_id)
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_events_by_entity",
        "time": elapsed,
        "events_found": len(events)
    }


def benchmark_get_chain_stats(blockchain: Any, name: str) -> dict:
    """Benchmark getting chain statistics."""
    start = time.perf_counter()
    stats = blockchain.get_chain_stats()
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_chain_stats",
        "time": elapsed,
        "stats": stats if isinstance(stats, dict) else {}
    }


def benchmark_serialization(blockchain: Any, name: str) -> dict:
    """Benchmark serialization to/from dict."""
    # to_dict
    start = time.perf_counter()
    data = blockchain.to_dict()
    to_dict_time = time.perf_counter() - start
    
    # from_dict
    blockchain_class = type(blockchain)
    start = time.perf_counter()
    restored = blockchain_class.from_dict(data)
    from_dict_time = time.perf_counter() - start
    
    return {
        "operation": "serialization",
        "to_dict_time": to_dict_time,
        "from_dict_time": from_dict_time,
        "total_time": to_dict_time + from_dict_time
    }


def run_single_benchmark(blockchain_class: Any, name: str, config: dict) -> dict:
    """Run all benchmarks for a single implementation."""
    
    results = {
        "implementation": name,
        "config": config,
        "benchmarks": []
    }
    
    try:
        # Create fresh blockchain
        blockchain = blockchain_class("BenchmarkChain")
        
        # 1. Benchmark event addition
        events = create_test_events(config['event_count'])
        result = benchmark_event_addition(blockchain, events, name)
        results["benchmarks"].append(result)
        
        # 2. Benchmark block creation
        blockchain = blockchain_class("BenchmarkChain2")  # Fresh chain
        result = benchmark_block_creation(blockchain, config['events_per_block'], config['num_blocks'], name)
        results["benchmarks"].append(result)
        
        # 3. Benchmark chain validation
        result = benchmark_chain_validation(blockchain, name)
        results["benchmarks"].append(result)
        
        # 4. Benchmark event query
        result = benchmark_event_query(blockchain, "entity_0", name)
        results["benchmarks"].append(result)
        
        # 5. Benchmark get_chain_stats
        result = benchmark_get_chain_stats(blockchain, name)
        results["benchmarks"].append(result)
        
        # 6. Benchmark serialization
        result = benchmark_serialization(blockchain, name)
        results["benchmarks"].append(result)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results["error"] = str(e)
        import traceback
        traceback.print_exc()
    
    return results


def run_comprehensive_benchmark():
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    
    # Benchmark configurations
    configs = [
        {"event_count": 100, "num_blocks": 10, "events_per_block": 10, "label": "Small"},
        {"event_count": 1000, "num_blocks": 50, "events_per_block": 20, "label": "Medium"},
        {"event_count": 5000, "num_blocks": 100, "events_per_block": 50, "label": "Large"},
    ]
    
    all_results = []
    
    for config in configs:
        # Python benchmark
        if PYTHON_AVAILABLE:
            result = run_single_benchmark(PythonBlockchain, "Python", config)
            result["config_label"] = config["label"]
            all_results.append(result)
        
        # Rust benchmark
        if RUST_AVAILABLE:
            result = run_single_benchmark(RustBlockchain, "Rust", config)
            result["config_label"] = config["label"]
            all_results.append(result)
    
    # Save results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'Blockchain_benchmark.json')
    with open(results_path, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(all_results: list):
    """Print a summary comparison table."""
    w = 100
    m_h = f"{'Metric/Config':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'BLOCKCHAIN BENCHMARK SUMMARY':^100}")
    print("=" * w)
    print(h)
    print("-" * w)

    def get_status_icon(py_val, rs_val, higher_is_better=True):
        if not py_val or not rs_val or py_val == 0 or rs_val == 0:
            return "N/A", ""
        sp = (rs_val / py_val) if higher_is_better else (py_val / rs_val)

        if sp > 1.5:
            return f"{sp:.2f}x", "🚀"
        if sp < 0.8:
            return f"{sp:.2f}x", "⚠️"
        return f"{sp:.2f}x", "➡️"

    py_res = [r for r in all_results if r.get("implementation") == "Python"]
    rs_res = [r for r in all_results if r.get("implementation") == "Rust"]

    if not py_res and not rs_res:
        print("No valid results to summarize.")
        return

    ops = [
        ("add_event", "Addition", True),
        ("finalize_block", "Finalization", True),
        ("is_chain_valid", "Validation", False),
        ("get_events_by_entity", "Query", False),
        ("serialization", "Serializ.", False)
    ]

    for op_id, op_name, higher_is_better in ops:
        for label in ["Small", "Medium", "Large"]:
            p_d = next((r for r in py_res if r.get("config_label") == label), None)
            r_d = next((r for r in rs_res if r.get("config_label") == label), None)

            p_b = next((b for b in p_d.get("benchmarks", [])
                if b.get("operation") == op_id), None) if p_d else None
            r_b = next((b for b in r_d.get("benchmarks", [])
                if b.get("operation") == op_id), None) if r_d else None

            pv, rv = 0, 0
            pt, rt = "N/A", "N/A"

            if p_b:
                if op_id == "add_event":
                    pv = p_b.get("ops_per_second", 0)
                    pt = f"{pv:>10,.0f} ops/s"
                elif op_id == "finalize_block":
                    pv = p_b.get("blocks_per_second", 0)
                    pt = f"{pv:>10,.2f} blk/s"
                else:
                    pv = p_b.get("time", p_b.get("total_time", 0))
                    pt = f"{pv * 1000:>10.2f} ms"

            if r_b:
                if op_id == "add_event":
                    rv = r_b.get("ops_per_second", 0)
                    rt = f"{rv:>10,.0f} ops/s"
                elif op_id == "finalize_block":
                    rv = r_b.get("blocks_per_second", 0)
                    rt = f"{rv:>10,.2f} blk/s"
                else:
                    rv = r_b.get("time", r_b.get("total_time", 0))
                    rt = f"{rv * 1000:>10.2f} ms"

            sp, icon = get_status_icon(pv, rv, higher_is_better)
            row = f"{op_name} ({label})"
            print(f"{row:<30} | {pt:<18} | {rt:<18} | {sp:<8} | {icon:<6}")
        print("-" * w)

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

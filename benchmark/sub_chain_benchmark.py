"""
Benchmark script for comparing Rust and Python implementations of SubChain.

This script measures and compares the performance of:
- SubChain creation
- Event addition
- Domain operations (start, complete, status)
- Block finalization
- Entity history retrieval
- Domain statistics
"""

import time
import json
import sys
import os
from typing import Any
from datetime import datetime
import logging

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Logging Configuration ---
# Suppress consensus validation warnings and errors during benchmark
logging.getLogger('hierachain.hierarchical.sub_chain').setLevel(logging.CRITICAL)
logging.getLogger('hierachain.consensus.ordering_service').setLevel(logging.CRITICAL)

# --- Implementation Imports ---
PYTHON_AVAILABLE = False
PythonSubChain = None

try:
    from hierachain.hierarchical.sub_chain import SubChain as _PythonSubChain
    PythonSubChain = _PythonSubChain
    PYTHON_AVAILABLE = True
except ImportError:
    pass

RUST_AVAILABLE = False
RustSubChain = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "SubChain"):
        RustSubChain = hierachain_consensus.SubChain
        RUST_AVAILABLE = True
except ImportError:
    pass


def create_subchain(subchain_class: Any, name: str, domain_type: str, impl_name: str) -> Any:
    """Create a SubChain instance."""
    try:
        return subchain_class(name, domain_type)
    except Exception as e:
        print(f"Error creating {impl_name} SubChain: {e}")
        return None


def benchmark_creation(subchain_class: Any, count: int, impl_name: str) -> dict[str, Any]:
    """Benchmark SubChain creation."""
    times = []
    chains = []
    for i in range(count):
        start = time.perf_counter()
        sc = create_subchain(subchain_class, f"sc_{i}", "logistics", impl_name)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        if sc:
            chains.append(sc)
    
    # Cleanup
    for sc in chains:
        try:
            sc.stop()
        except Exception:
            pass
    
    total_time = sum(times)
    return {
        "operation": "create_subchain",
        "count": count,
        "total_time": total_time,
        "ops_per_second": count / total_time if total_time > 0 else 0
    }


def benchmark_add_event(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark adding events."""
    start = time.perf_counter()
    for i in range(count):
        subchain.add_event({
            "entity_id": f"entity_{i % 50}",
            "event": "test_event",
            "data": {"index": i, "value": f"value_{i}"}
        })
    elapsed = time.perf_counter() - start
    return {
        "operation": "add_event",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_start_operation(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark starting operations."""
    start = time.perf_counter()
    for i in range(count):
        subchain.start_operation(f"entity_{i % 50}", "processing", {"batch": i})
    elapsed = time.perf_counter() - start
    return {
        "operation": "start_operation",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_complete_operation(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark completing operations."""
    start = time.perf_counter()
    for i in range(count):
        subchain.complete_operation(f"entity_{i % 50}", "processing", {"result": "success"})
    elapsed = time.perf_counter() - start
    return {
        "operation": "complete_operation",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_update_status(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark status updates."""
    statuses = ["pending", "processing", "completed", "shipped", "delivered"]
    start = time.perf_counter()
    for i in range(count):
        subchain.update_entity_status(f"entity_{i % 50}", statuses[i % len(statuses)])
    elapsed = time.perf_counter() - start
    return {
        "operation": "update_entity_status",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_get_entity_history(subchain: Any, entity_count: int) -> dict[str, Any]:
    """Benchmark getting entity history."""
    start = time.perf_counter()
    total_events = 0
    for i in range(entity_count):
        history = subchain.get_entity_history(f"entity_{i}")
        total_events += len(history) if history else 0
    elapsed = time.perf_counter() - start
    return {
        "operation": "get_entity_history",
        "count": entity_count,
        "total_events_found": total_events,
        "time": elapsed,
        "ops_per_second": entity_count / elapsed if elapsed > 0 else 0
    }


def benchmark_get_statistics(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark getting domain statistics."""
    start = time.perf_counter()
    for _ in range(count):
        subchain.get_domain_statistics()
    elapsed = time.perf_counter() - start
    return {
        "operation": "get_domain_statistics",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_finalize_block(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark block finalization."""
    times = []
    successful = 0
    for i in range(count):
        for j in range(5):
            subchain.add_event({
                "entity_id": f"entity_{j}",
                "event": "test_event",
                "data": {"batch": i, "item": j}
            })
        start = time.perf_counter()
        result = subchain.finalize_block()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        if result is not None:
            successful += 1
    total_time = sum(times) if times else 1
    return {
        "operation": "finalize_block",
        "count": count,
        "successful": successful,
        "time": total_time,
        "ops_per_second": count / total_time if total_time > 0 else 0
    }


def benchmark_should_submit_proof(subchain: Any, count: int) -> dict[str, Any]:
    """Benchmark checking proof submission."""
    start = time.perf_counter()
    true_count = 0
    for _ in range(count):
        if subchain.should_submit_proof():
            true_count += 1
    elapsed = time.perf_counter() - start
    return {
        "operation": "should_submit_proof",
        "count": count,
        "true_count": true_count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def run_single_benchmark(subchain_class: Any, impl_name: str, config: dict[str, Any]) -> dict[str, Any]:
    """Run all benchmarks for a single implementation."""
    results = {
        "implementation": impl_name,
        "config": config,
        "benchmarks": []
    }
    try:
        results["benchmarks"].append(benchmark_creation(subchain_class, config['creation_count'], impl_name))
        
        sc = create_subchain(subchain_class, "benchmark_chain", "logistics", impl_name)
        if not sc:
            results["error"] = "Failed to create SubChain"
            return results
        
        results["benchmarks"].append(benchmark_add_event(sc, config['event_count']))
        results["benchmarks"].append(benchmark_start_operation(sc, config['operation_count']))
        results["benchmarks"].append(benchmark_complete_operation(sc, config['operation_count']))
        results["benchmarks"].append(benchmark_update_status(sc, config['operation_count']))
        results["benchmarks"].append(benchmark_get_entity_history(sc, config['entity_count']))
        results["benchmarks"].append(benchmark_get_statistics(sc, config['stats_count']))
        results["benchmarks"].append(benchmark_should_submit_proof(sc, config['stats_count']))
        results["benchmarks"].append(benchmark_finalize_block(sc, config.get('finalize_count', 20)))
        
        try:
            sc.stop()
        except Exception:
            pass
    except Exception as e:
        results["error"] = str(e)
    return results


def run_comprehensive_benchmark():
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    if PYTHON_AVAILABLE:
        print("✓ Python SubChain implementation available")
    else:
        print("⚠ Warning: Python SubChain implementation not available")

    if RUST_AVAILABLE:
        print("✓ Rust SubChain implementation available")
    else:
        print("⚠ Warning: Rust module loaded but SubChain not found.")

    config = {
        "creation_count": 10,
        "event_count": 500,
        "operation_count": 200,
        "entity_count": 50,
        "stats_count": 100,
        "finalize_count": 20,
        "label": "Medium"
    }
    
    all_results = []
    if PYTHON_AVAILABLE and PythonSubChain is not None:
        result = run_single_benchmark(PythonSubChain, "Python", config)
        result["config_label"] = config["label"]
        all_results.append(result)
    
    if RUST_AVAILABLE and RustSubChain is not None:
        result = run_single_benchmark(RustSubChain, "Rust", config)
        result["config_label"] = config["label"]
        all_results.append(result)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'SubChain_benchmark.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print_summary(all_results)
    return all_results


def print_summary(all_results: list[dict[str, Any]]):
    """Print a summary comparison table."""
    w = 100
    m_h = f"{'Operation':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'SUBCHAIN BENCHMARK SUMMARY':^100}")
    print("=" * w)
    print(h)
    print("-" * w)

    def get_status_icon(py_v, rs_v):
        if not py_v or not rs_v or py_v == 0 or rs_v == 0:
            return "N/A", ""
        sp = rs_v / py_v
        if sp > 1.5:
            return f"{sp:.2f}x", "🚀"
        if sp < 0.8:
            return f"{sp:.2f}x", "⚠️"
        return f"{sp:.2f}x", "➡️"

    py_res = next((r for r in all_results if r.get("implementation") == "Python"), None)
    rs_res = next((r for r in all_results if r.get("implementation") == "Rust"), None)

    operations = [
        "create_subchain", "add_event", "start_operation",
        "complete_operation", "update_entity_status",
        "get_entity_history", "get_domain_statistics",
        "should_submit_proof", "finalize_block"
    ]
    
    for op in operations:
        p_b = next((b for b in py_res.get("benchmarks", []) 
                    if b.get("operation") == op), None) if py_res else None
        r_b = next((b for b in rs_res.get("benchmarks", []) 
                    if b.get("operation") == op), None) if rs_res else None
        
        pv = p_b.get("ops_per_second", 0) if p_b else 0
        rv = r_b.get("ops_per_second", 0) if r_b else 0
        
        pt = f"{pv:>12.1f} op/s" if pv else "N/A"
        rt = f"{rv:>12.1f} op/s" if rv else "N/A"
        
        sp, icon = get_status_icon(pv, rv)
        print(f"{op:<30} | {pt:<18} | {rt:<18} | {sp:<8} | {icon:<6}")

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

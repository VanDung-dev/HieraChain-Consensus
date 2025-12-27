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
import statistics
import os
from typing import Any
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Implementation Imports ---
PYTHON_AVAILABLE = False
PythonSubChain = None

try:
    from hierachain.hierarchical.sub_chain import SubChain as _PythonSubChain
    PythonSubChain = _PythonSubChain
    PYTHON_AVAILABLE = True
    print("✓ Python SubChain implementation available")
except ImportError as e:
    print(f"⚠ Warning: Python SubChain implementation not available: {e}")

RUST_AVAILABLE = False
RustSubChain = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "SubChain"):
        RustSubChain = hierachain_consensus.SubChain
        RUST_AVAILABLE = True
        print("✓ Rust SubChain implementation available")
    else:
        print("⚠ Warning: Rust module loaded but SubChain not found.")
except ImportError as e:
    print(f"⚠ Warning: Rust implementation not available: {e}")


def create_subchain(subchain_class: Any, name: str, domain_type: str, impl_name: str) -> Any:
    """Create a SubChain instance."""
    try:
        return subchain_class(name, domain_type)
    except Exception as e:
        print(f"Error creating {impl_name} SubChain: {e}")
        return None


def benchmark_creation(subchain_class: Any, count: int, impl_name: str) -> dict[str, Any]:
    """Benchmark SubChain creation."""
    times: list[float] = []
    chains: list[Any] = []
    
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
    
    return {
        "operation": "create_subchain",
        "count": count,
        "total_time": sum(times),
        "avg_time": statistics.mean(times) if times else 0,
        "ops_per_second": count / sum(times) if sum(times) > 0 else 0
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
    times: list[float] = []
    successful = 0
    
    for i in range(count):
        # Add some events first for each finalization
        for j in range(5):
            subchain.add_event({
                "entity_id": f"entity_{j}",
                "event": "test_event",
                "data": {"batch": i, "item": j}
            })
        
        # Try to finalize
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
    print(f"\n{'='*50}")
    print(f"🔧 Benchmarking {impl_name}")
    print(f"{'='*50}")
    
    results: dict[str, Any] = {
        "implementation": impl_name,
        "config": config,
        "benchmarks": []
    }
    
    try:
        # 1. Benchmark creation
        print(f"\n📦 Testing SubChain creation ({config['creation_count']} chains)...")
        result = benchmark_creation(subchain_class, config['creation_count'], impl_name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} creations/sec (avg: {result['avg_time']*1000:.2f}ms)")
        
        # Create fresh chain for remaining tests
        subchain = create_subchain(subchain_class, "benchmark_chain", "logistics", impl_name)
        if not subchain:
            results["error"] = "Failed to create SubChain"
            return results
        
        # 2. Benchmark add_event
        print(f"\n📝 Testing add_event ({config['event_count']} events)...")
        result = benchmark_add_event(subchain, config['event_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} events/sec")
        
        # 3. Benchmark start_operation
        print(f"\n🚀 Testing start_operation ({config['operation_count']} ops)...")
        result = benchmark_start_operation(subchain, config['operation_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} ops/sec")
        
        # 4. Benchmark complete_operation
        print(f"\n✓ Testing complete_operation ({config['operation_count']} ops)...")
        result = benchmark_complete_operation(subchain, config['operation_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} ops/sec")
        
        # 5. Benchmark update_entity_status
        print(f"\n🔄 Testing update_entity_status ({config['operation_count']} ops)...")
        result = benchmark_update_status(subchain, config['operation_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} ops/sec")
        
        # 6. Benchmark get_entity_history
        print(f"\n📜 Testing get_entity_history ({config['entity_count']} entities)...")
        result = benchmark_get_entity_history(subchain, config['entity_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} queries/sec")
        
        # 7. Benchmark get_domain_statistics
        print(f"\n📊 Testing get_domain_statistics ({config['stats_count']} calls)...")
        result = benchmark_get_statistics(subchain, config['stats_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} calls/sec")
        
        # 8. Benchmark should_submit_proof
        print(f"\n🔍 Testing should_submit_proof ({config['stats_count']} checks)...")
        result = benchmark_should_submit_proof(subchain, config['stats_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} checks/sec")
        
        # 9. Benchmark finalize_block
        print(f"\n📦 Testing finalize_block ({config.get('finalize_count', 20)} blocks)...")
        result = benchmark_finalize_block(subchain, config.get('finalize_count', 20))
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} blocks/sec")
        
        # Cleanup
        try:
            subchain.stop()
        except Exception:
            pass
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results["error"] = str(e)
        import traceback
        traceback.print_exc()
    
    return results


def run_comprehensive_benchmark() -> list[dict[str, Any]]:
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    print("🚀 Starting SubChain Benchmark")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Benchmark configuration
    config: dict[str, Any] = {
        "creation_count": 10,
        "event_count": 500,
        "operation_count": 200,
        "entity_count": 50,
        "stats_count": 100,
        "finalize_count": 20,
        "label": "Medium"
    }
    
    all_results: list[dict[str, Any]] = []
    
    print(f"\n\n{'#'*60}")
    print(f"# Configuration: {config['label']}")
    print(f"# Events: {config['event_count']}, Operations: {config['operation_count']}")
    print(f"{'#'*60}")
    
    # Python benchmark
    if PYTHON_AVAILABLE and PythonSubChain is not None:
        result = run_single_benchmark(PythonSubChain, "Python", config)
        result["config_label"] = config["label"]
        all_results.append(result)
    
    # Rust benchmark
    if RUST_AVAILABLE and RustSubChain is not None:
        result = run_single_benchmark(RustSubChain, "Rust", config)
        result["config_label"] = config["label"]
        all_results.append(result)
    
    # Save results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'SubChain_benchmark.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(all_results: list[dict[str, Any]]) -> None:
    """Print a summary comparison table."""
    print("\n" + "=" * 90)
    print("📈 SUBCHAIN BENCHMARK SUMMARY")
    print("=" * 90)
    
    python_result = next((r for r in all_results if r.get("implementation") == "Python" and "error" not in r), None)
    rust_result = next((r for r in all_results if r.get("implementation") == "Rust" and "error" not in r), None)
    
    if not python_result and not rust_result:
        print("  No valid results available")
        return
    
    operations = [
        "create_subchain", "add_event", "start_operation",
        "complete_operation", "update_entity_status",
        "get_entity_history", "get_domain_statistics",
        "should_submit_proof", "finalize_block"
    ]
    
    print(f"\n{'─'*90}")
    print(f"{'Operation':<30} | {'Python':>15} | {'Rust':>15} | {'Speedup':>12}")
    print(f"{'─'*90}")
    
    for op in operations:
        py_bench = None
        rs_bench = None
        
        if python_result:
            py_bench = next((b for b in python_result.get("benchmarks", []) if b.get("operation") == op), None)
        if rust_result:
            rs_bench = next((b for b in rust_result.get("benchmarks", []) if b.get("operation") == op), None)
        
        if py_bench and rs_bench:
            py_metric = py_bench.get("ops_per_second", 0)
            rs_metric = rs_bench.get("ops_per_second", 0)
            speedup = rs_metric / py_metric if py_metric > 0 else 0
            indicator = "🚀" if speedup > 1.5 else ("⚠️" if speedup < 0.8 else "➡️")
            print(f"  {op:<28} | {py_metric:>12.1f}/s | {rs_metric:>12.1f}/s | {indicator} {speedup:.2f}x")
        elif py_bench:
            py_metric = py_bench.get("ops_per_second", 0)
            print(f"  {op:<28} | {py_metric:>12.1f}/s | {'N/A':>12} | {'N/A':>12}")
        elif rs_bench:
            rs_metric = rs_bench.get("ops_per_second", 0)
            print(f"  {op:<28} | {'N/A':>12} | {rs_metric:>12.1f}/s | {'N/A':>12}")
    
    print(f"{'─'*90}")
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * 90)


if __name__ == "__main__":
    results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

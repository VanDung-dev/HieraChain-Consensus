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
import matplotlib.pyplot as plt
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
    print(f"\n{'='*50}")
    print(f"🔧 Benchmarking {name} Blockchain")
    print(f"{'='*50}")
    
    results = {
        "implementation": name,
        "config": config,
        "benchmarks": []
    }
    
    try:
        # Create fresh blockchain
        blockchain = blockchain_class("BenchmarkChain")
        
        # 1. Benchmark event addition
        print(f"\n📝 Testing event addition ({config['event_count']} events)...")
        events = create_test_events(config['event_count'])
        result = benchmark_event_addition(blockchain, events, name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} events/sec")
        
        # 2. Benchmark block creation
        print(f"\n📦 Testing block creation ({config['num_blocks']} blocks × {config['events_per_block']} events)...")
        blockchain = blockchain_class("BenchmarkChain2")  # Fresh chain
        result = benchmark_block_creation(blockchain, config['events_per_block'], config['num_blocks'], name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['blocks_per_second']:.2f} blocks/sec (avg: {result['avg_time']*1000:.2f}ms)")
        
        # 3. Benchmark chain validation
        print(f"\n🔍 Testing chain validation...")
        result = benchmark_chain_validation(blockchain, name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['time']*1000:.2f}ms (valid: {result['result']})")
        
        # 4. Benchmark event query
        print(f"\n🔎 Testing event query by entity...")
        result = benchmark_event_query(blockchain, "entity_0", name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['time']*1000:.4f}ms ({result['events_found']} events found)")
        
        # 5. Benchmark get_chain_stats
        print(f"\n📊 Testing get_chain_stats...")
        result = benchmark_get_chain_stats(blockchain, name)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['time']*1000:.4f}ms")
        
        # 6. Benchmark serialization
        print(f"\n💾 Testing serialization (to_dict/from_dict)...")
        result = benchmark_serialization(blockchain, name)
        results["benchmarks"].append(result)
        print(f"   ✅ to_dict: {result['to_dict_time']*1000:.2f}ms, from_dict: {result['from_dict_time']*1000:.2f}ms")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results["error"] = str(e)
        import traceback
        traceback.print_exc()
    
    return results


def run_comprehensive_benchmark():
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    print("🚀 Starting Blockchain Benchmark")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Benchmark configurations
    configs = [
        {"event_count": 100, "num_blocks": 10, "events_per_block": 10, "label": "Small"},
        {"event_count": 1000, "num_blocks": 50, "events_per_block": 20, "label": "Medium"},
        {"event_count": 5000, "num_blocks": 100, "events_per_block": 50, "label": "Large"},
    ]
    
    all_results = []
    
    for config in configs:
        print(f"\n\n{'#'*60}")
        print(f"# Configuration: {config['label']}")
        print(f"# Events: {config['event_count']}, Blocks: {config['num_blocks']}, Events/Block: {config['events_per_block']}")
        print(f"{'#'*60}")
        
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
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(all_results: list):
    """Print a summary comparison table."""
    print("\n" + "=" * 80)
    print("📈 BENCHMARK SUMMARY")
    print("=" * 80)
    
    # Group by config label
    python_results = [r for r in all_results if r.get("implementation") == "Python" and "error" not in r]
    rust_results = [r for r in all_results if r.get("implementation") == "Rust" and "error" not in r]
    
    if not python_results and not rust_results:
        print("No valid results to summarize.")
        return
    
    # Print comparison for each operation
    operations = ["add_event", "finalize_block", "is_chain_valid", "get_events_by_entity", "get_chain_stats", "serialization"]
    
    for op in operations:
        print(f"\n--- {op} ---")
        
        for label in ["Small", "Medium", "Large"]:
            py_data = next((r for r in python_results if r.get("config_label") == label), None)
            rs_data = next((r for r in rust_results if r.get("config_label") == label), None)
            
            if py_data:
                py_bench = next((b for b in py_data.get("benchmarks", []) if b.get("operation") == op), None)
            else:
                py_bench = None
                
            if rs_data:
                rs_bench = next((b for b in rs_data.get("benchmarks", []) if b.get("operation") == op), None)
            else:
                rs_bench = None
            
            if py_bench and rs_bench:
                # Calculate speedup
                if op == "add_event":
                    py_metric = py_bench.get("ops_per_second", 0)
                    rs_metric = rs_bench.get("ops_per_second", 0)
                    speedup = rs_metric / py_metric if py_metric > 0 else 0
                    print(f"  {label:8} | Python: {py_metric:>10.2f} ops/s | Rust: {rs_metric:>10.2f} ops/s | Speedup: {speedup:.2f}x")
                elif op == "finalize_block":
                    py_metric = py_bench.get("blocks_per_second", 0)
                    rs_metric = rs_bench.get("blocks_per_second", 0)
                    speedup = rs_metric / py_metric if py_metric > 0 else 0
                    print(f"  {label:8} | Python: {py_metric:>10.2f} blk/s | Rust: {rs_metric:>10.2f} blk/s | Speedup: {speedup:.2f}x")
                else:
                    py_time = py_bench.get("time", py_bench.get("total_time", 0)) * 1000
                    rs_time = rs_bench.get("time", rs_bench.get("total_time", 0)) * 1000
                    speedup = py_time / rs_time if rs_time > 0 else 0
                    print(f"  {label:8} | Python: {py_time:>10.4f} ms  | Rust: {rs_time:>10.4f} ms  | Speedup: {speedup:.2f}x")
            elif py_bench:
                print(f"  {label:8} | Python: available | Rust: N/A")
            elif rs_bench:
                print(f"  {label:8} | Python: N/A | Rust: available")
    
    # Overall average speedup
    if python_results and rust_results:
        total_py_time = 0
        total_rs_time = 0
        
        for py_r, rs_r in zip(python_results, rust_results):
            for py_b, rs_b in zip(py_r.get("benchmarks", []), rs_r.get("benchmarks", [])):
                py_t = py_b.get("time", py_b.get("total_time", 0))
                rs_t = rs_b.get("time", rs_b.get("total_time", 0))
                if py_t > 0 and rs_t > 0:
                    total_py_time += py_t
                    total_rs_time += rs_t
        
        if total_rs_time > 0:
            overall_speedup = total_py_time / total_rs_time
            print(f"\n{'='*80}")
            print(f"⚡ OVERALL AVERAGE SPEEDUP (Rust vs Python): {overall_speedup:.2f}x faster")
            print(f"{'='*80}")


def analyze_benchmark(file_path: str):
    """Read results and generate visualization charts."""
    try:
        with open(file_path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Could not find result file: {file_path}")
        return
    
    python_results = [d for d in data if d.get('implementation') == 'Python' and 'error' not in d]
    rust_results = [d for d in data if d.get('implementation') == 'Rust' and 'error' not in d]
    
    if not python_results and not rust_results:
        print("No valid data to plot.")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Blockchain Benchmark: Python vs Rust', fontsize=14, fontweight='bold')
    
    labels = ["Small", "Medium", "Large"]
    x = range(len(labels))
    width = 0.35
    
    # 1. Event Addition Throughput
    ax = axes[0, 0]
    py_vals = []
    rs_vals = []
    for label in labels:
        py_r = next((r for r in python_results if r.get("config_label") == label), None)
        rs_r = next((r for r in rust_results if r.get("config_label") == label), None)
        py_bench = next((b for b in (py_r or {}).get("benchmarks", []) if b.get("operation") == "add_event"), None)
        rs_bench = next((b for b in (rs_r or {}).get("benchmarks", []) if b.get("operation") == "add_event"), None)
        py_vals.append(py_bench.get("ops_per_second", 0) if py_bench else 0)
        rs_vals.append(rs_bench.get("ops_per_second", 0) if rs_bench else 0)
    
    if py_vals:
        ax.bar([i - width/2 for i in x], py_vals, width, label='Python', color='steelblue')
    if rs_vals:
        ax.bar([i + width/2 for i in x], rs_vals, width, label='Rust', color='darkorange')
    ax.set_title('Event Addition Throughput')
    ax.set_ylabel('Events/sec')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 2. Block Creation Throughput
    ax = axes[0, 1]
    py_vals = []
    rs_vals = []
    for label in labels:
        py_r = next((r for r in python_results if r.get("config_label") == label), None)
        rs_r = next((r for r in rust_results if r.get("config_label") == label), None)
        py_bench = next((b for b in (py_r or {}).get("benchmarks", []) if b.get("operation") == "finalize_block"), None)
        rs_bench = next((b for b in (rs_r or {}).get("benchmarks", []) if b.get("operation") == "finalize_block"), None)
        py_vals.append(py_bench.get("blocks_per_second", 0) if py_bench else 0)
        rs_vals.append(rs_bench.get("blocks_per_second", 0) if rs_bench else 0)
    
    if py_vals:
        ax.bar([i - width/2 for i in x], py_vals, width, label='Python', color='steelblue')
    if rs_vals:
        ax.bar([i + width/2 for i in x], rs_vals, width, label='Rust', color='darkorange')
    ax.set_title('Block Creation Throughput')
    ax.set_ylabel('Blocks/sec')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 3. Chain Validation Time
    ax = axes[1, 0]
    py_vals = []
    rs_vals = []
    for label in labels:
        py_r = next((r for r in python_results if r.get("config_label") == label), None)
        rs_r = next((r for r in rust_results if r.get("config_label") == label), None)
        py_bench = next((b for b in (py_r or {}).get("benchmarks", []) if b.get("operation") == "is_chain_valid"), None)
        rs_bench = next((b for b in (rs_r or {}).get("benchmarks", []) if b.get("operation") == "is_chain_valid"), None)
        py_vals.append(py_bench.get("time", 0) * 1000 if py_bench else 0)
        rs_vals.append(rs_bench.get("time", 0) * 1000 if rs_bench else 0)
    
    if py_vals:
        ax.bar([i - width/2 for i in x], py_vals, width, label='Python', color='steelblue')
    if rs_vals:
        ax.bar([i + width/2 for i in x], rs_vals, width, label='Rust', color='darkorange')
    ax.set_title('Chain Validation Time')
    ax.set_ylabel('Time (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # 4. Serialization Time
    ax = axes[1, 1]
    py_vals = []
    rs_vals = []
    for label in labels:
        py_r = next((r for r in python_results if r.get("config_label") == label), None)
        rs_r = next((r for r in rust_results if r.get("config_label") == label), None)
        py_bench = next((b for b in (py_r or {}).get("benchmarks", []) if b.get("operation") == "serialization"), None)
        rs_bench = next((b for b in (rs_r or {}).get("benchmarks", []) if b.get("operation") == "serialization"), None)
        py_vals.append(py_bench.get("total_time", 0) * 1000 if py_bench else 0)
        rs_vals.append(rs_bench.get("total_time", 0) * 1000 if rs_bench else 0)
    
    if py_vals:
        ax.bar([i - width/2 for i in x], py_vals, width, label='Python', color='steelblue')
    if rs_vals:
        ax.bar([i + width/2 for i in x], rs_vals, width, label='Rust', color='darkorange')
    ax.set_title('Serialization Time (to_dict + from_dict)')
    ax.set_ylabel('Time (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save chart
    output_dir = os.path.dirname(file_path)
    chart_path = os.path.join(output_dir, 'Blockchain_benchmark.png')
    plt.savefig(chart_path, dpi=150)
    print(f"📊 Chart saved to '{chart_path}'")
    plt.close()


if __name__ == "__main__":
    results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")
    
    # Generate charts
    time.sleep(0.5)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_path = os.path.join(script_dir, 'output', 'Blockchain_benchmark.json')
    
    if os.path.exists(results_path):
        analyze_benchmark(results_path)
    else:
        print("⚠ Result file not found, skipping chart generation.")

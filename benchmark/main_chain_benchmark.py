"""
Benchmark script for comparing Rust and Python implementations of MainChain.

This script measures and compares the performance of:
- Sub-chain registration
- Proof addition
- Proof verification
- Block finalization
- Integrity reporting
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
PythonMainChain = None

try:
    from hierachain.hierarchical.main_chain import MainChain as _PythonMainChain
    PythonMainChain = _PythonMainChain
    PYTHON_AVAILABLE = True
    print("✓ Python MainChain implementation available")
except ImportError as e:
    print(f"⚠ Warning: Python MainChain implementation not available: {e}")

RUST_AVAILABLE = False
RustMainChain = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "MainChain"):
        RustMainChain = hierachain_consensus.MainChain
        RUST_AVAILABLE = True
        print("✓ Rust MainChain implementation available")
        # Check for PoF support
        if hasattr(RustMainChain, "with_pof"):
            print("  ✓ Proof of Federation (PoF) support detected")
        if hasattr(RustMainChain, "consensus_type"):
            print("  ✓ consensus_type property available")
    else:
        print("⚠ Warning: Rust module loaded but MainChain not found.")
except ImportError as e:
    print(f"⚠ Warning: Rust implementation not available: {e}")


def benchmark_sub_chain_registration(main_chain: Any, count: int) -> dict[str, Any]:
    """Benchmark registering sub-chains."""
    start = time.perf_counter()
    for i in range(count):
        main_chain.register_sub_chain(f"SubChain_{i}", {"domain": f"domain_{i}"})
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "register_sub_chain",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_proof_addition(main_chain: Any, count: int) -> dict[str, Any]:
    """Benchmark adding proofs."""
    # Register a sub-chain first if not already
    main_chain.register_sub_chain("TestSubChain", {"domain": "test"})
    
    start = time.perf_counter()
    for i in range(count):
        main_chain.add_proof(
            "TestSubChain",
            f"proof_hash_{i:08x}",
            {"block_index": i, "events_count": 10, "merkle_root": f"root_{i}"}
        )
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "add_proof",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_proof_verification(main_chain: Any) -> dict[str, Any]:
    """Benchmark verifying proofs."""
    # Add some proofs first
    main_chain.register_sub_chain("VerifyTest", {"domain": "verify"})
    for i in range(100):
        main_chain.add_proof(
            "VerifyTest",
            f"verify_hash_{i:04x}",
            {"block_index": i}
        )
    
    # Finalize to commit proofs
    main_chain.finalize_block()
    
    # Benchmark verification
    start = time.perf_counter()
    found = 0
    for i in range(100):
        if main_chain.verify_proof(f"verify_hash_{i:04x}", "VerifyTest"):
            found += 1
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "verify_proof",
        "count": 100,
        "found": found,
        "time": elapsed,
        "ops_per_second": 100 / elapsed if elapsed > 0 else 0
    }


def benchmark_block_finalization(main_chain: Any, num_blocks: int, proofs_per_block: int) -> dict[str, Any]:
    """Benchmark block finalization."""
    main_chain.register_sub_chain("FinalizeTest", {"domain": "finalize"})
    
    times: list[float] = []
    for b in range(num_blocks):
        # Add proofs
        for p in range(proofs_per_block):
            main_chain.add_proof(
                "FinalizeTest",
                f"block_{b}_proof_{p}",
                {"block_index": b * proofs_per_block + p}
            )
        
        # Finalize
        start = time.perf_counter()
        main_chain.finalize_block()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    
    return {
        "operation": "finalize_block",
        "num_blocks": num_blocks,
        "proofs_per_block": proofs_per_block,
        "total_time": sum(times),
        "avg_time": statistics.mean(times) if times else 0,
        "blocks_per_second": num_blocks / sum(times) if sum(times) > 0 else 0
    }


def benchmark_integrity_report(main_chain: Any) -> dict[str, Any]:
    """Benchmark generating integrity reports."""
    # Register some sub-chains and add proofs
    for i in range(5):
        main_chain.register_sub_chain(f"ReportTest_{i}", {"domain": f"domain_{i}"})
        for j in range(10):
            main_chain.add_proof(f"ReportTest_{i}", f"report_proof_{i}_{j}", {"index": j})
    
    main_chain.finalize_block()
    
    start = time.perf_counter()
    report = main_chain.get_hierarchical_integrity_report()
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_hierarchical_integrity_report",
        "time": elapsed,
        "sub_chains_in_report": len(report.get("sub_chains", {})) if isinstance(report, dict) else 0
    }


def benchmark_get_stats(main_chain: Any) -> dict[str, Any]:
    """Benchmark getting main chain stats."""
    start = time.perf_counter()
    stats = main_chain.get_main_chain_stats()
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_main_chain_stats",
        "time": elapsed,
        "stats": stats if isinstance(stats, dict) else {}
    }


def create_main_chain(main_chain_class: Any, name: str, consensus_type: str = "proof_of_authority") -> Any:
    """Create a MainChain instance with the specified consensus type.
    
    This handles both Python and Rust implementations with their different APIs.
    """
    # Check if this is the Rust implementation
    if hasattr(main_chain_class, "with_pof") and consensus_type == "proof_of_federation":
        return main_chain_class.with_pof(name)
    
    # Try Rust-style constructor with consensus_type parameter
    try:
        return main_chain_class(name, consensus_type)
    except TypeError:
        # Fall back to Python-style (uses settings.CONSENSUS_TYPE)
        return main_chain_class(name)


def run_single_benchmark(main_chain_class: Any, impl_name: str, consensus_type: str, config: dict[str, Any]) -> dict[str, Any]:
    """Run all benchmarks for a single implementation."""
    full_name = f"{impl_name} ({consensus_type})"
    print(f"\n{'='*50}")
    print(f"🔧 Benchmarking {full_name}")
    print(f"{'='*50}")
    
    results: dict[str, Any] = {
        "implementation": impl_name,
        "consensus_type": consensus_type,
        "config": config,
        "benchmarks": []
    }
    
    try:
        # Create fresh main chain
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain", consensus_type)
        
        # Verify consensus type was set correctly (Rust only)
        if hasattr(main_chain, "consensus_type"):
            actual_ct = main_chain.consensus_type
            print(f"   Consensus: {actual_ct}")
        
        # 1. Benchmark sub-chain registration
        print(f"\n📝 Testing sub-chain registration ({config['sub_chain_count']} sub-chains)...")
        result = benchmark_sub_chain_registration(main_chain, config['sub_chain_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} registrations/sec")
        
        # 2. Fresh chain for proof tests
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain2", consensus_type)
        print(f"\n📦 Testing proof addition ({config['proof_count']} proofs)...")
        result = benchmark_proof_addition(main_chain, config['proof_count'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} proofs/sec")
        
        # 3. Proof verification
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain3", consensus_type)
        print(f"\n🔍 Testing proof verification...")
        result = benchmark_proof_verification(main_chain)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['ops_per_second']:.2f} verifications/sec (found: {result['found']})")
        
        # 4. Block finalization
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain4", consensus_type)
        print(f"\n⛓️ Testing block finalization ({config['num_blocks']} blocks × {config['proofs_per_block']} proofs)...")
        result = benchmark_block_finalization(main_chain, config['num_blocks'], config['proofs_per_block'])
        results["benchmarks"].append(result)
        print(f"   ✅ {result['blocks_per_second']:.2f} blocks/sec (avg: {result['avg_time']*1000:.2f}ms)")
        
        # 5. Integrity report
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain5", consensus_type)
        print(f"\n📊 Testing integrity report generation...")
        result = benchmark_integrity_report(main_chain)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['time']*1000:.4f}ms")
        
        # 6. Get stats
        print(f"\n📈 Testing get_main_chain_stats...")
        result = benchmark_get_stats(main_chain)
        results["benchmarks"].append(result)
        print(f"   ✅ {result['time']*1000:.4f}ms")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results["error"] = str(e)
        import traceback
        traceback.print_exc()
    
    return results


def run_comprehensive_benchmark() -> list[dict[str, Any]]:
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    print("🚀 Starting MainChain Benchmark (PoA + PoF)")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Benchmark configuration - use Medium only for faster testing
    config: dict[str, Any] = {
        "sub_chain_count": 50, 
        "proof_count": 500, 
        "num_blocks": 25, 
        "proofs_per_block": 20, 
        "label": "Medium"
    }
    
    # Consensus types to test
    consensus_types = ["proof_of_authority", "proof_of_federation"]
    
    all_results: list[dict[str, Any]] = []
    
    print(f"\n\n{'#'*60}")
    print(f"# Configuration: {config['label']}")
    print(f"# Sub-chains: {config['sub_chain_count']}, Proofs: {config['proof_count']}")
    print(f"{'#'*60}")
    
    for ct in consensus_types:
        print(f"\n\n{'='*60}")
        print(f"📋 CONSENSUS TYPE: {ct.upper()}")
        print(f"{'='*60}")
        
        # Python benchmark
        if PYTHON_AVAILABLE and PythonMainChain is not None:
            result = run_single_benchmark(PythonMainChain, "Python", ct, config)
            result["config_label"] = config["label"]
            all_results.append(result)
        
        # Rust benchmark
        if RUST_AVAILABLE and RustMainChain is not None:
            result = run_single_benchmark(RustMainChain, "Rust", ct, config)
            result["config_label"] = config["label"]
            all_results.append(result)
    
    # Save results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'MainChain_benchmark.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(all_results: list[dict[str, Any]]) -> None:
    """Print a summary comparison table."""
    print("\n" + "=" * 90)
    print("📈 MAINCHAIN BENCHMARK SUMMARY")
    print("=" * 90)
    
    # Group results by consensus type
    for ct in ["proof_of_authority", "proof_of_federation"]:
        ct_results = [r for r in all_results if r.get("consensus_type") == ct and "error" not in r]
        
        if not ct_results:
            continue
            
        ct_label = "PoA (Proof of Authority)" if ct == "proof_of_authority" else "PoF (Proof of Federation)"
        print(f"\n{'─'*90}")
        print(f"🔐 {ct_label}")
        print(f"{'─'*90}")
        
        python_result = next((r for r in ct_results if r.get("implementation") == "Python"), None)
        rust_result = next((r for r in ct_results if r.get("implementation") == "Rust"), None)
        
        if not python_result and not rust_result:
            print("  No results available")
            continue
        
        operations = ["register_sub_chain", "add_proof", "verify_proof", "finalize_block", 
                      "get_hierarchical_integrity_report", "get_main_chain_stats"]
        
        for op in operations:
            py_bench = None
            rs_bench = None
            
            if python_result:
                py_bench = next((b for b in python_result.get("benchmarks", []) if b.get("operation") == op), None)
            if rust_result:
                rs_bench = next((b for b in rust_result.get("benchmarks", []) if b.get("operation") == op), None)
            
            if py_bench and rs_bench:
                if "ops_per_second" in py_bench:
                    py_metric = py_bench.get("ops_per_second", 0)
                    rs_metric = rs_bench.get("ops_per_second", 0)
                    speedup = rs_metric / py_metric if py_metric > 0 else 0
                    indicator = "🚀" if speedup > 1.5 else ("⚠️" if speedup < 0.8 else "➡️")
                    print(f"  {op:40} | Py: {py_metric:>10.1f} ops/s | Rs: {rs_metric:>10.1f} ops/s | {indicator} {speedup:.2f}x")
                elif "blocks_per_second" in py_bench:
                    py_metric = py_bench.get("blocks_per_second", 0)
                    rs_metric = rs_bench.get("blocks_per_second", 0)
                    speedup = rs_metric / py_metric if py_metric > 0 else 0
                    indicator = "🚀" if speedup > 1.5 else ("⚠️" if speedup < 0.8 else "➡️")
                    print(f"  {op:40} | Py: {py_metric:>10.1f} blk/s | Rs: {rs_metric:>10.1f} blk/s | {indicator} {speedup:.2f}x")
                else:
                    py_time = py_bench.get("time", 0) * 1000
                    rs_time = rs_bench.get("time", 0) * 1000
                    speedup = py_time / rs_time if rs_time > 0 else 0
                    indicator = "🚀" if speedup > 1.5 else ("⚠️" if speedup < 0.8 else "➡️")
                    print(f"  {op:40} | Py: {py_time:>10.4f} ms   | Rs: {rs_time:>10.4f} ms   | {indicator} {speedup:.2f}x")
            elif py_bench:
                print(f"  {op:40} | Python only")
            elif rs_bench:
                print(f"  {op:40} | Rust only")
    
    print("\n" + "=" * 90)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * 90)


if __name__ == "__main__":
    results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

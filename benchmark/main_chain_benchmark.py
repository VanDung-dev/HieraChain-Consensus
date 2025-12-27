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


def benchmark_get_proofs_by_sub_chain(main_chain: Any, sub_chain_name: str) -> dict[str, Any]:
    """Benchmark getting all proofs for a sub-chain."""
    # Ensure there are proofs first
    main_chain.register_sub_chain(sub_chain_name, {"domain": "test"})
    for i in range(50):
        main_chain.add_proof(sub_chain_name, f"proof_{i}", {"index": i})
    main_chain.finalize_block()
    
    start = time.perf_counter()
    count = 100
    for _ in range(count):
        proofs = main_chain.get_proofs_by_sub_chain(sub_chain_name)
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_proofs_by_sub_chain",
        "count": count,
        "proofs_found": len(proofs) if hasattr(proofs, '__len__') else 0,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_get_sub_chain_summary(main_chain: Any, sub_chain_name: str) -> dict[str, Any]:
    """Benchmark getting sub-chain summary."""
    start = time.perf_counter()
    count = 100
    for _ in range(count):
        summary = main_chain.get_sub_chain_summary(sub_chain_name)
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_sub_chain_summary",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
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
    results: dict[str, Any] = {
        "implementation": impl_name,
        "consensus_type": consensus_type,
        "config": config,
        "benchmarks": []
    }
    
    try:
        # Create fresh main chain
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain", consensus_type)
        
        # 1. Benchmark sub-chain registration
        result = benchmark_sub_chain_registration(main_chain, config['sub_chain_count'])
        results["benchmarks"].append(result)
        # print(f"   ✅ {result['ops_per_second']:.2f} registrations/sec")

        # 2. Fresh chain for proof tests
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain2", consensus_type)
        result = benchmark_proof_addition(main_chain, config['proof_count'])
        results["benchmarks"].append(result)

        # 3. Proof verification
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain3", consensus_type)
        result = benchmark_proof_verification(main_chain)
        results["benchmarks"].append(result)
        
        # 4. Block finalization
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain4", consensus_type)
        result = benchmark_block_finalization(main_chain, config['num_blocks'], config['proofs_per_block'])
        results["benchmarks"].append(result)
        
        # 5. Integrity report
        main_chain = create_main_chain(main_chain_class, "BenchmarkMainChain5", consensus_type)
        result = benchmark_integrity_report(main_chain)
        results["benchmarks"].append(result)
        
        # 6. Get stats
        result = benchmark_get_stats(main_chain)
        results["benchmarks"].append(result)

        # 7. Get proofs by sub-chain (New)
        if hasattr(main_chain, "get_proofs_by_sub_chain"):
            result = benchmark_get_proofs_by_sub_chain(main_chain, "SubChainProofsTest")
            results["benchmarks"].append(result)
        
        # 8. Get sub-chain summary (New)
        if hasattr(main_chain, "get_sub_chain_summary"):
            result = benchmark_get_sub_chain_summary(main_chain, "SubChainSummaryTest")
            results["benchmarks"].append(result)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        results["error"] = str(e)
        import traceback
        traceback.print_exc()
    
    return results


def run_comprehensive_benchmark() -> list[dict[str, Any]]:
    """Run comprehensive benchmark comparing Python and Rust implementations."""
    
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
    
    for ct in consensus_types:
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
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(all_results: list[dict[str, Any]]) -> None:
    """Print a summary comparison table."""
    # Table width and headers
    w = 100
    m_h = f"{'Metric (Consensus)':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'MAINCHAIN BENCHMARK SUMMARY':^100}")
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

    ops = [
        ("register_sub_chain", "Registration", True),
        ("add_proof", "Proof Add", True),
        ("verify_proof", "Proof Verify", True),
        ("finalize_block", "Finalization", True),
        ("get_hierarchical_integrity_report", "Integrity Rep.", False),
        ("get_main_chain_stats", "Stats", False),
        ("get_proofs_by_sub_chain", "List Proofs", True),
        ("get_sub_chain_summary", "Summary", True)
    ]

    for op_id, op_name, higher_is_better in ops:
        for ct in ["proof_of_authority", "proof_of_federation"]:
            ct_s = "PoA" if ct == "proof_of_authority" else "PoF"
            ct_res = [r for r in all_results if r.get("consensus_type") == ct]
            
            p_d = next((r for r in ct_res if r.get("implementation") == "Python"), None)
            r_d = next((r for r in ct_res if r.get("implementation") == "Rust"), None)
            
            p_b = next((b for b in p_d.get("benchmarks", [])
                if b.get("operation") == op_id), None) if p_d else None
            r_b = next((b for b in r_d.get("benchmarks", [])
                if b.get("operation") == op_id), None) if r_d else None
            
            p_val, r_val = 0.0, 0.0
            pt, rt = "N/A", "N/A"

            if p_b:
                if "ops_per_second" in p_b:
                    p_val = float(p_b['ops_per_second'])
                    pt = f"{p_val:>10,.1f} op/s"
                elif "blocks_per_second" in p_b:
                    p_val = float(p_b['blocks_per_second'])
                    pt = f"{p_val:>10,.1f} blk/s"
                else:
                    p_val = float(p_b.get('time', 0))
                    pt = f"{p_val*1000:>10.2f} ms"

            if r_b:
                if "ops_per_second" in r_b:
                    r_val = float(r_b['ops_per_second'])
                    rt = f"{r_val:>10,.1f} op/s"
                elif "blocks_per_second" in r_b:
                    r_val = float(r_b['blocks_per_second'])
                    rt = f"{r_val:>10,.1f} blk/s"
                else:
                    r_val = float(r_b.get('time', 0))
                    rt = f"{r_val*1000:>10.2f} ms"

            sp_str, icon = get_status_icon(p_val, r_val, higher_is_better)
            row = f"{op_name} ({ct_s})"
            print(f"{row:<30} | {pt:<18} | {rt:<18} | {sp_str:<8} | {icon:<6}")
        print("-" * w)

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    benchmark_results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

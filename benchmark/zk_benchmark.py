"""
Benchmark script for ZK Proof Generation and Verification.

This script measures and compares the performance of:
- Mock proof generation (Python ZKProver)
- Mock proof verification (Python ZKVerifier)
- Proof format compatibility checks
- End-to-end ZK flow (SubChain -> MainChain with ZK)
"""

import time
import json
import sys
import os
import statistics
from typing import Any
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Implementation Imports ---
PROVER_AVAILABLE = False
VERIFIER_AVAILABLE = False
ZKProver = None
ZKVerifier = None

try:
    from hierachain.security.zk_prover import ZKProver as _ZKProver
    ZKProver = _ZKProver
    PROVER_AVAILABLE = True
    print("✓ ZKProver available")
except ImportError as e:
    print(f"⚠ Warning: ZKProver not available: {e}")

try:
    from hierachain.security.zk_verifier import ZKVerifier as _ZKVerifier, ZKPublicInputs
    ZKVerifier = _ZKVerifier
    VERIFIER_AVAILABLE = True
    print("✓ ZKVerifier available")
except ImportError as e:
    print(f"⚠ Warning: ZKVerifier not available: {e}")

# Check for Rust mock verifier
RUST_MOCK_AVAILABLE = False
try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "MockVerifier"):
        RUST_MOCK_AVAILABLE = True
        print("✓ Rust MockVerifier available")
except ImportError:
    pass


def benchmark_mock_proof_generation(prover: Any, count: int) -> dict[str, Any]:
    """Benchmark mock proof generation."""
    times = []
    proof_sizes = []
    
    for i in range(count):
        old_root = f"{i:064x}"
        new_root = f"{i+1:064x}"
        block_index = i
        
        start = time.perf_counter()
        result = prover.generate_proof(old_root, new_root, block_index)
        elapsed = time.perf_counter() - start
        
        times.append(elapsed)
        if result.proof:
            proof_sizes.append(len(result.proof))
    
    total_time = sum(times)
    return {
        "operation": "mock_proof_generation",
        "count": count,
        "total_time": total_time,
        "avg_time_ms": (total_time / count) * 1000 if count > 0 else 0,
        "ops_per_second": count / total_time if total_time > 0 else 0,
        "avg_proof_size_bytes": statistics.mean(proof_sizes) if proof_sizes else 0
    }


def benchmark_mock_verification(prover: Any, verifier: Any, count: int) -> dict[str, Any]:
    """Benchmark mock proof verification."""
    # Pre-generate proofs
    proofs_and_inputs = []
    for i in range(count):
        old_root = f"{i:064x}"
        new_root = f"{i+1:064x}"
        block_index = i
        
        result = prover.generate_proof(old_root, new_root, block_index)
        public_inputs = {
            "old_state_root": old_root,
            "new_state_root": new_root,
            "block_index": block_index,
            "sub_chain_name": ""
        }
        proofs_and_inputs.append((result.proof, public_inputs))
    
    # Benchmark verification
    times = []
    valid_count = 0
    
    for proof, inputs in proofs_and_inputs:
        start = time.perf_counter()
        is_valid = verifier.verify(proof, inputs)
        elapsed = time.perf_counter() - start
        
        times.append(elapsed)
        if is_valid:
            valid_count += 1
    
    total_time = sum(times)
    return {
        "operation": "mock_verification",
        "count": count,
        "valid_count": valid_count,
        "total_time": total_time,
        "avg_time_ms": (total_time / count) * 1000 if count > 0 else 0,
        "ops_per_second": count / total_time if total_time > 0 else 0
    }


def benchmark_format_compatibility(prover: Any, count: int) -> dict[str, Any]:
    """Benchmark proof format checks (magic bytes, hash structure)."""
    magic_bytes = b"mock_proof"
    
    valid_magic = 0
    valid_length = 0
    
    start = time.perf_counter()
    for i in range(count):
        result = prover.generate_proof(f"{i:064x}", f"{i+1:064x}", i)
        proof = result.proof
        
        if proof.startswith(magic_bytes):
            valid_magic += 1
        if len(proof) >= len(magic_bytes) + 32:  # magic + SHA256
            valid_length += 1
    
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "format_compatibility_check",
        "count": count,
        "valid_magic_bytes": valid_magic,
        "valid_length": valid_length,
        "success_rate": (valid_magic / count) * 100 if count > 0 else 0,
        "total_time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def benchmark_end_to_end_flow(count: int) -> dict[str, Any]:
    """Benchmark complete ZK flow: generate proof, verify, simulate MainChain accept."""
    if not PROVER_AVAILABLE or not VERIFIER_AVAILABLE:
        return {"operation": "end_to_end_flow", "error": "Components not available"}
    
    prover = ZKProver(mode="mock")
    verifier = ZKVerifier(mode="mock")
    
    times = []
    successful = 0
    
    for i in range(count):
        old_root = f"{i:064x}"
        new_root = f"{i+1:064x}"
        block_index = i
        
        start = time.perf_counter()
        
        # 1. Generate proof
        result = prover.generate_proof(old_root, new_root, block_index)
        if not result.success:
            continue
        
        # 2. Verify proof
        public_inputs = {
            "old_state_root": old_root,
            "new_state_root": new_root,
            "block_index": block_index,
            "sub_chain_name": ""
        }
        is_valid = verifier.verify(result.proof, public_inputs)
        
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        
        if is_valid:
            successful += 1
    
    total_time = sum(times) if times else 0
    return {
        "operation": "end_to_end_flow",
        "count": count,
        "successful": successful,
        "success_rate": (successful / count) * 100 if count > 0 else 0,
        "total_time": total_time,
        "avg_time_ms": (total_time / count) * 1000 if count > 0 else 0,
        "ops_per_second": count / total_time if total_time > 0 else 0
    }


def benchmark_prover_stats(prover: Any, count: int) -> dict[str, Any]:
    """Benchmark getting prover statistics."""
    # Generate some proofs first
    for i in range(10):
        prover.generate_proof(f"{i:064x}", f"{i+1:064x}", i)
    
    start = time.perf_counter()
    for _ in range(count):
        stats = prover.get_stats()
    elapsed = time.perf_counter() - start
    
    return {
        "operation": "get_prover_stats",
        "count": count,
        "time": elapsed,
        "ops_per_second": count / elapsed if elapsed > 0 else 0
    }


def run_comprehensive_benchmark() -> list[dict[str, Any]]:
    """Run comprehensive ZK benchmark."""
    if not PROVER_AVAILABLE:
        print("❌ ZKProver not available. Cannot run benchmarks.")
        return []
    
    if not VERIFIER_AVAILABLE:
        print("❌ ZKVerifier not available. Cannot run benchmarks.")
        return []
    
    # Create instances
    prover = ZKProver(mode="mock")
    verifier = ZKVerifier(mode="mock")
    
    config = {
        "proof_count": 500,
        "verify_count": 500,
        "format_check_count": 1000,
        "e2e_count": 200,
        "stats_count": 1000
    }
    
    print(f"\n{'='*80}")
    print(f"{'ZK PROOF BENCHMARK':^80}")
    print(f"{'='*80}")
    print(f"Config: {config}")
    print("-" * 80)
    
    all_results = []
    
    # 1. Mock Proof Generation
    print("Running: mock_proof_generation...")
    result = benchmark_mock_proof_generation(prover, config['proof_count'])
    all_results.append(result)
    print(f"   ✅ {result['ops_per_second']:.2f} proofs/sec, avg {result['avg_time_ms']:.3f}ms")
    
    # 2. Mock Verification
    print("Running: mock_verification...")
    result = benchmark_mock_verification(prover, verifier, config['verify_count'])
    all_results.append(result)
    print(f"   ✅ {result['ops_per_second']:.2f} verifications/sec, avg {result['avg_time_ms']:.3f}ms")
    
    # 3. Format Compatibility
    print("Running: format_compatibility_check...")
    result = benchmark_format_compatibility(prover, config['format_check_count'])
    all_results.append(result)
    print(f"   ✅ {result['success_rate']:.1f}% compatible, {result['ops_per_second']:.2f} checks/sec")
    
    # 4. End-to-End Flow
    print("Running: end_to_end_flow...")
    result = benchmark_end_to_end_flow(config['e2e_count'])
    all_results.append(result)
    print(f"   ✅ {result['success_rate']:.1f}% success, {result['ops_per_second']:.2f} flows/sec")
    
    # 5. Prover Stats
    print("Running: get_prover_stats...")
    result = benchmark_prover_stats(prover, config['stats_count'])
    all_results.append(result)
    print(f"   ✅ {result['ops_per_second']:.2f} stat calls/sec")
    
    # Save results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'ZK_benchmark.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print summary
    print_summary(all_results)
    
    return all_results


def print_summary(results: list[dict[str, Any]]) -> None:
    """Print benchmark summary."""
    w = 90
    print("\n" + "=" * w)
    print(f"{'ZK BENCHMARK SUMMARY':^90}")
    print("=" * w)
    print(f"{'Operation':<30} | {'Throughput':<20} | {'Avg Time':<15} | {'Success':<10}")
    print("-" * w)
    
    for r in results:
        op = r.get("operation", "unknown")
        ops = r.get("ops_per_second", 0)
        avg_ms = r.get("avg_time_ms", r.get("time", 0) * 1000)
        
        if "success_rate" in r:
            success = f"{r['success_rate']:.1f}%"
        elif "valid_count" in r:
            success = f"{r['valid_count']}/{r['count']}"
        else:
            success = "N/A"
        
        throughput = f"{ops:,.1f} op/s" if ops else "N/A"
        avg_time = f"{avg_ms:.3f} ms" if avg_ms else "N/A"
        
        print(f"{op:<30} | {throughput:<20} | {avg_time:<15} | {success:<10}")
    
    print("=" * w)


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

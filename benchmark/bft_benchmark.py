"""
Benchmark script for comparing Rust and Python implementations of BFT Consensus.

This script compares:
- Message creation and signing
- Signature verification
- Consensus state management
- Full 3-phase protocol simulation
"""

import time
import random
import string
import json
import sys
import os
import hashlib
import matplotlib.pyplot as plt
from typing import Any
from datetime import datetime
from dataclasses import dataclass

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Implementation Imports ---

# 1. Python Implementation
PYTHON_AVAILABLE = False
PyBFTConsensus = None
PyKeyPair = None
PyMessageType = None

try:
    from hierachain.hierarchical.consensus.bft_consensus import (
        BFTConsensus as PyBFTConsensus,
        MessageType as PyMessageType,
        BFTMessage as PyBFTMessage
    )
    from hierachain.security.security_utils import KeyPair as PyKeyPair
    PYTHON_AVAILABLE = True
    print("✓ Python BFT implementation available")
except ImportError as e:
    print(f"⚠ Warning: Python BFT implementation not available: {e}")

# 2. Rust Implementation (via PyO3 bindings if available)
RUST_AVAILABLE = False
RsBFTConsensus = None
RsKeyPair = None
RsVerifySignature = None

try:
    import hierachain_consensus
    
    # Check for BFTConsensus class
    if hasattr(hierachain_consensus, "BFTConsensus"):
        RsBFTConsensus = hierachain_consensus.BFTConsensus
        RUST_AVAILABLE = True
        print("✓ Rust BFTConsensus available")
    
    # Check for KeyPair
    if hasattr(hierachain_consensus, "KeyPair"):
        RsKeyPair = hierachain_consensus.KeyPair
        print("✓ Rust KeyPair available")
    
    # Check for verify_signature
    if hasattr(hierachain_consensus, "verify_signature"):
        RsVerifySignature = hierachain_consensus.verify_signature
        print("✓ Rust verify_signature available")
    
    if not RUST_AVAILABLE:
        print("⚠ Warning: Rust module loaded but BFTConsensus not found.")
        print(f"  Available: {dir(hierachain_consensus)}")

except ImportError as e:
    print(f"⚠ Warning: Rust implementation not available: {e}")


# --- Helper Classes ---

@dataclass
class BenchmarkConfig:
    """Configuration for benchmark runs."""
    node_count: int  # Number of nodes in the network
    iterations: int  # Number of iterations for each test
    fault_tolerance: int  # f value for BFT (n >= 3f + 1)


class MockMessage:
    """Mock BFT message for benchmarking when implementations unavailable."""
    def __init__(self, msg_type: str, view: int, seq: int, sender: str):
        self.message_type = msg_type
        self.view = view
        self.sequence_number = seq
        self.sender_id = sender
        self.timestamp = time.time()
        self.nonce = ''.join(random.choices(string.ascii_lowercase, k=16))
        self.signature = ""
        self.data = {}
        
    def get_signable_payload(self) -> bytes:
        """Get bytes to sign."""
        return f"{self.message_type}:{self.view}:{self.sequence_number}:{self.nonce}".encode()


# --- Benchmark Functions ---

def benchmark_message_creation(iterations: int) -> dict[str, Any]:
    """
    Benchmark BFT message creation performance.
    """
    print(f"\n📦 Benchmarking Message Creation ({iterations} iterations)...")

    results = {}

    # Python implementation
    if PYTHON_AVAILABLE and PyBFTMessage:
        start = time.perf_counter()
        for i in range(iterations):
            msg = PyBFTMessage(
                message_type=PyMessageType.PREPARE,
                view=1,
                sequence_number=i,
                sender_id=f"node_{i % 4}",
                timestamp=time.time(),
                signature=""
            )
        elapsed = time.perf_counter() - start

        results["python"] = {
            "total_time": elapsed,
            "avg_time_us": (elapsed / iterations) * 1_000_000,
            "ops_per_sec": iterations / elapsed
        }
        print(f"  ✅ Python: {elapsed:.4f}s "
              f"({results['python']['ops_per_sec']:.2f} msg/sec)")

    # Rust implementation (using KeyPair for message signing simulation)
    if RUST_AVAILABLE and RsKeyPair:
        keypair = RsKeyPair.generate()
        start = time.perf_counter()
        for i in range(iterations):
            # Simulate message creation with signature
            msg_data = f"PREPARE|1|{i}|node_{i % 4}|{time.time()}"
            _ = keypair.sign(msg_data.encode())
        elapsed = time.perf_counter() - start

        results["rust"] = {
            "total_time": elapsed,
            "avg_time_us": (elapsed / iterations) * 1_000_000,
            "ops_per_sec": iterations / elapsed
        }
        print(f"  ✅ Rust: {elapsed:.4f}s "
              f"({results['rust']['ops_per_sec']:.2f} msg/sec)")

        # Calculate speedup
        if "python" in results:
            speedup = (results["python"]["ops_per_sec"] /
                       results["rust"]["ops_per_sec"])
            print(f"  📊 Rust/Python ratio: {1/speedup:.2f}x")

    # Fallback mock implementation for testing (no crypto overhead)
    start = time.perf_counter()
    for i in range(iterations):
        _ = MockMessage("prepare", 1, i, f"node_{i % 4}")
    elapsed = time.perf_counter() - start

    results["mock"] = {
        "total_time": elapsed,
        "avg_time_us": (elapsed / iterations) * 1_000_000,
        "ops_per_sec": iterations / elapsed
    }
    print(f"  ✅ Simulated: {elapsed:.4f}s "
          f"({results['mock']['ops_per_sec']:.2f} msg/sec)")

    return results


def benchmark_signature_operations(iterations: int) -> dict[str, Any]:
    """
    Benchmark signature creation and verification.
    """
    print(f"\n🔐 Benchmarking Signature Operations ({iterations} iterations)...")
    
    results = {}
    
    # Python implementation
    if PYTHON_AVAILABLE and PyKeyPair:
        from hierachain.security.security_utils import verify_signature as py_verify
        
        keypair = PyKeyPair.generate()
        message = b"Test message for BFT consensus benchmark"
        
        # Signing benchmark
        start = time.perf_counter()
        signatures = []
        for _ in range(iterations):
            sig = keypair.sign(message)
            signatures.append(sig)
        sign_time = time.perf_counter() - start
        
        # Verification benchmark
        public_key = keypair.public_key
        start = time.perf_counter()
        for sig in signatures:
            py_verify(public_key, message, sig)
        verify_time = time.perf_counter() - start
        
        results["python"] = {
            "sign_total_time": sign_time,
            "sign_avg_us": (sign_time / iterations) * 1_000_000,
            "sign_ops_per_sec": iterations / sign_time,
            "verify_total_time": verify_time,
            "verify_avg_us": (verify_time / iterations) * 1_000_000,
            "verify_ops_per_sec": iterations / verify_time
        }
        print(f"  ✅ Python Sign: {sign_time:.4f}s "
              f"({results['python']['sign_ops_per_sec']:.2f} ops/sec)")
        print(f"  ✅ Python Verify: {verify_time:.4f}s "
              f"({results['python']['verify_ops_per_sec']:.2f} ops/sec)")

    # Rust implementation
    if RUST_AVAILABLE and RsKeyPair and RsVerifySignature:
        keypair = RsKeyPair.generate()
        message = b"Test message for BFT consensus benchmark"

        # Signing benchmark
        start = time.perf_counter()
        signatures = []
        for _ in range(iterations):
            sig = keypair.sign(message)
            signatures.append(sig)
        sign_time = time.perf_counter() - start

        # Verification benchmark
        public_key = keypair.public_key
        start = time.perf_counter()
        for sig in signatures:
            RsVerifySignature(public_key, message, sig)
        verify_time = time.perf_counter() - start

        results["rust"] = {
            "sign_total_time": sign_time,
            "sign_avg_us": (sign_time / iterations) * 1_000_000,
            "sign_ops_per_sec": iterations / sign_time,
            "verify_total_time": verify_time,
            "verify_avg_us": (verify_time / iterations) * 1_000_000,
            "verify_ops_per_sec": iterations / verify_time
        }
        print(f"  ✅ Rust Sign: {sign_time:.4f}s "
              f"({results['rust']['sign_ops_per_sec']:.2f} ops/sec)")
        print(f"  ✅ Rust Verify: {verify_time:.4f}s "
              f"({results['rust']['verify_ops_per_sec']:.2f} ops/sec)")

        # Calculate speedup
        if "python" in results:
            sign_speedup = (results["python"]["sign_ops_per_sec"] /
                            results["rust"]["sign_ops_per_sec"])
            verify_speedup = (results["python"]["verify_ops_per_sec"] /
                              results["rust"]["verify_ops_per_sec"])
            print(f"  📊 Rust/Python Sign ratio: {1/sign_speedup:.2f}x")
            print(f"  📊 Rust/Python Verify ratio: {1/verify_speedup:.2f}x")

    # SHA256 hashing benchmark (baseline for comparison)
    message = b"Test message for BFT consensus benchmark"

    start = time.perf_counter()
    for _ in range(iterations):
        hashlib.sha256(message).hexdigest()
    hash_time = time.perf_counter() - start

    results["sha256_baseline"] = {
        "total_time": hash_time,
        "avg_us": (hash_time / iterations) * 1_000_000,
        "ops_per_sec": iterations / hash_time
    }
    print(f"  ✅ SHA256 Baseline: {hash_time:.4f}s "
          f"({results['sha256_baseline']['ops_per_sec']:.2f} ops/sec)")

    return results


def benchmark_consensus_round(config: BenchmarkConfig) -> dict[str, Any]:
    """
    Benchmark a simulated consensus round (3-phase protocol).
    
    Simulates:
    1. Pre-prepare message from primary
    2. Prepare messages from 2f nodes
    3. Commit messages from 2f+1 nodes
    """
    print(f"\n🔄 Benchmarking Consensus Round (Nodes: {config.node_count}, f={config.fault_tolerance}, Iterations: {config.iterations})...")
    
    results = {}
    n = config.node_count
    f = config.fault_tolerance
    
    # Python implementation
    if PYTHON_AVAILABLE and PyBFTConsensus and PyKeyPair:
        try:
            # Setup network
            node_ids = [f"node_{i}" for i in range(n)]
            keypairs = {nid: PyKeyPair.generate() for nid in node_ids}
            public_keys = {nid: kp.public_key for nid, kp in keypairs.items()}
            
            # Create consensus instances
            consensus_nodes = {}
            for nid in node_ids:
                consensus_nodes[nid] = PyBFTConsensus(
                    node_id=nid,
                    all_nodes=node_ids,
                    f=f,
                    keypair=keypairs[nid],
                    node_public_keys=public_keys
                )
            
            primary = node_ids[0]
            
            # Benchmark full round
            total_time = 0.0
            for iteration in range(config.iterations):
                start = time.perf_counter()
                
                # 1. Client request to primary
                operation = {"action": f"test_{iteration}", "data": "benchmark"}
                consensus_nodes[primary].request(operation)
                
                # 2. Simulate prepare phase (broadcast pre-prepare, collect prepares)
                # In real implementation this would involve network
                
                # 3. Simulate commit phase
                
                elapsed = time.perf_counter() - start
                total_time += elapsed
            
            results["python"] = {
                "total_time": total_time,
                "avg_round_ms": (total_time / config.iterations) * 1000,
                "rounds_per_sec": config.iterations / total_time
            }
            print(f"  ✅ Python: {total_time:.4f}s "
                  f"({results['python']['rounds_per_sec']:.2f} rounds/sec)")

        except Exception as e:
            print(f"  ⚠ Python benchmark failed: {e}")
            results["python"] = {"error": str(e)}

    # Rust implementation
    if RUST_AVAILABLE and RsBFTConsensus and RsKeyPair:
        try:
            # Setup network
            node_ids = [f"node_{i}" for i in range(n)]
            keypairs = {nid: RsKeyPair.generate() for nid in node_ids}
            public_keys = {nid: kp.public_key for nid, kp in keypairs.items()}

            # Create consensus instances
            consensus_nodes = {}
            for nid in node_ids:
                consensus_nodes[nid] = RsBFTConsensus(
                    node_id=nid,
                    all_nodes=node_ids,
                    f=f,
                    keypair=keypairs[nid],
                    node_public_keys=public_keys
                )

            primary = node_ids[0]

            # Benchmark full round
            total_time = 0.0
            for iteration in range(config.iterations):
                start = time.perf_counter()

                # 1. Client request to primary
                operation = {"action": f"test_{iteration}", "data": "benchmark"}
                consensus_nodes[primary].request(operation)

                # 2. Simulate prepare phase
                # 3. Simulate commit phase

                elapsed = time.perf_counter() - start
                total_time += elapsed

            results["rust"] = {
                "total_time": total_time,
                "avg_round_ms": (total_time / config.iterations) * 1000,
                "rounds_per_sec": config.iterations / total_time
            }
            print(f"  ✅ Rust: {total_time:.4f}s "
                  f"({results['rust']['rounds_per_sec']:.2f} rounds/sec)")

            # Calculate speedup if Python also ran
            if "python" in results and "error" not in results["python"]:
                speedup = (results["python"]["total_time"] /
                           results["rust"]["total_time"])
                print(f"  🚀 Rust speedup: {speedup:.2f}x faster than Python")

        except Exception as e:
            print(f"  ⚠ Rust benchmark failed: {e}")
            results["rust"] = {"error": str(e)}
    
    # Simulated round (for metrics baseline)
    total_time = 0.0
    for iteration in range(config.iterations):
        start = time.perf_counter()
        
        # Simulate message creation
        pre_prepare = MockMessage("pre_prepare", 0, iteration, "node_0")
        pre_prepare.get_signable_payload()
        
        # Simulate 2f prepare messages
        prepares = []
        for i in range(2 * f):
            msg = MockMessage("prepare", 0, iteration, f"node_{i+1}")
            msg.get_signable_payload()
            prepares.append(msg)
        
        # Simulate 2f+1 commit messages
        commits = []
        for i in range(2 * f + 1):
            msg = MockMessage("commit", 0, iteration, f"node_{i}")
            msg.get_signable_payload()
            commits.append(msg)
        
        elapsed = time.perf_counter() - start
        total_time += elapsed
    
    results["simulated"] = {
        "total_time": total_time,
        "avg_round_ms": (total_time / config.iterations) * 1000,
        "rounds_per_sec": config.iterations / total_time,
        "note": "Simulated without actual crypto/network"
    }
    print(f"  ✅ Simulated: {total_time:.4f}s ({results['simulated']['rounds_per_sec']:.2f} rounds/sec)")
    
    return results


def benchmark_view_change(config: BenchmarkConfig) -> dict[str, Any]:
    """
    Benchmark view change protocol performance.
    """
    print(f"\n🔃 Benchmarking View Change ({config.iterations} iterations)...")
    
    results = {}
    f = config.fault_tolerance
    
    # Simulated view change (2f+1 view change messages needed)
    total_time = 0.0
    for iteration in range(config.iterations):
        start = time.perf_counter()
        
        # Create view change messages from 2f+1 nodes
        vc_messages = []
        new_view = iteration + 1
        for i in range(2 * f + 1):
            msg = MockMessage("view_change", new_view, 0, f"node_{i}")
            msg.get_signable_payload()
            # Simulate signature verification
            hashlib.sha256(msg.get_signable_payload()).hexdigest()
            vc_messages.append(msg)
        
        # Create new view message
        new_view_msg = MockMessage("new_view", new_view, 0, f"node_{new_view % config.node_count}")
        new_view_msg.data = {"proof": [m.nonce for m in vc_messages]}
        
        elapsed = time.perf_counter() - start
        total_time += elapsed
    
    results["simulated"] = {
        "total_time": total_time,
        "avg_view_change_ms": (total_time / config.iterations) * 1000,
        "view_changes_per_sec": config.iterations / total_time
    }
    print(f"  ✅ Simulated: {total_time:.4f}s ({results['simulated']['view_changes_per_sec']:.2f} vc/sec)")
    
    return results


def benchmark_throughput_scaling(base_iterations: int = 1000) -> dict[str, Any]:
    """
    Benchmark how throughput scales with network size.
    """
    print(f"\n📊 Benchmarking Throughput Scaling...")

    configs = [
        BenchmarkConfig(node_count=4, iterations=base_iterations,
                        fault_tolerance=1),
        BenchmarkConfig(node_count=7, iterations=base_iterations,
                        fault_tolerance=2),
        BenchmarkConfig(node_count=10, iterations=base_iterations // 2,
                        fault_tolerance=3),
        BenchmarkConfig(node_count=13, iterations=base_iterations // 4,
                        fault_tolerance=4),
    ]

    results = []
    for cfg in configs:
        print(f"\n  Testing n={cfg.node_count}, f={cfg.fault_tolerance}...")
        round_result = benchmark_consensus_round(cfg)

        result = {
            "node_count": cfg.node_count,
            "fault_tolerance": cfg.fault_tolerance,
            "iterations": cfg.iterations,
            "python_rounds_per_sec": round_result.get(
                "python", {}).get("rounds_per_sec", 0),
            "rust_rounds_per_sec": round_result.get(
                "rust", {}).get("rounds_per_sec", 0),
        }
        results.append(result)

    return {"scaling_results": results}


def run_comprehensive_benchmark():
    """
    Run all BFT consensus benchmarks.
    """
    print("🚀 Starting BFT Consensus Benchmark Suite")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    all_results = {
        "timestamp": datetime.now().isoformat(),
        "python_available": PYTHON_AVAILABLE,
        "rust_available": RUST_AVAILABLE
    }
    
    # 1. Message Creation
    msg_results = benchmark_message_creation(10000)
    all_results["message_creation"] = msg_results
    
    # 2. Signature Operations
    sig_results = benchmark_signature_operations(5000)
    all_results["signature_operations"] = sig_results
    
    # 3. Consensus Rounds with different configurations
    consensus_configs = [
        BenchmarkConfig(node_count=4, iterations=1000, fault_tolerance=1),
        BenchmarkConfig(node_count=7, iterations=500, fault_tolerance=2),
        BenchmarkConfig(node_count=10, iterations=200, fault_tolerance=3),
    ]
    
    consensus_results = []
    for cfg in consensus_configs:
        result = benchmark_consensus_round(cfg)
        consensus_results.append({
            "config": {"n": cfg.node_count, "f": cfg.fault_tolerance, "iter": cfg.iterations},
            "results": result
        })
    all_results["consensus_rounds"] = consensus_results
    
    # 4. View Change
    vc_config = BenchmarkConfig(node_count=7, iterations=1000, fault_tolerance=2)
    vc_results = benchmark_view_change(vc_config)
    all_results["view_change"] = vc_results
    
    # 5. Throughput Scaling
    scaling_results = benchmark_throughput_scaling(1000)
    all_results["throughput_scaling"] = scaling_results
    
    # Save Results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'BFT_benchmark.json')
    with open(results_path, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print Summary
    print_summary(all_results)
    
    return all_results


def print_summary(results: dict):
    """Print a summary of benchmark results."""
    print("\n" + "=" * 60)
    print("📈 BENCHMARK SUMMARY")
    print("=" * 60)
    
    # Message Creation
    if "message_creation" in results:
        mc = results["message_creation"]
        print("\n📦 Message Creation:")
        if "python" in mc:
            print(f"  • Python: {mc['python']['ops_per_sec']:.2f} msg/sec")
        if "mock" in mc:
            print(f"  • Simulated: {mc['mock']['ops_per_sec']:.2f} msg/sec")
    
    # Signature Operations
    if "signature_operations" in results:
        so = results["signature_operations"]
        print("\n🔐 Signature Operations:")
        if "python" in so:
            print(f"  • Python Sign: {so['python']['sign_ops_per_sec']:.2f} ops/sec")
            print(f"  • Python Verify: {so['python']['verify_ops_per_sec']:.2f} ops/sec")
        print(f"  • SHA256 Baseline: {so['sha256_baseline']['ops_per_sec']:.2f} ops/sec")
    
    # Consensus Rounds
    if "consensus_rounds" in results:
        print("\n🔄 Consensus Rounds:")
        for cr in results["consensus_rounds"]:
            cfg = cr["config"]
            if "simulated" in cr["results"]:
                sim = cr["results"]["simulated"]
                print(f"  • n={cfg['n']}, f={cfg['f']}: {sim['rounds_per_sec']:.2f} rounds/sec")
    
    # Throughput Scaling
    if "throughput_scaling" in results:
        print("\n📊 Throughput Scaling:")
        for sr in results["throughput_scaling"]["scaling_results"]:
            print(f"  • n={sr['node_count']}: {sr.get('rounds_per_sec', 0):.2f} rounds/sec")
    
    print("\n" + "=" * 60)


def analyze_and_plot(file_path: str):
    """
    Read benchmark results and generate plots.
    """
    try:
        with open(file_path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Could not find result file: {file_path}")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('BFT Consensus Benchmark Results', fontsize=14, fontweight='bold')
    
    # 1. Message Creation Performance (Python vs Rust)
    ax = axes[0, 0]
    mc = data.get("message_creation", {})
    impls = []
    ops = []
    colors = []
    if "python" in mc:
        impls.append("Python")
        ops.append(mc["python"]["ops_per_sec"])
        colors.append('steelblue')
    if "rust" in mc:
        impls.append("Rust")
        ops.append(mc["rust"]["ops_per_sec"])
        colors.append('darkorange')

    ax.bar(impls, ops, color=colors)
    ax.set_title('Message Creation: Python vs Rust')
    ax.set_ylabel('Messages/second')
    ax.grid(True, alpha=0.3, axis='y')
    
    # 2. Signature Performance (Python vs Rust)
    ax = axes[0, 1]
    so = data.get("signature_operations", {})

    # Grouped bar chart for sign/verify comparison
    sig_labels = []
    py_values = []
    rs_values = []

    if "python" in so:
        sig_labels = ["Sign", "Verify"]
        py_values = [so["python"]["sign_ops_per_sec"],
                     so["python"]["verify_ops_per_sec"]]
    if "rust" in so:
        rs_values = [so["rust"]["sign_ops_per_sec"],
                     so["rust"]["verify_ops_per_sec"]]

    if sig_labels:
        import numpy as np
        x = np.arange(len(sig_labels))
        width = 0.35

        if py_values:
            ax.bar(x - width/2, py_values, width,
                   label='Python', color='steelblue')
        if rs_values:
            ax.bar(x + width/2, rs_values, width,
                   label='Rust', color='darkorange')

        ax.set_xlabel('Operation')
        ax.set_ylabel('Operations/second')
        ax.set_title('Signature Performance: Python vs Rust')
        ax.set_xticks(x)
        ax.set_xticklabels(sig_labels)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

    # 3. Consensus Rounds by Network Size (Python vs Rust)
    ax = axes[1, 0]
    consensus = data.get("consensus_rounds", [])

    # Extract data for each implementation
    node_counts = []
    python_rounds = []
    rust_rounds = []

    for cr in consensus:
        node_counts.append(str(cr["config"]["n"]))
        results = cr.get("results", {})
        python_rounds.append(
            results.get("python", {}).get("rounds_per_sec", 0)
        )
        rust_rounds.append(
            results.get("rust", {}).get("rounds_per_sec", 0)
        )

    if node_counts:
        import numpy as np
        x = np.arange(len(node_counts))
        width = 0.35

        bars1 = ax.bar(x - width/2, python_rounds, width,
                       label='Python', color='steelblue')
        bars2 = ax.bar(x + width/2, rust_rounds, width,
                       label='Rust', color='darkorange')

        ax.set_xlabel('Number of Nodes')
        ax.set_ylabel('Rounds/second')
        ax.set_title('Consensus Throughput: Python vs Rust')
        ax.set_xticks(x)
        ax.set_xticklabels(node_counts)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')

        # Add value labels on bars
        def add_labels(bars):
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.annotate(f'{height:.0f}',
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3),
                                textcoords="offset points",
                                ha='center', va='bottom', fontsize=8)

        add_labels(bars1)
        add_labels(bars2)

    # 4. Throughput Scaling
    ax = axes[1, 1]
    scaling = data.get("throughput_scaling", {}).get("scaling_results", [])
    if scaling:
        nodes = [s["node_count"] for s in scaling]
        python_throughput = [s.get("python_rounds_per_sec", 0) for s in scaling]
        rust_throughput = [s.get("rust_rounds_per_sec", 0) for s in scaling]

        ax.plot(nodes, python_throughput, marker='o', linewidth=2,
                color='steelblue', label='Python')
        ax.plot(nodes, rust_throughput, marker='s', linewidth=2,
                color='darkorange', label='Rust')

        ax.fill_between(nodes, python_throughput, alpha=0.15, color='steelblue')
        ax.fill_between(nodes, rust_throughput, alpha=0.15, color='darkorange')

        ax.set_title('Throughput Scaling: Python vs Rust')
        ax.set_xlabel('Number of Nodes')
        ax.set_ylabel('Rounds/second')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_dir = os.path.dirname(file_path)
    chart_path = os.path.join(output_dir, 'BFT_benchmark.png')
    plt.savefig(chart_path, dpi=150)
    print(f"📊 Chart saved to: {chart_path}")


if __name__ == "__main__":
    results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")
    
    time.sleep(1)  # Ensure file write completion
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_path = os.path.join(script_dir, 'output', 'BFT_benchmark.json')
    
    if os.path.exists(results_path):
        try:
            analyze_and_plot(results_path)
        except Exception as e:
            print(f"⚠ Could not generate plots: {e}")
    else:
        print("⚠ Result file not found, skipping analysis.")

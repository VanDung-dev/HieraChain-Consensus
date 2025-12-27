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
from typing import Any
from datetime import datetime
from dataclasses import dataclass

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging to suppress debug output
import logging
logging.getLogger("hierachain").setLevel(logging.WARNING)

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
    """Benchmark BFT message creation performance."""

    results = {}

    # Python implementation
    if PYTHON_AVAILABLE and PyBFTMessage and PyKeyPair:
        # Generate keypair once for signing
        keypair = PyKeyPair.generate()
        
        start = time.perf_counter()
        for i in range(iterations):
            # Create message object
            msg = PyBFTMessage(
                message_type=PyMessageType.PREPARE,
                view=1,
                sequence_number=i,
                sender_id=f"node_{i % 4}",
                timestamp=time.time(),
                signature=""
            )
            # FORCE VALIDATION: Actually sign the message like Rust does
            payload = msg.get_signable_payload()
            msg.signature = keypair.sign(payload)
            
        elapsed = time.perf_counter() - start

        results["python"] = {
            "total_time": elapsed,
            "avg_time_us": (elapsed / iterations) * 1_000_000,
            "ops_per_sec": iterations / elapsed
        }

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

    return results


def benchmark_signature_operations(iterations: int) -> dict[str, Any]:
    """Benchmark signature creation and verification."""
    
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

    return results


def benchmark_consensus_round(config: BenchmarkConfig) -> dict[str, Any]:
    """
    Benchmark a simulated consensus round (3-phase protocol).
    
    Simulates:
    1. Pre-prepare message from primary
    2. Prepare messages from 2f nodes
    3. Commit messages from 2f+1 nodes
    """

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

                operation = {"action": f"test_{iteration}", "data": "benchmark"}
                consensus_nodes[primary].request(operation)
                
                elapsed = time.perf_counter() - start
                total_time += elapsed
            
            results["python"] = {
                "total_time": total_time,
                "avg_round_ms": (total_time / config.iterations) * 1000,
                "rounds_per_sec": config.iterations / total_time
            }

        except Exception as e:
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

                operation = {"action": f"test_{iteration}", "data": "benchmark"}
                consensus_nodes[primary].request(operation)

                elapsed = time.perf_counter() - start
                total_time += elapsed

            results["rust"] = {
                "total_time": total_time,
                "avg_round_ms": (total_time / config.iterations) * 1000,
                "rounds_per_sec": config.iterations / total_time
            }

        except Exception as e:
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
    
    return results


def benchmark_view_change(config: BenchmarkConfig) -> dict[str, Any]:
    """Benchmark view change protocol performance."""
    
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
    
    return results


def benchmark_throughput_scaling(base_iterations: int = 1000) -> dict[str, Any]:
    """Benchmark how throughput scales with network size."""

    configs = [
        BenchmarkConfig(node_count=4, iterations=base_iterations, fault_tolerance=1),
        BenchmarkConfig(node_count=7, iterations=base_iterations, fault_tolerance=2),
        BenchmarkConfig(node_count=10, iterations=base_iterations // 2, fault_tolerance=3),
        BenchmarkConfig(node_count=13, iterations=base_iterations // 4, fault_tolerance=4),
    ]

    results = []
    for cfg in configs:
        round_result = benchmark_consensus_round(cfg)

        result = {
            "node_count": cfg.node_count,
            "fault_tolerance": cfg.fault_tolerance,
            "iterations": cfg.iterations,
            "python_rounds_per_sec": round_result.get("python", {}).get("rounds_per_sec", 0),
            "rust_rounds_per_sec": round_result.get("rust", {}).get("rounds_per_sec", 0),
        }
        results.append(result)

    return {"scaling_results": results}


def run_comprehensive_benchmark():
    """
    Run all BFT consensus benchmarks.
    """
    
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
    """Print a summary of benchmark results comparing Python and Rust."""
    # Table width and headers
    w = 100
    m_h = f"{'Metric':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'BFT CONSENSUS BENCHMARK SUMMARY':^100}")
    print("=" * w)
    print(h)
    print("-" * w)

    def get_status_icon(py_val, rs_val):
        if not py_val or not rs_val or py_val == 0:
            return "N/A", ""
        speedup = rs_val / py_val
        if speedup > 1.5:
            return f"{speedup:.2f}x", "🚀"
        if speedup < 0.8:
            return f"{speedup:.2f}x", "⚠️"
        return f"{speedup:.2f}x", "➡️"

    # Message Creation
    if "message_creation" in results:
        mc = results["message_creation"]
        py_v = mc.get("python", {}).get("ops_per_sec", 0)
        rs_v = mc.get("rust", {}).get("ops_per_sec", 0)
        sp_str, icon = get_status_icon(py_v, rs_v)

        py_s = f"{py_v:>10,.0f} msg/s" if py_v else "N/A"
        rs_s = f"{rs_v:>10,.0f} msg/s" if rs_v else "N/A"
        print(f"{'Message Creation':<30} | {py_s:<18} | {rs_s:<18} | "f"{sp_str:<8} | {icon:<6}")

    # Signature Operations
    if "signature_operations" in results:
        so = results["signature_operations"]
        for op in ["sign", "verify"]:
            py_v = so.get("python", {}).get(f"{op}_ops_per_sec", 0)
            rs_v = so.get("rust", {}).get(f"{op}_ops_per_sec", 0)
            sp_str, icon = get_status_icon(py_v, rs_v)
            py_s = f"{py_v:>10,.0f} ops/s" if py_v else "N/A"
            rs_s = f"{rs_v:>10,.0f} ops/s" if rs_v else "N/A"
            label = f"Signature {op.capitalize()}"
            print(f"{label:<30} | {py_s:<18} | {rs_s:<18} | "f"{sp_str:<8} | {icon:<6}")

    # Consensus Rounds
    if "consensus_rounds" in results:
        print("-" * w)
        for cr in results["consensus_rounds"]:
            cfg, res = cr["config"], cr["results"]
            label = f"Round (n={cfg['n']}, f={cfg['f']})"
            py_v = res.get("python", {}).get("rounds_per_sec", 0)
            rs_v = res.get("rust", {}).get("rounds_per_sec", 0)

            p_res = res.get("python")
            r_res = res.get("rust")
            py_err = isinstance(p_res, dict) and "error" in p_res
            rs_err = isinstance(r_res, dict) and "error" in r_res

            py_s = "ERROR" if py_err else (
                f"{py_v:>10,.0f} rnd/s" if py_v else "N/A")
            rs_s = "ERROR" if rs_err else (
                f"{rs_v:>10,.0f} rnd/s" if rs_v else "N/A")

            sp_str, icon = get_status_icon(py_v, rs_v)
            print(f"{label:<30} | {py_s:<18} | {rs_s:<18} | "f"{sp_str:<8} | {icon:<6}")

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    benchmark_results = run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

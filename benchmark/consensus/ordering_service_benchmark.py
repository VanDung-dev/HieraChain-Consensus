"""
Benchmark script for comparing Rust and Python implementations of OrderingService.

This script uses a corrected methodology where services are initialized once and
reused across multiple benchmark runs to accurately measure performance.
"""

import time
import json
import sys
import statistics
import os
import math
from typing import List, Dict, Any
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Implementation Imports ---
try:
    from hierachain.consensus.ordering_service import (
        OrderingService as PythonOrderingService,
        OrderingNode as PythonOrderingNode
    )
    PYTHON_AVAILABLE = True
    print("✓ Python implementation available")
except ImportError as e:
    PYTHON_AVAILABLE = False
    print(f"⚠ Warning: Python implementation not available: {e}")

try:
    from hierachain_consensus import PyOrderingService as RustOrderingService, PyOrderingNode as RustOrderingNode
    RUST_AVAILABLE = True
    print("✓ Rust implementation available")
except ImportError:
    RUST_AVAILABLE = False
    print("⚠ Warning: Rust implementation not available. Only Python benchmark will run.")

# --- Helper Functions ---

def create_test_events(count: int) -> List[Dict[str, Any]]:
    """Creates a list of test events for benchmarking."""
    return [{
        "entity_id": f"entity_{i % 100}",
        "event": f"event_type_{i % 10}",
        "timestamp": time.time(),
        "details": {"source": "benchmark"},
        "data": json.dumps({"value": i}).encode('utf-8')
    } for i in range(count)]

def wait_for_processing(service: Any, event_count: int, block_size: int, initial_blocks: int):
    """
    Waits for the service to process all submitted events by checking queue
    and block creation status.
    """
    start_wait = time.perf_counter()
    max_wait_time = 30.0  # Increased timeout for larger batches
    
    # Calculate the number of blocks we expect to be created in this run
    expected_new_blocks = math.ceil(event_count / block_size)
    total_expected_blocks = initial_blocks + expected_new_blocks

    while time.perf_counter() - start_wait < max_wait_time:
        status = service.get_service_status()
        
        # Check the number of events waiting in the input channel/queue
        pending_queue = status.get('queues', {}).get('pending_events', -1)
        
        # Check the total number of blocks created by the service so far
        blocks_created = status.get('statistics', {}).get('blocks_created', -1)

        # If all events are processed and all expected blocks are created, we're done
        if pending_queue == 0 and blocks_created >= total_expected_blocks:
            print(f"  ...Processing finished in {time.perf_counter() - start_wait:.2f}s.")
            return
        
        time.sleep(0.1)  # Non-blocking sleep

    print(f"  ...Warning: Timed out after {max_wait_time}s waiting for processing.")
    print(f"     - Events in queue: {pending_queue}")
    print(f"     - Blocks created: {blocks_created} (Expected: {total_expected_blocks})")

def ensure_service_active(service: Any, timeout: float = 10.0) -> bool:
    """
    Ensure the provided service is in ACTIVE status.
    If the service exposes a start() method, call it once and wait for ACTIVE.
    Returns True if ACTIVE within timeout, False otherwise.
    """
    try:
        status = service.get_service_status()
    except Exception:
        status = {}

    current = status.get('status') or status.get('state') or status.get('service_status') or None
    # Normalize to string if enum-like object
    if hasattr(current, "value"):
        current = current.value

    if current and str(current).lower() == "active":
        return True

    # Try to start if possible
    try:
        if hasattr(service, "start"):
            service.start()
    except Exception:
        # ignore start exceptions; will check status below
        pass

    start_t = time.perf_counter()
    while time.perf_counter() - start_t < timeout:
        try:
            status = service.get_service_status()
            current = status.get('status') or status.get('state') or status.get('service_status') or None
            if hasattr(current, "value"):
                current = current.value
            if current and str(current).lower() == "active":
                return True
        except Exception:
            # continue polling if status retrieval fails transiently
            pass
        time.sleep(0.1)
    return False

# --- Main Benchmark Logic ---

def benchmark_implementation(service: Any, event_count: int, block_size: int) -> Dict[str, Any]:
    """
    Benchmarks a given service instance with a specified number of events.
    """
    implementation_name = "Python" if "hierachain.consensus" in str(type(service)) else "Rust"
    print(f"\n* Benchmarking {implementation_name} with {event_count} events...")

    # Ensure service is ACTIVE before sending events
    if not ensure_service_active(service, timeout=10.0):
        err_msg = f"Service failed to reach ACTIVE status for {implementation_name}."
        print(f"  ❌ {err_msg}")
        return {"implementation": implementation_name, "event_count": event_count, "error": err_msg}

    # Get initial state to correctly measure this run
    try:
        initial_status = service.get_service_status()
        initial_blocks = initial_status.get('statistics', {}).get('blocks_created', 0)
    except Exception:
        initial_blocks = 0

    events = create_test_events(event_count)
    
    # 1. Benchmark Submission
    start_submission = time.perf_counter()
    submission_errors = 0
    for event in events:
        try:
            service.receive_event(event, "test_channel", "test_org")
        except RuntimeError as re:
            # If service drifted into non-ACTIVE (e.g., maintenance), try to recover once
            submission_errors += 1
            print(f"  ⚠ Warning: receive_event error: {re}")
            if ensure_service_active(service, timeout=3.0):
                try:
                    service.receive_event(event, "test_channel", "test_org")
                    submission_errors -= 1  # recovered for this event
                except Exception as e:
                    print(f"  ❌ Failed to submit event after recovery attempt: {e}")
            else:
                # give up on further submissions
                break
        except Exception as e:
            submission_errors += 1
            print(f"  ⚠ Warning: unexpected receive_event exception: {e}")
    submission_time = time.perf_counter() - start_submission
    
    # If we couldn't submit many events, record an error
    if submission_errors > 0 and submission_errors >= event_count:
        err_msg = "All event submissions failed; skipping retrieval."
        print(f"  ❌ {err_msg}")
        return {"implementation": implementation_name, "event_count": event_count, "error": err_msg}

    # 2. Wait for all events to be processed into blocks
    wait_for_processing(service, event_count, block_size, initial_blocks)

    # 3. Benchmark Block Retrieval
    start_retrieval = time.perf_counter()
    blocks_retrieved = []
    retrieval_attempts = 0
    max_retrieval_loops = 10000
    while True and retrieval_attempts < max_retrieval_loops:
        try:
            block = service.get_next_block()
        except Exception as e:
            print(f"  ⚠ Warning: get_next_block exception: {e}")
            break
        if block is None:
            break
        blocks_retrieved.append(block)
        retrieval_attempts += 1
    retrieval_time = time.perf_counter() - start_retrieval
    
    # 4. Record Results
    result = {
        "implementation": implementation_name,
        "event_count": event_count,
        "submission_time": submission_time,
        "block_retrieval_time": retrieval_time,
        "events_per_second_submission": event_count / submission_time if submission_time > 0 else 0,
        "blocks_created_in_run": len(blocks_retrieved),
    }
    
    print(f"  ✅ Submission time: {submission_time:.4f}s")
    print(f"  📈 Submission throughput: {result['events_per_second_submission']:.2f} events/sec")
    print(f"  📦 Blocks created/retrieved in this run: {len(blocks_retrieved)}")
    
    return result

def run_comprehensive_benchmark():
    """
    Initializes services and runs a series of benchmarks, then prints a summary.
    """
    print("🚀 Starting comprehensive benchmark...")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    event_counts = [100, 1000, 5000, 10000]
    all_results = []
    
    # --- Common Configuration ---
    nodes_config = [{"node_id": "node1", "endpoint": "http://localhost:7050", "is_leader": True, "weight": 1.0, "status": "active", "last_heartbeat": time.time()}]
    service_config = {"block_size": 100, "batch_timeout": 0.5, "worker_threads": 4}

    # --- Benchmark Python ---
    if PYTHON_AVAILABLE:
        print("\n--- 🐍 PYTHON BENCHMARK ---")
        py_nodes = [PythonOrderingNode(**n) for n in nodes_config]
        python_service = PythonOrderingService(py_nodes, service_config)
        # Ensure the python service is ACTIVE before running any benchmarks
        if not ensure_service_active(python_service, timeout=10.0):
            err_msg = "Python service not ACTIVE after start; skipping Python benchmarks."
            print(f"  ❌ {err_msg}")
            all_results.append({"implementation": "Python", "error": err_msg})
        else:
            for count in event_counts:
                result = benchmark_implementation(python_service, count, service_config["block_size"])
                all_results.append(result)

    # --- Benchmark Rust ---
    if RUST_AVAILABLE:
        print("\n--- 🦀 RUST BENCHMARK ---")
        try:
            rust_nodes = [RustOrderingNode(**n) for n in nodes_config]
            rust_service = RustOrderingService(rust_nodes, service_config)
            if not ensure_service_active(rust_service, timeout=10.0):
                err_msg = "Rust service not ACTIVE after start; skipping Rust benchmarks."
                print(f"  ❌ {err_msg}")
                all_results.append({"implementation": "Rust", "error": err_msg})
            else:
                for count in event_counts:
                    result = benchmark_implementation(rust_service, count, service_config["block_size"])
                    all_results.append(result)
                if hasattr(rust_service, "stop"):
                    rust_service.stop()
        except Exception as e:
            print(f"  ❌ Rust initialization error: {e}")
            all_results.append({"implementation": "Rust", "error": str(e)})

    # --- Save and Print Summary ---
    # Determine project root relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    output_dir = os.path.join(project_root, 'output')
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'OrderingService_benchmark.json')
    print(f"DEBUG: Saving results to: {results_path}")
    
    with open(results_path, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print("\n" + "=" * 60)
    print("📈 BENCHMARK SUMMARY")
    print("=" * 60)
    
    valid_results = [r for r in all_results if "error" not in r]
    python_results = [r for r in valid_results if r['implementation'] == 'Python']
    rust_results = [r for r in valid_results if r['implementation'] == 'Rust']
    
    if python_results:
        avg_python_eps = statistics.mean([r['events_per_second_submission'] for r in python_results])
        print(f"\n🐍 Average Python submission performance: {avg_python_eps:.2f} events/second")
    
    if rust_results:
        avg_rust_eps = statistics.mean([r['events_per_second_submission'] for r in rust_results])
        print(f"🦀 Average Rust submission performance: {avg_rust_eps:.2f} events/second")
        
        if python_results and avg_python_eps > 0:
            improvement = ((avg_rust_eps - avg_python_eps) / avg_python_eps) * 100
            print(f"\n⚡ Overall Performance Improvement (Rust vs Python): {improvement:+.2f}%")

    print("\n" + "=" * 60)
    return all_results

if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")
    print("💾 Results saved to 'OrderingService_benchmark.json'")

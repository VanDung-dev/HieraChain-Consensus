"""
Benchmark script for comparing Rust and Python implementations of OrderingService.

This script uses a corrected methodology where services are initialized once and
reused across multiple benchmark runs to accurately measure performance.
"""

import time
import json
import sys
import os
from typing import Any
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# --- Implementation Imports ---
PYTHON_AVAILABLE = False
PythonOrderingService = None
PythonOrderingNode = None

try:
    from hierachain.consensus.ordering_service import (
        OrderingService as _PythonOrderingService,
        OrderingNode as _PythonOrderingNode
    )
    PythonOrderingService = _PythonOrderingService
    PythonOrderingNode = _PythonOrderingNode
    PYTHON_AVAILABLE = True
except ImportError:
    pass

RUST_AVAILABLE = False
RustOrderingService = None
RustOrderingNode = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "OrderingService"):
        RustOrderingService = hierachain_consensus.OrderingService
        RustOrderingNode = hierachain_consensus.OrderingNode
        RUST_AVAILABLE = True
except ImportError:
    pass


# --- Helper Functions ---

def create_test_events(count: int) -> list[dict[str, Any]]:
    """Creates a list of test events for benchmarking."""
    return [{
        "entity_id": f"entity_{i % 100}",
        "event": f"event_type_{i % 10}",
        "timestamp": time.time(),
        "details": {"source": "benchmark"},
        "data": json.dumps({"value": i}).encode('utf-8')
    } for i in range(count)]


def ensure_service_active(service: Any, timeout: float = 10.0) -> bool:
    """
    Ensure the provided service is in ACTIVE status.
    Returns True if ACTIVE within timeout, False otherwise.
    """
    try:
        status = service.get_service_status()
    except Exception:
        status = {}

    cur = status.get('status') or status.get('state') \
        or status.get('service_status') or None
    if hasattr(cur, "value"):
        cur = cur.value

    if cur and str(cur).lower() == "active":
        return True

    try:
        if hasattr(service, "start"):
            service.start()
    except Exception:
        pass

    start_t = time.perf_counter()
    while time.perf_counter() - start_t < timeout:
        try:
            status = service.get_service_status()
            cur = status.get('status') or status.get('state') \
                or status.get('service_status') or None
            if hasattr(cur, "value"):
                cur = cur.value
            if cur and str(cur).lower() == "active":
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def benchmark_implementation(service: Any, event_count: int) -> dict[str, Any]:
    """Benchmarks a given service instance with a specified number of events."""
    impl_name = "Python" if "hierachain.consensus" in str(type(service)) \
        else "Rust"

    if not ensure_service_active(service, timeout=15.0):
        err = f"Service failed to reach ACTIVE status for {impl_name}."
        print(f"  ❌ {err}")
        return {"implementation": impl_name, "event_count": event_count,
                "error": err}

    events = create_test_events(event_count)
    
    # 1. Benchmark Submission
    start_submission = time.perf_counter()
    submission_errors = 0
    for event in events:
        try:
            service.receive_event(event, "test_channel", "test_org")
        except Exception as e:
            submission_errors += 1
            print(f"  ⚠ Warning: receive_event exception: {e}")
            if submission_errors >= 10:
                break
    submission_time = time.perf_counter() - start_submission
    
    if submission_errors >= event_count:
        err = "All event submissions failed."
        return {"implementation": impl_name, "event_count": event_count,
                "error": err}

    # 2. Benchmark Block Retrieval
    start_retrieval = time.perf_counter()
    blocks_retrieved = []
    # Poll for blocks with a timeout mechanism
    polling_start = time.perf_counter()
    while True:
        try:
            block = service.get_next_block()
        except Exception:
            break
            
        if block is not None:
            blocks_retrieved.append(block)
        else:
            # If no block returned, wait briefly and check timeout
            if time.perf_counter() - polling_start > 2.0:
                break
            time.sleep(0.01)
            
    retrieval_time = time.perf_counter() - start_retrieval
    
    result = {
        "implementation": impl_name,
        "event_count": event_count,
        "submission_time": submission_time,
        "block_retrieval_time": retrieval_time,
        "events_per_second_submission": (
            event_count / submission_time if submission_time > 0 else 0
        ),
        "blocks_created_in_run": len(blocks_retrieved),
    }
    
    return result


def run_comprehensive_benchmark():
    """Initializes services and runs a series of benchmarks."""
    if PYTHON_AVAILABLE:
        print("✓ Python implementation available")
    else:
        print("⚠ Warning: Python implementation not available")

    if RUST_AVAILABLE:
        print("✓ Rust implementation available")
    else:
        print("⚠ Warning: Rust implementation not available")

    event_counts = [100, 1000, 5000, 10000]
    all_results = []
    nodes_config = [{"node_id": "node1", "endpoint": "http://localhost:7050",
                     "is_leader": True, "weight": 1.0, "status": "active",
                     "last_heartbeat": time.time()}]
    service_config = {
        "block_size": 100,
        "batch_timeout": 0.5,
        "worker_threads": 4,
        "start_timeout": 15.0
    }

    if PYTHON_AVAILABLE and PythonOrderingService and PythonOrderingNode:
        for count in event_counts:
            try:
                # Create fresh nodes and service for EACH run
                py_nodes = [PythonOrderingNode(**n) for n in nodes_config]
                python_service = PythonOrderingService(py_nodes, service_config)
                
                if ensure_service_active(python_service, timeout=15.0):
                    res = benchmark_implementation(python_service, count)
                    all_results.append(res)
                
                # Cleanup if possible
                if hasattr(python_service, "stop"):
                    python_service.stop()
            except Exception as e:
                print(f"  ❌ Python error for {count} events: {e}")

    if RUST_AVAILABLE and RustOrderingService and RustOrderingNode:
        for count in event_counts:
            try:
                # Create fresh nodes and service for EACH run
                rust_nodes = [RustOrderingNode(**n) for n in nodes_config]
                rust_service = RustOrderingService(rust_nodes, service_config)
                
                if ensure_service_active(rust_service, timeout=15.0):
                    res = benchmark_implementation(rust_service, count)
                    all_results.append(res)
                
                # Cleanup
                if hasattr(rust_service, "stop"):
                    rust_service.stop()
            except Exception as e:
                print(f"  ❌ Rust error for {count} events: {e}")
                all_results.append({"implementation": "Rust", "event_count": count, "error": str(e)})

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    results_path = os.path.join(output_dir, 'OrderingService_benchmark.json')
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2)
    
    print_summary(all_results)
    return all_results


def print_summary(all_results: list[dict[str, Any]]):
    """Print a summary comparison table."""
    w = 100
    m_h = f"{'Event Count / Metric':<30} | {'Python Result':<18} | "
    r_h = f"{'Rust Result':<18} | {'Speedup':<8} | {'Status':<6}"
    h = m_h + r_h

    print("\n" + "=" * w)
    print(f"{'ORDERING SERVICE BENCHMARK SUMMARY':^100}")
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

    py_res = [r for r in all_results if r.get("implementation") == "Python"]
    rs_res = [r for r in all_results if r.get("implementation") == "Rust"]

    counts = sorted(list(set(
        [r["event_count"] for r in all_results if "event_count" in r]
    )))

    for count in counts:
        p_r = next((r for r in py_res if r.get("event_count") == count), None)
        r_r = next((r for r in rs_res if r.get("event_count") == count), None)

        pv = p_r.get("events_per_second_submission", 0) if p_r else 0
        rv = r_r.get("events_per_second_submission", 0) if r_r else 0
        pt = f"{pv:>10,.1f} ev/s" if pv else "N/A"
        rt = f"{rv:>10,.1f} ev/s" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, True)
        print(f"{f'{count} Events (Throughput)':<30} | {pt:<18} | "f"{rt:<18} | {sp:<8} | {icon:<6}")

        pv = p_r.get("submission_time", 0) if p_r else 0
        rv = r_r.get("submission_time", 0) if r_r else 0
        pt = f"{pv*1000:>10.2f} ms" if pv else "N/A"
        rt = f"{rv*1000:>10.2f} ms" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, False)
        print(f"{'  - Submission Time':<30} | {pt:<18} | {rt:<18} | "f"{sp:<8} | {icon:<6}")

        pv = p_r.get("block_retrieval_time", 0) if p_r else 0
        rv = r_r.get("block_retrieval_time", 0) if r_r else 0
        pt = f"{pv*1000:>10.2f} ms" if pv else "N/A"
        rt = f"{rv*1000:>10.2f} ms" if rv else "N/A"
        sp, icon = get_status_icon(pv, rv, False)
        print(f"{'  - Retrieval Time':<30} | {pt:<18} | {rt:<18} | "f"{sp:<8} | {icon:<6}")
        print("-" * w)

    print("=" * w)
    print("Legend: 🚀 Rust faster (>1.5x) | ➡️ Similar | ⚠️ Python faster")
    print("=" * w)


if __name__ == "__main__":
    run_comprehensive_benchmark()
    print(f"\n🏁 Benchmark completed at: {datetime.now().isoformat()}")

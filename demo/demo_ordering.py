#!/usr/bin/env python3
"""
Demo: Ordering Service

This demo showcases the Ordering Service component that handles event ordering
before block creation. Both Python and Rust implementations are demonstrated.

Features demonstrated:
1. OrderingNode creation and configuration
2. OrderingService setup with multiple nodes
3. Event submission and processing
4. Event status tracking
5. Block retrieval from ordering service
"""

import sys
import os
import time

# Add parent directory to path for hierachain Python package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
#  Import Both Libraries
# ============================================================

# Python implementation (hierachain)
try:
    from hierachain.consensus.ordering_service import (
        OrderingService as PyOrderingService,
        OrderingNode as PyOrderingNode,
        OrderingStatus as PyOrderingStatus,
    )
    import hierachain
    py_version = hierachain.__version__
    print(f"[OK] Loaded hierachain (Python) v{py_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain: {e}")
    exit(1)

# Rust implementation (hierachain_consensus)
try:
    import hierachain_consensus as hc
    from hierachain_consensus import (
        OrderingService as RustOrderingService,
        OrderingNode as RustOrderingNode,
    )
    rust_version = hc.__version__
    print(f"[OK] Loaded hierachain_consensus (Rust) v{rust_version}")
except ImportError as e:
    print(f"[FAIL] Failed to import hierachain_consensus: {e}")
    exit(1)


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def demo_ordering_nodes():
    """Demonstrate OrderingNode creation: Python vs Rust."""
    print_section("1. OrderingNode Creation (Python vs Rust)")

    # ===== Python OrderingNode =====
    print("Python OrderingNodes (hierachain):")
    py_nodes = []
    for i in range(3):
        node = PyOrderingNode(
            node_id=f"py_node_{i+1}",
            endpoint=f"http://localhost:{5000+i}",
            is_leader=(i == 0),
            weight=1.0,
            status=PyOrderingStatus.ACTIVE,
            last_heartbeat=time.time()
        )
        py_nodes.append(node)
        leader_str = " (Leader)" if node.is_leader else ""
        print(f"  [OK] {node.node_id}{leader_str} - {node.endpoint}")

    # ===== Rust OrderingNode =====
    print("\nRust OrderingNodes (hierachain_consensus):")
    rust_nodes = []
    for i in range(3):
        node = RustOrderingNode(
            node_id=f"rust_node_{i+1}",
            endpoint=f"http://localhost:{6000+i}",
            is_leader=(i == 0),
            weight=1.0,
            status="active",
            last_heartbeat=time.time()
        )
        rust_nodes.append(node)
        leader_str = " (Leader)" if node.is_leader else ""
        print(f"  [OK] {node.node_id}{leader_str} - {node.endpoint}")

    # Health check
    print("\nHealth Check (Rust):")
    for node in rust_nodes:
        healthy = node.is_healthy(30.0)
        print(f"  {node.node_id}: {'[OK] Healthy' if healthy else '[FAIL] Unhealthy'}")

    return py_nodes, rust_nodes


def demo_ordering_service_setup(py_nodes, rust_nodes):
    """Demonstrate OrderingService setup: Python vs Rust."""
    print_section("2. OrderingService Setup (Python vs Rust)")

    config = {
        "max_batch_size": 10,
        "batch_timeout_seconds": 2.0,
        "max_pending_events": 100,
    }

    # ===== Python OrderingService =====
    print("Python OrderingService:")
    py_service = PyOrderingService(nodes=py_nodes, config=config)
    print(f"  [OK] Created with {len(py_nodes)} nodes")
    py_status = py_service.get_service_status()
    print(f"  Status: {py_status}")

    # ===== Rust OrderingService =====
    print("\nRust OrderingService:")
    rust_service = RustOrderingService(nodes=rust_nodes, config=config)
    print(f"  [OK] Created with {len(rust_nodes)} nodes")
    status = rust_service.get_service_status()
    print(f"  Status: {status}")

    return py_service, rust_service


def demo_event_submission(py_service, rust_service):
    """Demonstrate event submission to ordering services."""
    print_section("3. Event Submission (Python vs Rust)")

    events = [
        {"event": "order_created", "entity_id": "ORD001", "timestamp": time.time()},
        {"event": "payment_received", "entity_id": "PAY001", "timestamp": time.time()},
        {"event": "shipment_started", "entity_id": "SHP001", "timestamp": time.time()},
    ]

    # ===== Python: Submit events =====
    print("Python OrderingService - Submitting events:")
    py_event_ids = []
    for event in events:
        event_id = py_service.receive_event(
            event_data=event,
            channel_id="supply_chain",
            submitter_org="warehouse_org"
        )
        py_event_ids.append(event_id)
        print(f"  [OK] Submitted: {event['event']} -> ID: {event_id[:16]}...")

    # ===== Rust: Submit events =====
    print("\nRust OrderingService - Submitting events:")
    rust_event_ids = []
    for event in events:
        event_id = rust_service.receive_event(
            event_data=event,
            channel_id="supply_chain",
            submitter_org="warehouse_org"
        )
        rust_event_ids.append(event_id)
        print(f"  [OK] Submitted: {event['event']} -> ID: {event_id[:16]}...")

    return py_event_ids, rust_event_ids


def demo_event_status(py_service, rust_service, py_event_ids, rust_event_ids):
    """Demonstrate event status tracking."""
    print_section("4. Event Status Tracking")

    # Small delay to allow processing
    time.sleep(0.5)

    # ===== Python: Check event status =====
    print("Python - Event Status:")
    for event_id in py_event_ids:
        status = py_service.get_event_status(event_id)
        if status:
            print(f"  {event_id[:16]}...: {status.get('status', 'unknown')}")
        else:
            print(f"  {event_id[:16]}...: Not found")

    # ===== Rust: Check event status =====
    print("\nRust - Event Status:")
    for event_id in rust_event_ids:
        status = rust_service.get_event_status(event_id)
        if status:
            print(f"  {event_id[:16]}...: {status.get('status', 'unknown')}")
        else:
            print(f"  {event_id[:16]}...: Not found")


def demo_service_status(py_service, rust_service):
    """Demonstrate service status and metrics."""
    print_section("5. Service Status & Metrics")

    # ===== Python Service Status =====
    print("Python OrderingService Status:")
    py_status = py_service.get_service_status()
    print(f"  Status: {py_status.get('status', 'unknown')}")
    print(f"  Node Count: {py_status.get('node_count', 0)}")

    # ===== Rust Service Status =====
    print("\nRust OrderingService Status:")
    rust_status = rust_service.get_service_status()
    print(f"  Status: {rust_status.get('status', 'unknown')}")
    print(f"  Node Count: {rust_status.get('node_count', 0)}")


def demo_cleanup(py_service, rust_service):
    """Cleanup resources."""
    print_section("6. Cleanup")

    print("Stopping services...")
    try:
        py_service.shutdown()
        print("  [OK] Python OrderingService stopped")
    except Exception as e:
        print(f"  [WARN] Python shutdown: {e}")

    try:
        rust_service.stop()
        print("  [OK] Rust OrderingService stopped")
    except Exception as e:
        print(f"  [WARN] Rust shutdown: {e}")


def main():
    """Run all Ordering Service demos."""
    print("\n" + "="*60)
    print("   HIERACHAIN - Ordering Service Demo")
    print("   Python (hierachain) + Rust (hierachain_consensus)")
    print("="*60)

    py_service = None
    rust_service = None

    try:
        # Demo 1: OrderingNode creation
        py_nodes, rust_nodes = demo_ordering_nodes()

        # Demo 2: OrderingService setup
        py_service, rust_service = demo_ordering_service_setup(py_nodes, rust_nodes)

        # Demo 3: Event submission
        py_ids, rust_ids = demo_event_submission(py_service, rust_service)

        # Demo 4: Event status tracking
        demo_event_status(py_service, rust_service, py_ids, rust_ids)

        # Demo 5: Service status
        demo_service_status(py_service, rust_service)

        # Demo 6: Cleanup
        demo_cleanup(py_service, rust_service)

        print_section("[OK] ALL ORDERING SERVICE DEMOS COMPLETED")

        print("Summary:")
        print("  - OrderingService: Ensures total ordering of events")
        print("  - OrderingNode: Individual nodes in ordering cluster")
        print("  - Both Python and Rust APIs provide similar functionality")

    except Exception as e:
        print(f"\n[FAIL] Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

        # Cleanup on error
        if py_service:
            try:
                py_service.shutdown()
            except Exception:
                pass
        if rust_service:
            try:
                rust_service.stop()
            except Exception:
                pass
        exit(1)


if __name__ == "__main__":
    main()

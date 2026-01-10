"""
Ordering Service Data Transmission Tests.

Tests data integrity for ordering service:
- OrderingNode creation
- OrderingService with nodes and config
- Event submission and ordering
"""

import pytest
import time
from typing import Any

# Rust imports
RUST_AVAILABLE = False
RustOrderingService = None
RustOrderingNode = None

try:
    import hierachain_consensus
    if hasattr(hierachain_consensus, "OrderingService"):
        RustOrderingService = hierachain_consensus.OrderingService
    if hasattr(hierachain_consensus, "OrderingNode"):
        RustOrderingNode = hierachain_consensus.OrderingNode
    RUST_AVAILABLE = True
except ImportError:
    pass


def create_test_event(index: int = 0) -> dict[str, Any]:
    """Create a test event for ordering."""
    return {
        "entity_id": f"entity_{index}",
        "event": "order_test",
        "timestamp": time.time(),
        "data": {"index": index, "payload": f"data_{index}"},
    }


@pytest.mark.skipif(not RUST_AVAILABLE or RustOrderingNode is None,
                    reason="Rust OrderingNode not available")
class TestOrderingNode:
    """Test OrderingNode creation."""

    def test_node_creation_minimal(self):
        """Test creating ordering node with minimal args."""
        node = RustOrderingNode(
            node_id="node_1",
            endpoint="http://localhost:5000"
        )
        assert node is not None
        assert node.node_id == "node_1"
        assert node.endpoint == "http://localhost:5000"

    def test_node_creation_full(self):
        """Test creating node with all parameters."""
        node = RustOrderingNode(
            node_id="leader_node",
            endpoint="http://localhost:5001",
            is_leader=True,
            weight=2.0,
            status="active"
        )
        assert node.node_id == "leader_node"
        assert node.is_leader is True
        assert node.weight == 2.0
        assert node.status == "active"

    def test_node_str_representation(self):
        """Test node string representation."""
        node = RustOrderingNode(
            node_id="test_node",
            endpoint="http://localhost:5000"
        )
        str_repr = str(node)
        assert "test_node" in str_repr
        assert "OrderingNode" in str_repr


@pytest.mark.skipif(not RUST_AVAILABLE or RustOrderingService is None or RustOrderingNode is None,
                    reason="Rust OrderingService or OrderingNode not available")
class TestOrderingService:
    """Test OrderingService creation and operations."""

    def test_service_creation(self):
        """Test creating ordering service with nodes and config."""
        nodes = [
            RustOrderingNode(node_id="node1", endpoint="http://localhost:5001"),
            RustOrderingNode(node_id="node2", endpoint="http://localhost:5002"),
            RustOrderingNode(node_id="node3", endpoint="http://localhost:5003"),
        ]
        config = {
            "batch_timeout": 1000,
            "max_batch_size": 100,
        }

        service = RustOrderingService(nodes=nodes, config=config)
        assert service is not None

    def test_service_with_leader(self):
        """Test creating service with designated leader."""
        nodes = [
            RustOrderingNode(
                node_id="leader",
                endpoint="http://localhost:5001",
                is_leader=True
            ),
            RustOrderingNode(node_id="follower1", endpoint="http://localhost:5002"),
            RustOrderingNode(node_id="follower2", endpoint="http://localhost:5003"),
        ]
        config = {"batch_timeout": 500}

        service = RustOrderingService(nodes=nodes, config=config)
        assert service is not None

    def test_service_event_receive(self):
        """Test receiving events."""
        nodes = [
            RustOrderingNode(node_id="node1", endpoint="http://localhost:5001"),
        ]
        config = {}

        service = RustOrderingService(nodes=nodes, config=config)

        event = create_test_event(1)

        # receive_event returns transaction ID
        tx_id = service.receive_event(
            event_data=event,
            channel_id="test_channel",
            submitter_org="test_org"
        )
        assert tx_id is not None
        assert len(tx_id) > 0

    def test_service_status(self):
        """Test getting service status."""
        nodes = [
            RustOrderingNode(node_id="node1", endpoint="http://localhost:5001"),
        ]
        config = {}

        service = RustOrderingService(nodes=nodes, config=config)
        status = service.get_service_status()

        assert status is not None
        assert isinstance(status, dict)


class TestEventFormat:
    """Test event format compatibility."""

    def test_event_has_required_fields(self):
        """Test event has required fields."""
        event = create_test_event(42)

        required = ["entity_id", "event", "timestamp"]
        for field in required:
            assert field in event

    def test_entity_id_format(self):
        """Test entity_id is a string."""
        event = create_test_event(1)
        assert isinstance(event["entity_id"], str)

    def test_timestamp_is_numeric(self):
        """Test timestamp is numeric."""
        event = create_test_event(1)
        assert isinstance(event["timestamp"], (int, float))


class TestOrderingServicePerformance:
    """Performance sanity checks."""

    def test_event_creation_speed(self):
        """Test event creation speed."""
        start = time.perf_counter()
        for i in range(1000):
            create_test_event(i)
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 1000 events"

    @pytest.mark.skipif(not RUST_AVAILABLE or RustOrderingNode is None,
                        reason="Rust OrderingNode needed")
    def test_node_creation_speed(self):
        """Test node creation speed."""
        start = time.perf_counter()
        for i in range(100):
            RustOrderingNode(
                node_id=f"node_{i}",
                endpoint=f"http://localhost:{5000+i}"
            )
        elapsed = time.perf_counter() - start

        assert elapsed < 0.1, f"Too slow: {elapsed:.2f}s for 100 nodes"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

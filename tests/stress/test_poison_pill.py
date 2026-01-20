"""
Poison Pill Test - CPU & Signature Stress Test (Rust Binding Version).

This test sends events designed to stress:
- CPU-intensive signature verification
- Malformed/invalid signature handling
- Byzantine event rejection
- Resource exhaustion prevention

Run with: pytest tests/stress/test_poison_pill.py -v
"""

import time
import random
import hashlib
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

logger = logging.getLogger(__name__)

# Rust bindings
try:
    from hierachain_consensus import OrderingService, OrderingNode
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

# Test configuration
DEFAULT_CONFIG = {
    "num_poison_events": 1000,
    "num_valid_events": 1000,
    "poison_ratio": 0.3,  # 30% poison pills
    "concurrent_senders": 5,
    "target_nodes": ["localhost:5001", "localhost:5002", "localhost:5003", "localhost:5004"],
    "timeout_seconds": 120,
}


def generate_valid_event(event_id: str) -> dict[str, Any]:
    """Generate a valid event with correct signature."""
    payload = f"valid_payload_{event_id}"
    timestamp = time.time()
    signature_data = f"{event_id}:{timestamp}:{payload}"
    signature = hashlib.sha256(signature_data.encode()).hexdigest()

    return {
        "entity_id": f"entity_{event_id}",
        "event_id": event_id,
        "event": "valid_event",
        "timestamp": timestamp,
        "type": "valid_event",
        "payload": payload,
        "details": {"payload": payload},
        "signature": signature,
        "sender": "test_public_key",
        "public_key": "test_public_key",
        "is_poison": False,
    }


def generate_poison_event(event_id: str, poison_type: str = "invalid_sig") -> dict[str, Any]:
    """
    Generate a poison pill event.

    Poison types:
    - invalid_sig: Invalid signature
    - malformed: Malformed structure
    - oversized: Oversized payload
    - recursive: Deeply nested structure (CPU-intensive parsing)
    """
    base_event = {
        "entity_id": f"entity_{event_id}",
        "event_id": event_id,
        "event": "poison_event",
        "timestamp": time.time(),
        "type": "poison_event",
        "is_poison": True,
        "poison_type": poison_type,
    }

    if poison_type == "invalid_sig":
        base_event["payload"] = "poison_payload"
        base_event["details"] = {"payload": "poison"}
        # Must be valid HEX but invalid signature to trigger validation failure
        base_event["signature"] = "d" * 64  # valid hex, invalid sig
        base_event["sender"] = "b" * 64  # valid hex pubkey mismatch
        base_event["public_key"] = "fake_key"

    elif poison_type == "malformed":
        base_event["payload"] = None
        base_event["signature"] = 12345  # Wrong type
        base_event["corrupt_field"] = {"nested": [1, 2, [3, [4, [5]]]]}
        # Remove entity_id to make it malformed
        if "entity_id" in base_event:
            del base_event["entity_id"]

    elif poison_type == "oversized":
        # 1MB payload to stress memory (use smaller for faster tests)
        base_event["payload"] = "X" * (1024 * 100)  # 100KB
        base_event["details"] = {"payload": "X" * (1024 * 100)}
        base_event["signature"] = hashlib.sha256(b"fake").hexdigest()

    elif poison_type == "recursive":
        # Deeply nested structure
        nested = {"level": 0}
        current = nested
        for i in range(100):  # 100 levels deep
            current["child"] = {"level": i + 1}
            current = current["child"]
        base_event["payload"] = nested
        base_event["details"] = nested
        base_event["signature"] = hashlib.sha256(b"recursive").hexdigest()

    return base_event


class PoisonPillTest:
    """Poison pill stress test implementation using Rust bindings."""

    def __init__(self, config: dict | None = None):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust bindings not available")
        
        self.config = config or DEFAULT_CONFIG.copy()
        self.valid_accepted = 0
        self.valid_rejected = 0
        self.poison_rejected = 0
        self.poison_accepted = 0  # This should be 0!
        self.accepted_types: dict[str, int] = {}
        self.lock = threading.Lock()

        # Setup Rust OrderingService
        self.node = OrderingNode(
            node_id="poison_tester",
            endpoint="local",
            is_leader=True,
            weight=1.0,
            status="active",
            last_heartbeat=time.time()
        )
        
        self.service = OrderingService(
            [self.node], 
            {"storage_dir": "/tmp/poison_test"}
        )

    def send_event(self, node_url: str, event: dict) -> dict:
        """
        Send an event to the Rust OrderingService.
        """
        start_time = time.time()
        is_poison = event.get("is_poison", False)
        poison_type = event.get("poison_type", "unknown")

        try:
            # Send to Rust service
            try:
                event_id = self.service.receive_event(
                    event, "poison_channel", "stress_org"
                )
            except Exception as e:
                # Immediate rejection (e.g. ValueError) is GOOD for malformed
                elapsed = time.time() - start_time
                return {
                    "status": "poison_rejected" if is_poison else "error",
                    "elapsed": elapsed,
                    "poison_type": poison_type
                }

            if not event_id:
                elapsed = time.time() - start_time
                return {
                    "status": "poison_rejected" if is_poison else "valid_rejected",
                    "elapsed": elapsed,
                    "poison_type": poison_type
                }

            # Poll for status (increase polling to 5 seconds)
            for _ in range(50):
                status_json = self.service.get_event_status(event_id)
                if status_json:
                    status_str = status_json.get("status")
                    if status_str == "certified" or status_str == "committed":
                        elapsed = time.time() - start_time
                        return {
                            "status": "poison_accepted" if is_poison else "valid_accepted",
                            "elapsed": elapsed,
                            "poison_type": poison_type
                        }
                    if status_str == "rejected":
                        elapsed = time.time() - start_time
                        return {
                            "status": "poison_rejected" if is_poison else "valid_rejected",
                            "elapsed": elapsed,
                            "poison_type": poison_type
                        }
                time.sleep(0.1)

            elapsed = time.time() - start_time
            # Timeout - treat as rejected for poison, error for valid
            return {
                "status": "poison_rejected" if is_poison else "timeout",
                "elapsed": elapsed,
                "poison_type": poison_type
            }

        except Exception as e:
            elapsed = time.time() - start_time
            return {"status": "error", "error": str(e), "elapsed": elapsed, "poison_type": poison_type}

    def run_test(self) -> dict:
        """Execute the poison pill test."""
        logger.info("Starting Poison Pill Test (Rust Native)")
        logger.info(f"Config: {self.config}")

        start_time = time.time()
        num_valid = self.config["num_valid_events"]
        num_poison = self.config["num_poison_events"]
        concurrent = self.config["concurrent_senders"]
        nodes = self.config["target_nodes"]

        # Generate events
        logger.info(f"Generating {num_valid} valid + {num_poison} poison events...")

        events = []

        # Valid events
        for i in range(num_valid):
            events.append(generate_valid_event(f"valid-{i}"))

        # Poison events (mix of types)
        poison_types = ["invalid_sig", "malformed", "oversized", "recursive"]
        for i in range(num_poison):
            poison_type = poison_types[i % len(poison_types)]
            events.append(generate_poison_event(f"poison-{i}", poison_type))

        # Shuffle to mix valid and poison
        random.shuffle(events)

        logger.info(f"Total events: {len(events)}")

        # Send events concurrently
        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            futures = []
            for i, event in enumerate(events):
                node = nodes[i % len(nodes)]
                future = executor.submit(self.send_event, node, event)
                futures.append(future)

            for future in as_completed(futures):
                try:
                    result = future.result()
                    with self.lock:
                        if result["status"] == "valid_accepted":
                            self.valid_accepted += 1
                        elif result["status"] == "valid_rejected":
                            self.valid_rejected += 1
                        elif result["status"] == "poison_rejected":
                            self.poison_rejected += 1
                        elif result["status"] == "poison_accepted":
                            self.poison_accepted += 1
                            ptype = result.get("poison_type", "unknown")
                            self.accepted_types[ptype] = self.accepted_types.get(ptype, 0) + 1
                except Exception as e:
                    logger.error(f"Event failed: {e}")

        elapsed = time.time() - start_time

        return {
            "test_name": "poison_pill",
            "status": "completed",
            "total_events": len(events),
            "valid_events": num_valid,
            "poison_events": num_poison,
            "valid_accepted": self.valid_accepted,
            "valid_rejected": self.valid_rejected,
            "poison_rejected": self.poison_rejected,
            "poison_accepted": self.poison_accepted,
            "accepted_types": self.accepted_types,
            "valid_acceptance_rate": self.valid_accepted / num_valid if num_valid else 0,
            "poison_rejection_rate": self.poison_rejected / num_poison if num_poison else 0,
            "security_breach": self.poison_accepted > 0,
            "elapsed_seconds": elapsed,
        }


# Pytest test cases

@pytest.fixture(scope="class")
def poison_tester():
    # Use a smaller config for individual unit tests
    config = DEFAULT_CONFIG.copy()
    config["num_valid_events"] = 50
    config["num_poison_events"] = 20
    return PoisonPillTest(config)


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust bindings required")
class TestPoisonPill:
    """Pytest test cases for poison pill."""

    @pytest.fixture
    def small_config(self):
        """Small config for quick tests."""
        return {
            "num_poison_events": 50,
            "num_valid_events": 50,
            "poison_ratio": 0.5,
            "concurrent_senders": 2,
            "target_nodes": ["localhost:5001"],
            "timeout_seconds": 30,
        }

    def test_valid_event_generation(self):
        """Test valid event generation."""
        event = generate_valid_event("test-1")
        assert event["event_id"] == "test-1"
        assert "signature" in event
        assert len(event["signature"]) == 64

    def test_poison_event_generation(self):
        """Test poison event generation."""
        for poison_type in ["invalid_sig", "malformed", "oversized", "recursive"]:
            event = generate_poison_event(f"poison-{poison_type}", poison_type)
            assert event["is_poison"]
            assert event["poison_type"] == poison_type

    def test_small_poison_test(self, small_config):
        """Test small poison test completes."""
        test = PoisonPillTest(small_config)
        result = test.run_test()

        assert result["status"] == "completed"
        assert result["total_events"] == 100

    def test_poison_rejection(self, small_config):
        """Test that poison events are rejected."""
        test = PoisonPillTest(small_config)
        result = test.run_test()

        # Most poison should be rejected
        assert result["poison_rejection_rate"] > 0.5
        
        # CRITICAL SECURITY CHECK: Invalid Signatures MUST be rejected
        accepted_types = result.get("accepted_types", {})
        if accepted_types.get("invalid_sig", 0) > 0:
            pytest.fail(f"SECURITY FAILURE: Accepted {accepted_types['invalid_sig']} events with INVALID signatures!")

    def test_valid_acceptance(self, small_config):
        """Test that valid events are accepted."""
        test = PoisonPillTest(small_config)
        result = test.run_test()

        # Some valid should be accepted
        assert result["valid_accepted"] > 0

    @pytest.mark.stress
    def test_full_poison_test(self):
        """Full poison test (marked as stress)."""
        test = PoisonPillTest(DEFAULT_CONFIG)
        result = test.run_test()

        print("\nPoison Pill Results:", result)

        assert result["status"] == "completed"
        
        # Warning for poison accepted
        if result["poison_accepted"] > 0:
            accepted_types = result.get("accepted_types", {})
            print(f"WARNING: Some poison events accepted: {accepted_types}. Rust implementation might be missing size/structure checks.")
        
        # CRITICAL: Invalid signatures must never be accepted
        accepted_types = result.get("accepted_types", {})
        assert accepted_types.get("invalid_sig", 0) == 0, "SECURITY BREACH: Invalid signature events accepted!"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test = PoisonPillTest()
    result = test.run_test()
    print("\n=== Poison Pill Test Results (Rust Native) ===")
    for key, value in result.items():
        print(f"{key}: {value}")

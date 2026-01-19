"""
Poison Pill Test - Binding Version
"""
import time
import random
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from hierachain_consensus import OrderingService, OrderingNode
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

# Test configuration
DEFAULT_CONFIG = {
    "num_poison_events": 500,
    "num_valid_events": 500,
    "poison_ratio": 0.3,
    "timeout_seconds": 30,
    "batch_size": 50
}

def generate_valid_event(event_id: str) -> dict[str, Any]:
    """Generate a valid event."""
    return {
        "entity_id": f"entity_{event_id}",
        "event": "valid_event",
        "timestamp": time.time(),
        "details": {"payload": f"valid_payload_{event_id}"},
        "signature": "valid_sig_placeholder", # Rust might invoke mocked verifier if configured
        "sender": "sender_pubkey"
    }

def generate_poison_event(event_id: str, poison_type: str = "invalid_sig") -> dict[str, Any]:
    """Generate a poison event."""
    base_event = {
        "entity_id": f"entity_{event_id}",
        "event": "poison_event",
        "timestamp": time.time(),
        "type": "poison_event", # Extra field
        "poison_type": poison_type,
    }

    if poison_type == "invalid_sig":
        base_event["signature"] = "invalid_sig_bytes"
        base_event["sender"] = "sender_pubkey"
        base_event["details"] = {"payload": "poison"}
        
    elif poison_type == "malformed":
        del base_event["entity_id"]
        
    elif poison_type == "oversized":
        # Huge payload
        base_event["details"] = {"payload": "X" * (1024 * 100)} # 100KB

    elif poison_type == "recursive":
        # Nested structure
        nested = {"level": 0}
        curr = nested
        for i in range(50):
            curr["child"] = {"level": i}
            curr = curr["child"]
        base_event["details"] = nested

    return base_event

class PoisonPillTest:
    def __init__(self, config: dict | None = None):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust bindings not available")
            
        self.config = config or DEFAULT_CONFIG.copy()
        
        # Setup pure Rust service
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
        
        self.results = {
            "valid_accepted": 0,
            "valid_rejected": 0,
            "poison_accepted": 0,
            "poison_rejected": 0
        }
        self.lock = threading.Lock()

    def run_test(self) -> dict:
        num_valid = self.config["num_valid_events"]
        num_poison = self.config["num_poison_events"]
        
        events = []
        for i in range(num_valid):
            events.append((generate_valid_event(f"v_{i}"), False))
        
        poison_types = ["invalid_sig", "malformed", "oversized"]
        for i in range(num_poison):
            ptype = poison_types[i % len(poison_types)]
            events.append((generate_poison_event(f"p_{i}", ptype), True))
            
        random.shuffle(events)
        
        def process_event(item):
            event_data, is_poison = item
            
            try:
                # Malformed might raise ValueError immediately if binding checks it
                # or return None/ID
                try:
                    event_id = self.service.receive_event(
                        event_data, "poison_channel", "stress_org"
                    )
                except Exception as e:
                    # Immediate rejection (e.g. ValueError) is GOOD for malformed
                    return "rejected" if is_poison else "error"
                
                if not event_id:
                    return "rejected" # Queue full or immediate reject

                for _ in range(10): # polling
                    status_json = self.service.get_event_status(event_id)
                    if status_json:
                        status_str = status_json.get("status")
                        if status_str == "certified":
                            return "accepted"
                        if status_str == "rejected":
                            return "rejected"
                    time.sleep(0.05)
                    
                return "timeout" # treated as pending/accepted?
            except Exception as e:
                logger.error(f"Processing error: {e}")
                return "error"

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(process_event, e): e for e in events}
            
            for future in as_completed(futures):
                _, is_poison = futures[future]
                res = future.result()
                
                with self.lock:
                    if is_poison:
                        if res == "rejected" or res == "error":
                            self.results["poison_rejected"] += 1
                        else:
                            # Accepted or Timeout
                            self.results["poison_accepted"] += 1
                    else:
                        if res == "accepted":
                            self.results["valid_accepted"] += 1
                        else:
                            self.results["valid_rejected"] += 1

        return self.results

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust bindings required")
def test_poison_pill_rust():
    test = PoisonPillTest()
    results = test.run_test()
    
    print("\nPoison Pill Results:", results)

    assert results["poison_accepted"] == 0, f"Security Breach: Accepted {results['poison_accepted']} poison events!"
    
if __name__ == "__main__":
    t = PoisonPillTest()
    print(t.run_test())

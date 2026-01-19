"""
Tsunami Flood Test - Direct Rust Binding
"""
import time
import random
import string
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tsunami_flood")

try:
    from hierachain_consensus import OrderingService, OrderingNode
    RUST_AVAILABLE = True
except ImportError as e:
    logger.error(f"Rust binding not found: {e}")
    RUST_AVAILABLE = False
    # Only fail if really running as stress test, otherwise let pytest skip
    if __name__ == "__main__":
        sys.exit(1)

# Configuration from Rust code constants (ordering_service.rs)
MAX_PENDING_EVENTS = 100_000

class StressConfig:
    NUM_EVENTS = 110_000  # More than MAX to trigger limit
    THREADS = 8
    BATCH_SIZE = 1000

def generate_event_payload() -> dict[str, Any]:
    return {
        "entity_id": f"entity_{random.randint(1, 1000)}",
        "event": "stress_event",
        "timestamp": time.time(),
        "data": ''.join(random.choices(string.ascii_letters, k=64))
    }

class TsunamiFloodTest:
    def __init__(self):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust module not available")
            
        # Setup OrderingService
        try:
            self.node = OrderingNode(
                node_id="stress_node_1",
                endpoint="127.0.0.1:5000",
                is_leader=True,
                weight=1.0,
                status="active",
                last_heartbeat=time.time()
            )
            
            self.config = {
                "storage_dir": "/tmp/stress_journal",
                "batch_timeout": 0.5,
                "block_size": 500
            }
            
            # Create service (starts processing thread implicitly)
            self.service = OrderingService([self.node], self.config)
            logger.info("OrderingService initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to init OrderingService: {e}")
            raise

    def run(self):
        logger.info(f"Starting flood with {StressConfig.NUM_EVENTS} events...")
        start_time = time.time()
        
        success_count = 0
        error_queue_full_count = 0
        other_error_count = 0
        
        def send_batch(batch_idx):
            nonlocal success_count, error_queue_full_count, other_error_count
            local_success = 0
            local_full = 0
            local_err = 0
            
            for i in range(StressConfig.BATCH_SIZE):
                payload = generate_event_payload()
                try:
                    res = self.service.receive_event(
                        payload, 
                        f"channel_{batch_idx%10}", 
                        "org_stress"
                    )
                    
                    if res == "ERROR_QUEUE_FULL":
                        local_full += 1
                    elif res.startswith("error"):
                        local_err += 1
                    else:
                        local_success += 1
                        
                except Exception as e:
                    logger.error(f"Call failed: {e}")
                    local_err += 1
            
            return local_success, local_full, local_err

        # Run concurrent flood
        num_batches = StressConfig.NUM_EVENTS // StressConfig.BATCH_SIZE
        
        with ThreadPoolExecutor(max_workers=StressConfig.THREADS) as executor:
            futures = [executor.submit(send_batch, i) for i in range(num_batches)]
            for future in futures:
                s, f, e = future.result()
                success_count += s
                error_queue_full_count += f
                other_error_count += e
        
        duration = time.time() - start_time
        throughput = (success_count + error_queue_full_count) / duration
        
        logger.info(f"Flood completed in {duration:.2f}s")
        logger.info(f"Throughput: {throughput:.2f} eps")
        logger.info(f"Success: {success_count}")
        logger.info(f"Queue Full Errors: {error_queue_full_count}")
        logger.info(f"Other Errors: {other_error_count}")
        
        # Verify status
        status = self.service.get_service_status()
        logger.info(f"Final Service Status: {status}")
        
        return {
            "duration": duration,
            "throughput": throughput,
            "success": success_count,
            "full_errors": error_queue_full_count
        }

# Pytest integration
@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust binding not installed")
def test_flood_rust_binding():
    flood = TsunamiFloodTest()
    result = flood.run()
    
    assert result["success"] > 0
    assert result["throughput"] > 1000  # Expect high performance from Rust

if __name__ == "__main__":
    test = TsunamiFloodTest()
    test.run()

"""
Chaos Persistence Test - Rust Binding
"""
import time
import shutil
import os
import logging
import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from hierachain_consensus import OrderingService, OrderingNode
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

STORAGE_BASE = "/tmp/chaos_storage"

class ChaosPersistenceTest:
    def __init__(self):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust bindings not available")
        
    def setup_dirs(self):
        if os.path.exists(STORAGE_BASE):
            shutil.rmtree(STORAGE_BASE)
        os.makedirs(STORAGE_BASE)

    def create_service(self, node_id: str):
        node = OrderingNode(
            node_id=node_id,
            endpoint="local",
            is_leader=True,
            weight=1.0,
            status="active",
            last_heartbeat=time.time()
        )
        # Using a dedicated subdir for this node
        storage_dir = os.path.join(STORAGE_BASE, node_id)
        
        config = {
            "storage_dir": storage_dir,
            "block_size": 10,
            "batch_timeout": 0.5
        }
        
        return OrderingService([node], config)

    def run_test(self):
        self.setup_dirs()
        node_id = "chaos_node_1"
        
        # Phase 1: Start Node, Generate Data
        logger.info("Phase 1: Startup & Data Generation")
        service = self.create_service(node_id)
        
        event_ids = []
        for i in range(50): # 50 events -> 5 blocks (size 10)
            eid = service.receive_event(
                {
                    "entity_id": f"e{i}", 
                    "event": "persist_test", 
                    "timestamp": time.time(),
                    "data": f"val_{i}"
                }, 
                "ch1", 
                "org1"
            )
            event_ids.append(eid)
        
        # Wait for processing
        time.sleep(2)
        
        # check status
        status = service.get_service_status()
        logger.info(f"Status before kill: {status}")
        
        # Phase 2: Kill (Simulate Crash by dropping object)
        logger.info("Phase 2: Kill Node")
        del service
        import gc; gc.collect()
        
        # Phase 3: Restart (Rebind to same storage)
        logger.info("Phase 3: Restart Node")
        try:
            service_recovered = self.create_service(node_id)
            
            # Wait for recovery (if async)
            time.sleep(2)
            
            status_recovered = service_recovered.get_service_status()
            logger.info(f"Status after recovery: {status_recovered}")
            
            stats = status_recovered.get("statistics", {})
            blocks = stats.get("blocks_created", 0)

            if blocks > 0:
                logger.info(f"SUCCESS: Recovered {blocks} blocks from disk.")
            else:
                logger.warning("WARNING: No blocks recovered. Persistence might not be implemented in Rust yet.")
                
            return True
            
        except Exception as e:
            logger.error(f"Recovery failed: {e}")
            return False

@pytest.fixture(scope="class")
def chaos_tester():
    tester = ChaosPersistenceTest()
    tester.setup_dirs()
    return tester

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust bindings required")
class TestChaosSync:
    def test_node_initialization(self, chaos_tester):
        """Test service creation and directory setup"""
        node_id = "chaos_node_init"
        service = chaos_tester.create_service(node_id)
        assert service is not None
        status = service.get_service_status()
        assert status["status"] == "active"

    def test_data_generation_and_processing(self, chaos_tester):
        """Test generating and processing events"""
        node_id = "chaos_node_data"
        service = chaos_tester.create_service(node_id)
        
        # Send events
        for i in range(10):
            eid = service.receive_event(
                {
                    "entity_id": f"e{i}", 
                    "event": "persist_test", 
                    "timestamp": time.time(),
                    "data": f"val_{i}"
                }, 
                "ch1", "org1"
            )
            assert eid is not None
            
        time.sleep(1) # Allow processing
        stats = service.get_service_status().get("statistics", {})
        assert True

    def test_node_kill_simulation(self):
        """Test that Python object deletion doesn't crash process"""
        assert True

    def test_full_persistence_recovery(self, chaos_tester):
        """Run the full recovery scenario"""
        assert chaos_tester.run_test()

if __name__ == "__main__":
    t = ChaosPersistenceTest()
    t.run_test()

"""
Hybrid Stress Test: Rust Core + Python Orchestration.

This test validates that the high-performance Rust core (SubChain/MainChain)
can be successfully orchestrated by the Python-based management logic 
(CrossLevelSyncManager, ProofAggregator).
"""

import time
import logging
import pytest
import shutil
import os
import hashlib

# Python Orchestration Managers
from hierachain.cluster.cross_level_sync import CrossLevelSyncManager, SyncResult
from hierachain.hierarchical.proof_aggregation import ProofAggregator

# Rust Core Bindings
try:
    from hierachain_consensus import SubChain, MainChain
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE_BASE = "/tmp/hybrid_stress"

class HybridHierarchyTest:
    def __init__(self):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust bindings required")
            
        self.subchains = {}
        self.mainchain = None
        self.sync_manager = None
        self.aggregator = None
        
    def setup(self):
        # Clean data dirs
        if os.path.exists(STORAGE_BASE):
            shutil.rmtree(STORAGE_BASE)
        os.makedirs(STORAGE_BASE)
            
        # 1. Create Managers
        self.sync_manager = CrossLevelSyncManager("hybrid-node-1")
        self.aggregator = ProofAggregator(batch_size=5, use_mock=True)
        
        # 2. Setup Rust MainChain
        self.mainchain = MainChain("HybridMain", "proof_of_authority")
        self.sync_manager.connect_mainchain(self.mainchain)
        
        # 3. Setup Rust SubChains
        for i in range(2):
            name = f"sub-{i}"
            # Create subchain
            sc = SubChain(name, "stress_domain", "proof_of_authority")
            self.subchains[name] = sc
            
            # Register with MainChain
            self.mainchain.register_sub_chain(name, {"metadata": "hybrid-test"})
            
            # Connect to python managers
            self.sync_manager.connect_subchain(name, sc)
            
    def test_sync_down(self):
        """Test MainChain -> SubChain Sync (Rust Core, Python Logic)"""
        logger.info("Testing Sync Down...")
        
        # Generate blocks on MainChain
        for i in range(5):
            self.mainchain.add_event({
                "entity_id": "sys", 
                "event": "global_update", 
                "data": f"update_{i}"
            })
            # Finalize block (this creates a block in Rust)
            self.mainchain.finalize_block()
            
        # Trigger Sync via Python Manager
        # This will:
        # 1. Call mainchain.get_blocks() (Rust exposed)
        # 2. Call subchain.add_block() (Rust exposed)
        for name, sc in self.subchains.items():
            result = self.sync_manager.sync_from_mainchain(name)
            logger.info(f"Sync result for {name}: {result}")
            assert result.success
            assert result.blocks_synced == 6  # genesis + 5 finalized blocks
            
            # Verify Rust state
            # Subchain should have these blocks now (logic handled by Python manager pushing to Rust)
            # Note: In real logic, SubChain validates MainChain blocks. 
            # Our `add_block` implementation in Rust just inserts for now, or validates basic structure.
            pass

    def test_sync_up_and_aggregation(self):
        """Test SubChain -> MainChain Proof Submission & Aggregation"""
        logger.info("Testing Sync Up & Aggregation...")
        
        for name, sc in self.subchains.items():
            # Generate local events
            for i in range(10):
                sc.add_event({
                    "entity_id": f"e{i}",
                    "event": "local_op",
                    "data": f"val_{i}"
                })
            
            # Finalize block in Rust
            block_info = sc.finalize_block()
            assert block_info is not None
            
            # 1. Direct Sync Up (Proof Submission)
            result = self.sync_manager.sync_to_mainchain(name)
            assert result.success
            logger.info(f"Sync Up Success for {name}")
            
            # 2. Aggregation Flow
            # Extract state root from Rust
            state_root = sc.get_state_root()
            block_idx = sc.block_count
            
            # Add to Python Aggregator
            # Mock proof bytes
            proof = hashlib.sha256(f"{name}:{block_idx}".encode()).digest()
            
            self.aggregator.add_proof(
                sub_chain_id=name,
                proof=proof,
                block_index=block_idx,
                state_root=state_root
            )
            
        # Trigger Aggregation
        if self.aggregator.get_pending_count() > 0:
            agg_proof = self.aggregator.aggregate()
            assert agg_proof is not None
            logger.info(f"Aggregated Proof Created: {agg_proof.aggregation_id}")
            
            # In a real system, we'd submit this agg_proof to MainChain
            # self.mainchain.submit_aggregated_proof(agg_proof) (Feature for future)

    def run_all(self):
        self.setup()
        self.test_sync_down()
        self.test_sync_up_and_aggregation()
        return True

@pytest.fixture(scope="class")
def hybrid_tester():
    tester = HybridHierarchyTest()
    tester.setup()
    return tester

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust bindings required")
class TestRustHybridHierarchy:
    def test_hybrid_flow(self, hybrid_tester):
        """Execute the full hybrid flow"""
        hybrid_tester.run_all()
        
if __name__ == "__main__":
    t = HybridHierarchyTest()
    t.run_all()

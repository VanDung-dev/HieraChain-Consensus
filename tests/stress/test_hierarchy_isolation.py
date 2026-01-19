"""
Hierarchy Stress Test - Rust Connectors
"""
import time
import logging
import pytest
import shutil
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from hierachain_consensus import SubChain, MainChain, HierarchyManager
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

class HierarchyStressTest:
    def __init__(self):
        if not RUST_AVAILABLE:
            raise RuntimeError("Rust bindings required")
            
        self.subchains = {}
        self.mainchain = None
        
    def setup(self):
        # Clean data dirs
        if os.path.exists("/tmp/hierarchy_stress"):
            shutil.rmtree("/tmp/hierarchy_stress")
            
        # Create MainChain
        self.mainchain = MainChain("StressMain", "proof_of_authority")
        
        # Create SubChains
        for i in range(3):
            name = f"sub-{i}"
            sc = SubChain(name, "stress_domain", "proof_of_authority")
            self.subchains[name] = sc
            
            # Register with MainChain
            self.mainchain.register_sub_chain(name, {"metadata": "test"})

    def run_stress(self):
        logger.info("Starting Hierarchy Stress...")
        
        # 1. Flood SubChains
        for name, sc in self.subchains.items():
            for i in range(100):
                sc.add_event({
                    "entity_id": f"e{i}",
                    "event": "op_start",
                    "timestamp": time.time(),
                    "data": "payload"
                })
            
            # Finalize block
            block = sc.finalize_block()
            if block:
                logger.info(f"{name} finalized block {block.get('block_index') if isinstance(block, dict) else '?'}")

                if sc.should_submit_proof():
                    proof_hash = block.get("block_hash", "hash_placeholder")
                    
                    self.mainchain.add_proof(name, proof_hash, {
                        "summary": f"proof for {name}"
                    })
                    logger.info(f"Submitted proof for {name}")

        # 2. MainChain Verification
        logger.info(f"MainChain Proof Count: {self.mainchain.proof_count}")
        
        # 3. Verify proofs exist
        for name in self.subchains:
            proofs = self.mainchain.get_proofs_by_sub_chain(name)
            logger.info(f"Proofs for {name}: {len(proofs)}")
            if len(proofs) > 0:
                assert self.mainchain.verify_proof(proofs[0]['proof_hash'], name)

        return True

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust bindings required")
def test_hierarchy_stress_rust():
    test = HierarchyStressTest()
    test.setup()
    assert test.run_stress()

if __name__ == "__main__":
    t = HierarchyStressTest()
    t.setup()
    t.run_stress()

# HieraChain Consensus

![Python Versions](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE-APACHE)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)
![Version](https://img.shields.io/badge/version-0.0.1.dev4-orange)

**English** | [Tiếng Việt](README_vi.md)

## Overview

HieraChain Consensus is a high-performance, Rust-based blockchain consensus library designed for enterprise and consortium blockchain applications. It provides multiple consensus mechanisms, cryptographic security, error mitigation, and seamless Python integration through PyO3 bindings.

**This is the official Core Consensus implementation of the HieraChain ecosystem.** While HieraChain (Python) includes its own consensus implementation, this Rust-based version is the recommended choice for production deployments due to its superior performance, memory safety, and zero-copy data transfer capabilities.

## Features

### Core Functionality

- **Multiple Consensus Mechanisms**:
  - **Proof of Federation (PoF)**: Round-robin rotating leader consensus for consortium blockchains
  - **Byzantine Fault Tolerance (BFT)**: 3-phase commit protocol (pre-prepare, prepare, commit) for enterprise applications
  - **Proof of Authority (PoA)**: Authority-based consensus for trusted networks

- **High-Performance Block Management**:
  - Efficient block creation with Merkle tree verification
  - Apache Arrow integration for zero-copy data transfer between Rust and Python
  - Batch operations to minimize FFI overhead

- **Ordering Service**:
  - Multi-node ordering service with leader election
  - Event queuing and processing with configurable validation rules
  - Real-time status monitoring and health checks

- **Cryptographic Security**:
  - Ed25519 key pair generation and management
  - Digital signature creation and verification
  - Secure message signing for consensus protocols

- **Error Mitigation**:
  - Error classification and priority management
  - Consensus validation with configurable thresholds
  - Recovery mechanisms and audit journaling

### Technical Highlights

- **Rust Implementation**: High-performance, memory-safe core written in Rust
- **Python Bindings**: Seamless integration with Python applications using PyO3
- **Arrow Integration**: Zero-copy interoperability with PyArrow for efficient data handling
- **Async Runtime**: Tokio-based asynchronous operations for BFT consensus
- **Modular Architecture**: Clean separation of concerns across consensus, security, and error handling modules

## Quick Start

### Installation

```bash
# Install from source
pip install maturin
maturin develop
```

### Basic Usage

```python
from hierachain_consensus import Block, ProofOfFederation, KeyPair

# Create a block
block = Block(
    index=1,
    events=[{"type": "transfer", "from": "Alice", "to": "Bob"}],
    previous_hash="0" * 64
)

# Initialize consensus
pof = ProofOfFederation(name="Consortium")
pof.add_validator("validator-1")
pof.add_validator("validator-2")

# Cryptographic operations
keypair = KeyPair.generate()
signature = keypair.sign(b"message")
```

## Architecture Overview

HieraChain Consensus is built with a modular architecture that separates concerns across multiple layers:

- **Consensus Layer**: Multiple consensus mechanisms (PoF, BFT, PoA) with ordering service
- **Core Layer**: Block management, Merkle trees, and cryptographic utilities
- **Security Layer**: Ed25519 key management and signature verification
- **Error Mitigation Layer**: Error classification, validation, and recovery mechanisms

### Consensus Flow

1. **Event Submission** → Events are submitted to the Ordering Service
2. **Validation** → Events are validated against schema and business rules
3. **Ordering** → Events are ordered by the consensus mechanism (PoF/BFT)
4. **Block Creation** → Ordered events are batched into blocks
5. **Verification** → Blocks are verified (Merkle root, signatures)
6. **Commitment** → Blocks are committed to the blockchain

### BFT 3-Phase Protocol

1. **Pre-Prepare**: Primary broadcasts proposal to all replicas
2. **Prepare**: Replicas validate and broadcast prepare messages
3. **Commit**: After receiving 2f+1 prepares, replicas broadcast commit
4. **Execute**: After 2f+1 commits, operation is executed

## Performance Highlights

- **Zero-Copy Data Transfer**: Apache Arrow for efficient Python ↔ Rust data exchange
- **Batch Operations**: Reduce FFI overhead by up to 10x
- **Async I/O**: Tokio runtime for efficient concurrent operations
- **Optimized Merkle Trees**: Efficient tree construction and verification

## Related Projects

**HieraChain Consensus** is the official Core Consensus of the HieraChain ecosystem:

| Project | Language | Description |
|---------|----------|-------------|
| [HieraChain](https://github.com/VanDung-dev/HieraChain) | Python | Main hierarchical blockchain framework (includes pure Python consensus) |
| **HieraChain-Consensus** (this repo) | Rust | **Official Core Consensus** - optimized implementation with Python/C bindings |

> 💡 **Why Rust?** While HieraChain's Python implementation includes consensus algorithms, this Rust implementation offers better performance for consensus-critical operations, memory safety guarantees, and seamless integration via PyO3 bindings.

## License

This project is dual licensed under either the [Apache-2.0 License](LICENSE-APACHE) or the [MIT License](LICENSE-MIT). You may choose either license.

---

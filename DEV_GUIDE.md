# HieraChain Consensus Developer Guide

This guide contains all the information developers need to work with the **HieraChain Consensus** library, a high-performance Rust-based consensus engine with Python bindings.

---

## Installation

### Prerequisites

- **Rust**: Latest stable version ([Install Rust](https://rustup.rs/))
- **Python**: Version 3.10, 3.11, 3.12, or 3.13
- **Maturin**: Build tool for Rust-Python extensions

### Setting Up the Environment

1. Create and activate a virtual environment (recommended):

    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # macOS/Linux
    source .venv/bin/activate
    ```

2. Install `maturin`:

    ```bash
    pip install maturin
    ```

### Building and Installing

To compile the Rust code and install it as a Python package in your current environment:

- **Build**:

    ```bash
    maturin develop --release
    ```

---

## Project Structure

The project is organized as follows:

- `src/`: Core Rust source code for consensus algorithms (PoF, PoA, BFT), cryptography, and block management.
- `benchmark/`: Python scripts for benchmarking the consensus mechanisms.
- `Cargo.toml`: Rust package configuration and dependencies.
- `pyproject.toml`: Python package configuration.

---

## Using the Package

After installation, you can import components from the `hierachain_consensus` package in Python:

```python
from hierachain_consensus import Block, ProofOfFederation, KeyPair, ProofOfAuthority, BFTConsensus

# Example: Generating a keypair
keypair = KeyPair.generate()
print(f"Public Key: {keypair.public_key_hex()}")
```

---

## Development Workflow

### Code Style & Linting

Before submitting code, ensure it meets the project's quality standards.

- **Format Rust code**:

    ```bash
    cargo fmt
    ```

- **Run Clippy (Linter)**:

    ```bash
    cargo clippy
    ```

### Running Tests

The core logic is written in Rust and should be tested using Cargo.

- **Run all Rust Unit Tests**:

    ```bash
    cargo test
    ```

- **Run specific test**:

    ```bash
    cargo test tests::test_name
    ```

---

## Running Benchmarks

Performance is a key feature of HieraChain Consensus. Several benchmark scripts are provided in the `benchmark/` directory to evaluate different consensus mechanisms.

Ensure you have installed the package in **release mode** (`maturin develop --release`) for accurate results.

### Available Benchmarks

- **Proof of Federation (PoF)**:

    ```bash
    python benchmark/pof_benchmark.py
    ```

- **Proof of Authority (PoA)**:

    ```bash
    python benchmark/poa_benchmark.py
    ```

- **Byzantine Fault Tolerance (BFT)**:

    ```bash
    python benchmark/bft_benchmark.py
    ```

- **Ordering Service**:

    ```bash
    python benchmark/ordering_service_benchmark.py
    ```

### Visualization

Some benchmarks may generate plots or data files. Ensure you have the necessary Python libraries installed (e.g., `matplotlib`, `pandas`) if the scripts require them:

```bash
pip install matplotlib pandas
```

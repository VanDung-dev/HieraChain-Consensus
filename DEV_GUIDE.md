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

---

## Docker Stress Testing

Run stress tests in Docker containers with 4 HieraChain nodes (1 CPU, 1GiB RAM each):

- Build and run stress tests with HTML report:

    ```bash
    docker compose -f docker/docker-compose.test.yml --profile stress-test run stress-tester python -m pytest tests/stress/ -v --html=/app/log/report/stress_test_report.html --self-contained-html
    ```

- Run real network stress tests (sends actual HTTP requests to nodes):

    ```bash
    docker compose -f docker/docker-compose.test.yml --profile stress-test run stress-tester python -m pytest tests/stress/test_real_network.py -v -s
    ```

- Run without HTML report:

    ```bash
    docker compose -f docker/docker-compose.test.yml --profile stress-test run stress-tester
    ```

- Stop and clean up containers:

    ```bash
    docker compose -f docker/docker-compose.test.yml down --remove-orphans
    ```

Reports are saved to `log/report/` directory.

---

## Kubernetes Stress Testing

Run stress tests in Kubernetes
> **Recommendation:** Use Docker Compose for local dev. Use Kubernetes when you need a production-like environment.

**Quick Start:**

```bash
# Build image & deploy
docker build -t hierachain:latest -f docker/Dockerfile .
kubectl apply -k docker/k8s/

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app=hierachain -n hierachain --timeout=120s

# Expose the API to local host
kubectl port-forward service/hierachain-api 32661:2661 -n hierachain --address 0.0.0.0 &

# Test API
curl http://localhost:32661/api/v1/health

# Run stress test
docker compose -f docker/docker-compose.k8s-stress.yml --profile stress-test run stress-tester python -m pytest tests/stress/ -v --html=/app/log/report/stress_test_report.html --self-contained-html

# Cleanup
kubectl delete -k docker/k8s/
```

### High Performance Stress Testing (Recommended)

To avoid network bottlenecks from `kubectl port-forward`, run the stress tests directly inside the cluster as a Kubernetes Job:

1. **Ensure Docker image includes tests** (rebuild if needed):

    ```bash
    # Dockerfile must include: COPY tests/ ./tests/
    docker build -t hierachain:latest -f docker/Dockerfile .
    kind load docker-image hierachain:latest --name hiera-cluster
    ```

2. **Deploy the Stress Tester Job**:

    ```bash
    kubectl delete job stress-tester -n hierachain --ignore-not-found
    kubectl apply -f docker/k8s/stress-tester-job.yaml
    ```

3. **View Results**:

    ```bash
    kubectl logs -f job/stress-tester -n hierachain
    ```

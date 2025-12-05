# HieraChain Consensus (In the development stage)

HieraChain Consensus is a Rust-based blockchain consensus library that provides core consensus mechanisms for the HieraChain blockchain platform. It implements consensus algorithms, node management, and message handling with Python bindings for easy integration.

This repository contains the consensus component of the larger [HieraChain project](https://github.com/VanDung-dev/HieraChain).

## Features

- **Rust Implementation**: High-performance consensus algorithms written in Rust
- **Python Bindings**: Seamless integration with Python applications using PyO3
- **Modular Design**: Well-structured modules for consensus, nodes, and messaging
- **JSON Interoperability**: Built-in support for converting between Python objects and JSON

## Installation

### Prerequisites

- Rust toolchain (latest stable version)
- Python 3.10 or higher
- pip

### Installing Python Package

```bash
pip install maturin
maturin develop
```

## Development

### Running Tests

```bash
cargo test
```

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Rust](https://www.rust-lang.org/)
- Python bindings powered by [PyO3](https://pyo3.rs/)
- JSON handling with [serde](https://serde.rs/)
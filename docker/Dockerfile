# HieraChain Docker Image
# For testing and deployment

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# 1. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Setup Virtual Environment (Required for maturin develop)
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# 3. Install Python Dependencies
COPY requirements.txt requirements_py.txt ./
# Note: pip install will now install into the venv because of PATH
RUN pip install --no-cache-dir -r requirements.txt -r requirements_py.txt

# 4. Install Rust Toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# 5. Build and Install hierachain-consensus
RUN pip install maturin

COPY pyproject.toml Cargo.toml Cargo.lock README.md ./
COPY hierachain-consensus/ ./hierachain-consensus/

# Build and install the Rust extension using maturin develop --release as requested
# This will install the package into the active virtual environment (/opt/venv)
RUN maturin develop --release

# 6. Copy HieraChain Python Source
COPY hierachain/ ./hierachain/

# 7. Setup Entrypoint
COPY docker/hrc /usr/local/bin/hrc
RUN sed -i 's/\r$//' /usr/local/bin/hrc && chmod +x /usr/local/bin/hrc

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV LOG_LEVEL=INFO
ENV PYTHONPATH="/app:${PYTHONPATH}"

EXPOSE 5000 8000

CMD ["hrc", "start", "--host", "0.0.0.0", "--port", "2661"]

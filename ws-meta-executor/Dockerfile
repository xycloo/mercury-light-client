# Dockerfile for the mini-tplus binary, part of the tplus-core workspace.
# Build context: Root of the tplus-core repository.

# ---- Build Stage ----
# Uses Rust 1.86, consistent with the project's toolchain.
FROM rust:1.86 AS builder

WORKDIR /usr/src/app

# Copy workspace definition and lockfile first to leverage Docker cache.
COPY Cargo.toml Cargo.lock ./

COPY execution-handler/ ./execution-handler/
COPY ws-meta-executor/ ./ws-meta-executor/

# The -p flag specifies the package within the workspace.
RUN cargo build --release -p inner-onclose-handler

# ---- Runtime Stage ----
# Use Debian Bookworm (slim variant) as the base, which includes OpenSSL 3.x.
FROM debian:bookworm-slim

# Install essential runtime dependencies:
# - libssl3: OpenSSL libraries.
# - ca-certificates: For HTTPS communication.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates && \
    # Clean up apt cache to reduce image size.
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/app/target/release/inner-onclose-handler /usr/local/bin/inner-onclose-handler

# For runtime configuration, mount a volume or use Kubernetes ConfigMaps. Leaving for debugging.
# COPY config /app/config

ENV RUST_LOG="debug"

CMD ["/usr/local/bin/inner-onclose-handler"]

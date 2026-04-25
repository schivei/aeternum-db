# Multi-stage build for AeternumDB
FROM rust:1.85-slim as builder

WORKDIR /build

# Copy core engine
COPY core/ ./core/

# Build the core
WORKDIR /build/core
RUN cargo build --release

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /build/core/target/release/aeternumdb /usr/local/bin/

# Create data directory
RUN mkdir -p /var/lib/aeternumdb

# Expose default port
EXPOSE 5432

# Set default command
CMD ["aeternumdb", "--lite"]

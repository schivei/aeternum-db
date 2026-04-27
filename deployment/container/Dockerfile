# Multi-stage build for AeternumDB
FROM rust:slim AS builder

WORKDIR /build

# Upgrade base packages in builder to apply latest security patches
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Copy core engine
COPY core/ ./core/

# Build the core
WORKDIR /build/core
RUN cargo build --release

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies and apply all available security patches
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /build/core/target/release/aeternumdb /usr/local/bin/

# Create a non-root user and data directory
RUN groupadd -r aeternumdb && useradd -r -g aeternumdb aeternumdb \
    && mkdir -p /var/lib/aeternumdb \
    && chown -R aeternumdb:aeternumdb /var/lib/aeternumdb

# Expose default port
EXPOSE 5432

# Run as non-root user
USER aeternumdb

# Set default command
CMD ["aeternumdb", "--lite"]

# Multi-stage build for AeternumDB
FROM rust:alpine3.21 AS builder

WORKDIR /build

# Install build dependencies (musl headers for linking)
RUN apk add --no-cache musl-dev

# Copy core engine
COPY core/ ./core/

# Build the core
WORKDIR /build/core
RUN cargo build --release

# Final stage – Alpine has far fewer CVEs than Debian-based images
FROM alpine:3.21

# Install runtime dependencies (ca-certificates, musl libc already included)
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /build/core/target/release/aeternumdb /usr/local/bin/

# Create a non-root user and data directory
RUN addgroup -S aeternumdb && adduser -S -G aeternumdb aeternumdb \
    && mkdir -p /var/lib/aeternumdb \
    && chown -R aeternumdb:aeternumdb /var/lib/aeternumdb

# Expose default port
EXPOSE 5432

# Run as non-root user
USER aeternumdb

# Set default command
CMD ["aeternumdb", "--lite"]

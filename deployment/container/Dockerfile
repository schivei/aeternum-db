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
FROM alpine:3.23

# Upgrade all pre-installed base packages to their latest patched versions
# (fixes CVEs in busybox, libcrypto3/libssl3, musl, etc. that ship in the base image)
# then install the only runtime dependency we need
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /build/core/target/release/aeternumdb /usr/local/bin/

# Create a non-root user and data directory
RUN addgroup -S -g 1000 aeternumdb && adduser -S -D -H -u 1000 -G aeternumdb aeternumdb \
    && mkdir -p /var/lib/aeternumdb \
    && chown -R 1000:1000 /var/lib/aeternumdb

# Expose default port
EXPOSE 5432

# Run as non-root user
USER aeternumdb

# Set default command
CMD ["aeternumdb", "--lite"]

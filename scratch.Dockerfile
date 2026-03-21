# Multi-stage build for a "scratch" image with glibc dependencies
# Stage 1: Build environment (Ubuntu)
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libssl-dev \
    zlib1g-dev \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Makefile .
COPY src/ src/

# Build and create chroot directory with all dependencies included
RUN make chroot

# Stage 2: Scratch image
FROM scratch

# Set library search path to the flattened lib directory
ENV LD_LIBRARY_PATH=/usr/lib

# Copy the constructed root filesystem
COPY --from=builder /app/build/chroot /

# Expose the default port
EXPOSE 6432

# Set the entrypoint
ENTRYPOINT ["/pg_gateway"]

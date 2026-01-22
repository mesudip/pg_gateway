# Multi-stage build for pg_gateway
# Stage 1: Build environment
FROM debian:bookworm-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    make \
    pkg-config \
    libpq-dev \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source files
COPY Makefile .
COPY src/ src/

# Build the application
RUN make all

# Stage 2: Runtime environment
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /bin/false pggateway

WORKDIR /app

# Copy the built binary from builder stage
COPY --from=builder /app/build/pg_gateway /usr/local/bin/pg_gateway

# Set default environment variables
ENV CANDIDATES=""
ENV PGUSER="postgres"
ENV PGPASSWORD=""
ENV PGDATABASE="postgres"
ENV LISTEN_HOST="::"
ENV LISTEN_PORT="5432"
ENV METRICS_HOST="::"
ENV METRICS_PORT="9090"

# Run as non-root user
USER pggateway

# Expose the gateway port and metrics port
EXPOSE 5432 9090

# Start the load balancer (reads LISTEN_HOST and LISTEN_PORT from env)
ENTRYPOINT ["/usr/local/bin/pg_gateway"]
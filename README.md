# pg_gateway

**High-performance PostgreSQL load balancer for intelligent primary routing**

`pg_gateway` is a lightweight TCP proxy that automatically routes connections to the current PostgreSQL primary in a replicated cluster. Built in C with zero-copy forwarding (using `splice` and `epoll` on Linux), it adds minimal latency while ensuring your applications always connect to the write-capable node.

> **Note**: This project relies on Linux-specific features (`epoll`, `splice`). For macOS/Windows users, please run via Docker.
> ** Warning**: This Project is still in initial phase, and not yet ready for production
---

## Quick Start

### Using Docker

**Pull from GitHub Container Registry:**
```bash
docker pull ghcr.io/mesudip/pg_gateway:latest
```

**Run with minimal configuration:**
```bash
docker run -d \
  --net=host \
  -e CANDIDATES="10.0.0.10:5432,10.0.0.11:5432" \
  -e PGUSER=postgres \ # used for healthcheck 
  -e PGPASSWORD=secret \
  -e LISTEN_HOST=0.0.0.0 \
  -e LISTEN_PORT=6432 \
  ghcr.io/mesudip/pg_gateway:latest
```
*(Note: `--net=host` is recommended for performance, but port mapping `-p 6432:6432` works too)*

---

## Configuration

Configuration is controlled via environment variables.

### Core Routing

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `CANDIDATES` | **Required.** Comma-separated list of backend PostgreSQL addresses. | - | `pg1:5432,pg2:5432` |
| `LISTEN_HOST` | Interface address to bind the load balancer to. | `localhost` | `0.0.0.0` |
| `LISTEN_PORT` | Port to accept client connections on. | `5432` | `6432` |

### Health Checks

The gateway continuously polls candidates to find the primary. It uses standard `libpq` environment variables for authentication.

| Variable | Description | Default |
|----------|-------------|---------|
| `PGUSER` | Username for health check connections. | - |
| `PGPASSWORD` | Password for health check connections. | - |
| `PGDATABASE` | Database name to connect to. | `postgres` |
| `CONNECT_TIMEOUT_MS` | Timeout for health check probes (milliseconds). | `800` |

### Performance & Internals

| Variable | Description | Default |
|----------|-------------|---------|
| `NUM_THREADS` | Number of worker threads for connection handling. | `1` |
| `TCP_KEEPALIVE` | Enable TCP keepalives (1=On, 0=Off). | `1` (On) |
| `METRICS_HOST` | Host to bind the Prometheus metrics server. | `::` |
| `METRICS_PORT` | Port for Prometheus metrics. | `9090` |

---

## Features

*   **Zero-Copy Forwarding**: Leverages Linux `splice()` to move data between sockets without copying to user-space, maximizing throughput and minimizing CPU usage.
*   **Event-Driven**: Uses `epoll` for efficient, non-blocking I/O handling of thousands of concurrent connections.
*   **Multi-Threaded**: Distributes client connections across multiple worker threads (configurable via `NUM_THREADS`).
*   **Smart Routing**: Automatically detects the new primary during failovers and updates routing tables instantly.
*   **Observability**: Built-in Prometheus exporter at `/metrics` (default port 9090) exposing connection counts, throughput, and backend health.

---

## Development & Building

Since this project uses Linux-specific APIs, development on macOS requires a containerized environment or VM.

### Build with Docker

```bash
docker build -t pg_gateway .
```

### Build from Source (Linux)

**Requirements:**
- Linux Kernel 2.6.35+
- `gcc`, `make`, `pkg-config`
- `libpq-dev`

```bash
# Build dynamic binary
make all

# Build static binary (useful for distroless containers)
make static

# Run tests (requires Python 3 & pytest)
make test
```

## License

MIT License – see [LICENSE](LICENSE) for details.

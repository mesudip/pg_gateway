# Developer Guide

Complete guide for contributing to `pg_gateway`, running tests, benchmarks, and understanding the codebase.

---

## Table of Contents
- [Prerequisites](#prerequisites)
- [Development Setup](#development-setup)
- [Building from Source](#building-from-source)
- [Testing](#testing)
- [Benchmarking](#benchmarking)
- [CI/CD Workflows](#cicd-workflows)
- [Project Structure](#project-structure)
- [Contributing](#contributing)

---

## Prerequisites

- **Docker** + **Docker Compose** (for Patroni test cluster)
- **Python 3.11+** (tested on 3.13)
- **Make** and **GCC** (for local C builds)
- **libpq-dev** (PostgreSQL client library headers)

---

## Development Setup

### 1. Clone and Setup Virtual Environment
```bash
git clone https://github.com/<owner>/pg_gateway.git
cd pg_gateway

# Create and activate venv
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Build Docker Image
```bash
docker build -t pg-gateway:latest .
```

The multi-stage Dockerfile:
- **Builder stage**: Compiles `src/gateway.c` using the `Makefile` with `libpq` linkage
- **Runtime stage**: Minimal Debian image with `libpq5` and `netcat-openbsd`

### 3. Start Patroni Test Cluster
```bash
docker compose -f docker-compose-patroni.yml up -d --build
```

Cluster components:
- 3 etcd nodes (built from official Patroni repo)
- 3 Patroni PostgreSQL nodes (ports 5433, 5434, 5435)
- HAProxy (primary: 5000, replica: 5001)
- pg_gateway (port 6432)

Wait 30–90 seconds for cluster health. Verify with:
```bash
docker compose -f docker-compose-patroni.yml ps
```

---

## Building from Source

### Local C Build (without Docker)
```bash
# Install libpq development headers
# Ubuntu/Debian: sudo apt-get install libpq-dev pkg-config
# macOS: brew install libpq pkg-config

# Build
make all

# Run locally
./build/pg_gateway 0.0.0.0 6432
```

The gateway uses:
- `epoll` for event-driven I/O
- `splice` for zero-copy TCP forwarding
- `libpq` for PostgreSQL health checks
- Atomic operations for lock-free primary updates

---

## Testing

### Test Infrastructure Overview

The test infrastructure automatically manages the entire environment:
- **Patroni Repo**: Auto-cloned from GitHub on first run (added to .gitignore)
- **Docker Build**: Patroni image built locally from cloned repo
- **Cluster Setup**: 3-node etcd cluster + 3 Patroni PostgreSQL nodes
- **Health Checks**: Automatic cluster readiness validation (15-90s wait)
- **Cleanup**: Automatic teardown after tests

**Just run `pytest tests/` and it handles everything automatically.**

### Test Structure
```
tests/
├── conftest.py            # Pytest fixtures (cluster mgmt, connections)
├── test_connection.py     # Connection pooling, CRUD, transactions
├── test_failover.py       # Primary failover scenarios
└── test_benchmarks.py     # Performance benchmarks (gateway vs primary)
```

### Fixture System

**Core Fixtures (conftest.py):**

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `patroni_cluster` | session | Manages cluster lifecycle (clone, build, start, stop) |
| `gateway_connection_params` | session | Connection params to gateway (port 6432) |
| `patroni_hosts` | session | List of Patroni node (host, port, api_port) tuples |
| `patroni_credentials` | session | Credentials dict (user, password, database) |
| `db_connection` | function | Per-test connection through gateway |
| `test_database` | session | Test database creation/cleanup |
| `test_db_connection` | function | Per-test connection to test database |

### Fixture Auto-Setup Flow

```
pytest runs
  ↓
patroni_cluster fixture initializes
  ↓
1. Clone Patroni repo (if not exists):
   git clone https://github.com/patroni/patroni.git ./patroni-repo
  ↓
2. Build Docker image:
   docker build -t patroni ./patroni-repo
  ↓
3. Start Docker Compose:
   docker compose -f docker-compose-patroni.yml up -d --build
   - Starts: etcd1, etcd2, etcd3 (DCS)
   - Starts: patroni1, patroni2, patroni3 (PostgreSQL HA)
  ↓
4. Wait for cluster health:
   - Wait minimum 15s for initialization
   - Poll health check until healthy or 90s timeout
   - Connects via HAProxy primary (port 5000)
  ↓
Tests run
  ↓
Cleanup: docker compose down -v
```

### Automatic Cluster Configuration

**Patroni Nodes:**
- patroni1: PostgreSQL port 5433, REST API port 8008
- patroni2: PostgreSQL port 5434, REST API port 8009
- patroni3: PostgreSQL port 5435, REST API port 8010

**Access Endpoints:**
- **Gateway** (under test): localhost:6432
- **HAProxy Primary**: localhost:5000
- **HAProxy Replica**: localhost:5001
- **Direct Patroni nodes**: localhost:5433, 5434, 5435
- **Patroni REST APIs**: localhost:8008, 8009, 8010

**Database Credentials:**
- User: `postgres`
- Password: `postgres`
- Database: `postgres` (test database: `test_pg_gateway`)
├── test_failover.py       # Primary failover scenarios
└── test_benchmarks.py     # Performance benchmarks (gateway vs primary)
```

### Run All Tests
```bash
bash scripts/run_tests.sh all
```

### Run Specific Test Suites
```bash
# Connection tests only
bash scripts/run_tests.sh connection

# Failover tests only
bash scripts/run_tests.sh failover

# Quick tests (skip slow)
bash scripts/run_tests.sh quick
```

### Manual Test Execution
```bash
# All tests with HTML report
pytest tests/ \
  --html=reports/test_report.html \
  --self-contained-html \
  -v

# Connection tests only
pytest tests/test_connection.py -v

# Failover tests (marked slow)
pytest tests/test_failover.py -v -m failover
```

### Test Reports
- HTML reports generated in `reports/` directory
- CI publishes to GitHub Pages: `https://<owner>.github.io/<repo>/tests/`

---

## Benchmarking

### Run Benchmarks
```bash
# Using helper script (recommended)
bash scripts/run_tests.sh benchmark

# Manual execution
pytest tests/test_benchmarks.py \
  --benchmark-enable \
  --benchmark-autosave \
  --benchmark-save-data \
  --benchmark-json=reports/benchmark.json \
  --html=reports/benchmark_report.html \
  --self-contained-html \
  -v -m benchmark

# Generate tabular comparison (ms units)
python scripts/generate_benchmark_report.py \
  reports/benchmark.json \
  reports/benchmark_table.html
```

### Benchmark Categories
- **Connection Benchmarks**: Latency, connection+query
- **Query Benchmarks**: Simple SELECT, NOW(), parameterized, JSON
- **Data Benchmarks**: Insert, select by ID, range queries, aggregates
- **Concurrency Benchmarks**: Concurrent connections and queries
- **Throughput Benchmarks**: Batch queries and inserts
- **Latency Benchmarks**: Round-trip ping, prepared statements

### Understanding Results
All benchmarks run twice:
- `[gateway]` – Through pg_gateway on port 6432
- `[primary]` – Direct connection to primary node

Tabular report (`benchmark_table.html`) shows:
- Times in **milliseconds**
- Min/Max/Mean/Median/Stddev
- Rounds and iterations per test

Expected overhead: **30-70μs** (microseconds) for gateway vs direct.

### Benchmark Reports
- **JSON**: `reports/benchmark.json`
- **pytest-html**: `reports/benchmark_report.html`
- **Tabular (ms)**: `reports/benchmark_table.html`
- CI publishes to: `https://<owner>.github.io/<repo>/benchmark/`

---

## CI/CD Workflows

### Docker Publishing (`.github/workflows/docker-publish.yml`)
**Triggers**: Push to main/master, version tags (v*)

Builds and publishes to GitHub Container Registry:
```
ghcr.io/<owner>/pg_gateway:latest
ghcr.io/<owner>/pg_gateway:main
ghcr.io/<owner>/pg_gateway:v1.0.0
ghcr.io/<owner>/pg_gateway:sha-<commit>
```

### Test Workflow (`.github/workflows/test.yml`)
**Triggers**: Push/PR to main/master

Runs:
- Connection tests
- Failover tests
- Full test suite

Publishes HTML reports to GitHub Pages under `/tests/`.

### Benchmark Workflow (`.github/workflows/benchmark.yml`)
**Triggers**: Push to main/master, weekly (Mondays), manual

Runs:
- Full benchmark suite (gateway vs primary)
- Generates JSON, pytest-html, and tabular reports

Publishes to GitHub Pages under `/benchmark/`.

### Enable GitHub Pages
1. Repo Settings → Pages
2. Source: Deploy from branch
3. Branch: `gh-pages` / root
4. Workflows auto-deploy on push

---

## Project Structure

```
pg_gateway/
├── src/
│   └── gateway.c              # Main TCP proxy code
├── tests/
│   ├── conftest.py            # Pytest fixtures
│   ├── test_connection.py     # Connection tests
│   ├── test_failover.py       # Failover tests
│   └── test_benchmarks.py     # Benchmarks
├── scripts/
│   ├── run_tests.sh           # Test runner script
│   └── generate_benchmark_report.py  # Tabular HTML generator
├── Dockerfile                 # Multi-stage build
├── Makefile                   # C build system
├── docker-compose-patroni.yml # Local test cluster
├── requirements.txt           # Python dependencies
├── pytest.ini                 # Pytest configuration
├── README.md                  # User-facing docs
└── DEVELOPER.md              # This file
```

### Key Files

#### `src/gateway.c`
- Main event loop with `epoll`
- Health check thread using `libpq`
- Connection forwarding with `splice` (zero-copy)
- Primary detection logic

#### `tests/conftest.py`
- `PatroniCluster` class for Docker Compose management
- Fixtures for gateway and primary connections
- Health check logic (HAProxy port 5000)
- Failover trigger methods

#### `Dockerfile`
- Builder: gcc, make, pkg-config, libpq-dev
- Runtime: Debian slim with libpq5, netcat-openbsd
- Entry: `pg_gateway` binary
- Healthcheck: `nc -z localhost $PORT`

---

## Contributing

### Code Style
- **C code**: Linux kernel style (tabs, K&R braces)
- **Python**: PEP 8, use Black formatter
- **Comments**: Explain "why", not "what"

### Pull Request Process
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make changes with clear commit messages
4. Add/update tests for new functionality
5. Run full test suite: `bash scripts/run_tests.sh all`
6. Run benchmarks: `bash scripts/run_tests.sh benchmark`
7. Push and create a Pull Request

### Commit Messages
```
feat: add connection pooling support
fix: handle EPIPE in splice forwarding
test: add parameterized query benchmarks
docs: update benchmark interpretation guide
```

### Running Pre-commit Checks
```bash
# Ensure tests pass
bash scripts/run_tests.sh all

# Check for common issues
docker compose -f docker-compose-patroni.yml config
```

---

## Troubleshooting

### Cluster Won't Start
```bash
# Check logs
docker compose -f docker-compose-patroni.yml logs

# Restart cluster
docker compose -f docker-compose-patroni.yml down -v
docker compose -f docker-compose-patroni.yml up -d --build
```

### Tests Fail with "Cluster not healthy"
- Increase wait time in `conftest.py` (`MAX_HEALTH_WAIT`)
- Check HAProxy is accessible: `curl http://localhost:5000` (should refuse non-PG)
- Verify Patroni nodes: `docker compose -f docker-compose-patroni.yml ps`

### Build Fails with "libpq not found"
```bash
# Ubuntu/Debian
sudo apt-get install libpq-dev pkg-config

# macOS
brew install libpq pkg-config
export PKG_CONFIG_PATH="/opt/homebrew/opt/libpq/lib/pkgconfig"
```

### Gateway Not Detecting Primary
- Check `CANDIDATES` environment variable format: `host1:port1,host2:port2`
- Ensure candidate PostgreSQL nodes are accessible
- Check gateway logs: `docker logs <gateway_container>`

---

## Resources

- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [libpq C Library](https://www.postgresql.org/docs/current/libpq.html)
- [Patroni Documentation](https://patroni.readthedocs.io/)
- [pytest-benchmark](https://pytest-benchmark.readthedocs.io/)

---

## Questions?

Open an issue or start a discussion on GitHub. We're here to help!

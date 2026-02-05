# pg_gateway

**High-performance PostgreSQL load balancer for intelligent primary routing**

`pg_gateway` is a lightweight TCP proxy that automatically routes connections to the current PostgreSQL primary in a replicated cluster. Built in C with zero-copy forwarding, it adds minimal latency while ensuring your applications always connect to the write-capable node.

---


## Quick Start

### Pull from GitHub Container Registry
```bash
docker pull ghcr.io/mesudip/pg_gateway:latest
```

### Run
```bash
docker run -d \
  -p 6432:6432 \
  -e CANDIDATES="pg1:5432,pg2:5432,pg3:5432" \
  ghcr.io/mesudip/pg_gateway:latest 0.0.0.0 6432
```

Connect your application to `localhost:6432` and let `pg_gateway` handle primary routing automatically.

---

## Configuration

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `CANDIDATES` | Comma-separated PostgreSQL hosts:ports | `pg1:5432,pg2:5432,pg3:5432` |

**Command Arguments:**
- `<listen_addr>` – Address to bind (default: `0.0.0.0`)
- `<listen_port>` – Port to listen on (default: `6432`)

---
## Why pg_gateway?

🚀 **Fast** – Built with `epoll` and `splice` for zero-copy TCP proxying  
🎯 **Smart** – Automatically detects and routes to the current primary  
🐳 **Cloud-Native** – Runs as a sidecar or standalone container  
📊 **Battle-Tested** – Comprehensive test suite with failover scenarios  
🔄 **HA-Ready** – Works seamlessly with Patroni, Stolon, and other HA solutions  

### Performance

Independent benchmarks show `pg_gateway` adds **30-70μs** overhead compared to direct connections—negligible for real-world workloads while providing automatic failover handling.

[📊 View Live Benchmarks](https://mesudip.github.io/pg_gateway/benchmark/)

---
## Use Cases

- **Kubernetes/Cloud** – Deploy as a sidecar to abstract primary routing from applications
- **Microservices** – Single connection string for all services, automatic failover handling  
- **CI/CD** – Simplified configuration with dynamic primary detection
- **Legacy Apps** – Add HA capabilities without application changes

---

## How It Works

1. Periodically probes candidate PostgreSQL nodes using `libpq`
2. Identifies the current primary via `pg_is_in_recovery()`
3. Routes all incoming connections to the primary
4. Sends PostgreSQL error packets if no primary is available

---

## Documentation

- [Developer Guide](DEVELOPER.md) – Build from source, run tests, contribute
- [Test Reports](https://mesudip.github.io/pg_gateway/tests/) – CI test results
- [Benchmarks](https://mesudip.github.io/pg_gateway/benchmark/) – Performance metrics

---

## License

MIT License – see [LICENSE](LICENSE) for details.

---

## Contributing

Contributions welcome! See [DEVELOPER.md](DEVELOPER.md) for setup instructions, testing guidelines, and development workflows.

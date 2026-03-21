import pytest
import requests
import time
import os

# Define constants here instead of importing from conftest
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "localhost")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "6432"))

METRICS_PORT = 9090
METRICS_URL = f"http://{GATEWAY_HOST}:{METRICS_PORT}/metrics"

class TestMetrics:
    """Test Prometheus metrics endpoint."""

    def get_metrics_response(self):
        response = requests.get(METRICS_URL)
        assert response.status_code == 200
        return response

    def parse_metrics_response(self, response):
        metrics = {}
        lag_series = {}

        for line in response.text.splitlines():
            if line.startswith("pg_gateway_replica_lag_seconds{"):
                metric, value = line.split()
                backend = metric.split('backend="', 1)[1].split('"', 1)[0]
                lag_series[backend] = float(value)
                continue

            if line.startswith("#") or not line.strip():
                continue

            parts = line.split()
            if len(parts) >= 2:
                metrics[parts[0]] = float(parts[1])

        return metrics, lag_series

    def get_metrics(self):
        """Fetch and parse metrics."""
        metrics, _ = self.parse_metrics_response(self.get_metrics_response())
        return metrics

    def get_replica_lag_series(self):
        _, lag_series = self.parse_metrics_response(self.get_metrics_response())
        return lag_series

    def test_when_metrics_endpoint_is_accessed_then_it_is_reachable(self, patroni_cluster):
        """Test that metrics endpoint is up and returns 200 OK."""
        response = requests.get(METRICS_URL)
        assert response.status_code == 200
        assert "text/plain" in response.headers["Content-Type"]

    def test_when_multiple_connections_opened_then_active_count_increments(self):
        """Test strict accuracy of connection metrics."""
        # Get baseline
        m_base = self.get_metrics()
        base_active = m_base.get("pg_gateway_connections_active", 0)
        
        conns = []
        target_count = 5
        try:
            # Open multiple connections
            import psycopg2
            for _ in range(target_count):
                c = psycopg2.connect(
                    host=GATEWAY_HOST,
                    port=GATEWAY_PORT,
                    user="postgres",
                    password="postgres",
                    database="postgres"
                )
                conns.append(c)
            
            # Allow metric update
            time.sleep(0.5)
            
            m_new = self.get_metrics()
            new_active = m_new.get("pg_gateway_connections_active", 0)
            
            # Check exact increase
            # Note: other tests might be running or connections lingering, assuming isolated run here
            assert new_active >= base_active + target_count
            
        finally:
            for c in conns:
                c.close()
                
        # Wait for cleanup
        time.sleep(0.5)
        m_final = self.get_metrics()
        final_active = m_final.get("pg_gateway_connections_active", 0)
        
        # Check return to baseline (approximate, as other things might connect)
        assert final_active < new_active

    def test_when_server_metrics_are_checked_then_counts_are_valid(self, patroni_cluster):
        """Test that server counts are sanity checkable."""
        metrics = self.get_metrics()
        
        # Check existence of keys
        assert "pg_gateway_servers_total" in metrics
        assert "pg_gateway_servers_healthy" in metrics
        assert "pg_gateway_servers_unhealthy" in metrics
        
        total = metrics["pg_gateway_servers_total"]
        healthy = metrics["pg_gateway_servers_healthy"]
        unhealthy = metrics["pg_gateway_servers_unhealthy"]
        
        # Based on docker-compose, we have 3 patroni nodes
        assert total == 3.0
        
        # At least one should be healthy (the primary) if the cluster is up
        assert healthy > 0
        assert unhealthy == total - healthy

    def test_when_metrics_are_checked_then_replica_lag_is_exposed(self, patroni_cluster):
        """Test that replica lag gauge is exported for replica backends."""
        lag_series = self.get_replica_lag_series()
        assert lag_series, "Expected replica lag metrics for at least one replica"

        expected_backends = {"patroni1:5432", "patroni2:5432", "patroni3:5432"}
        assert set(lag_series).issubset(expected_backends)
        assert len(lag_series) == len(set(lag_series))

        for lag_seconds in lag_series.values():
            assert lag_seconds >= 0.0

    def test_when_metrics_are_scraped_then_lag_series_match_the_reported_snapshot(self, patroni_cluster):
        """Test that one scrape exposes a coherent snapshot of counts and backend lag labels."""
        metrics, lag_series = self.parse_metrics_response(self.get_metrics_response())

        total = metrics["pg_gateway_servers_total"]
        healthy = metrics["pg_gateway_servers_healthy"]

        assert len(lag_series) <= total
        assert len(lag_series) <= healthy
        assert all(backend in {"patroni1:5432", "patroni2:5432", "patroni3:5432"} for backend in lag_series)

    def test_when_node_stops_then_healthy_server_count_decreases(self):
        """Test that server health metrics update when a node goes down and comes back up."""
        import subprocess

        # Ensure all nodes are running first to avoid flake from previous tests
        containers = ["patroni1", "patroni2", "patroni3"]
        for c in containers:
             subprocess.run(["docker", "start", c], check=False, capture_output=True)
        
        # Wait for cluster to stabilize (expecting 3 healthy)
        start_wait = time.time()
        while time.time() - start_wait < 30:
            m = self.get_metrics()
            if m.get("pg_gateway_servers_healthy", 0) >= 3:
                break
            time.sleep(1)

        # We'll stop one of the patroni nodes and verify the healthy count drops
        # We choose patroni3 assuming it's likely a replica or at least one of 3
        container_to_stop = "patroni3"
        
        m_base = self.get_metrics()
        healthy_base = m_base.get("pg_gateway_servers_healthy", 0)
        unhealthy_base = m_base.get("pg_gateway_servers_unhealthy", 0)
        
        # Guard clause: if we still don't have enough healthy nodes to test this effectively
        if healthy_base == 0:
            pytest.skip("Cluster has no healthy nodes, cannot test count decrease")
            
        try:
            print(f"Stopping {container_to_stop}...")
            subprocess.run(["docker", "stop", container_to_stop], check=True, capture_output=True)
            
            # Wait for pg_gateway health check (default 2s) + some buffer
            time.sleep(4)
            
            m_new = self.get_metrics()
            healthy_new = m_new.get("pg_gateway_servers_healthy", 0)
            unhealthy_new = m_new.get("pg_gateway_servers_unhealthy", 0)
            lag_series_new = self.get_replica_lag_series()
            
            # Verify shift from healthy to unhealthy
            # Ensure we don't go below 0
            expected_healthy = max(0, healthy_base - 1)
            
            assert healthy_new == expected_healthy
            assert unhealthy_new == unhealthy_base + 1
            assert "patroni3:5432" not in lag_series_new
            
        finally:
            # Restore the cluster state
            print(f"Starting {container_to_stop}...")
            subprocess.run(["docker", "start", container_to_stop], check=True, capture_output=True)
            # Give it time to become healthy again
            time.sleep(15)
            
            m_final = self.get_metrics()
            healthy_final = m_final.get("pg_gateway_servers_healthy", 0)
            lag_series_final = self.get_replica_lag_series()
            
            # We only check if it recovered at least partially
            # It might not reach full 3 immediately if leadership changed
            assert healthy_final > healthy_new
            assert len(lag_series_final) > len(lag_series_new)

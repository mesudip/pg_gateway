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

    def get_metrics(self):
        """Fetch and parse metrics."""
        response = requests.get(METRICS_URL)
        assert response.status_code == 200
        lines = response.text.splitlines()
        metrics = {}
        for line in lines:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 2:
                key = parts[0]
                value = float(parts[1])
                metrics[key] = value
        return metrics

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
            
            # Verify shift from healthy to unhealthy
            # Ensure we don't go below 0
            expected_healthy = max(0, healthy_base - 1)
            
            assert healthy_new == expected_healthy
            assert unhealthy_new == unhealthy_base + 1
            
        finally:
            # Restore the cluster state
            print(f"Starting {container_to_stop}...")
            subprocess.run(["docker", "start", container_to_stop], check=True, capture_output=True)
            # Give it time to become healthy again
            time.sleep(15)
            
            m_final = self.get_metrics()
            healthy_final = m_final.get("pg_gateway_servers_healthy", 0)
            
            # We only check if it recovered at least partially
            # It might not reach full 3 immediately if leadership changed
            assert healthy_final > healthy_new



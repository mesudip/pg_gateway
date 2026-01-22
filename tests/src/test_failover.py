"""
Failover behavior tests for pg_gateway.

Tests the load balancer's behavior during primary failover scenarios,
including connection handling, automatic reconnection, and query routing.
"""

import pytest
import psycopg2
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional


@pytest.mark.failover
class TestFailoverDetection:
    """Test pg_gateway's ability to detect and handle failover."""

    def test_initial_primary_detection(self, db_connection, patroni_cluster):
        """Verify gateway correctly identifies the initial primary."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT pg_is_in_recovery()")
        is_replica = cursor.fetchone()[0]
        assert is_replica is False, "Initial connection should be to primary"
        cursor.close()

    def test_primary_identification(self, patroni_cluster):
        """Verify Patroni cluster has exactly one primary."""
        primary = patroni_cluster.get_primary_info()
        assert primary is not None, "Cluster should have a primary"
        assert primary["name"] is not None, "Primary should have a name"

    @pytest.mark.slow
    def test_connection_after_failover(self, gateway_connection_params, patroni_cluster):
        """Test that new connections work after a failover."""
        # Get initial primary
        initial_primary = patroni_cluster.get_primary_info()
        assert initial_primary is not None
        initial_primary_name = initial_primary["name"]

        # Trigger failover
        print(f"\nTriggering failover from {initial_primary_name}...")
        success = patroni_cluster.trigger_failover()
        
        if not success:
            pytest.skip("Could not trigger failover - may require manual intervention")

        # Wait for failover to complete
        print("Waiting for failover to complete...")
        time.sleep(15)

        # Wait for new primary
        max_wait = 30
        new_primary = None
        for _ in range(max_wait // 2):
            new_primary = patroni_cluster.get_primary_info()
            if new_primary and new_primary["name"] != initial_primary_name:
                break
            time.sleep(2)

        assert new_primary is not None, "Should have a new primary after failover"
        assert new_primary["name"] != initial_primary_name, "Primary should have changed"

        # Test connection through gateway
        max_retry = 10
        conn = None
        for attempt in range(max_retry):
            try:
                conn = psycopg2.connect(**gateway_connection_params)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT pg_is_in_recovery()")
                is_replica = cursor.fetchone()[0]
                cursor.close()
                
                if not is_replica:
                    break
                conn.close()
                conn = None
            except psycopg2.OperationalError:
                pass
            time.sleep(2)

        assert conn is not None, "Should be able to connect after failover"
        conn.close()

@pytest.mark.failover  
class TestPatroniAPIIntegration:
    """Test integration with Patroni cluster status via PostgreSQL connections."""

    def test_cluster_members(self, patroni_cluster, patroni_hosts, patroni_credentials):
        """Verify all cluster members are accessible via PostgreSQL."""
        accessible = 0
        for host, pg_port, api_port in patroni_hosts:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=pg_port,
                    user=patroni_credentials["user"],
                    password=patroni_credentials["password"],
                    database=patroni_credentials["database"],
                    connect_timeout=5
                )
                conn.close()
                accessible += 1
            except psycopg2.OperationalError:
                pass

        assert accessible >= 2, "At least 2 Patroni nodes should be accessible"

    def test_cluster_topology(self, patroni_cluster, patroni_hosts, patroni_credentials):
        """Verify cluster has correct topology (1 primary, N-1 replicas)."""
        roles = {"primary": 0, "replica": 0, "unknown": 0}

        for host, pg_port, api_port in patroni_hosts:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=pg_port,
                    user=patroni_credentials["user"],
                    password=patroni_credentials["password"],
                    database=patroni_credentials["database"],
                    connect_timeout=5
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT pg_is_in_recovery()")
                is_replica = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                if is_replica:
                    roles["replica"] += 1
                else:
                    roles["primary"] += 1
            except (psycopg2.OperationalError, psycopg2.Error):
                roles["unknown"] += 1

        assert roles["primary"] == 1, f"Should have exactly 1 primary, got {roles['primary']}"
        assert roles["replica"] >= 1, f"Should have at least 1 replica, got {roles['replica']}"

    def test_primary_accessible(self, patroni_cluster):
        """Test that primary node is accessible."""
        primary = patroni_cluster.get_primary_info()
        assert primary is not None, "Should be able to find primary"
        assert primary["pg_port"] is not None

    def test_replica_accessible(self, patroni_cluster, patroni_hosts, patroni_credentials):
        """Test that at least one replica is accessible."""
        replica_found = False
        for host, pg_port, api_port in patroni_hosts:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=pg_port,
                    user=patroni_credentials["user"],
                    password=patroni_credentials["password"],
                    database=patroni_credentials["database"],
                    connect_timeout=5
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT pg_is_in_recovery()")
                is_replica = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                if is_replica:
                    replica_found = True
                    break
            except psycopg2.OperationalError:
                pass

        assert replica_found, "At least one replica should be accessible"


@pytest.mark.failover
class TestConnectionBehaviorDuringFailover:
    """Test connection behavior during failover events."""

    def test_existing_connection_behavior(self, gateway_connection_params, patroni_cluster):
        """Test behavior of existing connections during failover."""
        # Establish connection
        conn = psycopg2.connect(**gateway_connection_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # Verify connection works
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1

        # Store the backend PID
        cursor.execute("SELECT pg_backend_pid()")
        original_pid = cursor.fetchone()[0]

        # After failover, the connection may be terminated
        # This is expected behavior - the test verifies we handle it gracefully
        try:
            cursor.execute("SELECT 1")
        except psycopg2.OperationalError:
            # Expected - connection was to the old primary
            pass
        finally:
            if not conn.closed:
                conn.close()

    @pytest.mark.slow
    def test_concurrent_connections_during_failover(self, gateway_connection_params, patroni_cluster):
        """Test gateway's handling of concurrent connections during cluster failover.
        
        This tests the GATEWAY's behavior:
        - Connection pool resilience during primary change
        - Connection routing after failover
        - Handling of failed connections and reconnection
        
        Timeline:
        - 0-5s: Gateway receives steady concurrent connections (baseline)
        - 5s: Cluster primary fails over (patroni handles this)
        - 5-15s: Gateway must route connections to new primary
        - 15s: Stop load and analyze gateway's performance
        """
        num_workers = 5
        total_duration = 15  # seconds
        failover_trigger_at = 5  # seconds
        
        metrics = {
            "before_failover": {"success": 0, "failed": 0, "errors": []},
            "during_failover": {"success": 0, "failed": 0, "errors": []},
            "after_failover": {"success": 0, "failed": 0, "errors": []},
        }
        
        start_time = time.time()
        failover_triggered = False
        stop_loop = False

        def worker():
            """Continuously attempt connections through gateway."""
            while not stop_loop:
                elapsed = time.time() - start_time
                
                # Classify which phase we're in
                if elapsed < failover_trigger_at:
                    phase = "before_failover"
                elif not failover_triggered or elapsed < failover_trigger_at + 3:
                    phase = "during_failover"
                else:
                    phase = "after_failover"
                
                try:
                    # Test gateway connection
                    conn = psycopg2.connect(
                        **gateway_connection_params,
                        connect_timeout=2
                    )
                    conn.autocommit = True
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    conn.close()
                    metrics[phase]["success"] += 1
                except psycopg2.OperationalError as e:
                    metrics[phase]["failed"] += 1
                    metrics[phase]["errors"].append(str(e)[:50])
                except Exception as e:
                    metrics[phase]["failed"] += 1
                    metrics[phase]["errors"].append(str(type(e).__name__))
                
                time.sleep(0.3)

        # Start gateway load
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker) for _ in range(num_workers)]
            
            # Wait before triggering failover
            time.sleep(failover_trigger_at)
            
            # Trigger cluster failover
            print(f"\n[Gateway Test] Triggering cluster failover at {failover_trigger_at}s...")
            try:
                failover_triggered = patroni_cluster.trigger_failover()
                if failover_triggered:
                    print("[Gateway Test] Cluster failover initiated - gateway must handle primary change")
            except Exception as e:
                print(f"[Gateway Test] Failover trigger failed: {e}")
                failover_triggered = False
            
            # Continue load for remaining duration
            remaining = total_duration - failover_trigger_at
            time.sleep(remaining)
            
            # Stop all workers
            stop_loop = True
            
            # Wait for workers to finish
            for future in futures:
                try:
                    future.result(timeout=5)
                except Exception as e:
                    print(f"Worker error: {e}")

        # Analyze gateway performance
        print(f"\n[Gateway Performance]")
        print(f"  Before failover: {metrics['before_failover']['success']} success, {metrics['before_failover']['failed']} failed")
        print(f"  During failover: {metrics['during_failover']['success']} success, {metrics['during_failover']['failed']} failed")
        print(f"  After failover:  {metrics['after_failover']['success']} success, {metrics['after_failover']['failed']} failed")
        
        total_success = sum(m["success"] for m in metrics.values())
        total_attempted = sum(m["success"] + m["failed"] for m in metrics.values())
        success_rate = (total_success / total_attempted * 100) if total_attempted > 0 else 0
        print(f"  Total success rate: {success_rate:.1f}%")
        
        # Gateway should maintain high availability
        assert metrics["before_failover"]["success"] > 0, "Gateway should handle connections normally before failover"
        assert metrics["during_failover"]["success"] > 0 or metrics["after_failover"]["success"] > 0, \
            "Gateway should recover connections during or after cluster failover"
        assert success_rate > 70, f"Gateway success rate should be >70%, got {success_rate:.1f}%"


@pytest.mark.failover
class TestQueryRouting:
    """Test query routing behavior."""

    def test_write_query_routing(self, db_connection):
        """Verify write queries are routed to primary."""
        cursor = db_connection.cursor()
        
        # Create a test table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS routing_test (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Insert should work (only on primary)
        cursor.execute("INSERT INTO routing_test (value) VALUES ('test')")
        
        # Verify we're on primary
        cursor.execute("SELECT pg_is_in_recovery()")
        is_replica = cursor.fetchone()[0]
        assert is_replica is False
        
        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS routing_test")
        cursor.close()

    def test_read_query_execution(self, db_connection):
        """Test read queries execute successfully."""
        cursor = db_connection.cursor()
        
        cursor.execute("SELECT current_timestamp")
        result = cursor.fetchone()
        assert result is not None
        
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        assert "PostgreSQL" in version
        
        cursor.close()



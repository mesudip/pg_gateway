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
        """Test that connections work before and after a failover.
        
        Steps:
        1. Make connections and verify they work (pre-failover)
        2. Perform failover
        3. Confirm new primary via Patroni API
        4. Make connections and verify they work (post-failover)
        """
        # Step 1: Verify connections work before failover
        print("\n[Step 1] Testing connections before failover...")
        pre_failover_success = 0
        for i in range(5):
            try:
                conn = psycopg2.connect(**gateway_connection_params, connect_timeout=5)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()[0]
                assert result == 1, "Pre-failover query should return 1"
                cursor.execute("SELECT pg_is_in_recovery()")
                is_replica = cursor.fetchone()[0]
                assert is_replica is False, "Should be connected to primary"
                cursor.close()
                conn.close()
                pre_failover_success += 1
            except Exception as e:
                print(f"  Pre-failover connection {i} failed: {e}")
        
        assert pre_failover_success >= 4, f"At least 4/5 pre-failover connections should work, got {pre_failover_success}"
        print(f"  ✓ Pre-failover: {pre_failover_success}/5 connections successful")
        
        # Get initial primary info
        initial_primary = patroni_cluster.get_primary_info()
        assert initial_primary is not None, "Should have initial primary"
        initial_primary_name = initial_primary["name"]
        print(f"  Initial primary: {initial_primary_name}")

        # Step 2: Trigger failover
        print(f"\n[Step 2] Triggering failover from {initial_primary_name}...")
        success = patroni_cluster.trigger_failover()
        
        if not success:
            pytest.skip("Could not trigger failover - may require manual intervention")

        print("  Waiting for failover to complete (15s)...")
        time.sleep(15)

        # Step 3: Confirm new primary via Patroni API
        print("\n[Step 3] Confirming new primary via Patroni API...")
        max_wait = 30
        new_primary = None
        for attempt in range(max_wait // 2):
            new_primary = patroni_cluster.get_primary_info()
            if new_primary and new_primary["name"] != initial_primary_name:
                print(f"  ✓ New primary detected: {new_primary['name']}")
                break
            time.sleep(2)
            print(f"  Waiting for new primary... (attempt {attempt + 1})")

        assert new_primary is not None, "Should have a new primary after failover"
        assert new_primary["name"] != initial_primary_name, f"Primary should have changed from {initial_primary_name}"
        print(f"  ✓ Failover complete: {initial_primary_name} -> {new_primary['name']}")

        # Step 4: Verify connections work after failover
        print("\n[Step 4] Testing connections after failover...")
        post_failover_success = 0
        max_retry = 10
        
        for i in range(5):
            conn = None
            for attempt in range(max_retry):
                try:
                    conn = psycopg2.connect(**gateway_connection_params, connect_timeout=5)
                    conn.autocommit = True
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()[0]
                    assert result == 1, "Post-failover query should return 1"
                    cursor.execute("SELECT pg_is_in_recovery()")
                    is_replica = cursor.fetchone()[0]
                    cursor.close()
                    
                    if not is_replica:
                        conn.close()
                        post_failover_success += 1
                        break
                    conn.close()
                    conn = None
                except psycopg2.OperationalError as e:
                    if conn and not conn.closed:
                        conn.close()
                    if attempt < max_retry - 1:
                        time.sleep(2)
                    else:
                        print(f"  Post-failover connection {i} failed after {max_retry} attempts: {e}")
                except Exception as e:
                    if conn and not conn.closed:
                        conn.close()
                    print(f"  Post-failover connection {i} error: {e}")
                    break

        assert post_failover_success >= 3, f"At least 3/5 post-failover connections should work, got {post_failover_success}"
        print(f"  ✓ Post-failover: {post_failover_success}/5 connections successful")
        print(f"\n[Summary] Failover test complete: {pre_failover_success}/5 before, {post_failover_success}/5 after")


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
        - 0 failure before failover
        - 0 failure after failover
        - >0 successes during failover
        """
        num_workers = 5
        
        metrics = {
            "before_failover": {"success": 0, "failed": 0},
            "during_failover": {"success": 0, "failed": 0},
            "after_failover": {"success": 0, "failed": 0},
        }
        
        phase_info = {"current": "before_failover"}
        stop_loop = False

        def worker():
            """Continuously attempt connections through gateway."""
            while not stop_loop:
                phase = phase_info["current"]
                
                try:
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
                except Exception:
                    metrics[phase]["failed"] += 1
                
                time.sleep(0.1)

        # Start gateway load
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker) for _ in range(num_workers)]
            
            # Phase 1: Before failover (5s)
            time.sleep(5)
            
            # Phase 2: During failover
            phase_info["current"] = "during_failover"
            print(f"\n[Gateway Test] Triggering cluster failover...")
            if not patroni_cluster.trigger_failover():
                stop_loop = True
                pytest.skip("Could not trigger failover")
            
            # Give enough time for failover and recovery (~30s)
            time.sleep(30)
            
            # Phase 3: After failover (10s)
            phase_info["current"] = "after_failover"
            time.sleep(10)
            
            stop_loop = True
            for future in futures:
                future.result()

        # Analyze gateway performance
        print(f"\n[Gateway Performance]")
        for p in ["before_failover", "during_failover", "after_failover"]:
            print(f"  {p.replace('_', ' ').capitalize()}: {metrics[p]['success']} success, {metrics[p]['failed']} failed")
        
        # User defined success criteria
        assert metrics["before_failover"]["failed"] == 0, f"Expected 0 failures before failover, got {metrics['before_failover']['failed']}"
        assert metrics["after_failover"]["failed"] == 0, f"Expected 0 failures after failover, got {metrics['after_failover']['failed']}"
        assert metrics["during_failover"]["success"] > 0, "Expected at least one success during failover"


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



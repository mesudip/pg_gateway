"""
Pytest fixtures for pg_gateway testing with Patroni cluster.

This module provides fixtures to:
- Start/stop Docker Compose Patroni cluster
- Wait for cluster health (10-40 seconds)
- Provide database connections via pg_gateway
- Manage test database lifecycle
"""

import os
import time
import subprocess
import pytest
import psycopg2
import requests
import docker
from pathlib import Path
from contextlib import contextmanager
from typing import Generator, Optional

# Configuration
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE_FILE = os.path.join(TESTS_DIR, "docker-compose-patroni.yml")
PATRONI_REPO_URL = "https://github.com/patroni/patroni.git"
PATRONI_REPO_DIR = os.path.join(PROJECT_ROOT, "patroni-repo")
PATRONI_IMAGE_NAME = "patroni"

# Connection settings
GATEWAY_HOST = "localhost"
GATEWAY_PORT = 6432
HAPROXY_PRIMARY_PORT = 5000  # HAProxy primary endpoint
HAPROXY_REPLICA_PORT = 5001  # HAProxy replica endpoint
PATRONI_HOSTS = [
    ("localhost", 5433, 8008),  # patroni1: pg_port, api_port
    ("localhost", 5434, 8009),  # patroni2
    ("localhost", 5435, 8010),  # patroni3
]
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DATABASE = "postgres"
TEST_DATABASE = "test_pg_gateway"

# Timing
MIN_HEALTH_WAIT = 15  # seconds
MAX_HEALTH_WAIT = 90  # seconds
HEALTH_CHECK_INTERVAL = 3  # seconds


class PatroniCluster:
    """Helper class to manage Patroni cluster operations."""

    def __init__(self):
        self.docker_client = docker.from_env()
        self._compose_started = False

    def _ensure_patroni_repo(self) -> bool:
        """Clone Patroni repo if it doesn't exist."""
        if os.path.exists(PATRONI_REPO_DIR):
            print(f"  ✓ Patroni repo exists at {PATRONI_REPO_DIR}")
            return True
        
        print(f"  Cloning Patroni repo from {PATRONI_REPO_URL}...")
        try:
            result = subprocess.run(
                ["git", "clone", PATRONI_REPO_URL, PATRONI_REPO_DIR],
                check=True,
                capture_output=True,
                text=True,
                timeout=120
            )
            print(f"  ✓ Cloned Patroni repo to {PATRONI_REPO_DIR}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Failed to clone: {e.stderr}")
            return False
        except subprocess.TimeoutExpired:
            print(f"  ✗ Clone timed out")
            return False

    def _build_patroni_image(self) -> bool:
        """Build the Patroni Docker image from the repo."""
        print(f"  Building Docker image '{PATRONI_IMAGE_NAME}' from {PATRONI_REPO_DIR}...")
        try:
            result = subprocess.run(
                ["docker", "build", "-t", PATRONI_IMAGE_NAME, "."],
                check=True,
                capture_output=True,
                text=True,
                cwd=PATRONI_REPO_DIR,
                timeout=600
            )
            print(f"  ✓ Built Docker image '{PATRONI_IMAGE_NAME}'")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Build failed: {e.stderr[-500:]}")  # Last 500 chars
            return False
        except subprocess.TimeoutExpired:
            print(f"  ✗ Build timed out (600s)")
            return False

    def start(self) -> bool:
        """Start the Docker Compose stack."""
        # Step 1: Ensure Patroni repo is cloned
        print(f"[1] Ensuring Patroni repo at {PATRONI_REPO_DIR}...")
        if not self._ensure_patroni_repo():
            print(f"[ERROR] Failed to clone/verify Patroni repo")
            return False
        
        # Step 2: Build the Patroni image
        print(f"[2] Building Patroni Docker image '{PATRONI_IMAGE_NAME}'...")
        if not self._build_patroni_image():
            print(f"[ERROR] Failed to build Patroni image")
            return False
        
        # Step 3: Start Docker Compose
        print(f"[3] Starting Docker Compose with: {COMPOSE_FILE}")
        print(f"    Working directory: {PROJECT_ROOT}")
        try:
            result = subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--build"],
                check=True,
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT,
            )
            print(f"[✓] Docker Compose started successfully")
            self._compose_started = True
            return True
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Failed to start Docker Compose")
            print(f"  stdout: {e.stdout}")
            print(f"  stderr: {e.stderr}")
            return False

    def stop(self):
        """Stop the Docker Compose stack and save gateway logs."""
        if self._compose_started:
            # Save gateway logs before stopping
            try:
                Path(REPORTS_DIR).mkdir(parents=True, exist_ok=True)
                log_file = os.path.join(REPORTS_DIR, "gateway.log")
                result = subprocess.run(
                    ["docker", "logs", "pg_gateway"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                with open(log_file, "w") as f:
                    f.write(result.stdout)
                    if result.stderr:
                        f.write("\n=== STDERR ===\n")
                        f.write(result.stderr)
                print(f"[INFO] Gateway logs saved to {log_file}")
            except Exception as e:
                print(f"[WARN] Failed to save gateway logs: {e}")
            
            # Stop the compose stack
            subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"],
                capture_output=True,
                cwd=PROJECT_ROOT,
            )
            self._compose_started = False

    def wait_for_healthy(self, timeout: int = MAX_HEALTH_WAIT) -> bool:
        """
        Wait for the Patroni cluster to become healthy.
        
        Waits minimum MIN_HEALTH_WAIT seconds, then checks until healthy
        or MAX_HEALTH_WAIT is reached.
        """
        print(f"\nWaiting minimum {MIN_HEALTH_WAIT}s for cluster initialization...")
        time.sleep(MIN_HEALTH_WAIT)

        elapsed = MIN_HEALTH_WAIT
        while elapsed < timeout:
            if self._check_cluster_health():
                print(f"Cluster healthy after {elapsed}s")
                return True
            time.sleep(HEALTH_CHECK_INTERVAL)
            elapsed += HEALTH_CHECK_INTERVAL
            print(f"Waiting for cluster health... ({elapsed}s)")

        print(f"Cluster not healthy after {timeout}s")
        return False

    def _check_cluster_health(self) -> bool:
        """Check if the Patroni cluster has a healthy primary via HAProxy."""
        # Try connecting through HAProxy primary port
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=HAPROXY_PRIMARY_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                database=PG_DATABASE,
                connect_timeout=5
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT pg_is_in_recovery()")
            is_replica = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            # HAProxy primary port should connect to non-recovery (primary) node
            return not is_replica
        except (psycopg2.OperationalError, psycopg2.Error) as e:
            print(f"Health check failed: {e}")
            return False

    def get_primary_info(self) -> Optional[dict]:
        """Get information about the current primary node by checking each Patroni node."""
        for host, pg_port, api_port in PATRONI_HOSTS:
            try:
                # Try to connect to PostgreSQL and check if it's primary
                conn = psycopg2.connect(
                    host=host,
                    port=pg_port,
                    user=PG_USER,
                    password=PG_PASSWORD,
                    database=PG_DATABASE,
                    connect_timeout=3
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT pg_is_in_recovery()")
                is_replica = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                if not is_replica:
                    return {
                        "host": host,
                        "pg_port": pg_port,
                        "api_port": api_port,
                        "name": f"patroni{PATRONI_HOSTS.index((host, pg_port, api_port)) + 1}",
                    }
            except (psycopg2.OperationalError, psycopg2.Error):
                continue
        return None

    def trigger_failover(self, target_node: Optional[str] = None) -> bool:
        """Trigger a failover by stopping the primary container."""
        primary = self.get_primary_info()
        if not primary:
            return False

        try:
            # Stop the primary container to trigger failover
            container_name = primary["name"]
            subprocess.run(
                ["docker", "stop", container_name],
                check=True,
                capture_output=True,
                timeout=30
            )
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return False


@pytest.fixture(scope="session")
def patroni_cluster() -> Generator[PatroniCluster, None, None]:
    """
    Session-scoped fixture that starts the Patroni cluster once for all tests.
    
    The cluster is started at the beginning of the test session and
    stopped after all tests complete.
    """
    cluster = PatroniCluster()

    print("\n" + "=" * 60)
    print("Starting Patroni cluster for testing...")
    print("=" * 60)

    if not cluster.start():
        pytest.fail("Failed to start Patroni cluster")

    if not cluster.wait_for_healthy():
        cluster.stop()
        pytest.fail("Patroni cluster did not become healthy in time")

    yield cluster

    print("\n" + "=" * 60)
    print("Stopping Patroni cluster...")
    print("=" * 60)
    cluster.stop()


@pytest.fixture(scope="session")
def patroni_hosts():
    """Return list of Patroni host configurations."""
    return PATRONI_HOSTS


@pytest.fixture(scope="session")
def patroni_credentials():
    """Return Patroni connection credentials."""
    return {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "database": PG_DATABASE,
    }


@pytest.fixture(scope="session")
def gateway_dsn(patroni_cluster: PatroniCluster) -> str:
    """Return the DSN for connecting through pg_gateway."""
    return f"host={GATEWAY_HOST} port={GATEWAY_PORT} user={PG_USER} password={PG_PASSWORD} dbname={PG_DATABASE}"


@pytest.fixture(scope="session")
def gateway_connection_params(patroni_cluster: PatroniCluster) -> dict:
    """Return connection parameters for psycopg2."""
    return {
        "host": GATEWAY_HOST,
        "port": GATEWAY_PORT,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "database": PG_DATABASE,
    }

@pytest.fixture(scope="session")
def primary_connection_params(patroni_cluster: PatroniCluster) -> dict:
    """Return connection parameters for direct connection to current primary."""
    primary = patroni_cluster.get_primary_info()
    if not primary:
        pytest.fail("Could not determine primary node for direct connection benchmarks")
    return {
        "host": primary["host"],
        "port": primary["pg_port"],
        "user": PG_USER,
        "password": PG_PASSWORD,
        "database": PG_DATABASE,
    }


@pytest.fixture(scope="function")
def db_connection(gateway_connection_params: dict) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Function-scoped fixture providing a database connection through pg_gateway.
    
    Connection is automatically closed after each test.
    """
    conn = None
    try:
        conn = psycopg2.connect(**gateway_connection_params)
        conn.autocommit = True
        yield conn
    finally:
        if conn and not conn.closed:
            conn.close()


@pytest.fixture(scope="session")
def test_database(gateway_connection_params: dict) -> Generator[str, None, None]:
    """
    Session-scoped fixture that creates a test database.
    
    Creates the database at start and drops it at the end.
    """
    conn = psycopg2.connect(**gateway_connection_params)
    conn.autocommit = True
    cursor = conn.cursor()

    # Drop if exists and create fresh
    cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE}")
    cursor.execute(f"CREATE DATABASE {TEST_DATABASE}")
    cursor.close()
    conn.close()

    yield TEST_DATABASE

    # Cleanup
    conn = psycopg2.connect(**gateway_connection_params)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE}")
    cursor.close()
    conn.close()


@pytest.fixture(scope="function")
def test_db_connection(
    gateway_connection_params: dict, test_database: str
) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Function-scoped fixture providing a connection to the test database.
    """
    params = gateway_connection_params.copy()
    params["database"] = test_database
    conn = None
    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        yield conn
    finally:
        if conn and not conn.closed:
            conn.close()


@contextmanager
def connection_pool(params: dict, size: int = 10):
    """Context manager for creating a pool of connections."""
    connections = []
    try:
        for _ in range(size):
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            connections.append(conn)
        yield connections
    finally:
        for conn in connections:
            if conn and not conn.closed:
                conn.close()


# Pytest configuration
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "failover: marks tests that involve failover scenarios"
    )
    config.addinivalue_line(
        "markers", "benchmark: marks benchmark tests"
    )


def pytest_html_report_title(report):
    """Customize HTML report title."""
    report.title = "pg_gateway Test Report"

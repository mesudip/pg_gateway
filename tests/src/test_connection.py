"""
Connection behavior tests for pg_gateway.

Tests basic connection functionality, connection pooling,
and various connection scenarios through the load balancer.
"""

import pytest
import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestBasicConnection:
    """Test basic connection functionality through pg_gateway."""

    def test_simple_connection(self, db_connection):
        """Test that a simple connection can be established."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        cursor.close()

    def test_connection_to_primary(self, db_connection):
        """Verify connection is routed to the primary node."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT pg_is_in_recovery()")
        is_replica = cursor.fetchone()[0]
        assert is_replica is False, "Connection should be to primary (not in recovery)"
        cursor.close()

    def test_server_version(self, db_connection):
        """Test retrieving server version through gateway."""
        cursor = db_connection.cursor()
        cursor.execute("SHOW server_version")
        version = cursor.fetchone()[0]
        assert version is not None
        # Patroni uses PostgreSQL 16 or 17
        assert any(v in version for v in ["16", "17"])
        cursor.close()

    def test_current_database(self, db_connection):
        """Test that we're connected to the correct database."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT current_database()")
        db_name = cursor.fetchone()[0]
        assert db_name == "postgres"
        cursor.close()

    def test_connection_user(self, db_connection):
        """Test that we're connected as the correct user."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT current_user")
        user = cursor.fetchone()[0]
        assert user == "postgres"
        cursor.close()


class TestConnectionPool:
    """Test connection pooling behavior through pg_gateway."""

    def test_multiple_sequential_connections(self, gateway_connection_params):
        """Test opening multiple connections sequentially."""
        connections = []
        try:
            for i in range(10):
                conn = psycopg2.connect(**gateway_connection_params)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                assert cursor.fetchone()[0] == 1
                cursor.close()
                connections.append(conn)
        finally:
            for conn in connections:
                conn.close()

    def test_concurrent_connections(self, gateway_connection_params):
        """Test opening multiple connections concurrently."""
        num_connections = 20
        results = []

        def connect_and_query():
            conn = psycopg2.connect(**gateway_connection_params)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT pg_backend_pid()")
            pid = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return pid

        with ThreadPoolExecutor(max_workers=num_connections) as executor:
            futures = [executor.submit(connect_and_query) for _ in range(num_connections)]
            for future in as_completed(futures):
                results.append(future.result())

        # All connections should have gotten unique backend PIDs
        assert len(results) == num_connections
        # PIDs should be integers
        assert all(isinstance(pid, int) for pid in results)

    def test_connection_reuse(self, db_connection):
        """Test that a single connection can be reused for multiple queries."""
        cursor = db_connection.cursor()
        
        for i in range(100):
            cursor.execute(f"SELECT {i}")
            assert cursor.fetchone()[0] == i
        
        cursor.close()


class TestConnectionResilience:
    """Test connection resilience and error handling."""

    def test_reconnect_after_close(self, gateway_connection_params):
        """Test that we can reconnect after closing a connection."""
        conn1 = psycopg2.connect(**gateway_connection_params)
        cursor1 = conn1.cursor()
        cursor1.execute("SELECT 1")
        cursor1.close()
        conn1.close()

        # Should be able to connect again
        conn2 = psycopg2.connect(**gateway_connection_params)
        cursor2 = conn2.cursor()
        cursor2.execute("SELECT 1")
        assert cursor2.fetchone()[0] == 1
        cursor2.close()
        conn2.close()

    def test_rapid_connect_disconnect(self, gateway_connection_params):
        """Test rapid connection/disconnection cycles."""
        for _ in range(50):
            conn = psycopg2.connect(**gateway_connection_params)
            conn.close()

    @pytest.mark.timeout(30)
    def test_connection_timeout_handling(self):
        """Test connection to invalid host times out properly."""
        with pytest.raises((psycopg2.OperationalError, OSError)):
            psycopg2.connect(
                host="localhost",
                port=59999,  # Non-existent port
                user="postgres",
                password="postgres",
                database="postgres",
                connect_timeout=3,
            )


class TestDataOperations:
    """Test data operations through pg_gateway."""

    def test_create_table(self, test_db_connection):
        """Test creating a table through the gateway."""
        cursor = test_db_connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cursor.execute("SELECT to_regclass('test_table')")
        assert cursor.fetchone()[0] is not None
        cursor.close()

    def test_insert_and_select(self, test_db_connection):
        """Test inserting and selecting data."""
        cursor = test_db_connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insert_test (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Insert data
        cursor.execute("INSERT INTO insert_test (value) VALUES (%s) RETURNING id", ("test_value",))
        inserted_id = cursor.fetchone()[0]
        
        # Select data
        cursor.execute("SELECT value FROM insert_test WHERE id = %s", (inserted_id,))
        value = cursor.fetchone()[0]
        
        assert value == "test_value"
        cursor.close()

    def test_transaction(self, test_db_connection):
        """Test transaction handling through the gateway."""
        test_db_connection.autocommit = False
        cursor = test_db_connection.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_test (
                    id SERIAL PRIMARY KEY,
                    value INT
                )
            """)
            test_db_connection.commit()
            
            # Start transaction
            cursor.execute("INSERT INTO transaction_test (value) VALUES (1)")
            cursor.execute("INSERT INTO transaction_test (value) VALUES (2)")
            test_db_connection.commit()
            
            cursor.execute("SELECT SUM(value) FROM transaction_test")
            total = cursor.fetchone()[0]
            assert total == 3
            
        finally:
            cursor.close()
            test_db_connection.rollback()  # Ensure no active transaction
            test_db_connection.autocommit = True

    def test_rollback(self, test_db_connection):
        """Test rollback functionality."""
        test_db_connection.autocommit = False
        cursor = test_db_connection.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rollback_test (
                    id SERIAL PRIMARY KEY,
                    value INT
                )
            """)
            test_db_connection.commit()
            
            cursor.execute("INSERT INTO rollback_test (value) VALUES (100)")
            test_db_connection.rollback()
            
            cursor.execute("SELECT COUNT(*) FROM rollback_test")
            count = cursor.fetchone()[0]
            assert count == 0
            
        finally:
            cursor.close()
            test_db_connection.rollback()  # Ensure no active transaction
            test_db_connection.autocommit = True


class TestLargeData:
    """Test handling of large data through pg_gateway."""

    def test_large_result_set(self, test_db_connection):
        """Test retrieving a large result set."""
        cursor = test_db_connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS large_data (
                id SERIAL PRIMARY KEY,
                data TEXT
            )
        """)
        
        # Insert many rows
        cursor.execute("""
            INSERT INTO large_data (data)
            SELECT md5(random()::text)
            FROM generate_series(1, 1000)
        """)
        
        cursor.execute("SELECT COUNT(*) FROM large_data")
        count = cursor.fetchone()[0]
        assert count == 1000
        
        # Fetch all rows
        cursor.execute("SELECT * FROM large_data")
        rows = cursor.fetchall()
        assert len(rows) == 1000
        
        cursor.close()

    def test_large_insert(self, test_db_connection):
        """Test inserting large text data."""
        cursor = test_db_connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS large_text (
                id SERIAL PRIMARY KEY,
                content TEXT
            )
        """)
        
        # Insert 1MB of text
        large_text = "x" * (1024 * 1024)
        cursor.execute("INSERT INTO large_text (content) VALUES (%s) RETURNING id", (large_text,))
        inserted_id = cursor.fetchone()[0]
        
        cursor.execute("SELECT LENGTH(content) FROM large_text WHERE id = %s", (inserted_id,))
        length = cursor.fetchone()[0]
        assert length == 1024 * 1024
        
        cursor.close()

"""
Benchmark tests for pg_gateway.

Uses pytest-benchmark to measure performance metrics and generates
HTML reports via pytest-html.
"""

import pytest
import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager


@pytest.mark.benchmark
class TestConnectionBenchmarks:
    """Benchmark connection-related operations."""

    @pytest.mark.parametrize("conn_params,conn_label", [
        pytest.param("gateway_connection_params", "gateway", id="gateway"),
        pytest.param("primary_connection_params", "primary", id="primary"),
    ])
    def test_connection_latency(self, benchmark, request, conn_params, conn_label):
        """Benchmark: Time to establish a new connection."""
        params = request.getfixturevalue(conn_params)
        def connect():
            conn = psycopg2.connect(**params)
            conn.close()
            return True

        result = benchmark(connect)
        assert result is True

    @pytest.mark.parametrize("conn_params,conn_label", [
        pytest.param("gateway_connection_params", "gateway", id="gateway"),
        pytest.param("primary_connection_params", "primary", id="primary"),
    ])
    def test_connection_with_query(self, benchmark, request, conn_params, conn_label):
        """Benchmark: Connection + simple query + disconnect."""
        params = request.getfixturevalue(conn_params)
        def connect_query_close():
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True

        result = benchmark(connect_query_close)
        assert result is True


@pytest.mark.benchmark
class TestQueryBenchmarks:
    """Benchmark query execution performance."""

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_simple_select(self, benchmark, request, conn_params):
        """Benchmark: Simple SELECT query."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def simple_query():
            cursor.execute("SELECT 1")
            return cursor.fetchone()[0]

        result = benchmark(simple_query)
        assert result == 1
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_select_now(self, benchmark, request, conn_params):
        """Benchmark: SELECT NOW() query."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def now_query():
            cursor.execute("SELECT NOW()")
            return cursor.fetchone()

        result = benchmark(now_query)
        assert result is not None
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_parameterized_query(self, benchmark, request, conn_params):
        """Benchmark: Parameterized query."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def param_query():
            cursor.execute("SELECT %s::int + %s::int", (10, 20))
            return cursor.fetchone()[0]

        result = benchmark(param_query)
        assert result == 30
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_json_query(self, benchmark, request, conn_params):
        """Benchmark: JSON processing query."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def json_query():
            cursor.execute("""
                SELECT jsonb_build_object(
                    'id', 1,
                    'name', 'test',
                    'values', jsonb_build_array(1, 2, 3)
                )
            """)
            return cursor.fetchone()[0]

        result = benchmark(json_query)
        assert result["id"] == 1
        cursor.close()
        conn.close()


@pytest.mark.benchmark
class TestDataBenchmarks:
    """Benchmark data operations."""

    @pytest.fixture(autouse=True)
    def setup_benchmark_table(self, test_db_connection):
        """Create table for benchmarks."""
        cursor = test_db_connection.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS benchmark_data;
            CREATE TABLE benchmark_data (
                id SERIAL PRIMARY KEY,
                value INT,
                text_data TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            -- Pre-populate with some data
            INSERT INTO benchmark_data (value, text_data)
            SELECT 
                (random() * 1000)::int,
                md5(random()::text)
            FROM generate_series(1, 1000);
        """)
        cursor.close()
        yield
        cursor = test_db_connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS benchmark_data")
        cursor.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_insert_single(self, benchmark, request, test_database, conn_params):
        """Benchmark: Single row INSERT."""
        params = request.getfixturevalue(conn_params).copy()
        params["database"] = test_database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def insert_one():
            cursor.execute(
                "INSERT INTO benchmark_data (value, text_data) VALUES (%s, %s)",
                (42, "benchmark_test")
            )
            return True

        result = benchmark(insert_one)
        assert result is True
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_select_by_id(self, benchmark, request, test_database, conn_params):
        """Benchmark: SELECT by primary key."""
        params = request.getfixturevalue(conn_params).copy()
        params["database"] = test_database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def select_by_id():
            cursor.execute("SELECT * FROM benchmark_data WHERE id = %s", (500,))
            return cursor.fetchone()

        result = benchmark(select_by_id)
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_select_range(self, benchmark, request, test_database, conn_params):
        """Benchmark: SELECT range of rows."""
        params = request.getfixturevalue(conn_params).copy()
        params["database"] = test_database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def select_range():
            cursor.execute(
                "SELECT * FROM benchmark_data WHERE id BETWEEN %s AND %s",
                (100, 200)
            )
            return cursor.fetchall()

        result = benchmark(select_range)
        assert len(result) > 0
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_aggregate_query(self, benchmark, request, test_database, conn_params):
        """Benchmark: Aggregate query."""
        params = request.getfixturevalue(conn_params).copy()
        params["database"] = test_database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def aggregate():
            cursor.execute("""
                SELECT 
                    COUNT(*) as cnt,
                    AVG(value) as avg_val,
                    MAX(value) as max_val,
                    MIN(value) as min_val
                FROM benchmark_data
            """)
            return cursor.fetchone()

        result = benchmark(aggregate)
        assert result[0] > 0  # count should be positive
        cursor.close()
        conn.close()


@pytest.mark.benchmark
class TestConcurrencyBenchmarks:
    """Benchmark concurrent operations."""

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_concurrent_connections(self, benchmark, request, conn_params):
        """Benchmark: Opening 10 concurrent connections."""
        params = request.getfixturevalue(conn_params)
        def open_concurrent():
            connections = []
            try:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [
                        executor.submit(psycopg2.connect, **params)
                        for _ in range(10)
                    ]
                    connections = [f.result() for f in as_completed(futures)]
                return len(connections)
            finally:
                for conn in connections:
                    if conn and not conn.closed:
                        conn.close()

        result = benchmark(open_concurrent)
        assert result == 10

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_concurrent_queries(self, benchmark, request, conn_params):
        """Benchmark: 10 concurrent queries on separate connections."""
        params = request.getfixturevalue(conn_params)
        def concurrent_queries():
            def query_worker():
                conn = psycopg2.connect(**params)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("SELECT pg_sleep(0.001), 1")  # 1ms sleep + select
                result = cursor.fetchone()[1]
                cursor.close()
                conn.close()
                return result

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(query_worker) for _ in range(10)]
                results = [f.result() for f in as_completed(futures)]
            return sum(results)

        result = benchmark(concurrent_queries)
        assert result == 10


@pytest.mark.benchmark
class TestThroughputBenchmarks:
    """Benchmark throughput scenarios."""

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_query_throughput(self, benchmark, request, conn_params):
        """Benchmark: Queries per iteration (100 queries)."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def batch_queries():
            for i in range(100):
                cursor.execute("SELECT %s", (i,))
                cursor.fetchone()
            return True

        result = benchmark(batch_queries)
        assert result is True
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_insert_throughput(self, benchmark, request, test_database, conn_params):
        """Benchmark: Inserts per iteration (100 inserts)."""
        params = request.getfixturevalue(conn_params).copy()
        params["database"] = test_database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS throughput_test (
                id SERIAL PRIMARY KEY,
                value INT
            )
        """)

        def batch_inserts():
            for i in range(100):
                cursor.execute("INSERT INTO throughput_test (value) VALUES (%s)", (i,))
            return True

        result = benchmark(batch_inserts)
        assert result is True
        cursor.execute("DROP TABLE IF EXISTS throughput_test")
        cursor.close()
        conn.close()


@pytest.mark.benchmark
class TestLatencyBenchmarks:
    """Benchmark latency characteristics."""

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_round_trip_latency(self, benchmark, request, conn_params):
        """Benchmark: Round-trip latency (ping-like)."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()

        def ping():
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True

        # Run with specific iterations for latency measurement
        result = benchmark.pedantic(
            ping,
            iterations=100,
            rounds=10,
        )
        cursor.close()
        conn.close()

    @pytest.mark.parametrize("conn_params", [
        pytest.param("gateway_connection_params", id="gateway"),
        pytest.param("primary_connection_params", id="primary"),
    ])
    def test_prepared_statement_latency(self, benchmark, request, conn_params):
        """Benchmark: Prepared statement execution latency."""
        params = request.getfixturevalue(conn_params)
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()
        # Prepare a statement by executing it once
        cursor.execute("PREPARE bench_stmt AS SELECT $1::int + $2::int")

        def execute_prepared():
            cursor.execute("EXECUTE bench_stmt(10, 20)")
            return cursor.fetchone()[0]

        result = benchmark(execute_prepared)
        assert result == 30
        
        cursor.execute("DEALLOCATE bench_stmt")
        cursor.close()
        conn.close()


# Custom benchmark stats for HTML report
def pytest_benchmark_update_json(config, benchmarks, output_json):
    """Add custom metadata to benchmark results."""
    output_json["metadata"] = {
        "test_suite": "pg_gateway",
        "description": "PostgreSQL Load Balancer Performance Benchmarks",
    }


# Pytest-html hooks for better reporting
def pytest_html_results_summary(prefix, summary, postfix):
    """Add benchmark summary to HTML report."""
    prefix.extend([
        "<h2>Benchmark Summary</h2>",
        "<p>Performance benchmarks for pg_gateway PostgreSQL load balancer.</p>",
        "<p>Lower times indicate better performance.</p>",
    ])

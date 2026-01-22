import psycopg2
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import random
import statistics

# Config
PGPOOL_HOST = 'localhost'
PGPOOL_PORT = 6432
DB_NAME = 'test'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
NUM_REQUESTS = 50   # total requests per thread, performed one by one.
CONCURRENT_TESTS = 50  # number of concurrent threads




# Shared data
insert_durations = []
read_durations = []
read_failures = 0
read_successes = 0
lock = threading.Lock()

def setup_database():
    try:
        conn = psycopg2.connect(
            host=PGPOOL_HOST,
            port=PGPOOL_PORT,
            dbname='postgres',
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True  # Critical: must be set before cursor()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cur.fetchone()
        if not exists:
            print(f"[SETUP] Database '{DB_NAME}' does not exist. Creating...")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"[SETUP] Database '{DB_NAME}' created.")
        else:
            print(f"[SETUP] Database '{DB_NAME}' already exists.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to verify/create database '{DB_NAME}': {e}")
        exit(1)

def create_table():
    try:
        with psycopg2.connect(
            host=PGPOOL_HOST,
            port=PGPOOL_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS test_stress (
                        id SERIAL PRIMARY KEY,
                        data TEXT NOT NULL
                    );
                """)
                conn.commit()
                print("[INIT] Table 'test_stress' is ready.")
    except Exception as e:
        print(f"[ERROR] Failed to create table: {e}")
        exit(1)

def stress_worker(thread_id):
    global read_failures, read_successes
    try:
        conn = psycopg2.connect(
            host=PGPOOL_HOST,
            port=PGPOOL_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        for i in range(NUM_REQUESTS):
            payload = f"thread-{thread_id}-req-{i}-{random.randint(0, 100000)}"
            try:
                # INSERT
                start_insert = time.perf_counter()
                cur.execute("INSERT INTO test_stress (data) VALUES (%s)", (payload,))
                end_insert = time.perf_counter()

                # SELECT
                start_read = time.perf_counter()
                cur.execute("SELECT data FROM test_stress WHERE data = %s", (payload,))
                result = cur.fetchone()
                end_read = time.perf_counter()

                insert_time = end_insert - start_insert
                read_time = end_read - start_read

                with lock:
                    insert_durations.append(insert_time)
                    read_durations.append(read_time)

                    if result and result[0] == payload:
                        read_successes += 1
                        print(f"[OK] Thread {thread_id} Request {i}")
                    else:
                        read_failures += 1
                        print(f"[FAIL] Thread {thread_id} Request {i} -> Inconsistent read")
            except Exception as e:
                with lock:
                    read_failures += 1
                    print(f"[ERROR] Thread {thread_id} Request {i}: {e}")
        cur.close()
        conn.close()
    except Exception as e:
        with lock:
            print(f"[CONN ERROR] Thread {thread_id}: {e}")

def summarize(name, durations):
    if not durations:
        print(f"No data for {name}")
        return

    mean = statistics.mean(durations)
    median = statistics.median(durations)
    stdev = statistics.stdev(durations) if len(durations) > 1 else 0.0
    p99 = sorted(durations)[int(0.99 * len(durations)) - 1]

    print(f"\n{name} Duration Stats (seconds):")
    print(f"  Count      : {len(durations)}")
    print(f"  Mean       : {mean:.6f}")
    print(f"  Median     : {median:.6f}")
    print(f"  Std Dev    : {stdev:.6f}")
    print(f"  99th %tile : {p99:.6f}")

def print_summary():
    total_reads = read_successes + read_failures
    print("\n\U0001F4CA Final Summary:")
    print(f"  Total Reads        : {total_reads}")
    print(f"  Successful Reads   : {read_successes}")
    print(f"  Failed Reads       : {read_failures}")
    success_rate = (read_successes / total_reads) * 100 if total_reads else 0
    print(f"  Success Rate       : {success_rate:.2f}%")

    summarize("INSERT", insert_durations)
    summarize("READ", read_durations)

def main():
    setup_database()
    create_table()

    start = time.time()
    print(f"\n\U0001F680 Starting stress test with {CONCURRENT_TESTS} threads and {NUM_REQUESTS} requests per thread...\n")

    with ThreadPoolExecutor(max_workers=CONCURRENT_TESTS) as executor:
        futures = [executor.submit(stress_worker, tid) for tid in range(CONCURRENT_TESTS)]
        for f in futures:
            f.result()

    total_time = time.time() - start
    print(f"\n✅ Stress test completed in {total_time:.2f} seconds")
    print_summary()

if __name__ == "__main__":
    main()

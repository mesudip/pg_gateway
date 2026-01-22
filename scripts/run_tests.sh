#!/bin/bash
# Script to run pg_gateway tests with various options

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPORTS_DIR="$PROJECT_ROOT/reports"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Create reports directory
mkdir -p "$REPORTS_DIR"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check dependencies
check_dependencies() {
    print_status "Checking dependencies..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker compose &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed"
        exit 1
    fi
    
    print_status "All dependencies found"
}

# Function to install Python dependencies
install_deps() {
    print_status "Installing Python dependencies..."
    pip install -r "$PROJECT_ROOT/requirements.txt"
}

# Function to run all tests
run_all_tests() {
    print_status "Running all tests..."
    cd "$PROJECT_ROOT"
    pytest tests/ \
        --html="$REPORTS_DIR/test_report.html" \
        --self-contained-html \
        -v
}

# Function to run connection tests only
run_connection_tests() {
    print_status "Running connection tests..."
    cd "$PROJECT_ROOT"
    pytest tests/test_connection.py \
        --html="$REPORTS_DIR/connection_report.html" \
        --self-contained-html \
        -v
}

# Function to run failover tests only
run_failover_tests() {
    print_status "Running failover tests..."
    cd "$PROJECT_ROOT"
    pytest tests/test_failover.py \
        --html="$REPORTS_DIR/failover_report.html" \
        --self-contained-html \
        -v -m failover
}

# Function to run benchmarks
run_benchmarks() {
    print_status "Running benchmarks..."
    cd "$PROJECT_ROOT"
    pytest tests/test_benchmarks.py \
        --benchmark-enable \
        --benchmark-autosave \
        --benchmark-save-data \
        --benchmark-json="$REPORTS_DIR/benchmark.json" \
        --html="$REPORTS_DIR/benchmark_report.html" \
        --self-contained-html \
        -v -m benchmark
    # Generate tabular summary HTML from JSON
    if [ -f "$PROJECT_ROOT/.venv/bin/python" ]; then
        "$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/scripts/generate_benchmark_report.py" \
            "$REPORTS_DIR/benchmark.json" "$REPORTS_DIR/benchmark_table.html"
    else
        python3 "$PROJECT_ROOT/scripts/generate_benchmark_report.py" \
            "$REPORTS_DIR/benchmark.json" "$REPORTS_DIR/benchmark_table.html"
    fi
}

# Function to run quick tests (skip slow ones)
run_quick_tests() {
    print_status "Running quick tests (skipping slow tests)..."
    cd "$PROJECT_ROOT"
    pytest tests/ \
        -m "not slow" \
        --html="$REPORTS_DIR/quick_report.html" \
        --self-contained-html \
        -v
}

# Function to start the cluster manually
start_cluster() {
    print_status "Starting Patroni cluster..."
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose-patroni.yml up -d --build
    print_status "Waiting for cluster to be ready..."
    sleep 30
    print_status "Cluster started. Use 'docker compose -f docker-compose-patroni.yml logs -f' to view logs"
}

# Function to stop the cluster
stop_cluster() {
    print_status "Stopping Patroni cluster..."
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose-patroni.yml down -v
    print_status "Cluster stopped"
}

# Function to show cluster status
cluster_status() {
    print_status "Cluster status:"
    docker compose -f "$PROJECT_ROOT/docker-compose-patroni.yml" ps
    
    echo ""
    print_status "Patroni nodes status:"
    for port in 8008 8009 8010; do
        echo -n "  Port $port: "
        curl -s "http://localhost:$port/patroni" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f\"{data.get('patroni', {}).get('name', 'unknown')} - {data.get('role', 'unknown')} - {data.get('state', 'unknown')}\")
except:
    print('not responding')
" || echo "not responding"
    done
}

# Show usage
usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all           Run all tests"
    echo "  connection    Run connection tests only"
    echo "  failover      Run failover tests only"
    echo "  benchmark     Run benchmarks with HTML report"
    echo "  quick         Run quick tests (skip slow)"
    echo "  start         Start the Patroni cluster"
    echo "  stop          Stop the Patroni cluster"
    echo "  status        Show cluster status"
    echo "  install       Install Python dependencies"
    echo "  check         Check system dependencies"
    echo ""
    echo "Examples:"
    echo "  $0 start      # Start cluster"
    echo "  $0 benchmark  # Run benchmarks (default)"
    echo "  $0 all        # Run all tests"
    echo "  $0 stop       # Stop cluster"
}

# Main
case "${1:-benchmark}" in
    all)
        check_dependencies
        run_all_tests
        ;;
    connection)
        check_dependencies
        run_connection_tests
        ;;
    failover)
        check_dependencies
        run_failover_tests
        ;;
    benchmark)
        check_dependencies
        run_benchmarks
        ;;
    quick)
        check_dependencies
        run_quick_tests
        ;;
    start)
        check_dependencies
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    status)
        cluster_status
        ;;
    install)
        install_deps
        ;;
    check)
        check_dependencies
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        print_error "Unknown command: $1"
        usage
        exit 1
        ;;
esac

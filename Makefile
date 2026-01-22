# Makefile for pg_gateway (PostgreSQL TCP Load Balancer)

CC ?= gcc
CFLAGS = -O3 -pthread -Wall -Wextra $(shell pkg-config --cflags libpq 2>/dev/null || echo "-I/usr/include/postgresql")
LDFLAGS = $(shell pkg-config --libs libpq 2>/dev/null || echo "-lpq")
STATIC_LDFLAGS = -static $(shell pkg-config --libs --static libpq 2>/dev/null || echo "-lpq -lssl -lcrypto -lz -lpthread -lm")

SRC_DIR = src
BUILD_DIR = build
TARGET = $(BUILD_DIR)/pg_gateway
TARGET_STATIC = $(BUILD_DIR)/pg_gateway-static

SOURCES = $(SRC_DIR)/gateway.c

.PHONY: all clean install static venv test

all: $(TARGET)

static: $(TARGET_STATIC)

# Virtual environment targets
venv:
	@if [ ! -d ".venv" ]; then \
		echo "Creating virtual environment..."; \
		python3 -m venv .venv; \
		echo "Installing test dependencies..."; \
		.venv/bin/pip install --upgrade pip; \
		.venv/bin/pip install -r tests/requirements.txt; \
		echo "✓ Virtual environment created and dependencies installed"; \
	else \
		echo "✓ Virtual environment already exists at .venv"; \
	fi

# Test targets
test: venv
	@echo "Running tests..."
	cd tests && ../.venv/bin/pytest -c pytest.ini

test-quick: venv
	@echo "Running quick tests (skipping slow)..."
	cd tests && ../.venv/bin/pytest -c pytest.ini -m "not slow"

test-failover: venv
	@echo "Running failover tests..."
	cd tests && ../.venv/bin/pytest -c pytest.ini -m failover

test-benchmark: venv
	@echo "Running benchmarks..."
	cd tests && ../.venv/bin/pytest -c pytest.ini -m benchmark --benchmark-enable

test-report: venv
	@echo "Running tests with HTML report..."
	cd tests && ../.venv/bin/pytest -c pytest.ini \
		--html=../reports/test_report.html \
		--self-contained-html

static: $(TARGET_STATIC)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(TARGET): $(SOURCES) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(TARGET_STATIC): $(SOURCES) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -static -o $@ $< $(STATIC_LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/pg_gateway

# Development targets
debug: CFLAGS = -g -pthread -Wall -Wextra -DDEBUG
debug: clean $(TARGET)

# Run with default settings (for testing)
run: $(TARGET)
	CANDIDATES=localhost:5432 \
	PGUSER=postgres \
	PGPASSWORD=postgres \
	PGDATABASE=postgres \
	$(TARGET) 0.0.0.0 6432

# Makefile for pg_gateway (PostgreSQL TCP Load Balancer)

CC ?= gcc
CFLAGS = -O3 -pthread -Wall -Wextra $(shell pkg-config --cflags libpq 2>/dev/null || echo "-I/usr/include/postgresql")
LDFLAGS = $(shell pkg-config --libs libpq 2>/dev/null || echo "-lpq")
STATIC_LDFLAGS = -static $(shell pkg-config --libs --static libpq 2>/dev/null || echo "-lpq -lssl -lcrypto -lz -lpthread -lm")

SRC_DIR = src
BUILD_DIR = build
TARGET = $(BUILD_DIR)/pg_gateway
TARGET_STATIC = $(BUILD_DIR)/pg_gateway-static

SOURCES = $(SRC_DIR)/main.c $(SRC_DIR)/gateway.c $(SRC_DIR)/health_check.c
OBJECTS = $(BUILD_DIR)/main.o $(BUILD_DIR)/gateway.o $(BUILD_DIR)/health_check.o

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

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c $(SRC_DIR)/gateway.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c -o $@ $<

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $(OBJECTS) $(LDFLAGS)

$(TARGET_STATIC): $(OBJECTS)
	$(CC) $(CFLAGS) -static -o $@ $(OBJECTS) $(STATIC_LDFLAGS)

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

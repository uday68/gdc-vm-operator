# Makefile for GDC Air-Gapped Virtual Machine Operator
# Supported Go Version: 1.22+

BINARY_DIR := bin
RUNNER_BIN := $(BINARY_DIR)/vm-runner
VMCTL_BIN := $(BINARY_DIR)/vmctl

.PHONY: all build test test-coverage lint clean init run

all: build test

build:
	@echo "==> Building binaries..."
	go build -o $(RUNNER_BIN) ./cmd/vm-runner
	go build -o $(VMCTL_BIN) ./cmd/vmctl

test:
	@echo "==> Running unit tests..."
	go test -v ./...

test-coverage:
	@echo "==> Running test suite with code coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	@echo "==> Running static analysis..."
	go vet ./...

init: build
	@echo "==> Initializing air-gap environment..."
	./$(VMCTL_BIN) init

run: build
	@echo "==> Running operator reconciliation daemon..."
	./$(VMCTL_BIN) run

clean:
	@echo "==> Cleaning generated binaries and runtime data..."
	rm -rf $(BINARY_DIR) data coverage.out


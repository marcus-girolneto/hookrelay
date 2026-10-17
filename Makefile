SHELL := /bin/bash
.DEFAULT_GOAL := help

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN     := bin
PKGS    := ./...

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build hookrelay and the example receiver into ./bin
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN)/hookrelay ./cmd/hookrelay
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN)/receiver ./cmd/receiver

.PHONY: test
test: ## Run unit tests with the race detector (no Docker needed)
	go test -race $(PKGS)

.PHONY: test-integration
test-integration: ## Run unit and integration tests (requires Docker)
	go test -race -tags integration $(PKGS)

.PHONY: cover
cover: ## Run all tests with coverage and print a per-function summary
	go test -race -tags integration -coverpkg=./internal/...,./signature -coverprofile=coverage.out $(PKGS)
	go tool cover -func=coverage.out | tail -n 1

.PHONY: bench
bench: ## Run benchmarks (Postgres benchmark requires Docker)
	go test -run='^$$' -bench=. -benchmem ./signature ./internal/backoff ./internal/delivery
	go test -tags integration -run='^$$' -bench=DispatchPostgres -benchtime=3x ./internal/delivery

.PHONY: fuzz
fuzz: ## Fuzz the signature verifier for 30s per target
	go test -run='^$$' -fuzz=FuzzRoundTrip -fuzztime=30s ./signature
	go test -run='^$$' -fuzz=FuzzVerifyNoPanic -fuzztime=30s ./signature

.PHONY: lint
lint: ## Run go vet and golangci-lint
	go vet $(PKGS)
	golangci-lint run

.PHONY: fmt
fmt: ## Format code with gofumpt and goimports
	golangci-lint fmt

.PHONY: run
run: ## Run hookrelay locally against HOOKRELAY_DATABASE_URL
	go run ./cmd/hookrelay

.PHONY: docker-build
docker-build: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t hookrelay:local .

.PHONY: up
up: ## Start the demo stack (Postgres, hookrelay, receiver)
	docker compose up -d --build

.PHONY: down
down: ## Stop the demo stack and remove its data
	docker compose down -v

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN) coverage.out

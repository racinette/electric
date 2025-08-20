# Makefile for Go client integration tests

# Docker compose file location
COMPOSE_FILE = docker-compose.yml

# Test configuration
ELECTRIC_BASE_URL = http://localhost:3000
ELECTRIC_DB_HOST = localhost
ELECTRIC_DB_PORT = 54321
ELECTRIC_DB_NAME = electric
ELECTRIC_DB_USER = postgres
ELECTRIC_DB_PASSWORD = password
ELECTRIC_DB_SCHEMA = electric_test

# Export environment variables for tests
export ELECTRIC_BASE_URL
export ELECTRIC_DB_HOST
export ELECTRIC_DB_PORT
export ELECTRIC_DB_NAME
export ELECTRIC_DB_USER
export ELECTRIC_DB_PASSWORD
export ELECTRIC_DB_SCHEMA

.PHONY: help setup start stop clean test integration-test integration-test-verbose logs deps

help: ## Show this help
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Install Go dependencies
	@echo "Installing Go dependencies..."
	go mod download
	go mod tidy

setup: deps ## Set up the testing environment
	@echo "Setting up integration test environment..."
	docker compose -f $(COMPOSE_FILE) pull

start: ## Start the integration test services
	@echo "Starting integration test services..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for services to be ready..."
	@for i in $$(seq 1 30); do \
		if curl -s -f http://localhost:3000/v1/health >/dev/null 2>&1; then \
			echo "Electric service is ready!"; \
			break; \
		fi; \
		echo "Waiting for Electric service... ($$i/30)"; \
		sleep 2; \
	done
	@if ! curl -s -f http://localhost:3000/v1/health >/dev/null 2>&1; then \
		echo "Electric service failed to start within timeout"; \
		docker compose -f $(COMPOSE_FILE) logs; \
		exit 1; \
	fi

stop: ## Stop the integration test services
	@echo "Stopping integration test services..."
	docker compose -f $(COMPOSE_FILE) down

clean: stop ## Clean up all containers, volumes, and networks
	@echo "Cleaning up integration test environment..."
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	docker system prune -f

logs: ## Show logs from all services
	docker compose -f $(COMPOSE_FILE) logs -f

test: ## Run unit tests
	@echo "Running unit tests..."
	go test -v ./...

integration-test: start ## Run integration tests
	@echo "Running integration tests..."
	@trap '$(MAKE) stop' EXIT; \
	sleep 2; \
	cd integration && go test -v ./...

integration-test-verbose: start ## Run integration tests with verbose output
	@echo "Running integration tests with verbose output..."
	@trap '$(MAKE) stop' EXIT; \
	sleep 2; \
	cd integration && go test -v -args -test.v

integration-test-no-cleanup: start ## Run integration tests without stopping services
	@echo "Running integration tests (services will remain running)..."
	sleep 2
	cd integration && go test -v ./...

# Development helpers
dev-start: start ## Start services and keep them running for development
	@echo "Services started. Use 'make stop' to stop them."
	@echo "Electric URL: $(ELECTRIC_BASE_URL)"
	@echo "PostgreSQL: $(ELECTRIC_DB_HOST):$(ELECTRIC_DB_PORT)/$(ELECTRIC_DB_NAME)"

dev-logs-electric: ## Show Electric service logs
	docker compose -f $(COMPOSE_FILE) logs -f electric

dev-logs-postgres: ## Show PostgreSQL logs  
	docker compose -f $(COMPOSE_FILE) logs -f postgres

dev-shell-postgres: ## Connect to PostgreSQL with psql
	docker compose -f $(COMPOSE_FILE) exec postgres psql -U $(ELECTRIC_DB_USER) -d $(ELECTRIC_DB_NAME)

# CI/Testing helpers
ci-test: setup integration-test ## Full CI test pipeline

# Quick shortcuts
up: start ## Alias for start
down: stop ## Alias for stop

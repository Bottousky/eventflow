# EventFlow developer commands. The targets are intentionally close to
# the underlying Go invocations so the Makefile is a discoverable alias
# of the documented workflows in the README, not a parallel system.

GO ?= go
PKGS ?= ./...

.PHONY: build vet test test-race coverage test-all bench run-api run-worker run-demo fmt clean docker-build docker-up docker-down

build:
	$(GO) build $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

# Unit + integration tests, no race detector.
test:
	$(GO) test -count=1 $(PKGS)

# Race detector; requires CGO (gcc available).
test-race:
	CGO_ENABLED=1 $(GO) test -race -count=1 $(PKGS)

# Coverage summary printed per package.
coverage:
	$(GO) test -cover -count=1 $(PKGS)

# Full local gauntlet: fmt, vet, build, test, test-race (best-effort),
# coverage. Mirrors the CI workflow.
test-all: fmt vet build test coverage

bench:
	$(GO) test -bench=. -benchtime=2s -run=^$ ./internal/orchestrator ./internal/stream

run-api:
	$(GO) run ./cmd/eventflow api

run-worker:
	$(GO) run ./cmd/eventflow worker

run-demo:
	$(GO) run ./cmd/eventflow demo

docker-build:
	docker build -t eventflow:dev .

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

clean:
	$(GO) clean
	mavis-trash 'eventflow.db' 'eventflow.db-*' || true

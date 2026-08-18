# EventFlow — Notification Orchestrator

> A small but serious reference implementation of an event-driven notification
> backend in Go. Demonstrates ordered event processing, idempotency, retries
> with exponential backoff, a dead-letter queue, persistence, observability,
> bounded concurrency, graceful shutdown, and a serious test suite — all in
> a repository that compiles, tests and runs without external services.

## What it demonstrates

- **REST API** that accepts events and reports their delivery status
  (`POST /events`, `GET /events/{id}`, `GET /notifications/{id}`,
  `GET /health`, `GET /metrics`).
- **Append-only event stream** with a monotonically increasing `seq`,
  per-ordering-key order preservation, and a cursor so workers can resume
  from the last processed position.
- **Notification orchestrator** that fans events out to channel senders
  (email, push, in-app) with:
  - bounded per-ordering-key concurrency (different keys in parallel,
    same key sequential),
  - exponential backoff with a 2 s cap,
  - permanent vs. transient error distinction,
  - in-memory idempotency cache (mirrors Redis `SET NX`) plus a
    crash-safe `notifications` table so a restarted worker never sends
    twice,
  - a dead-letter queue for inspection after the retry budget is spent.
- **Persistence** in SQLite via the pure-Go
  [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) driver —
  no CGO, no external services. The schema is created on first use.
- **Observability** with structured logs (`log/slog`) and a Prometheus
  text format `/metrics` endpoint (counters for received/processed/
  delivered/retries/duplicates/dead-lettered/errors and a per-record
  processing-duration histogram).
- **Request IDs** via `X-Request-ID` (echoed if provided, generated
  otherwise) and propagated through the request context.
- **Configuration** via environment variables (`HTTP_ADDR`, `DB_PATH`,
  `MAX_ATTEMPTS`, `WORKER_CONCURRENCY`, `POLL_INTERVAL`, `BASE_BACKOFF`,
  `LOG_LEVEL`) and command-line flags.
- **Graceful shutdown** of both the API (HTTP `Server.Shutdown`) and the
  worker (context-aware `Run` loop).
- **Tests** — unit, integration and end-to-end — including a race-aware
  suite when CGO is available.

## Quick start

```bash
# Clone
git clone https://github.com/Bottousky/eventflow.git
cd eventflow

# Run the test suite (no external services required)
go test ./...

# Start the API
go run ./cmd/eventflow api
# in another terminal: start the worker
go run ./cmd/eventflow worker
# in a third: post some sample events
go run ./cmd/eventflow demo
```

Then poke the endpoints:

```bash
# Send an event
curl -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  -d '{
    "type": "order.shipped",
    "ordering_key": "user-1",
    "recipient": "ana@example.com",
    "channels": ["email", "push"],
    "payload": {"order_id": "ORD-1"}
  }'

# Read it back
curl http://localhost:8080/events/evt_...

# Health and metrics
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

`go run ./cmd/eventflow worker -fail email=2` makes the email sender fail
twice before succeeding — useful for watching the retry path without
modifying any code.

`go run ./cmd/eventflow worker -fail push=always` makes the push sender
fail forever — useful for watching a delivery end up in the dead-letter
queue.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST /events| A[REST API]
    A -->|append| S[(Event Stream<br/>SQLite)]
    A -->|metrics| M[/metrics]
    O[Notification<br/>Orchestrator] -->|ReadAfter seq| S
    O -->|EnsureNotification / MarkDelivered / DLQ| N[(notifications,<br/>delivery_attempts,<br/>dead_letters<br/>SQLite)]
    O -->|SetNX| K[(Idempotency KVS)]
    O -->|Send| E[Email]
    O -->|Send| P[Push]
    O -->|Send| I[In-App]
    C -->|GET /events/{id}<br/>GET /notifications/{id}| A
```

The diagram is the architecture, not the codebase map. The components
map to packages:

| Concept             | Package                                  |
|---------------------|------------------------------------------|
| REST API            | `internal/api`                           |
| Event Stream        | `internal/stream`                        |
| Orchestrator        | `internal/orchestrator`                  |
| Notification store  | `internal/store`                         |
| Idempotency cache   | `internal/kvs`                           |
| Channel senders     | `internal/senders`                       |
| Metrics             | `internal/obs`                           |
| Configuration       | `internal/config`                        |
| Domain types        | `internal/events`                        |
| CLI                 | `cmd/eventflow`                          |

## Failure scenarios

EventFlow was designed to make failure observable and reproducible.

### Transient errors with exponential backoff

`go run ./cmd/eventflow worker -fail email=2` makes the email sender fail
twice before succeeding. The orchestrator retries with backoff, then
succeeds. `GET /events/{id}` reports `status=delivered` with
`attempts=2`.

### Permanent errors short-circuit the retry budget

A sender that wraps its error in `*events.PermanentError` will not be
retried; the notification is moved to the DLQ on the first failure.
This mirrors providers that respond 4xx-style (bad recipient, invalid
template) and should never be retried.

### Exhausted retries → dead-letter queue

`go run ./cmd/eventflow worker -fail push=always` makes the push sender
fail forever. After the configured attempt budget is spent, the
notification is marked `status=dead` and a row is written to
`dead_letters`. The orchestrator's `delivery_attempts` table records
every failed attempt for inspection.

### Replay after restart never re-delivers

The orchestrator's `notifications` table is the source of truth: if a
row is already `delivered` or `dead`, `ProcessOnce` skips it. The
in-memory KVS also deduplicates in-flight delivery across worker
instances, mirroring Redis `SET NX PX` semantics.

## Concurrency model

The orchestrator is the only place with goroutines. `ProcessOnce` reads
a batch from the stream, groups the records by `ordering_key`, and
spawns one goroutine per group, capped by `WORKER_CONCURRENCY` (default
4). Each goroutine processes its group sequentially — that is what
preserves per-key order — while different groups run in parallel. The
cursor advances in seq order up to the highest seq whose record was
attempted.

The orchestrator also reacts to context cancellation: `Run` returns
promptly when the context is canceled, and `ProcessOnce` aborts
in-flight processing.

## Observability

### Structured logs

Every API call, event append, delivery attempt, retry, dead-letter
write, and orchestrator cycle is logged via `log/slog`. The log level
is configurable with `LOG_LEVEL=debug|info|warn|error`.

### Metrics

`GET /metrics` returns Prometheus text exposition format. The current
metric set:

```
eventflow_events_received_total
eventflow_events_processed_total
eventflow_notifications_delivered_total
eventflow_delivery_retries_total
eventflow_duplicates_suppressed_total
eventflow_events_dead_lettered_total
eventflow_processing_errors_total
eventflow_processing_duration_seconds (histogram, 5 buckets)
```

The output is intended to be scraped by Prometheus, Datadog, Grafana
Cloud or any other agent that understands the text format.

## Testing

```bash
# Unit + integration tests
go test ./...

# Race detector (requires CGO; run on CI)
go test -race ./...

# Coverage summary
go test -cover ./...

# Benchmarks
go test -bench=. ./internal/orchestrator
go test -bench=. ./internal/stream
```

The test suite covers:
- API validation, request IDs, `/metrics` content
- Stream append, ordering, cursor round-trips, duplicate-id rejection
- KVS `SetNX` dedup, TTL expiration
- Store idempotency, attempts, dead-letter flow, notifications-by-event
- Sender: fail-then-succeed, fail-always, permanent error, canceled ctx
- Orchestrator: append-order processing, retries, DLQ, replay, in-flight
  dedup, multi-channel fan-out, **bounded concurrency across keys**,
  **sequential processing within a key**, graceful shutdown
- End-to-end: two separate stacks sharing a SQLite file, the API
  accepts an event, the worker processes it, the API reports the
  final delivery state.

Benchmark results are local reproductions, not production guarantees —
see `docs/DESIGN_DECISIONS.md`.

## Repository structure

```
eventflow/
├── cmd/eventflow/         # CLI entrypoint (api / worker / demo)
├── internal/
│   ├── api/               # REST surface, request_id middleware
│   ├── config/            # env-var configuration
│   ├── events/            # domain types (Event, Channel, Notification, PermanentError)
│   ├── kvs/               # in-memory idempotency cache
│   ├── obs/               # metrics (Prometheus text format)
│   ├── orchestrator/      # worker loop, retry, DLQ, bounded concurrency
│   ├── senders/           # simulated channels, failure injection
│   ├── store/             # SQLite persistence (notifications, attempts, DLQ, cursor)
│   └── stream/            # append-only event stream
├── migrations/            # canonical SQL (mirrors the inline schema)
├── test/                  # end-to-end test
├── docs/                  # ARCHITECTURE, DESIGN_DECISIONS, FAILURE_MODEL
├── .github/workflows/     # CI (test, vet, race, build, coverage)
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── .env.example
├── go.mod / go.sum
└── README.md
```

## What this project intentionally does not do

- No real email/push/in-app providers. The senders are simulated. The
  `Sender` interface is the integration point — replace it with a real
  provider by writing a new `Send` implementation.
- No broker like Kafka or SQS. The event stream is a SQLite table;
  ordered append and per-cursor reads are enough to demonstrate the
  concept. Mapping this to a real broker means swapping the
  `stream.Stream` implementation; the orchestrator's interface stays
  the same.
- No Postgres / MySQL. The `store.Store` uses `database/sql`; swap the
  driver and the schema migrations for any RDBMS.
- No JWT, no auth. Public network exposure of the API is **not** in
  scope here; if you expose it, put it behind an authenticating proxy.
- No web UI. The repo is the API, the worker, and the tests.
- No `any` in the hot path. Strict typing and explicit error handling
  are deliberate.

## Design notes

See `docs/ARCHITECTURE.md`, `docs/DESIGN_DECISIONS.md` and
`docs/FAILURE_MODEL.md` for the long-form reasoning behind the layout
above.

## Independent reference implementation

> EventFlow is an independent reference implementation built from scratch
> to demonstrate general backend engineering concepts. It contains no
> employer source code, proprietary architecture, internal APIs, schemas,
> data or confidential information.

The patterns EventFlow demonstrates — ordered event processing,
idempotency, retries, dead-lettering, observability, bounded
concurrency, context propagation, strict typing — are general
backend-engineering concepts that show up across many production
systems. They are not tied to any specific employer, internal
architecture, vendor or proprietary product.

## License

MIT — see `LICENSE`.

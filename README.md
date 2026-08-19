# EventFlow — Notification Orchestrator

> A small but serious reference implementation of an event-driven notification
> backend in Go. Demonstrates ordered event processing, durable idempotency
> with provider-supplied keys, retries with exponential backoff, an atomic
> dead-letter queue, persistence, observability, bounded concurrency, graceful
> shutdown, and a serious test suite — all in a repository that compiles,
> tests and runs without external services.

## What it demonstrates

- **REST API** that accepts events and reports their delivery status
  (`POST /events`, `GET /events/{id}`, `GET /notifications/{id}`,
  `GET /health`, `GET /metrics`).
- **Append-only event stream** with a monotonically increasing `seq`,
  per-ordering-key order preservation, and a cursor so workers can resume
  from the last processed position.
- **Notification orchestrator** that fans events out to channel senders
  (email, push, in-app) with:
  - **at-least-once** delivery with two layers of deduplication:
    the persistent `notifications` table (source of truth for
    terminal state) plus the in-memory KVS that mirrors Redis
    `SET NX PX` for in-flight cross-replica dedup,
  - reusable provider idempotency keys built from the notification's
    stable primary key — providers that support idempotency can
    collapse the duplicate that the crash window can otherwise
    produce (see [FAILURE_MODEL.md](docs/FAILURE_MODEL.md)),
  - bounded per-ordering-key concurrency (different keys in parallel,
    same key sequential),
  - exponential backoff with a 2 s cap,
  - permanent vs. transient error distinction,
  - an atomic dead-letter queue (`status=dead` + `dead_letters` row in
    the same SQL transaction, with full rollback on failure).
- **Persistence** in SQLite via the pure-Go
  [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) driver —
  no CGO, no external services. The schema is created on first use.
- **Observability** with structured logs (`log/slog`) and Prometheus
  text format `/metrics` endpoints. The **API process** exposes
  `events_received`; the **worker process** exposes the orchestrator
  counters and the processing-duration histogram on its own
  configurable listen address (default `:9090`, set `METRICS_ADDR=""`
  to disable). Prometheus scrapes both.
- **Request IDs** via `X-Request-ID` (echoed if provided, generated
  otherwise) and propagated through the request context.
- **Configuration** via environment variables (`HTTP_ADDR`,
  `METRICS_ADDR`, `DB_PATH`, `MAX_ATTEMPTS`, `WORKER_CONCURRENCY`,
  `POLL_INTERVAL`, `BASE_BACKOFF`, `LOG_LEVEL`) and command-line flags.
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
curl http://localhost:9090/metrics   # worker counters + histogram
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
    A -->|events_received| MA[/metrics API :8080]
    O[Notification<br/>Orchestrator] -->|ReadAfter seq| S
    O -->|EnsureNotification / MarkDelivered / atomic DLQ| N[(notifications,<br/>delivery_attempts,<br/>dead_letters<br/>SQLite)]
    O -->|SetNX| K[(Idempotency KVS)]
    O -->|Send| E[Email]
    O -->|Send| P[Push]
    O -->|Send| I[In-App]
    O -->|processed / delivered / retries / dead_lettered| MW[/metrics worker :9090]
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
`dead_letters` — both writes are inside the same SQL transaction so a
mid-write failure rolls back the status change too. The orchestrator's
`delivery_attempts` table records every failed attempt for inspection.

### Replay after restart is at-least-once

The orchestrator reads from the stream using a cursor. If the worker
crashes between processing a record and advancing the cursor, the
record is re-read on restart. The notification row is the source of
truth: if it is already `delivered` or `dead`, `ProcessOnce` skips the
`Sender.Send` call.

The window between `Sender.Send` returning success and `MarkDelivered`
committing is **still at-least-once**: a crash in that window makes the
next attempt re-send. The recommended mitigation is to pass the
notification row's stable primary key as a provider-side idempotency
key; providers that support it collapse the duplicate on replay. If
the provider does not support idempotency keys, duplication is
possible — see [FAILURE_MODEL.md](docs/FAILURE_MODEL.md) for the full
discussion.

## Concurrency model

The orchestrator is the only place with goroutines. `ProcessOnce` reads
a batch from the stream, groups the records by `ordering_key`, and
spawns one goroutine per group, capped by `WORKER_CONCURRENCY` (default
4). Each goroutine processes its group sequentially — that is what
preserves per-key order — while different groups run in parallel.

A record only advances the cursor when every channel it carries
reached a terminal durable state (`delivered` or `dead`). A record
that ends in a non-terminal state stops the cursor for itself and for
every record behind it in the same ordering_key partition; the next
`ProcessOnce` re-reads the pending record together with everything
that followed it. Inside a group, a non-terminal record also stops the
goroutine so per-key order survives an infrastructure failure.

The orchestrator reacts to context cancellation: `Run` returns
promptly when the context is canceled, and `ProcessOnce` aborts
in-flight processing.

## Observability

### Structured logs

Every API call, event append, delivery attempt, retry, dead-letter
write, and orchestrator cycle is logged via `log/slog`. The log level
is configurable with `LOG_LEVEL=debug|info|warn|error`.

### Metrics

EventFlow exposes **two** `/metrics` endpoints, one per process. The
shape matches Prometheus text exposition format so a Prometheus
server, Datadog Agent, Grafana Alloy, or any other OpenMetrics-compatible
agent can scrape both.

| Process | Endpoint (default) | Counters / histogram                                  |
|---------|--------------------|------------------------------------------------------|
| API     | `:8080/metrics`    | `eventflow_events_received_total`                     |
| Worker  | `:9090/metrics`    | `eventflow_events_processed_total`, `…_delivered_total`, `…_retries_total`, `…_dead_lettered_total`, `eventflow_processing_duration_seconds` (histogram) |

A scrape config that targets both endpoints lets a real deployment see
the same metric set as the demo. Disable the worker endpoint by setting
`METRICS_ADDR=""`.

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
- Store idempotency, attempts, dead-letter flow, notifications-by-event,
  **DLQ atomicity (status update + dead_letters insert rolled back
  together if either step fails)**
- Sender: fail-then-succeed, fail-always, permanent error, canceled ctx
- Orchestrator: append-order processing, retries, DLQ, replay, in-flight
  dedup, multi-channel fan-out, bounded concurrency across keys,
  sequential processing within a key, **cursor does not advance past
  a non-terminal record**, **ordering_key partition stops after a
  non-terminal failure**, **KVS lock released on non-terminal results
  and kept on terminal ones**, graceful shutdown
- Cross-process metrics split (API sees only `events_received`, worker
  sees the orchestrator counters and histogram)
- Histogram: bucket counts, `_sum`, `_count` follow the Prometheus
  convention (one observation increments exactly one bucket)
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
│   ├── orchestrator/      # worker loop, retry, atomic DLQ, bounded concurrency
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
  provider by writing a new `Send` implementation. When you do, pass
  `n.ID` (or a hash of `event_id+channel`) as the provider-side
  idempotency key so the at-least-once crash window cannot produce a
  duplicate downstream.
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
durable idempotency, retries, atomic dead-lettering, observability,
bounded concurrency, context propagation, strict typing — are general
backend-engineering concepts that show up across many production
systems. They are not tied to any specific employer, internal
architecture, vendor or proprietary product.

## License

MIT — see `LICENSE`.

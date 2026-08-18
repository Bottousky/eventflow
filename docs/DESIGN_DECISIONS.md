# EventFlow Design Decisions

This document records the trade-offs that shaped the EventFlow code, and
why each alternative was rejected. The intent is to make the engineering
thinking visible without requiring the reader to reverse-engineer it
from the source.

## Why SQLite (and `modernc.org/sqlite`)

The orchestrator and the stream are the interesting parts of an
event-driven system. A real broker (Kafka, SQS) and a real RDBMS
(Postgres, MySQL) require infrastructure to run, which makes the
project harder to clone-and-try.

A pure-Go SQLite driver gives us a durable store with a real schema,
a real SQL interface and per-row atomicity — without CGO and without
"docker compose up postgres". The `database/sql` interface means the
storage layer is the only piece that has to change when porting to a
real RDBMS. Everything above it stays the same.

`max_open_conns=1` plus `journal_mode=WAL` and `busy_timeout=5000` give
us multi-process safety for the API and worker pair while avoiding
SQLite's "database is locked" failure mode under concurrent writes.

## Why one file, not two layers (Redis + Postgres)

The canonical event-driven system has a broker (Redis Streams, Kafka,
SQS) for ordering and a database (Postgres) for state. EventFlow
collapses both into a single SQLite file:

- The `events` table is the append-only log (broker).
- The `notifications`, `delivery_attempts`, `dead_letters` and
  `cursor` tables are the state store (database).
- The `internal/kvs` package models the in-memory idempotency cache
  that would, in production, be Redis.

The collapse is honest about what is being demonstrated: the *interface*
between the orchestrator and the storage layer, not the absolute
throughput of a real broker. The KVS is kept as a separate package so
the swap to a real Redis is mechanical (replace `kvs.Store` with a
Redis-backed implementation of the same `SetNX` interface).

## Why bounded concurrency per ordering_key, not a worker pool

A single worker pool with a shared channel would have a hidden bug:
if worker A picks up event 1 for key `user-1` and worker B picks up
event 2 for the same key, the two events can be processed in either
order. The ordering contract for an `ordering_key` would be silently
broken.

The chosen design groups records by `ordering_key` and runs each group
sequentially. Different groups run in parallel, capped by
`WORKER_CONCURRENCY`. This guarantees per-key order at the cost of
some sequential work within a key. In production this is the same
trade-off Kafka makes with keyed partitioning.

## Why cursor advances in seq order, not "last attempted"

The simplest implementation of `ProcessOnce` would advance the cursor
to the last record's `seq` after every record is attempted. But if
goroutine A processes seq 4, goroutine B fails at seq 5, the cursor
should not be at 5 (because seq 5 was not actually completed). The
chosen design keeps a per-batch "processed" set and only advances the
cursor through the longest contiguous prefix of processed records. On
retry, unprocessed records are re-read and idempotency prevents
duplicate delivery.

## Why exponential backoff, not jittered

A reference implementation should be deterministic. The
exponential-backoff schedule `100ms, 200ms, 400ms, ...` is exactly
reproducible across runs. A real production system would add jitter
(per-process randomisation) to avoid thundering-herd recovery; that
is left as a deliberate `TODO` because it would make tests flaky
without adding to what the demo demonstrates.

## Why two `Sender` failure modes (transient + permanent)

Not every failure is the same. A temporary network blip deserves a
retry; a malformed email address never will. Without the distinction,
every failure burns the retry budget, which means DLQ entries that
look like "permanent error after 3 retries" instead of "permanent
error caught on the first try". Wrapping the error in
`*events.PermanentError` lets the orchestrator short-circuit to the
DLQ on the first attempt, which is what a 4xx-style provider
response would do in production.

## Why `log/slog` and not `logrus` or `zap`

The Go standard library's `log/slog` is enough for what this project
demonstrates. The dependency footprint stays small, the structured-
logging story is built into the language, and there is no third-party
upstream to vet. For production systems that need samplers, hooks or
custom encoders, `slog` can be extended with custom handlers.

## Why Prometheus text format, not JSON, for `/metrics`

Prometheus is the de-facto scraping protocol for backend services. A
real production system would expose this exact wire format so a
Prometheus server, Datadog Agent, Grafana Alloy, or any other
OpenMetrics-compatible agent could scrape it without translation. The
JSON output is trivial to parse but is not what production monitoring
expects.

## Why request IDs, not OpenTelemetry

OpenTelemetry would be the right answer at production scale. For a
reference implementation, the `X-Request-ID` middleware and a context
key for it is enough to demonstrate the pattern: every request gets
an ID, the ID is echoed in the response, and downstream code can
attach it to log lines. Adding OpenTelemetry would pull in
`go.opentelemetry.io/otel` and a tracer implementation, which is more
weight than this project is meant to carry.

## Why benchmarks are local reproductions, not guarantees

`BenchmarkEventAppend` and `BenchmarkOrchestratorProcessing` are
included so a reader can see the cost of the orchestrator's
coordination. The numbers depend on the host's CPU, disk, and SQLite
build, so they are explicitly described in the README as
**reproducible on the developer's machine, not a production SLA**.
Drawing a marketing curve from `go test -bench` would be
disingenuous.

## What is intentionally out of scope

- Real email/push/in-app providers. The senders are simulated; the
  `Sender` interface is the integration point.
- Authentication. The API is unauthenticated; if exposed, it goes
  behind a proxy.
- A web UI. The repo is the API, the worker, and the tests.
- A migration framework. The schema is `CREATE TABLE IF NOT EXISTS`
  inside Go; adding `golang-migrate` would add weight without adding
  capability this demo needs.
- Schema introspection. There is no `DESCRIBE` endpoint; the schema
  is in `internal/store`, `internal/stream` and `migrations/0001_init.sql`.

# EventFlow Architecture

## Goal

Build a small but serious event-driven notification backend in Go that
demonstrates general backend engineering concepts in a way a recruiter
or engineering manager can verify end-to-end in a few minutes.

The design must be honest about the trade-offs the demo makes — small
enough to be readable, large enough to demonstrate production-shaped
behaviour (idempotency, retries, DLQ, observability, bounded
concurrency, graceful shutdown).

## Components

| Component            | Package                | Responsibility                                              |
|----------------------|------------------------|-------------------------------------------------------------|
| REST API             | `internal/api`         | Accept events, return status, expose metrics                |
| Event Stream         | `internal/stream`      | Append-only, seq-ordered, cursor-resumable                  |
| Orchestrator         | `internal/orchestrator`| Fan out to senders, retry, dead-letter                      |
| Notification store   | `internal/store`       | Persist notifications, attempts, DLQ, cursor                |
| Idempotency cache    | `internal/kvs`         | Per-(event, channel) `SetNX` dedup                          |
| Channel senders      | `internal/senders`     | Pluggable delivery (email, push, in-app), simulated        |
| Metrics              | `internal/obs`         | Counters + histogram, Prometheus text format                |
| Configuration        | `internal/config`      | Env-var-driven config with defaults                         |
| Domain types         | `internal/events`      | Event, Channel, Notification, PermanentError                |
| CLI                  | `cmd/eventflow`        | `api`, `worker`, `demo` subcommands                         |

## Data flow

```
client
  │  POST /events
  ▼
REST API ──append──► Event Stream (SQLite events table)
  │                     │
  │  metrics            │ ReadAfter(seq > cursor, limit)
  ▼                     ▼
/metrics           Notification Orchestrator
                          │
                ┌─────────┼─────────┐
                ▼         ▼         ▼
              Email      Push     In-App
                │         │         │
                └────┬────┴────┬────┘
                     ▼         ▼
               Idempotency   Notification store
                  KVS        (delivered/dead + attempts)
                                │
                                ▼
                           dead_letters
```

The API and worker are separate processes that share the SQLite file
the same way production services would share a broker and a database.
This is what `test/e2e_test.go` exercises.

## Ordering model

The stream assigns each event a monotonically increasing `seq` on
append. `ReadAfter(cursor, limit)` returns records in `seq` order. The
orchestrator groups records by `ordering_key` and processes each group
sequentially while different groups run in parallel up to
`WORKER_CONCURRENCY`. This preserves per-key order without serialising
the whole pipeline.

## Idempotency model

Two layers protect against duplicate delivery:

1. **Notification store** (persistent). The orchestrator calls
   `EnsureNotification` which inserts or fetches the row keyed by
   `(event_id, channel)`. The row carries a status of
   `pending` / `delivered` / `dead`. A `delivered` or `dead` row is
   never re-sent, regardless of how many times the worker restarts.
2. **In-memory KVS** (in-flight). After the store check, the
   orchestrator calls `kvs.SetNX("event:channel")`. This mirrors Redis
   `SET NX PX` and is what prevents two worker replicas from
   double-sending the same notification while the persistent row is
   still `pending`.

The KVS is the right tool for "two replicas running at the same time";
the store is the right tool for "this delivery was already finalized
yesterday". Together they cover both the hot and the cold case.

## Retry and backoff

Each delivery attempt uses exponential backoff:

```
backoff(attempt) = min(BASE_BACKOFF << (attempt - 1), 2s)
```

`MAX_ATTEMPTS` is the per-channel cap. After the last attempt the row
is marked `dead` and a `dead_letters` row is written. A delivery that
returns `*events.PermanentError` short-circuits to the DLQ without
spending the retry budget, modelling providers that respond 4xx-style.

## Observability

The `obs` package keeps a small set of named counters plus a histogram
of per-record processing time. The `/metrics` endpoint renders them in
Prometheus text format. Logs are written through `log/slog` at the
configured level; every important transition (event accepted,
delivery succeeded, retry, dead-letter) includes the
`event_id`, `channel`, `ordering_key` and `attempt` so a single
`grep` can reconstruct a notification's history.

## Configuration

`internal/config.FromEnv` reads the well-known env vars and falls back
to sane defaults if any are missing. The defaults match the values
documented in `.env.example`. The `cmd/eventflow` entrypoint uses the
env-var values as flag defaults so `go run ./cmd/eventflow api
-addr :9001` overrides `HTTP_ADDR` for one process.

## Why these choices

The decisions that shaped the architecture are recorded in
[`DESIGN_DECISIONS.md`](./DESIGN_DECISIONS.md). The failure modes the
code is designed to handle are enumerated in
[`FAILURE_MODEL.md`](./FAILURE_MODEL.md).

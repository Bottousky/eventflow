# EventFlow Failure Model

This document enumerates the failure modes the code is designed to
handle, how each one is detected, and what the user-visible behaviour
is. The intent is to make the failure semantics explicit so they can
be reviewed, tested and reasoned about.

## Sender failures

### Transient failure (retry)

A `Sender.Send` call returns a non-permanent error. The orchestrator:

1. Increments `delivery_retries_total`.
2. Logs the attempt and the error.
3. Records the attempt in `delivery_attempts` (durable audit).
4. Sleeps for `backoff(attempt) = min(BASE_BACKOFF << (attempt - 1), 2s)`.
5. Retries up to `MAX_ATTEMPTS` times.

If any attempt succeeds, the notification is marked `delivered` and
`notifications_delivered_total` is incremented. If the budget is
exhausted, the notification is marked `dead` and a row is inserted
into `dead_letters`. `events_dead_lettered_total` is incremented.

### Permanent failure (skip retry)

A `Sender.Send` call returns a `*events.PermanentError` (or any error
wrapping one). The orchestrator:

1. Logs the attempt as "permanent".
2. Skips the retry loop and goes straight to dead-lettering.

A `*events.PermanentError` models a 4xx-style provider response:
"this recipient is invalid", "this template was rejected", "this
account is suspended". Retrying such a failure just wastes the budget
and delays the operator's signal that the failure is permanent.

### Context cancellation

If `ctx.Done()` fires during a `Sender.Send` call, the sender returns
the context error and the orchestrator stops processing the current
batch. The notification row stays at its current status (likely
`pending`), and on the next `ProcessOnce` the orchestrator retries it.

## Duplicates

### Same event appended twice

The stream's `events` table has a `UNIQUE(id)` constraint. The second
append returns `ErrDuplicateID`, and the API responds with 500. The
event-id uniqueness is the at-most-once append contract; producers are
expected to retry on a 5xx with the same event id.

### Same event processed twice (worker restart)

The orchestrator reads from the stream using a cursor. If the worker
crashes between processing a record and advancing the cursor, the
record is re-read on restart. The notification row is already
`delivered` or `dead`, so `EnsureNotification` returns the existing
row and the orchestrator skips it. No re-send happens.

### Same event processed by two workers at the same time

Two workers running concurrently against the same store would both
find the notification `pending` and both call `Sender.Send`. The
in-memory KVS prevents this: each worker calls `kvs.SetNX(key)` on
the `(event_id, channel)` key. Only the first one wins; the second
sees the key already set, increments `duplicates_suppressed_total`,
and skips the send.

In a real production deployment the KVS would be Redis so all worker
replicas share it; in the demo, each worker has its own in-memory
KVS, which is enough to demonstrate the pattern within a single
process.

## Storage failures

### SQLite is locked

The database driver retries for `busy_timeout=5000` milliseconds
before failing. The API responds 500; the worker logs the error and
retries on the next poll.

### Disk full

The driver returns an error on the next `Exec`. The API responds 500;
the worker logs and retries. There is no automatic recovery for
disk-full — operators have to act.

## Network failures

### API cannot reach the worker

The worker is a separate process. If the worker is down, events are
still accepted and appended to the stream. When the worker comes back
up, it reads the stream from the cursor and processes the backlog.

### Worker cannot reach a sender provider

Senders are simulated in this demo, so this is not directly exercised.
A real `Sender` implementation would treat provider timeouts and 5xx
responses as transient errors and let the orchestrator's retry loop
handle them.

## Process lifecycle

### API shutdown

The CLI installs a signal handler for `SIGINT`/`SIGTERM`. On signal,
`http.Server.Shutdown` is called with a 5-second timeout. In-flight
requests have 5 seconds to complete; new requests are rejected.
In-flight `stream.Append` calls are wrapped in `context.Context`, so
the SQLite driver cancels them on shutdown.

### Worker shutdown

The CLI installs a signal handler and propagates the context to
`Orchestrator.Run`. The Run loop checks `ctx.Err()` between
iterations and returns `nil` (a clean exit). If the worker is mid-
batch when the context is cancelled, the in-flight goroutines see
`ctx.Done()` and abort their current record; the cursor does not
advance past the aborted record, so it is re-processed on the next
start.

### Crash mid-batch

The orchestrator's cursor advances in seq order up to the highest
contiguous processed record. A crash leaves the cursor at the last
fully-processed record; on restart, the orchestrator re-reads from
there. The notification store is the source of truth for "already
delivered", so re-processing is safe and idempotent.

## Out of scope

The following failure modes are **not** handled because they are not
in the demo's scope:

- **Authentication bypass.** The API has no auth; deploy it behind a
  proxy if it is reachable from the public internet.
- **Rate limiting at the API.** A misbehaving client could flood the
  API with events. Use an upstream proxy or a rate limiter middleware
  if this matters.
- **Multi-region replication.** EventFlow runs against a single
  SQLite file. A regional outage takes the system with it.
- **Schema migration failures.** The schema is `CREATE TABLE IF NOT
  EXISTS`; there is no migration step. If the schema changes, the
  code is updated and the operator deletes the database (acceptable
  for a demo, not for production).

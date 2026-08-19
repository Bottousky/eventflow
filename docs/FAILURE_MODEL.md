# EventFlow Failure Model

This document enumerates the failure modes the code is designed to
handle, how each one is detected, and what the user-visible behaviour
is. The intent is to make the failure semantics explicit so they can
be reviewed, tested and reasoned about.

## Delivery semantics: at-least-once, not exactly-once

EventFlow provides **at-least-once** delivery with **durable/local
deduplication** and **provider-supplied idempotency keys**. It does
not implement exactly-once — that property is fundamentally
unachievable without either two-phase commit between the orchestrator
and the provider, or provider-side idempotency, and we make no
attempt to fake it.

The two layers that make the system safe for at-least-once:

1. **Notification store** (persistent). The orchestrator calls
   `EnsureNotification` which inserts or fetches the row keyed by
   `(event_id, channel)`. The row carries a status of
   `pending` / `delivered` / `dead`. A `delivered` or `dead` row is
   never re-sent, regardless of how many times the worker restarts —
   **provided `MarkDelivered` or `MarkDead` actually committed**.
2. **In-memory KVS** (in-flight). After the store check, the
   orchestrator calls `kvs.SetNX("event:channel")`. This mirrors Redis
   `SET NX PX` and is what prevents two worker replicas from
   double-sending the same notification while the persistent row is
   still `pending`.

The KVS is the right tool for "two replicas running at the same time";
the store is the right tool for "this delivery was already finalized
yesterday". Together they cover both the hot and the cold case —
**except** for the crash window described next.

## The crash window: a real, narrow at-least-once gap

There is a window in the current implementation between
`Sender.Send` returning success and `MarkDelivered` committing. A
crash in that window — the process dies after the provider accepted
the message but before the row was updated — leaves the notification
row at `pending`. The next worker that reads the record sees
`pending`, calls `Sender.Send` again, and the provider receives the
message a second time.

The system does **not** prevent this. The honest guarantee is:

- **Within the orchestrator's process**, the row's status moves to
  `delivered` exactly once (because `MarkDelivered` is a single SQL
  UPDATE on a UNIQUE-indexed row).
- **Across an orchestrator crash**, the row may still be `pending`
  on restart. The provider may receive the message twice.

The recommended mitigation in production is to derive a stable
provider-side idempotency key from the notification row's primary
key (`notifications.id`, formatted as a string), and pass it to the
provider's API as the idempotency key. Providers that support this
(every major email/push/SMS gateway does) collapse the duplicate on
replay. If the provider does **not** support idempotency keys, the
caller must accept that duplicate delivery is possible.

This is documented honestly here rather than hidden behind the phrase
"exactly once" so a reviewer can reason about it.

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
into `dead_letters`. **Both writes happen inside the same SQL
transaction**, with rollback on either failure. `events_dead_lettered_total`
is incremented.

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
the context error and the orchestrator reports `ChannelPending`. The
notification row stays at `pending` and the KVS lock is released so
the next worker can retry without waiting for the TTL (24h) to
expire. The cursor does not advance past this record; the next
`ProcessOnce` re-reads it.

## Duplicates

### Same event appended twice

The stream's `events` table has a `UNIQUE(id)` constraint. The second
append returns `ErrDuplicateID`, and the API responds with 500. The
event-id uniqueness is the at-most-once append contract; producers are
expected to retry on a 5xx with the same event id.

### Same event processed twice (worker restart, terminal state)

The orchestrator reads from the stream using a cursor. If the worker
crashes between processing a record and advancing the cursor, the
record is re-read on restart. The notification row is already
`delivered` or `dead`, so `EnsureNotification` returns the existing
row and the orchestrator skips the send. **This is the replay-safety
property the persistent store gives us.**

### Same event processed twice (worker restart, crash window)

If the worker crashed in the window between `Sender.Send` returning
success and `MarkDelivered` committing, the row is still `pending` on
restart. The next `ProcessOnce` calls `Sender.Send` again. The
provider sees a duplicate. Provider-side idempotency keys are the
mitigation; see "The crash window" above.

### Same event processed by two workers at the same time

Two workers running concurrently against the same store would both
find the notification `pending` and both call `Sender.Send`. The
in-memory KVS prevents this: each worker calls `kvs.SetNX(key)` on
the `(event_id, channel)` key. Only the first one wins; the second
sees the key already set, increments `duplicates_suppressed_total`,
reports `ChannelDuplicateSuppressed`, and the cursor stops before
the record so the next `ProcessOnce` re-reads it once the winning
worker has committed.

In a real production deployment the KVS would be Redis so all worker
replicas share it; in the demo, each worker has its own in-memory
KVS, which is enough to demonstrate the pattern within a single
process.

## Cursor and terminal states

The orchestrator's cursor is gated on **terminal durable state**:

- A record advances the cursor only when every channel it carries
  reached a terminal state (`delivered` or `dead`).
- A record that ends in a non-terminal state (pending, duplicate-
  suppressed) blocks the cursor for itself and for every record
  behind it in the same `ordering_key` partition.
- Within an `ordering_key` partition, when one record ends in a
  non-terminal state the goroutine stops processing later records
  in the same group. They are not lost — they are re-read on the
  next `ProcessOnce` once the pending record resolves.
- Records behind a pending record can be re-attempted on a future
  poll. Their terminal durable state makes the replay harmless for
  finalized ones, and the at-least-once + provider-idempotency
  story covers the crash-window case.

## Atomicity: status update + dead-letter insert

`MarkDead` runs `UPDATE notifications SET status='dead'` and
`INSERT INTO dead_letters ...` inside a single SQL transaction.
Either both commits, or both roll back. A `dead` notification with
no DLQ entry is impossible. The opposite was true in the previous
implementation and is covered by
`TestDeadLetterRollsBackOnInsertFailure`.

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
contiguous `RecordTerminal` record. A crash leaves the cursor at the
last fully-terminal record; on restart, the orchestrator re-reads
from there. Notifications whose status is `delivered` or `dead` are
skipped on replay; notifications still at `pending` are retried.

## Observability

### Metrics split by process

The API process and the worker process each own their own `*Metrics`
instance. The API exposes `/metrics` (default `:8080`) with
`events_received`. The worker exposes its own `/metrics` (default
`:9090`, configurable via `METRICS_ADDR`) with the orchestrator
counters (`events_processed`, `notifications_delivered`,
`delivery_retries`, `duplicates_suppressed`, `events_dead_lettered`,
`processing_errors`) and the processing-duration histogram.

In the previous implementation, the worker counters were
incremented inside the worker but never exposed anywhere: the API's
`/metrics` only knew `events_received`. Prometheus scrape config
should target both endpoints; both use the standard
`text/plain; version=0.0.4` exposition format.

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

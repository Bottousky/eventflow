-- EventFlow canonical schema. The Go code applies this schema on first
-- use via CREATE TABLE IF NOT EXISTS, so applying it manually is only
-- required if you want to inspect or warm the database ahead of time.
-- Keep this file in sync with internal/store/store.go and
-- internal/stream/stream.go.

CREATE TABLE IF NOT EXISTS events (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  id           TEXT NOT NULL UNIQUE,
  type         TEXT NOT NULL,
  ordering_key TEXT NOT NULL,
  recipient    TEXT NOT NULL,
  channels     TEXT NOT NULL,
  payload      TEXT NOT NULL DEFAULT '{}',
  created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_ordering_key ON events (ordering_key, seq);

CREATE TABLE IF NOT EXISTS notifications (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id   TEXT NOT NULL,
  channel    TEXT NOT NULL,
  status     TEXT NOT NULL,
  attempts   INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  UNIQUE (event_id, channel)
);
CREATE INDEX IF NOT EXISTS notifications_event_id ON notifications (event_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  notification_id INTEGER NOT NULL REFERENCES notifications(id),
  error           TEXT NOT NULL,
  at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS delivery_attempts_notification_id ON delivery_attempts (notification_id);

CREATE TABLE IF NOT EXISTS dead_letters (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id   TEXT NOT NULL,
  channel    TEXT NOT NULL,
  error      TEXT NOT NULL,
  at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS dead_letters_event_id ON dead_letters (event_id);

CREATE TABLE IF NOT EXISTS cursor (
  id       INTEGER PRIMARY KEY CHECK (id = 1),
  last_seq INTEGER NOT NULL
);

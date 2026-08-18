// Package stream implements the append-only event stream. Events are stored
// in a SQLite table with an autoincrement sequence, which gives a total
// append order; consumers read forward from a cursor, so events sharing an
// ordering key are always processed in the order they were appended. In
// production this component maps to Kafka, SQS or a similar broker.
package stream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Bottousky/eventflow/internal/events"
)

// Record is one stored stream entry: an event plus its stream sequence.
type Record struct {
	Seq   int64
	Event events.Event
}

// Stream appends and reads events on a shared *sql.DB.
type Stream struct {
	db *sql.DB
}

// New creates the stream schema on db.
func New(db *sql.DB) (*Stream, error) {
	const schema = `
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
CREATE INDEX IF NOT EXISTS events_ordering_key ON events (ordering_key, seq);`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create stream schema: %w", err)
	}
	return &Stream{db: db}, nil
}

// ErrDuplicateID is returned when an event ID is appended twice. Event IDs
// are unique, which gives producers at-most-once append semantics on retry.
var ErrDuplicateID = errors.New("event id already exists")

// Append stores e and returns its stream sequence.
func (s *Stream) Append(ctx context.Context, e events.Event) (int64, error) {
	channels, err := json.Marshal(e.Channels)
	if err != nil {
		return 0, fmt.Errorf("marshal channels: %w", err)
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO events (id, type, ordering_key, recipient, channels, payload, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.Type, e.OrderingKey, e.Recipient, string(channels), string(payload),
		e.CreatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return 0, ErrDuplicateID
		}
		return 0, fmt.Errorf("append event: %w", err)
	}
	return res.LastInsertId()
}

func isUniqueViolation(err error) bool {
	var sqliteErr interface{ Code() int }
	if errors.As(err, &sqliteErr) {
		// 1555 = SQLITE_CONSTRAINT_PRIMARYKEY, 2067 = SQLITE_CONSTRAINT_UNIQUE.
		return sqliteErr.Code() == 1555 || sqliteErr.Code() == 2067
	}
	return false
}

// ReadAfter returns up to limit records with seq > afterSeq, in ascending
// sequence order. Reading in ascending seq order is what guarantees that
// events sharing an ordering key are consumed in append order.
func (s *Stream) ReadAfter(ctx context.Context, afterSeq int64, limit int) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, id, type, ordering_key, recipient, channels, payload, created_at
		 FROM events WHERE seq > ? ORDER BY seq ASC LIMIT ?`, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("read stream: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var channels, payload, created string
		if err := rows.Scan(&r.Seq, &r.Event.ID, &r.Event.Type, &r.Event.OrderingKey,
			&r.Event.Recipient, &channels, &payload, &created); err != nil {
			return nil, fmt.Errorf("scan record: %w", err)
		}
		if err := json.Unmarshal([]byte(channels), &r.Event.Channels); err != nil {
			return nil, fmt.Errorf("decode channels of event %s: %w", r.Event.ID, err)
		}
		if err := json.Unmarshal([]byte(payload), &r.Event.Payload); err != nil {
			return nil, fmt.Errorf("decode payload of event %s: %w", r.Event.ID, err)
		}
		r.Event.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Get loads one event by ID.
func (s *Stream) Get(ctx context.Context, id string) (events.Event, error) {
	var e events.Event
	var channels, payload, created string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, type, ordering_key, recipient, channels, payload, created_at
		 FROM events WHERE id = ?`, id).
		Scan(&e.ID, &e.Type, &e.OrderingKey, &e.Recipient, &channels, &payload, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return events.Event{}, fmt.Errorf("event %s: %w", id, sql.ErrNoRows)
	}
	if err != nil {
		return events.Event{}, fmt.Errorf("get event: %w", err)
	}
	if err := json.Unmarshal([]byte(channels), &e.Channels); err != nil {
		return events.Event{}, fmt.Errorf("decode channels: %w", err)
	}
	if err := json.Unmarshal([]byte(payload), &e.Payload); err != nil {
		return events.Event{}, fmt.Errorf("decode payload: %w", err)
	}
	e.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return e, nil
}

// Package store persists notifications, delivery attempts, the dead-letter
// queue and the worker cursor in SQLite. It uses the pure-Go
// modernc.org/sqlite driver so the whole system builds and tests without
// CGO or external services. In production this maps to Postgres/MySQL.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Bottousky/eventflow/internal/events"
	_ "modernc.org/sqlite" // database/sql driver "sqlite"
)

// Open opens (creating if needed) the SQLite database at path and applies
// pragmas that make concurrent access from the API and worker processes
// safe. Use ":memory:" for an in-memory database in tests.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection avoids "database is locked" errors with SQLite's
	// writer serialization while keeping WAL available for multi-process use.
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}
	return db, nil
}

// Store owns the delivery-related tables on a shared *sql.DB.
type Store struct {
	db *sql.DB
}

// New creates the store schema on db.
func New(db *sql.DB) (*Store, error) {
	const schema = `
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
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  notification_id INTEGER NOT NULL REFERENCES notifications(id),
  error          TEXT NOT NULL,
  at             TEXT NOT NULL
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
);`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create store schema: %w", err)
	}
	return &Store{db: db}, nil
}

const timeFormat = time.RFC3339Nano

// EnsureNotification inserts a pending (event, channel) notification row, or
// returns the existing one. It is idempotent by construction (UNIQUE
// constraint).
func (s *Store) EnsureNotification(ctx context.Context, eventID string, ch events.Channel) (events.Notification, error) {
	now := time.Now().UTC().Format(timeFormat)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO notifications (event_id, channel, status, updated_at)
	 VALUES (?, ?, ?, ?)
	 ON CONFLICT (event_id, channel) DO NOTHING`,
		eventID, string(ch), string(events.StatusPending), now)
	if err != nil {
		return events.Notification{}, fmt.Errorf("ensure notification: %w", err)
	}
	return s.GetNotification(ctx, eventID, ch)
}

// ErrNotFound is returned when a notification row does not exist.
var ErrNotFound = errors.New("notification not found")

// GetNotification loads one notification row by (event_id, channel).
func (s *Store) GetNotification(ctx context.Context, eventID string, ch events.Channel) (events.Notification, error) {
	return s.getNotificationByEventChannel(ctx, "WHERE event_id = ? AND channel = ?", eventID, string(ch))
}

// GetNotificationByID loads one notification row by its primary key.
func (s *Store) GetNotificationByID(ctx context.Context, id int64) (events.Notification, error) {
	return s.getNotificationByEventChannel(ctx, "WHERE id = ?", id)
}

func (s *Store) getNotificationByEventChannel(ctx context.Context, where string, args ...any) (events.Notification, error) {
	var n events.Notification
	var status, channel, updated string
	err := s.db.QueryRowContext(ctx,
		`SELECT id, event_id, channel, status, attempts, last_error, updated_at
	 FROM notifications `+where, args...).
		Scan(&n.ID, &n.EventID, &channel, &status, &n.Attempts, &n.LastError, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return events.Notification{}, ErrNotFound
	}
	if err != nil {
		return events.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	n.Channel = events.Channel(channel)
	n.Status = events.Status(status)
	n.UpdatedAt, _ = time.Parse(timeFormat, updated)
	return n, nil
}

// NotificationsForEvent lists every notification row of an event, ordered by
// id (which reflects the channel processing order).
func (s *Store) NotificationsForEvent(ctx context.Context, eventID string) ([]events.Notification, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, channel, status, attempts, last_error, updated_at
	 FROM notifications WHERE event_id = ? ORDER BY id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()
	var out []events.Notification
	for rows.Next() {
		var n events.Notification
		var status, channel, updated string
		if err := rows.Scan(&n.ID, &n.EventID, &channel, &status, &n.Attempts, &n.LastError, &updated); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		n.Channel = events.Channel(channel)
		n.Status = events.Status(status)
		n.UpdatedAt, _ = time.Parse(timeFormat, updated)
		out = append(out, n)
	}
	return out, rows.Err()
}

// RecordAttempt appends a failed attempt and bumps the attempt counter.
func (s *Store) RecordAttempt(ctx context.Context, notificationID int64, cause error) error {
	now := time.Now().UTC().Format(timeFormat)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin attempt tx: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO delivery_attempts (notification_id, error, at) VALUES (?, ?, ?)`,
		notificationID, cause.Error(), now); err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE notifications SET attempts = attempts + 1, last_error = ?, updated_at = ? WHERE id = ?`,
		cause.Error(), now, notificationID); err != nil {
		return fmt.Errorf("bump attempts: %w", err)
	}
	return tx.Commit()
}

// MarkDelivered sets the notification status to delivered.
func (s *Store) MarkDelivered(ctx context.Context, notificationID int64) error {
	return s.setStatus(ctx, notificationID, events.StatusDelivered)
}

// MarkDead sets the notification status to dead and writes a dead-letter
// entry for later inspection and reprocessing.
//
// Both writes happen inside the same SQL transaction so that the system
// never sees a `dead` notification without a matching dead_letters row.
// If either step fails (disk full, schema drift, lock timeout), the
// transaction is rolled back and the notification stays in `pending`
// state, which means a future ProcessOnce will retry the delivery
// instead of silently losing the operator signal.
func (s *Store) MarkDead(ctx context.Context, n events.Notification, cause error) error {
	now := time.Now().UTC().Format(timeFormat)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dead-letter tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx,
		`UPDATE notifications SET status = ?, updated_at = ? WHERE id = ?`,
		string(events.StatusDead), now, n.ID); err != nil {
		return fmt.Errorf("set status dead: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO dead_letters (event_id, channel, error, at) VALUES (?, ?, ?, ?)`,
		n.EventID, string(n.Channel), cause.Error(), now); err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dead-letter tx: %w", err)
	}
	committed = true
	return nil
}

// DeadLetter is one dead-letter queue entry.
type DeadLetter struct {
	ID      int64     `json:"id"`
	EventID string    `json:"event_id"`
	Channel string    `json:"channel"`
	Error   string    `json:"error"`
	At      time.Time `json:"at"`
}

// DeadLetters lists the dead-letter queue, oldest first.
func (s *Store) DeadLetters(ctx context.Context) ([]DeadLetter, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, event_id, channel, error, at FROM dead_letters ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var dl DeadLetter
		var at string
		if err := rows.Scan(&dl.ID, &dl.EventID, &dl.Channel, &dl.Error, &at); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		dl.At, _ = time.Parse(timeFormat, at)
		out = append(out, dl)
	}
	return out, rows.Err()
}

func (s *Store) setStatus(ctx context.Context, notificationID int64, status events.Status) error {
	now := time.Now().UTC().Format(timeFormat)
	if _, err := s.db.ExecContext(ctx,
		`UPDATE notifications SET status = ?, updated_at = ? WHERE id = ?`,
		string(status), now, notificationID); err != nil {
		return fmt.Errorf("set status %s: %w", status, err)
	}
	return nil
}

// Cursor returns the sequence the worker last processed (0 on first run).
func (s *Store) Cursor(ctx context.Context) (int64, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, `SELECT last_seq FROM cursor WHERE id = 1`).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor: %w", err)
	}
	return seq, nil
}

// SetCursor persists the worker cursor.
func (s *Store) SetCursor(ctx context.Context, seq int64) error {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO cursor (id, last_seq) VALUES (1, ?)
	 ON CONFLICT (id) DO UPDATE SET last_seq = excluded.last_seq`, seq); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	return nil
}

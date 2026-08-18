// Package api exposes the REST surface of EventFlow: accepting events,
// reporting their delivery status, health and metrics.
package api

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/obs"
	"github.com/Bottousky/eventflow/internal/store"
	"github.com/Bottousky/eventflow/internal/stream"
)

// Server is the EventFlow HTTP API.
type Server struct {
	stream  *stream.Stream
	store   *store.Store
	metrics *obs.Metrics
	logger  *slog.Logger
	newID   func() string
}

// New builds a Server. logger may be nil.
func New(st *stream.Stream, db *store.Store, metrics *obs.Metrics, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		stream: st, store: db, metrics: metrics, logger: logger,
		newID: func() string {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			return "evt_" + hex.EncodeToString(b)
		},
	}
}

// Handler returns the API routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.postEvent)
	mux.HandleFunc("GET /events/{id}", s.getEvent)
	mux.HandleFunc("GET /notifications/{id}", s.getNotification)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", s.metrics.Handler())
	return s.withRequestID(mux)
}

type postEventRequest struct {
	Type        string            `json:"type"`
	OrderingKey string            `json:"ordering_key"`
	Recipient   string            `json:"recipient"`
	Channels    []events.Channel  `json:"channels"`
	Payload     map[string]string `json:"payload"`
}

type postEventResponse struct {
	EventID string `json:"event_id"`
	Seq     int64  `json:"seq"`
	Status  string `json:"status"`
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	var req postEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	e := events.Event{
		ID:          s.newID(),
		Type:        req.Type,
		OrderingKey: req.OrderingKey,
		Recipient:   req.Recipient,
		Channels:    req.Channels,
		Payload:     req.Payload,
		CreatedAt:   time.Now().UTC(),
	}
	if err := e.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	seq, err := s.stream.Append(r.Context(), e)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "append event failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not append event")
		return
	}
	s.metrics.Inc(obs.EventsReceived)
	s.logger.InfoContext(r.Context(), "event accepted",
		"event_id", e.ID, "type", e.Type, "seq", seq, "ordering_key", e.OrderingKey)
	writeJSON(w, http.StatusAccepted, postEventResponse{EventID: e.ID, Seq: seq, Status: "accepted"})
}

type eventStatusResponse struct {
	Event         events.Event          `json:"event"`
	Notifications []events.Notification `json:"notifications"`
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	e, err := s.stream.Get(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	if err != nil {
		s.logger.ErrorContext(r.Context(), "get event failed", "event_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not load event")
		return
	}
	notifications, err := s.store.NotificationsForEvent(r.Context(), id)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "list notifications failed", "event_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not load notifications")
		return
	}
	if notifications == nil {
		notifications = []events.Notification{} // never serialize null
	}
	writeJSON(w, http.StatusOK, eventStatusResponse{Event: e, Notifications: notifications})
}

func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "notification id must be a positive integer")
		return
	}
	n, err := s.store.GetNotificationByID(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "notification not found")
		return
	}
	if err != nil {
		s.logger.ErrorContext(r.Context(), "get notification failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "could not load notification")
		return
	}
	writeJSON(w, http.StatusOK, n)
}

// withRequestID assigns or echoes a request ID, makes it available via
// context, and emits it as the X-Request-ID response header.
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			var b [8]byte
			_, _ = rand.Read(b[:])
			id = "req_" + hex.EncodeToString(b[:])
		}
		w.Header().Set("X-Request-ID", id)
		ctx := contextWithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type ctxKey int

const ctxKeyRequestID ctxKey = 1

// RequestIDFromContext returns the request id stored in ctx, or "" if none.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRequestID).(string)
	return v
}

func contextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

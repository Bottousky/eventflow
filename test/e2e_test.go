// Package e2e runs the whole pipeline the way it runs in production: the
// API and the worker are two separate stacks sharing the same SQLite file,
// one accepting events and the other orchestrating delivery.
package e2e

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Bottousky/eventflow/internal/api"
	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/kvs"
	"github.com/Bottousky/eventflow/internal/obs"
	"github.com/Bottousky/eventflow/internal/orchestrator"
	"github.com/Bottousky/eventflow/internal/senders"
	"github.com/Bottousky/eventflow/internal/store"
	"github.com/Bottousky/eventflow/internal/stream"
)

func TestEndToEndDeliveryAcrossSeparateStacks(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "eventflow.db")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// "Process 1": the API.
	apiDB, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("api open db: %v", err)
	}
	defer apiDB.Close()
	apiStream, err := stream.New(apiDB)
	if err != nil {
		t.Fatalf("api stream: %v", err)
	}
	apiStore, err := store.New(apiDB)
	if err != nil {
		t.Fatalf("api store: %v", err)
	}
	metrics := obs.New()
	srv := httptest.NewServer(api.New(apiStream, apiStore, metrics, logger).Handler())
	defer srv.Close()

	// "Process 2": the worker, with its own connection to the same file.
	workerDB, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("worker open db: %v", err)
	}
	defer workerDB.Close()
	workerStream, err := stream.New(workerDB)
	if err != nil {
		t.Fatalf("worker stream: %v", err)
	}
	workerStore, err := store.New(workerDB)
	if err != nil {
		t.Fatalf("worker store: %v", err)
	}
	// The email channel fails twice before succeeding, to exercise retries.
	registry := senders.DefaultRegistry(logger)
	registry[events.ChannelEmail].(*senders.Simulated).FailFirstN = 2
	cfg := orchestrator.DefaultConfig()
	cfg.Sleep = func(context.Context, time.Duration) error { return nil }
	o := orchestrator.New(workerStream, workerStore, kvs.New(24*time.Hour, time.Now), registry, obs.New(), logger, cfg)

	// Client: submit an event through the REST API.
	resp, err := http.Post(srv.URL+"/events", "application/json", strings.NewReader(`{
		"type": "order.shipped",
		"ordering_key": "user-1001",
		"recipient": "ana@example.com",
		"channels": ["email", "push"],
		"payload": {"order_id": "ORD-7841"}
	}`))
	if err != nil {
		t.Fatalf("post event: %v", err)
	}
	var accepted struct {
		EventID string `json:"event_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode accept: %v", err)
	}
	resp.Body.Close()
	if accepted.EventID == "" {
		t.Fatal("API did not return an event_id")
	}

	// Worker: process the stream.
	processed, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	// Client: the event status endpoint reports the final delivery state.
	statusResp, err := http.Get(srv.URL + "/events/" + accepted.EventID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	defer statusResp.Body.Close()
	var status struct {
		Notifications []struct {
			Channel  string `json:"channel"`
			Status   string `json:"status"`
			Attempts int    `json:"attempts"`
		} `json:"notifications"`
	}
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(status.Notifications) != 2 {
		t.Fatalf("notifications = %+v, want 2 channels", status.Notifications)
	}
	byChannel := map[string]struct {
		Status   string
		Attempts int
	}{}
	for _, n := range status.Notifications {
		byChannel[n.Channel] = struct {
			Status   string
			Attempts int
		}{n.Status, n.Attempts}
	}
	email := byChannel["email"]
	if email.Status != "delivered" || email.Attempts != 2 {
		t.Fatalf("email notification = %+v, want delivered after 2 recorded attempts", email)
	}
	if push := byChannel["push"]; push.Status != "delivered" || push.Attempts != 0 {
		t.Fatalf("push notification = %+v, want delivered on first attempt", push)
	}
	if got := metrics.Snapshot()[obs.EventsReceived]; got != 1 {
		t.Fatalf("events_received = %d, want 1", got)
	}

	// The /notifications/{id} endpoint must return each delivered row.
	for _, n := range status.Notifications {
		notification, err := workerStore.GetNotification(ctx, accepted.EventID, events.Channel(n.Channel))
		if err != nil {
			t.Fatalf("get notification: %v", err)
		}
		notifResp, err := http.Get(srv.URL + "/notifications/" + itoa(notification.ID))
		if err != nil {
			t.Fatalf("get notif endpoint: %v", err)
		}
		notifResp.Body.Close()
		if notifResp.StatusCode != http.StatusOK {
			t.Fatalf("notif endpoint status = %d, want 200", notifResp.StatusCode)
		}
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

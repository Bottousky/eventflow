package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

// testServer bundles the httptest server with the underlying DB and
// stream so tests can directly create events and notifications.
type testServer struct {
	srv *httptest.Server
	db  *store.Store
	st  *stream.Stream
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	st, err := stream.New(db)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	dbs, err := store.New(db)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	srv := httptest.NewServer(api.New(st, dbs, obs.New(), nil).Handler())
	t.Cleanup(func() {
		srv.Close()
		db.Close()
	})
	return &testServer{srv: srv, db: dbs, st: st}
}

// newTestKVS returns a fresh in-memory KVS with a long TTL. The tests
// that drive the orchestrator from the API test suite use it instead of
// the production KVS, because the KVS is per-orchestrator.
func newTestKVS(t *testing.T) *kvs.Store {
	t.Helper()
	return kvs.New(time.Hour, time.Now)
}

func postEvent(t *testing.T, ts *testServer, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(ts.srv.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, out
}

func TestPostEventAccepted(t *testing.T) {
	ts := newTestServer(t)
	status, out := postEvent(t, ts, `{
		"type": "order.shipped",
		"ordering_key": "user-1",
		"recipient": "ana@example.com",
		"channels": ["email", "push"],
		"payload": {"order_id": "ORD-1"}
	}`)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %v)", status, out)
	}
	id, _ := out["event_id"].(string)
	if !strings.HasPrefix(id, "evt_") {
		t.Fatalf("event_id = %q, want evt_ prefix", id)
	}
	if out["status"] != "accepted" {
		t.Fatalf("status field = %v", out["status"])
	}
}

func TestPostEventValidation(t *testing.T) {
	ts := newTestServer(t)
	cases := map[string]string{
		"missing type":      `{"ordering_key": "u1", "recipient": "a@b.c", "channels": ["email"]}`,
		"missing key":       `{"type": "t", "recipient": "a@b.c", "channels": ["email"]}`,
		"missing recipient": `{"type": "t", "ordering_key": "u1", "channels": ["email"]}`,
		"empty channels":    `{"type": "t", "ordering_key": "u1", "recipient": "a@b.c", "channels": []}`,
		"unknown channel":   `{"type": "t", "ordering_key": "u1", "recipient": "a@b.c", "channels": ["sms"]}`,
		"invalid json":      `not-json`,
	}
	for name, body := range cases {
		status, out := postEvent(t, ts, body)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", name, status)
		}
		if out["error"] == nil || out["error"] == "" {
			t.Fatalf("%s: response must carry an error message, got %v", name, out)
		}
	}
}

func TestGetEventStatus(t *testing.T) {
	ts := newTestServer(t)
	_, out := postEvent(t, ts, `{
		"type": "password.reset_requested",
		"ordering_key": "user-7",
		"recipient": "luis@example.com",
		"channels": ["email"]
	}`)
	id, _ := out["event_id"].(string)

	resp, err := http.Get(ts.srv.URL + "/events/" + id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Event struct {
			ID          string `json:"event_id"`
			OrderingKey string `json:"ordering_key"`
		} `json:"event"`
		Notifications []any `json:"notifications"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Event.ID != id || body.Event.OrderingKey != "user-7" {
		t.Fatalf("event mismatch: %+v", body.Event)
	}
	// No worker is running in this test, so notifications should be
	// initialized to an empty list (not null) in the response.
	if body.Notifications == nil {
		t.Fatal("notifications field must be an empty array, got null")
	}
}

func TestGetEventNotFound(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.srv.URL + "/events/evt_missing")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	ts := newTestServer(t)

	resp, err := http.Get(ts.srv.URL + "/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}

	if _, out := postEvent(t, ts, `{
		"type": "t", "ordering_key": "u1", "recipient": "a@b.c", "channels": ["email"]
	}`); out["event_id"] == nil {
		t.Fatal("post failed")
	}

	resp, err = http.Get(ts.srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("metrics content-type = %q, want text/plain", ct)
	}
	text := string(raw)
	wantLine := "eventflow_events_received_total 1"
	if !strings.Contains(text, wantLine) {
		t.Fatalf("metrics output missing %q\nfull body:\n%s", wantLine, text)
	}
}

func TestRequestIDIsPropagated(t *testing.T) {
	ts := newTestServer(t)

	// When the caller provides one, the server must echo it back.
	req, _ := http.NewRequest("GET", ts.srv.URL+"/health", nil)
	req.Header.Set("X-Request-ID", "req_fixed_42")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Request-ID"); got != "req_fixed_42" {
		t.Fatalf("X-Request-ID = %q, want req_fixed_42", got)
	}

	// When the caller does not provide one, the server must generate one.
	resp, err = http.Get(ts.srv.URL + "/health")
	if err != nil {
		t.Fatalf("health no-id: %v", err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Request-ID"); !strings.HasPrefix(got, "req_") {
		t.Fatalf("X-Request-ID auto = %q, want req_ prefix", got)
	}
}

func TestGetNotificationEndpoint(t *testing.T) {
	ts := newTestServer(t)
	ctx := context.Background()

	// Non-numeric id must be 400.
	resp, err := http.Get(ts.srv.URL + "/notifications/notanumber")
	if err != nil {
		t.Fatalf("get notif bad: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad id status = %d, want 400", resp.StatusCode)
	}

	// Non-existent id must be 404.
	resp, err = http.Get(ts.srv.URL + "/notifications/9999")
	if err != nil {
		t.Fatalf("get notif 404: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing id status = %d, want 404", resp.StatusCode)
	}

	// Post an event and process it through the orchestrator so a
	// notification row is actually created in the store.
	_, out := postEvent(t, ts, `{
		"type": "order.shipped",
		"ordering_key": "user-1",
		"recipient": "ana@example.com",
		"channels": ["email"]
	}`)
	id, _ := out["event_id"].(string)
	kv := newTestKVS(t)
	o := orchestrator.New(ts.st, ts.db, kv, senders.DefaultRegistry(nil), obs.New(), nil, orchestrator.DefaultConfig())
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	notif, err := ts.db.GetNotification(ctx, id, events.ChannelEmail)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}

	resp, err = http.Get(ts.srv.URL + "/notifications/" + intToStr(notif.ID))
	if err != nil {
		t.Fatalf("get notif: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("notif status = %d, want 200", resp.StatusCode)
	}
	var body events.Notification
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.EventID != id || body.Channel != events.ChannelEmail || body.Status != events.StatusDelivered {
		t.Fatalf("notification body = %+v, want event_id=%s channel=email status=delivered", body, id)
	}
}

func TestPostEventSeqIsMonotonic(t *testing.T) {
	ts := newTestServer(t)
	var seqs []int64
	for i := 0; i < 3; i++ {
		status, out := postEvent(t, ts, `{
			"type": "t", "ordering_key": "u", "recipient": "a@b.c", "channels": ["email"]
		}`)
		if status != http.StatusAccepted {
			t.Fatalf("post %d: status = %d", i, status)
		}
		seqs = append(seqs, int64(out["seq"].(float64)))
	}
	if !(seqs[0] < seqs[1] && seqs[1] < seqs[2]) {
		t.Fatalf("seqs not monotonic: %v", seqs)
	}
}

func TestPostEventGeneratesDistinctIDs(t *testing.T) {
	ts := newTestServer(t)
	body := `{
		"type": "t", "ordering_key": "u", "recipient": "a@b.c", "channels": ["email"]
	}`
	resp, err := http.Post(ts.srv.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post 1: %v", err)
	}
	var out1 map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out1)
	resp.Body.Close()

	resp, err = http.Post(ts.srv.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post 2: %v", err)
	}
	var out2 map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out2)
	resp.Body.Close()

	id1, _ := out1["event_id"].(string)
	id2, _ := out2["event_id"].(string)
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("expected two distinct event_ids, got %q and %q", id1, id2)
	}
}

// intToStr converts a notification id to its decimal string form.
func intToStr(v int64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
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

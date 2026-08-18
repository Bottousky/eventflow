package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestFromEnvUsesDefaultsWhenUnset(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("DB_PATH", "")
	t.Setenv("MAX_ATTEMPTS", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("BASE_BACKOFF", "")
	t.Setenv("LOG_LEVEL", "")

	got, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	def := Default()
	if got != def {
		t.Fatalf("FromEnv with no env = %+v, want defaults %+v", got, def)
	}
}

func TestFromEnvReadsOverrides(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9001")
	t.Setenv("DB_PATH", "/tmp/eventflow.db")
	t.Setenv("MAX_ATTEMPTS", "5")
	t.Setenv("WORKER_CONCURRENCY", "8")
	t.Setenv("POLL_INTERVAL", "2s")
	t.Setenv("BASE_BACKOFF", "50ms")
	t.Setenv("LOG_LEVEL", "debug")

	got, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if got.HTTPAddr != ":9001" {
		t.Fatalf("HTTPAddr = %q, want :9001", got.HTTPAddr)
	}
	if got.DBPath != "/tmp/eventflow.db" {
		t.Fatalf("DBPath = %q", got.DBPath)
	}
	if got.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d", got.MaxAttempts)
	}
	if got.WorkerConcurrency != 8 {
		t.Fatalf("WorkerConcurrency = %d", got.WorkerConcurrency)
	}
	if got.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v", got.PollInterval)
	}
	if got.BaseBackoff != 50*time.Millisecond {
		t.Fatalf("BaseBackoff = %v", got.BaseBackoff)
	}
	if got.LogLevel != slog.LevelDebug {
		t.Fatalf("LogLevel = %v", got.LogLevel)
	}
}

func TestFromEnvRejectsInvalidValues(t *testing.T) {
	cases := map[string]string{
		"MAX_ATTEMPTS":       "0",
		"WORKER_CONCURRENCY": "-1",
		"POLL_INTERVAL":      "not-a-duration",
		"LOG_LEVEL":          "verbose",
	}
	for key, value := range cases {
		t.Setenv(key, value)
		if _, err := FromEnv(); err == nil {
			t.Fatalf("FromEnv with %s=%q must fail", key, value)
		}
		// Reset the env for the next case so we don't poison subsequent runs.
		t.Setenv(key, "")
	}
}

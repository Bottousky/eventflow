// Package config loads EventFlow configuration from environment variables
// (with a sane default for every key). The defaults match what a developer
// would want when running `go run ./cmd/eventflow api` with no setup.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full set of tunables for the API and worker processes.
// Every field is loaded from an environment variable; see the field
// comment for the name and default.
type Config struct {
	// HTTP_ADDR overrides the API listen address (default ":8080").
	HTTPAddr string
	// DB_PATH is the SQLite file the API and worker share (default "eventflow.db").
	DBPath string
	// MAX_ATTEMPTS caps the number of send attempts before dead-lettering (default 3).
	MaxAttempts int
	// WORKER_CONCURRENCY bounds the number of in-flight ordering-key partitions
	// the worker processes in parallel (default 4, minimum 1).
	WorkerConcurrency int
	// POLL_INTERVAL is the wait between empty stream polls in the worker (default 500ms).
	PollInterval time.Duration
	// BASE_BACKOFF is the first retry backoff (default 100ms, doubled per attempt, capped at 2s).
	BaseBackoff time.Duration
	// LOG_LEVEL is the slog log level: debug|info|warn|error (default "info").
	LogLevel slog.Level
}

// Default returns the default configuration used when no environment
// variables are set.
func Default() Config {
	return Config{
		HTTPAddr:          ":8080",
		DBPath:            "eventflow.db",
		MaxAttempts:       3,
		WorkerConcurrency: 4,
		PollInterval:      500 * time.Millisecond,
		BaseBackoff:       100 * time.Millisecond,
		LogLevel:          slog.LevelInfo,
	}
}

// FromEnv builds a Config by reading well-known environment variables
// and falling back to the default for any missing key.
func FromEnv() (Config, error) {
	c := Default()
	var err error
	if c.HTTPAddr, err = getString("HTTP_ADDR", c.HTTPAddr); err != nil {
		return Config{}, err
	}
	if c.DBPath, err = getString("DB_PATH", c.DBPath); err != nil {
		return Config{}, err
	}
	if c.MaxAttempts, err = getInt("MAX_ATTEMPTS", c.MaxAttempts); err != nil {
		return Config{}, err
	}
	if c.WorkerConcurrency, err = getInt("WORKER_CONCURRENCY", c.WorkerConcurrency); err != nil {
		return Config{}, err
	}
	if c.PollInterval, err = getDuration("POLL_INTERVAL", c.PollInterval); err != nil {
		return Config{}, err
	}
	if c.BaseBackoff, err = getDuration("BASE_BACKOFF", c.BaseBackoff); err != nil {
		return Config{}, err
	}
	if c.LogLevel, err = getLevel("LOG_LEVEL", c.LogLevel); err != nil {
		return Config{}, err
	}
	if c.WorkerConcurrency < 1 {
		return Config{}, fmt.Errorf("WORKER_CONCURRENCY must be >= 1, got %d", c.WorkerConcurrency)
	}
	if c.MaxAttempts < 1 {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be >= 1, got %d", c.MaxAttempts)
	}
	return c, nil
}

// ApplyLog sets slog's default logger to a text handler at the configured
// level. It returns the configured logger so callers can also pass it
// explicitly to components that want a non-default logger.
func (c Config) ApplyLog() *slog.Logger {
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: c.LogLevel})
	logger := slog.New(h)
	slog.SetDefault(logger)
	return logger
}

func getString(key, def string) (string, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	return v, nil
}

func getInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func getLevel(key string, def slog.Level) (slog.Level, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s: unknown level %q (debug|info|warn|error)", key, v)
	}
}

// Command eventflow runs the EventFlow system: a REST API that accepts
// events, and a worker that orchestrates notification delivery.
//
// Usage:
//
//	eventflow api    [-addr :8080] [-db eventflow.db]
//	eventflow worker [-db eventflow.db] [-fail email=2] [-once] [-metrics :9090]
//	eventflow demo   [-api http://localhost:8080]
//
// All flags are optional: sensible defaults are read from environment
// variables (HTTP_ADDR, METRICS_ADDR, DB_PATH, MAX_ATTEMPTS,
// WORKER_CONCURRENCY, POLL_INTERVAL, BASE_BACKOFF, LOG_LEVEL) when the
// flag is omitted.
//
// The api and worker are separate processes sharing the SQLite stream,
// the same way production services would share a broker and a database.
// Each process exposes its own /metrics endpoint: the API serves the
// events_received counter, the worker serves the orchestrator counters
// and the processing-duration histogram. Prometheus scrapes both.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Bottousky/eventflow/internal/api"
	"github.com/Bottousky/eventflow/internal/config"
	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/kvs"
	"github.com/Bottousky/eventflow/internal/obs"
	"github.com/Bottousky/eventflow/internal/orchestrator"
	"github.com/Bottousky/eventflow/internal/senders"
	"github.com/Bottousky/eventflow/internal/store"
	"github.com/Bottousky/eventflow/internal/stream"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cfg, err := config.FromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(2)
	}
	logger := cfg.ApplyLog()

	var runErr error
	switch os.Args[1] {
	case "api":
		runErr = runAPI(os.Args[2:], cfg, logger)
	case "worker":
		runErr = runWorker(os.Args[2:], cfg, logger)
	case "demo":
		runErr = runDemo(os.Args[2:], cfg)
	default:
		usage()
		os.Exit(2)
	}
	if runErr != nil {
		logger.Error("fatal", "error", runErr)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: eventflow <command> [flags]

commands:
  api      run the REST API (accepts events, serves health/metrics/status)
  worker   run the notification orchestrator and senders
  demo     post sample events to a running API

environment variables (override defaults):
  HTTP_ADDR            listen address for the API (default :8080)
  METRICS_ADDR         listen address for the worker's /metrics (default :9090; "" disables)
  DB_PATH              SQLite database path (default eventflow.db)
  MAX_ATTEMPTS         max send attempts before dead-lettering (default 3)
  WORKER_CONCURRENCY   max in-flight ordering-key partitions (default 4)
  POLL_INTERVAL        wait between empty stream polls (default 500ms)
  BASE_BACKOFF         first retry backoff, doubled per attempt (default 100ms)
  LOG_LEVEL            debug|info|warn|error (default info)`)
}

// openStack opens the database and every component that shares it.
func openStack(dbPath string) (*stream.Stream, *store.Store, func() error, error) {
	db, err := store.Open(dbPath)
	if err != nil {
		return nil, nil, nil, err
	}
	st, err := stream.New(db)
	if err != nil {
		db.Close()
		return nil, nil, nil, err
	}
	dbs, err := store.New(db)
	if err != nil {
		db.Close()
		return nil, nil, nil, err
	}
	return st, dbs, db.Close, nil
}

func runAPI(args []string, cfg config.Config, logger *slog.Logger) error {
	fs := flag.NewFlagSet("api", flag.ExitOnError)
	addr := fs.String("addr", cfg.HTTPAddr, "listen address (overrides HTTP_ADDR)")
	dbPath := fs.String("db", cfg.DBPath, "SQLite database path (overrides DB_PATH)")
	_ = fs.Parse(args)

	st, dbs, closeDB, err := openStack(*dbPath)
	if err != nil {
		return err
	}
	defer closeDB()

	metrics := obs.New()
	srv := &http.Server{Addr: *addr, Handler: api.New(st, dbs, metrics, logger).Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("api listening", "addr", *addr, "db", *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runWorker(args []string, cfg config.Config, logger *slog.Logger) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	dbPath := fs.String("db", cfg.DBPath, "SQLite database path (overrides DB_PATH)")
	once := fs.Bool("once", false, "process pending events once and exit")
	fail := fs.String("fail", "", "deterministic failure injection, e.g. email=2 or push=always")
	metricsAddr := fs.String("metrics", cfg.MetricsAddr, "metrics HTTP listen address (overrides METRICS_ADDR; empty disables)")
	_ = fs.Parse(args)

	st, dbs, closeDB, err := openStack(*dbPath)
	if err != nil {
		return err
	}
	defer closeDB()

	registry := senders.DefaultRegistry(logger)
	if err := injectFailures(registry, *fail); err != nil {
		return err
	}

	orchCfg := orchestrator.Config{
		PollInterval:   cfg.PollInterval,
		BatchSize:      100,
		MaxAttempts:    cfg.MaxAttempts,
		BaseBackoff:    cfg.BaseBackoff,
		MaxConcurrency: cfg.WorkerConcurrency,
		Sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
	metrics := obs.New()
	o := orchestrator.New(st, dbs, kvs.New(24*time.Hour, time.Now), registry, metrics, logger, orchCfg)

	if *once {
		n, err := o.ProcessOnce(context.Background())
		logger.Info("one-shot processing done", "processed", n)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start the metrics HTTP server before Run so Prometheus can scrape
	// the worker counters from the very first batch. Set METRICS_ADDR=""
	// (or -metrics "") to disable it.
	var metricsSrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsSrv.Shutdown(shutdownCtx)
		}()
		go func() {
			logger.Info("worker metrics listening", "addr", *metricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("worker metrics server stopped", "error", err)
			}
		}()
	} else {
		logger.Info("worker metrics disabled (METRICS_ADDR is empty)")
	}

	return o.Run(ctx)
}

// injectFailures parses "email=2,push=always" into sender failure settings.
func injectFailures(registry senders.Registry, spec string) error {
	if spec == "" {
		return nil
	}
	for _, part := range strings.Split(spec, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid -fail entry %q, want channel=N|always", part)
		}
		ch := events.Channel(kv[0])
		sender, ok := registry[ch]
		if !ok {
			return fmt.Errorf("unknown channel %q", kv[0])
		}
		sim, ok := sender.(*senders.Simulated)
		if !ok {
			return fmt.Errorf("channel %q sender does not support failure injection", kv[0])
		}
		if kv[1] == "always" {
			sim.FailFirstN = senders.FailAlways
			continue
		}
		var n int
		if _, err := fmt.Sscanf(kv[1], "%d", &n); err != nil || n < 0 {
			return fmt.Errorf("invalid -fail count %q", kv[1])
		}
		sim.FailFirstN = n
	}
	return nil
}

// runDemo posts a few realistic events to a running API.
func runDemo(args []string, cfg config.Config) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	apiURL := fs.String("api", "http://localhost"+cfg.HTTPAddr, "base URL of a running eventflow api")
	_ = fs.Parse(args)

	samples := []map[string]any{
		{
			"type": "order.shipped", "ordering_key": "user-1001", "recipient": "ana@example.com",
			"channels": []string{"email", "push"},
			"payload":  map[string]string{"order_id": "ORD-7841", "carrier": "andean-express"},
		},
		{
			"type": "order.out_for_delivery", "ordering_key": "user-1001", "recipient": "ana@example.com",
			"channels": []string{"push", "in_app"},
			"payload":  map[string]string{"order_id": "ORD-7841"},
		},
		{
			"type": "password.reset_requested", "ordering_key": "user-2044", "recipient": "luis@example.com",
			"channels": []string{"email"},
			"payload":  map[string]string{"expires_in_minutes": "30"},
		},
	}

	for _, sample := range samples {
		body, _ := json.Marshal(sample)
		resp, err := http.Post(*apiURL+"/events", "application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("post event: %w (is the api running?)", err)
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		raw, _ := json.Marshal(out)
		fmt.Printf("POST /events -> %d %s\n", resp.StatusCode, raw)
	}
	fmt.Println("now run `eventflow worker -once` (or keep the worker running) and inspect GET /events/{event_id} and GET /metrics")
	return nil
}

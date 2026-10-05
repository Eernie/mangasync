// Command mangasync syncs Komga read progress to trackers and drives downloads from tracker statuses.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mangasync/internal/adapters/registry"
	"mangasync/internal/app"
	"mangasync/internal/config"
	"mangasync/internal/core"
	"mangasync/internal/store"
	"mangasync/internal/sync/download"
	"mangasync/internal/sync/progress"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mangasync:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	reader, err := registry.Reader(cfg.Reader, os.LookupEnv)
	if err != nil {
		return err
	}
	trackers := map[string]core.Tracker{}
	getTracker := func(name string) (core.Tracker, error) {
		if t, ok := trackers[name]; ok {
			return t, nil
		}
		t, err := registry.Tracker(name, os.LookupEnv, cfg.MatchThreshold, cfg.ReconcileInterval)
		if err != nil {
			return nil, err
		}
		trackers[name] = t
		return t, nil
	}
	var progressTrackers []core.Tracker
	for _, name := range cfg.Trackers {
		t, err := getTracker(name)
		if err != nil {
			return err
		}
		progressTrackers = append(progressTrackers, t)
	}

	a := &app.App{
		Reader:            reader,
		Progress:          &progress.Syncer{Reader: reader, Trackers: progressTrackers, Store: st, DryRun: cfg.DryRun, Log: log},
		ReconcileInterval: cfg.ReconcileInterval,
		DownloadInterval:  cfg.DownloadInterval,
		Debounce:          cfg.SSEDebounce,
		Log:               log,
	}
	if cfg.DownloadTracker != "" {
		t, err := getTracker(cfg.DownloadTracker)
		if err != nil {
			return err
		}
		lister, ok := t.(core.LibraryLister)
		if !ok {
			return fmt.Errorf("DOWNLOAD_TRACKER %q cannot list its library", cfg.DownloadTracker)
		}
		d, err := registry.Downloader(ctx, cfg.Downloader, os.LookupEnv, cfg.MatchThreshold)
		if err != nil {
			return err
		}
		a.Download = &download.Syncer{
			Reader: reader, Tracker: t, Lister: lister, Downloader: d, Store: st,
			Acquire: cfg.Acquire, Release: cfg.Release, Threshold: cfg.MatchThreshold, DryRun: cfg.DryRun, Log: log,
		}
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: healthHandler(), ReadHeaderTimeout: 5 * time.Second}
	srvErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", "err", err)
			srvErr <- err
			stop()
		}
	}()

	log.Info("mangasync started", "reader", cfg.Reader, "trackers", cfg.Trackers,
		"download_tracker", cfg.DownloadTracker, "downloader", cfg.Downloader, "dry_run", cfg.DryRun)
	a.Run(ctx)
	stop() // restore default signal handling so a second SIGTERM/Ctrl-C kills a stuck shutdown
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	select {
	case err := <-srvErr:
		return fmt.Errorf("health server: %w", err)
	default:
	}
	return shutdownErr
}

func healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

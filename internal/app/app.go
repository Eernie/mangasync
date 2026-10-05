package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/sync/download"
	"mangasync/internal/sync/progress"
)

// stableStream is how long an event stream must stay open before its backoff resets to 1s.
// A var so tests can lower it.
var stableStream = time.Minute

type App struct {
	Reader            core.Reader
	Progress          *progress.Syncer
	Download          *download.Syncer // nil disables download sync
	ReconcileInterval time.Duration
	DownloadInterval  time.Duration
	Debounce          time.Duration
	Log               *slog.Logger

	queue *Queue
}

// Run starts all loops and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) {
	a.queue = NewQueue()
	var wg sync.WaitGroup
	start := func(f func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(ctx)
		}()
	}
	start(a.worker)
	start(func(ctx context.Context) { every(ctx, a.ReconcileInterval, a.reconcile) })
	start(a.watch)
	if a.Download != nil {
		start(func(ctx context.Context) { every(ctx, a.DownloadInterval, a.download) })
	}
	wg.Wait()
}

// every runs fn now and then every d until ctx is done.
func every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	fn(ctx)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

func (a *App) worker(ctx context.Context) {
	for {
		ref, ok := a.queue.Pop(ctx)
		if !ok {
			return
		}
		if err := a.Progress.SyncSeries(ctx, ref); err != nil && ctx.Err() == nil {
			a.Log.Debug("series sync incomplete", "ref", ref, "err", err)
		}
	}
}

func (a *App) reconcile(ctx context.Context) {
	series, err := a.Reader.ListAllSeries(ctx)
	if err != nil {
		if ctx.Err() == nil {
			a.Log.Error("reconcile: list series", "err", err)
		}
		return
	}
	for _, s := range series {
		a.queue.Push(s.Ref)
	}
	a.Log.Info("reconcile queued", "series", len(series))
}

func (a *App) download(ctx context.Context) {
	if err := a.Download.Run(ctx); err != nil && ctx.Err() == nil {
		a.Log.Debug("download sync incomplete", "err", err)
	}
}

func (a *App) watch(ctx context.Context) {
	w, ok := a.Reader.(core.ProgressWatcher)
	if !ok {
		a.Log.Info("reader has no live events; relying on reconcile", "reader", a.Reader.Name())
		return
	}
	deb := NewDebouncer(a.Debounce, a.queue.Push)
	defer deb.Stop()
	backoff := time.Second
	for ctx.Err() == nil {
		ch, err := w.WatchProgress(ctx)
		if err != nil {
			a.Log.Warn("event stream connect failed", "err", err, "retry_in", backoff)
		} else {
			a.Log.Info("event stream connected")
			connected := time.Now()
			for ref := range ch {
				deb.Trigger(ref)
			}
			if ctx.Err() != nil {
				return
			}
			// Only a stream that stayed up resets the backoff; a server that accepts and
			// immediately closes keeps growing it instead of being retried every second.
			if time.Since(connected) >= stableStream {
				backoff = time.Second
			}
			a.Log.Warn("event stream closed", "retry_in", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

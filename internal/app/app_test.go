package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
	"mangasync/internal/store"
	"mangasync/internal/sync/download"
	"mangasync/internal/sync/progress"
)

// watchReader adds live events to the fake reader.
type watchReader struct {
	*coretest.FakeReader
	events chan string
	hidden map[string]bool // refs left out of ListAllSeries: they only arrive via a live event
}

func (w *watchReader) ListAllSeries(ctx context.Context) ([]core.Series, error) {
	all, err := w.FakeReader.ListAllSeries(ctx)
	if err != nil {
		return nil, err
	}
	var out []core.Series
	for _, s := range all {
		if !w.hidden[s.Ref] {
			out = append(out, s)
		}
	}
	return out, nil
}

func (w *watchReader) WatchProgress(ctx context.Context) (<-chan string, error) {
	out := make(chan string)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ref := <-w.events:
				out <- ref
			}
		}
	}()
	return out, nil
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

func TestAppRunsAllLoops(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	reader := &watchReader{
		FakeReader: &coretest.FakeReader{
			ReaderName: "komga",
			Series: map[string]core.Series{
				"S1": {Ref: "S1", Title: "Chainsaw Man"},
				"S2": {Ref: "S2", Title: "Dandadan"},
				"S3": {Ref: "S3", Title: "Berserk"},
				"S4": {Ref: "S4", Title: "Vagabond"},
			},
			Progress: map[string]core.ReadProgress{
				"S1": {Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2},
				"S2": {Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 3, LastReadNumber: 3},
				"S3": {Unit: core.UnitChapter, BooksTotal: 10}, // unstarted, not in the tracker list
				"S4": {Unit: core.UnitChapter, BooksTotal: 10}, // unstarted, already in the tracker list
			},
		},
		events: make(chan string, 1),
		hidden: map[string]bool{"S2": true}, // S2 only arrives via a live event
	}
	tr := &coretest.FakeTracker{
		TrackerName: "mangabaka",
		IDsByRef:    map[string]string{"S1": "1", "S2": "2", "S3": "3", "S4": "4"},
		Entries:     map[string]*core.Entry{"4": {Status: core.StatusDropped}},
		Library:     []core.LibraryEntry{{Series: core.Series{Ref: "9", Title: "Frieren"}, Status: core.StatusPlanning}},
	}
	dl := &coretest.FakeDownloader{DownloaderName: "suwayomi", Search: map[string]core.Candidate{"9": {Ref: "130", Title: "Frieren"}}}

	a := &App{
		Reader:   reader,
		Progress: &progress.Syncer{Reader: reader, Trackers: []core.Tracker{tr}, Store: st, Log: log},
		Download: &download.Syncer{Reader: reader, Tracker: tr, Lister: tr, Downloader: dl, Store: st,
			Acquire: []core.Status{core.StatusPlanning}, Threshold: 0.9, Log: log},
		ReconcileInterval: time.Hour, DownloadInterval: time.Hour, Debounce: 10 * time.Millisecond, Log: log,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()

	savedIDs := func() map[string]bool {
		ids := map[string]bool{}
		for _, s := range tr.SavedEntries() {
			ids[s.ID] = true
		}
		return ids
	}
	eventually(t, func() bool { return savedIDs()["1"] })                   // reconcile at startup
	eventually(t, func() bool { return savedIDs()["3"] })                   // unstarted series: planning
	eventually(t, func() bool { return len(dl.AcquiredCandidates()) == 1 }) // download sync at startup
	reader.events <- "S2"
	eventually(t, func() bool { return savedIDs()["2"] }) // live event

	eventually(t, func() bool { return tr.ResolveCallCount() == 4 }) // S4 has been looked at too
	for _, sv := range tr.SavedEntries() {
		if sv.ID == "3" && (sv.Update.Status == nil || *sv.Update.Status != core.StatusPlanning || sv.Update.Chapter != nil) {
			t.Errorf("unstarted series saved as %+v, want status planning only", sv.Update)
		}
		if sv.ID == "4" {
			t.Errorf("existing tracker entry was modified: %+v", sv.Update)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// closedStreamReader accepts every connection and closes the stream immediately.
type closedStreamReader struct {
	*coretest.FakeReader
	calls atomic.Int32
}

func (c *closedStreamReader) WatchProgress(context.Context) (<-chan string, error) {
	c.calls.Add(1)
	ch := make(chan string)
	close(ch)
	return ch, nil
}

// A server that answers 200 and closes at once must not be reconnected every second: backoff keeps
// growing (1s, 2s, 4s ...) because the stream was never stable.
func TestWatchBacksOffWhenStreamClosesImmediately(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reader := &closedStreamReader{FakeReader: &coretest.FakeReader{ReaderName: "komga"}}
	a := &App{Reader: reader, ReconcileInterval: time.Hour, Debounce: time.Millisecond, Log: log}

	ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
	defer cancel()
	a.Run(ctx)

	// Connects at 0s, 1s, then waits 2s more (3s): 2 calls fit in 2.5s. Resetting the backoff after
	// every connect would give a third call at 2s.
	if n := reader.calls.Load(); n != 2 {
		t.Fatalf("WatchProgress called %d times in 2.5s, want 2 (1s then 2s backoff)", n)
	}
}

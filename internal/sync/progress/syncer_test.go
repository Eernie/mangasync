package progress

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
	"mangasync/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func reader(p core.ReadProgress) *coretest.FakeReader {
	return &coretest.FakeReader{
		ReaderName: "komga",
		Series:     map[string]core.Series{"S1": {Ref: "S1", Title: "Chainsaw Man"}},
		Progress:   map[string]core.ReadProgress{"S1": p},
		Started:    []string{"S1"},
	}
}

func TestSyncCreatesEntryAndCachesMapping(t *testing.T) {
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1677"}}
	st := newStore(t)
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 244, BooksRead: 104, LastReadNumber: 104}),
		Trackers: []core.Tracker{tr}, Store: st, Log: quietLog()}

	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	saved := tr.SavedEntries()
	if len(saved) != 1 || saved[0].ID != "1677" || *saved[0].Update.Status != core.StatusReading || *saved[0].Update.Chapter != 104 {
		t.Fatalf("saved = %+v", saved)
	}
	// Second run: mapping is cached, entry is up to date.
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	if tr.ResolveCallCount() != 1 || len(tr.SavedEntries()) != 1 {
		t.Fatalf("resolve calls = %d, saves = %d; want 1, 1", tr.ResolveCallCount(), len(tr.SavedEntries()))
	}
	m, _ := st.GetMapping(t.Context(), "komga", "S1", "mangabaka")
	if m == nil || m.Status != store.Matched || m.TrackerID != "1677" || *m.LastProgress != 104 {
		t.Fatalf("mapping = %+v", m)
	}
}

func TestUnmatchedIsRetriedAfter24h(t *testing.T) {
	tr := &coretest.FakeTracker{TrackerName: "mangabaka"} // resolves nothing
	now := time.Unix(1_700_000_000, 0)
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 1, LastReadNumber: 1}),
		Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog(), Now: func() time.Time { return now }}

	for range 2 {
		if err := s.SyncSeries(t.Context(), "S1"); err != nil {
			t.Fatal(err)
		}
	}
	if tr.ResolveCallCount() != 1 {
		t.Fatalf("resolve calls within 24h = %d, want 1", tr.ResolveCallCount())
	}
	now = now.Add(25 * time.Hour)
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	if tr.ResolveCallCount() != 2 {
		t.Fatalf("resolve calls after 25h = %d, want 2", tr.ResolveCallCount())
	}
}

func TestOneTrackerFailingDoesNotStopOthers(t *testing.T) {
	bad := &coretest.FakeTracker{TrackerName: "bad", IDsByRef: map[string]string{"S1": "x"}, SaveErr: errors.New("boom")}
	good := &coretest.FakeTracker{TrackerName: "good", IDsByRef: map[string]string{"S1": "y"}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2}),
		Trackers: []core.Tracker{bad, good}, Store: newStore(t), Log: quietLog()}

	err := s.SyncSeries(t.Context(), "S1")
	if err == nil {
		t.Fatal("expected error from failing tracker")
	}
	if len(good.SavedEntries()) != 1 {
		t.Fatalf("good tracker saves = %d, want 1", len(good.SavedEntries()))
	}
}

func TestCompletedOnlyWhenSeriesEnded(t *testing.T) {
	all := core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 81, BooksRead: 81, LastReadNumber: 80, MaxNumber: 80}
	for _, ended := range []bool{false, true} {
		tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"}, Ended: map[string]bool{"1": ended}}
		s := &Syncer{Reader: reader(all), Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog()}
		if err := s.SyncSeries(t.Context(), "S1"); err != nil {
			t.Fatal(err)
		}
		want := core.StatusReading
		if ended {
			want = core.StatusCompleted
		}
		if got := *tr.SavedEntries()[0].Update.Status; got != want {
			t.Errorf("ended=%v: status %q, want %q", ended, got, want)
		}
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2}),
		Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog(), DryRun: true}
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	if len(tr.SavedEntries()) != 0 {
		t.Fatalf("dry run saved %v", tr.SavedEntries())
	}
}

func TestMappingRecordsWhatWasPushed(t *testing.T) {
	// The tracker already has chapter 200; only the status moves, so the pushed progress is 200.
	hi := 200.0
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"},
		Entries: map[string]*core.Entry{"1": {Status: core.StatusPlanning, Chapter: &hi}}}
	st := newStore(t)
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 300, BooksRead: 104, LastReadNumber: 104}),
		Trackers: []core.Tracker{tr}, Store: st, Log: quietLog()}
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	saved := tr.SavedEntries()
	if len(saved) != 1 || saved[0].Update.Chapter != nil || *saved[0].Update.Status != core.StatusReading {
		t.Fatalf("saved = %+v", saved)
	}
	m, _ := st.GetMapping(t.Context(), "komga", "S1", "mangabaka")
	if m == nil || m.LastStatus != core.StatusReading || m.LastProgress == nil || *m.LastProgress != 200 {
		t.Fatalf("mapping = %+v", m)
	}
}

func TestSyncSeriesStopsWhenContextCancelled(t *testing.T) {
	a := &coretest.FakeTracker{TrackerName: "a", IDsByRef: map[string]string{"S1": "x"}}
	b := &coretest.FakeTracker{TrackerName: "b", IDsByRef: map[string]string{"S1": "y"}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2}),
		Trackers: []core.Tracker{a, b}, Store: newStore(t), Log: quietLog()}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := s.SyncSeries(ctx, "S1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if a.ResolveCallCount()+b.ResolveCallCount() != 0 {
		t.Fatal("no tracker should be touched after cancellation")
	}
}

// errorCounter is a slog handler that records the messages of Error records.
type errorCounter struct{ msgs *[]string }

func (h errorCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h errorCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelError {
		*h.msgs = append(*h.msgs, r.Message)
	}
	return nil
}
func (h errorCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h errorCounter) WithGroup(string) slog.Handler      { return h }

func TestReaderFailuresAreLoggedOnceAtError(t *testing.T) {
	for name, rd := range map[string]*coretest.FakeReader{
		"get series":   {ReaderName: "komga", Series: map[string]core.Series{}},
		"get progress": {ReaderName: "komga", Series: map[string]core.Series{"S1": {Ref: "S1", Title: "X"}}, Progress: map[string]core.ReadProgress{}},
	} {
		t.Run(name, func(t *testing.T) {
			var msgs []string
			s := &Syncer{Reader: rd, Store: newStore(t), Log: slog.New(errorCounter{&msgs})}
			if err := s.SyncSeries(t.Context(), "S1"); err == nil {
				t.Fatal("want an error")
			}
			if len(msgs) != 1 {
				t.Fatalf("Error records = %v, want exactly one", msgs)
			}
		})
	}
}

func TestSyncWritesStartDateInConfiguredLocation(t *testing.T) {
	loc, err := time.LoadLocation("Pacific/Kiritimati") // UTC+14, never the same date as UTC at noon
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC) // 2026-06-18 02:00 in Kiritimati
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2, FirstReadAt: first, LastReadAt: first}),
		Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog(), Location: loc}
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	saved := tr.SavedEntries()
	if len(saved) != 1 || saved[0].Update.StartDate == nil || *saved[0].Update.StartDate != "2026-06-18" {
		t.Fatalf("saved = %+v", saved)
	}
	if saved[0].Update.FinishDate != nil {
		t.Errorf("finish date = %q, want none for a reading target", *saved[0].Update.FinishDate)
	}
}

func TestSyncTwiceWithDatesSavesOnce(t *testing.T) {
	first := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2, FirstReadAt: first}),
		Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog(), Location: time.UTC}
	for range 2 {
		if err := s.SyncSeries(t.Context(), "S1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(tr.SavedEntries()); n != 1 {
		t.Fatalf("saves = %d, want 1", n)
	}
}

func TestSyncFillsMissingStartDateOnUpToDateEntry(t *testing.T) {
	first := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	ch := 2.0
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"},
		Entries: map[string]*core.Entry{"1": {Status: core.StatusReading, Chapter: &ch}}}
	s := &Syncer{Reader: reader(core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2, FirstReadAt: first}),
		Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog(), Location: time.UTC}
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	saved := tr.SavedEntries()
	if len(saved) != 1 || *saved[0].Update.StartDate != "2026-06-17" || saved[0].Update.Status != nil || saved[0].Update.Chapter != nil {
		t.Fatalf("saved = %+v", saved)
	}
}

func TestUnstartedSeriesIsPlannedOnlyWhenNotInTrackerList(t *testing.T) {
	unstarted := core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 10}

	// Not in the tracker list: created as planning, without progress or dates.
	tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"}}
	s := &Syncer{Reader: reader(unstarted), Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog()}
	if err := s.SyncSeries(t.Context(), "S1"); err != nil {
		t.Fatal(err)
	}
	saved := tr.SavedEntries()
	if len(saved) != 1 || *saved[0].Update.Status != core.StatusPlanning ||
		saved[0].Update.Chapter != nil || saved[0].Update.Volume != nil ||
		saved[0].Update.StartDate != nil || saved[0].Update.FinishDate != nil {
		t.Fatalf("saved = %+v", saved)
	}

	// Already in the list under any status: nothing is saved.
	for _, st := range []core.Status{
		core.StatusConsidering, core.StatusPlanning, core.StatusReading, core.StatusPaused,
		core.StatusDropped, core.StatusCompleted, core.StatusRereading, core.StatusUnknown,
	} {
		tr := &coretest.FakeTracker{TrackerName: "mangabaka", IDsByRef: map[string]string{"S1": "1"},
			Entries: map[string]*core.Entry{"1": {Status: st}}}
		s := &Syncer{Reader: reader(unstarted), Trackers: []core.Tracker{tr}, Store: newStore(t), Log: quietLog()}
		if err := s.SyncSeries(t.Context(), "S1"); err != nil {
			t.Fatal(err)
		}
		if got := tr.SavedEntries(); len(got) != 0 {
			t.Errorf("existing %q entry was modified: %+v", st, got)
		}
	}
}

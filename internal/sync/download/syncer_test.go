package download

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

type fixture struct {
	reader *coretest.FakeReader
	tr     *coretest.FakeTracker
	dl     *coretest.FakeDownloader
	st     *store.Store
	now    time.Time
	s      *Syncer
}

func newFixture(t *testing.T, entries ...core.LibraryEntry) *fixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fx := &fixture{
		reader: &coretest.FakeReader{ReaderName: "komga", Series: map[string]core.Series{}},
		tr:     &coretest.FakeTracker{TrackerName: "mangabaka", Library: entries},
		dl:     &coretest.FakeDownloader{DownloaderName: "suwayomi", Library: map[string]core.Candidate{}, Search: map[string]core.Candidate{}},
		st:     st,
		now:    time.Unix(1_700_000_000, 0).UTC(),
	}
	fx.s = &Syncer{
		Reader: fx.reader, Tracker: fx.tr, Lister: fx.tr, Downloader: fx.dl, Store: st,
		Acquire:   []core.Status{core.StatusPlanning, core.StatusReading, core.StatusRereading},
		Release:   []core.Status{core.StatusDropped},
		Threshold: 0.9, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return fx.now },
	}
	return fx
}

func (fx *fixture) run(t *testing.T) {
	t.Helper()
	if err := fx.s.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (fx *fixture) record(t *testing.T, id string) *store.DownloadRecord {
	t.Helper()
	r, err := fx.st.GetDownload(t.Context(), "mangabaka", id, "suwayomi")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func entry(id, title string, st core.Status, ids core.IDs) core.LibraryEntry {
	return core.LibraryEntry{Series: core.Series{Ref: id, Title: title, IDs: ids}, Status: st}
}

func TestPlanningIsAcquiredOnce(t *testing.T) {
	fx := newFixture(t, entry("1", "Frieren", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "130", Title: "Frieren - Beyond Journey's End", SourceName: "Weeb Central (EN)"}

	fx.run(t)
	fx.run(t)

	if got := fx.dl.AcquiredCandidates(); len(got) != 1 || got[0].Ref != "130" {
		t.Fatalf("acquired = %+v", got)
	}
	if fx.dl.FindCallCount() != 1 {
		t.Fatalf("find calls = %d, want 1", fx.dl.FindCallCount())
	}
	if r := fx.record(t, "1"); r.Status != store.Acquired || r.CandidateRef != "130" || r.Source != "Weeb Central (EN)" {
		t.Fatalf("record = %+v", r)
	}
}

func TestReadingAlreadyInDownloaderLibrary(t *testing.T) {
	fx := newFixture(t, entry("1", "Chainsaw Man", core.StatusReading, nil))
	fx.dl.Library["1"] = core.Candidate{Ref: "33", Title: "Chainsaw Man"}

	fx.run(t)

	if fx.dl.FindCallCount() != 0 || len(fx.dl.AcquiredCandidates()) != 0 {
		t.Fatalf("find=%d acquired=%v; want no search and no acquire", fx.dl.FindCallCount(), fx.dl.AcquiredCandidates())
	}
	if r := fx.record(t, "1"); r.Status != store.Acquired || r.CandidateRef != "33" {
		t.Fatalf("record = %+v", r)
	}
}

func TestAlreadyInReaderIsSkipped(t *testing.T) {
	fx := newFixture(t,
		entry("1", "Naruto Shinden", core.StatusReading, core.IDs{core.IDAniList: "42"}),
		entry("2", "Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga", core.StatusPlanning, nil),
	)
	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Naruto - The Seventh Hokage", IDs: core.IDs{core.IDAniList: "42"}}
	fx.reader.Series["K2"] = core.Series{Ref: "K2", Title: "Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"}

	fx.run(t)

	if fx.dl.FindCallCount() != 0 || len(fx.dl.AcquiredCandidates()) != 0 {
		t.Fatalf("find=%d acquired=%v; want none", fx.dl.FindCallCount(), fx.dl.AcquiredCandidates())
	}
	if fx.record(t, "1") != nil || fx.record(t, "2") != nil {
		t.Fatal("already-in-reader must not write records")
	}
}

func TestNotFoundBacksOff(t *testing.T) {
	fx := newFixture(t, entry("1", "Obscure", core.StatusPlanning, nil))

	fx.run(t)
	r := fx.record(t, "1")
	if r.Status != store.NotFound || r.Attempts != 1 || !r.RetryAfter.Equal(fx.now.Add(24*time.Hour)) {
		t.Fatalf("after first miss: %+v", r)
	}
	fx.run(t) // still within backoff
	if fx.dl.FindCallCount() != 1 {
		t.Fatalf("find calls within backoff = %d, want 1", fx.dl.FindCallCount())
	}
	fx.now = fx.now.Add(25 * time.Hour)
	fx.run(t)
	r = fx.record(t, "1")
	if fx.dl.FindCallCount() != 2 || r.Attempts != 2 || !r.RetryAfter.Equal(fx.now.Add(72*time.Hour)) {
		t.Fatalf("after second miss: find=%d record=%+v", fx.dl.FindCallCount(), r)
	}
}

func TestDroppedIsReleasedThenReacquired(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusDropped, nil))
	fx.dl.Library["1"] = core.Candidate{Ref: "50", Title: "Fire Force"}

	fx.run(t)
	fx.run(t)
	if got := fx.dl.ReleasedCandidates(); len(got) != 1 || got[0].Ref != "50" {
		t.Fatalf("released = %+v", got)
	}
	if r := fx.record(t, "1"); r.Status != store.Released {
		t.Fatalf("record = %+v", r)
	}

	// User changes their mind: back to plan to read. It's gone from the library now.
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusPlanning, nil)})
	fx.dl.SetLibrary(map[string]core.Candidate{})
	fx.dl.Search["1"] = core.Candidate{Ref: "50", Title: "Fire Force"}
	fx.run(t)
	if got := fx.dl.AcquiredCandidates(); len(got) != 1 {
		t.Fatalf("re-acquired = %+v", got)
	}
}

func TestDroppedNotInLibraryIsRecordedWithoutRelease(t *testing.T) {
	fx := newFixture(t, entry("1", "Never had it", core.StatusDropped, nil))
	fx.run(t)
	if len(fx.dl.ReleasedCandidates()) != 0 || fx.record(t, "1").Status != store.Released {
		t.Fatal("expected a released record and no Release call")
	}
}

func TestOtherStatusesAreIgnored(t *testing.T) {
	fx := newFixture(t,
		entry("1", "A", core.StatusConsidering, nil),
		entry("2", "B", core.StatusPaused, nil),
		entry("3", "C", core.StatusCompleted, nil),
	)
	fx.run(t)
	if fx.dl.FindCallCount() != 0 || fx.record(t, "1") != nil {
		t.Fatal("considering/paused/completed must do nothing")
	}
}

func TestAcquireErrorIsRetried(t *testing.T) {
	fx := newFixture(t, entry("1", "Frieren", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "130", Title: "Frieren"}
	fx.dl.SetAcquireErr(errors.New("suwayomi down"))

	if err := fx.s.Run(t.Context()); err == nil {
		t.Fatal("expected error")
	}
	if r := fx.record(t, "1"); r.Status != store.InProgress {
		t.Fatalf("record = %+v", r)
	}
	fx.dl.SetAcquireErr(nil)
	fx.run(t)
	if len(fx.dl.AcquiredCandidates()) != 1 || fx.record(t, "1").Status != store.Acquired {
		t.Fatal("expected retry to acquire")
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	fx := newFixture(t,
		entry("1", "Frieren", core.StatusPlanning, nil),
		entry("2", "Obscure", core.StatusPlanning, nil),
		entry("3", "Fire Force", core.StatusDropped, nil),
	)
	fx.dl.Search["1"] = core.Candidate{Ref: "130", Title: "Frieren"}
	fx.dl.Library["3"] = core.Candidate{Ref: "50", Title: "Fire Force"}
	fx.s.DryRun = true

	fx.run(t)
	if len(fx.dl.AcquiredCandidates()) != 0 || len(fx.dl.ReleasedCandidates()) != 0 {
		t.Fatal("dry run must not acquire or release")
	}
	for _, id := range []string{"1", "2", "3"} {
		if fx.record(t, id) != nil {
			t.Fatalf("dry run wrote a record for %s", id)
		}
	}
}

func TestNotFoundBackoffSchedule(t *testing.T) {
	want := []time.Duration{24 * time.Hour, 72 * time.Hour, 168 * time.Hour, 336 * time.Hour, 720 * time.Hour, 720 * time.Hour}
	for i, w := range want {
		if got := NotFoundBackoff(i + 1); got != w {
			t.Errorf("NotFoundBackoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestDroppedThenReacquiredEvenThoughKeptFilesAreInReader(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusDropped, nil))
	fx.dl.Library["1"] = core.Candidate{Ref: "50", Title: "Fire Force", SourceName: "Src"}
	fx.run(t)
	if r := fx.record(t, "1"); r.Status != store.Released || r.CandidateRef != "50" {
		t.Fatalf("record = %+v", r)
	}

	// Released files stay on disk, so the reader now has the series.
	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Fire Force"}
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusPlanning, nil)})
	fx.dl.SetLibrary(map[string]core.Candidate{})
	fx.dl.Search["1"] = core.Candidate{Ref: "50", Title: "Fire Force"}
	fx.run(t)
	if got := fx.dl.AcquiredCandidates(); len(got) != 1 || got[0].Ref != "50" {
		t.Fatalf("re-acquired = %+v", got)
	}
}

func TestDroppedIsReleasedByRecordedCandidate(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "50", Title: "Fire Force", SourceName: "Src"}
	fx.run(t) // acquired with ref 50; the fake library stays empty, so title matching would miss
	if r := fx.record(t, "1"); r.Status != store.Acquired || r.CandidateRef != "50" {
		t.Fatalf("record = %+v", r)
	}

	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusDropped, nil)})
	fx.run(t)
	if got := fx.dl.ReleasedCandidates(); len(got) != 1 || got[0].Ref != "50" || got[0].SourceName != "Src" {
		t.Fatalf("released = %+v", got)
	}
	if r := fx.record(t, "1"); r.Status != store.Released || r.CandidateRef != "50" || r.Source != "Src" {
		t.Fatalf("record = %+v", r)
	}

	// Back to planning; the kept files are in the reader but the record shows they are ours.
	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Fire Force"}
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusPlanning, nil)})
	fx.run(t)
	if got := fx.dl.AcquiredCandidates(); len(got) != 2 || got[1].Ref != "50" {
		t.Fatalf("acquired = %+v", got)
	}
	if r := fx.record(t, "1"); r.Status != store.Acquired {
		t.Fatalf("record = %+v", r)
	}
}

func TestDroppedMissKeepsPreviousCandidateMarker(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusDropped, nil))
	if err := fx.st.PutDownload(t.Context(), store.DownloadRecord{
		Tracker: "mangabaka", TrackerID: "1", Downloader: "suwayomi", Status: store.NotFound,
		CandidateRef: "50", Source: "Src", UpdatedAt: fx.now,
	}); err != nil {
		t.Fatal(err)
	}
	fx.run(t) // no recorded acquire to release, FindInLibrary misses
	if len(fx.dl.ReleasedCandidates()) != 0 {
		t.Fatalf("released = %+v", fx.dl.ReleasedCandidates())
	}
	if r := fx.record(t, "1"); r.Status != store.Released || r.CandidateRef != "50" || r.Source != "Src" {
		t.Fatalf("record = %+v", r)
	}
}

func TestDroppedDryRunWithRecordedCandidateChangesNothing(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "50", Title: "Fire Force"}
	fx.run(t)
	fx.s.DryRun = true
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusDropped, nil)})
	fx.run(t)
	if len(fx.dl.ReleasedCandidates()) != 0 || fx.record(t, "1").Status != store.Acquired {
		t.Fatal("dry-run must not release or write")
	}
}

func TestInProgressWithCandidateIgnoresReaderCheck(t *testing.T) {
	fx := newFixture(t, entry("1", "Frieren", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "130", Title: "Frieren"}
	fx.dl.SetAcquireErr(errors.New("suwayomi down"))
	if err := fx.s.Run(t.Context()); err == nil {
		t.Fatal("expected error")
	}
	// Part of the chapters made it into the reader before the failure.
	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Frieren"}
	fx.dl.SetAcquireErr(nil)
	fx.run(t)
	if len(fx.dl.AcquiredCandidates()) != 1 || fx.record(t, "1").Status != store.Acquired {
		t.Fatal("expected the in-progress acquire to be retried")
	}
}

func TestReleasedWithoutCandidateStillRespectsReaderCheck(t *testing.T) {
	fx := newFixture(t, entry("1", "Never had it", core.StatusDropped, nil))
	fx.run(t) // not in the downloader: released record without a candidate
	if r := fx.record(t, "1"); r.Status != store.Released || r.CandidateRef != "" {
		t.Fatalf("record = %+v", r)
	}

	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Never had it"}
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Never had it", core.StatusPlanning, nil)})
	fx.run(t)
	if fx.dl.FindCallCount() != 0 || len(fx.dl.AcquiredCandidates()) != 0 {
		t.Fatalf("find=%d acquired=%v; series is in the reader, want skip", fx.dl.FindCallCount(), fx.dl.AcquiredCandidates())
	}
}

func TestRunStopsWhenContextCancelled(t *testing.T) {
	fx := newFixture(t,
		entry("1", "A", core.StatusPlanning, nil),
		entry("2", "B", core.StatusPlanning, nil),
	)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := fx.s.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if fx.dl.FindCallCount() != 0 {
		t.Fatalf("find calls = %d, want 0", fx.dl.FindCallCount())
	}
}

func TestManagedMarkerSurvivesNotFoundRetries(t *testing.T) {
	fx := newFixture(t, entry("1", "Fire Force", core.StatusDropped, nil))
	fx.dl.Library["1"] = core.Candidate{Ref: "50", Title: "Fire Force", SourceName: "Src"}
	fx.run(t) // released, record keeps ref 50

	// Back to planning; the kept files are in the reader; the source search misses once.
	fx.reader.Series["K1"] = core.Series{Ref: "K1", Title: "Fire Force"}
	fx.tr.SetLibrary([]core.LibraryEntry{entry("1", "Fire Force", core.StatusPlanning, nil)})
	fx.dl.SetLibrary(map[string]core.Candidate{})
	fx.run(t)
	r := fx.record(t, "1")
	if r.Status != store.NotFound || r.CandidateRef != "50" || r.Source != "Src" {
		t.Fatalf("after miss: %+v", r)
	}

	fx.now = fx.now.Add(25 * time.Hour)
	fx.dl.Search["1"] = core.Candidate{Ref: "50", Title: "Fire Force", SourceName: "Src"}
	fx.run(t)
	if len(fx.dl.AcquiredCandidates()) != 1 || fx.record(t, "1").Status != store.Acquired {
		t.Fatalf("expected acquire after backoff; acquired=%v", fx.dl.AcquiredCandidates())
	}
}

func TestInProgressIsResumedEvenThoughManagaIsNowInDownloaderLibrary(t *testing.T) {
	fx := newFixture(t, entry("1", "Frieren", core.StatusPlanning, nil))
	cand := core.Candidate{Ref: "130", Title: "Frieren", SourceName: "Src"}
	fx.dl.Search["1"] = cand
	fx.dl.SetAcquireErr(errors.New("chapter fetch failed"))
	if err := fx.s.Run(t.Context()); err == nil {
		t.Fatal("expected error")
	}
	if r := fx.record(t, "1"); r.Status != store.InProgress || r.CandidateRef != "130" {
		t.Fatalf("record = %+v", r)
	}

	// The failed Acquire had already added it to the downloader library.
	fx.dl.SetLibrary(map[string]core.Candidate{"1": cand})
	fx.dl.SetAcquireErr(nil)
	fx.run(t)

	got := fx.dl.AcquiredCandidates()
	if len(got) != 1 || got[0].Ref != "130" || got[0].SourceName != "Src" {
		t.Fatalf("acquired = %+v, want the interrupted acquire resumed", got)
	}
	if r := fx.record(t, "1"); r.Status != store.Acquired || r.CandidateRef != "130" || r.Source != "Src" {
		t.Fatalf("record = %+v", r)
	}
}

func TestResumeRespectsDryRunAndKeepsRecordOnError(t *testing.T) {
	fx := newFixture(t, entry("1", "Frieren", core.StatusPlanning, nil))
	fx.dl.Search["1"] = core.Candidate{Ref: "130", Title: "Frieren", SourceName: "Src"}
	fx.dl.SetAcquireErr(errors.New("down"))
	_ = fx.s.Run(t.Context())

	if err := fx.s.Run(t.Context()); err == nil { // resume fails again
		t.Fatal("expected error")
	}
	if r := fx.record(t, "1"); r.Status != store.InProgress || r.CandidateRef != "130" || r.Source != "Src" {
		t.Fatalf("record = %+v", r)
	}

	fx.dl.SetAcquireErr(nil)
	fx.s.DryRun = true
	fx.run(t)
	if len(fx.dl.AcquiredCandidates()) != 0 || fx.record(t, "1").Status != store.InProgress {
		t.Fatal("dry run must neither acquire nor change the record")
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

type failingLister struct{ *coretest.FakeTracker }

func (failingLister) ListLibrary(context.Context, []core.Status) ([]core.LibraryEntry, error) {
	return nil, errors.New("boom")
}

type failingReader struct{ *coretest.FakeReader }

func (failingReader) ListAllSeries(context.Context) ([]core.Series, error) {
	return nil, errors.New("boom")
}

func TestListFailuresAreLoggedOnceAtError(t *testing.T) {
	for name, tweak := range map[string]func(*Syncer){
		"list tracker library": func(s *Syncer) { s.Lister = failingLister{s.Tracker.(*coretest.FakeTracker)} },
		"list reader series":   func(s *Syncer) { s.Reader = failingReader{s.Reader.(*coretest.FakeReader)} },
	} {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			var msgs []string
			fx.s.Log = slog.New(errorCounter{&msgs})
			tweak(fx.s)
			if err := fx.s.Run(t.Context()); err == nil {
				t.Fatal("want an error")
			}
			if len(msgs) != 1 {
				t.Fatalf("Error records = %v, want exactly one", msgs)
			}
		})
	}
}

package coretest

import (
	"context"
	"testing"

	"mangasync/internal/core"
)

func containsRef(ss []core.Series, ref string) bool {
	for _, s := range ss {
		if s.Ref == ref {
			return true
		}
	}
	return false
}

// ReaderContract checks invariants every Reader adapter must hold. ref must be a started series.
func ReaderContract(t *testing.T, r core.Reader, ref string) {
	t.Helper()
	ctx := context.Background()
	if r.Name() == "" {
		t.Error("Name() is empty")
	}
	started, err := r.ListStartedSeries(ctx)
	if err != nil {
		t.Fatalf("ListStartedSeries: %v", err)
	}
	if !containsRef(started, ref) {
		t.Errorf("ListStartedSeries does not contain %q", ref)
	}
	all, err := r.ListAllSeries(ctx)
	if err != nil {
		t.Fatalf("ListAllSeries: %v", err)
	}
	if !containsRef(all, ref) {
		t.Errorf("ListAllSeries does not contain %q", ref)
	}
	s, err := r.GetSeries(ctx, ref)
	if err != nil {
		t.Fatalf("GetSeries: %v", err)
	}
	if s.Ref != ref || s.Title == "" {
		t.Errorf("GetSeries(%q) = %+v; want matching Ref and a Title", ref, s)
	}
	p, err := r.GetProgress(ctx, ref)
	if err != nil {
		t.Fatalf("GetProgress: %v", err)
	}
	if p.Unit != core.UnitChapter && p.Unit != core.UnitVolume {
		t.Errorf("GetProgress unit = %q", p.Unit)
	}
	if p.BooksRead > p.BooksTotal {
		t.Errorf("GetProgress: BooksRead %d > BooksTotal %d", p.BooksRead, p.BooksTotal)
	}
}

// TrackerContract checks invariants every Tracker adapter must hold. s must resolve to wantID.
func TrackerContract(t *testing.T, tr core.Tracker, s core.Series, wantID string) {
	t.Helper()
	ctx := context.Background()
	if tr.Name() == "" {
		t.Error("Name() is empty")
	}
	id, found, err := tr.Resolve(ctx, s)
	if err != nil || !found || id != wantID {
		t.Fatalf("Resolve = %q, %v, %v; want %q, true, nil", id, found, err, wantID)
	}
	if _, err := tr.GetEntry(ctx, id); err != nil {
		t.Errorf("GetEntry(%q): %v", id, err)
	}
	if _, err := tr.SeriesEnded(ctx, id); err != nil {
		t.Errorf("SeriesEnded(%q): %v", id, err)
	}
}

// DownloaderContract checks invariants every Downloader adapter must hold. s must be findable.
func DownloaderContract(t *testing.T, d core.Downloader, s core.Series) {
	t.Helper()
	ctx := context.Background()
	if d.Name() == "" {
		t.Error("Name() is empty")
	}
	if _, err := d.FindInLibrary(ctx, s); err != nil {
		t.Errorf("FindInLibrary: %v", err)
	}
	best, _, err := d.Find(ctx, s)
	if err != nil || best == nil || best.Ref == "" {
		t.Fatalf("Find = %+v, %v; want a candidate with a Ref", best, err)
	}
}

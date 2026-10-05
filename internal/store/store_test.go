package store

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"mangasync/internal/core"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMappingRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "s.db"))

	got, err := s.GetMapping(ctx, "komga", "S1", "mangabaka")
	if err != nil || got != nil {
		t.Fatalf("missing mapping: got %+v, %v; want nil, nil", got, err)
	}

	at := time.Unix(1_700_000_000, 0).UTC()
	m := SeriesMapping{Reader: "komga", ReaderRef: "S1", Tracker: "mangabaka", Status: Unmatched, LastAttempt: at}
	if err := s.PutMapping(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetMapping(ctx, "komga", "S1", "mangabaka")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != Unmatched || !got.LastAttempt.Equal(at) || got.LastProgress != nil || got.TrackerID != "" {
		t.Fatalf("got %+v", got)
	}

	p := 104.5
	m = SeriesMapping{Reader: "komga", ReaderRef: "S1", Tracker: "mangabaka", TrackerID: "1677", Status: Matched,
		LastAttempt: at, LastStatus: core.StatusReading, LastProgress: &p}
	if err := s.PutMapping(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetMapping(ctx, "komga", "S1", "mangabaka")
	if got.Status != Matched || got.TrackerID != "1677" || got.LastStatus != core.StatusReading || *got.LastProgress != 104.5 {
		t.Fatalf("upsert: got %+v", got)
	}
}

func TestDownloadRoundTripAndPersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "s.db")
	s := open(t, path)

	if got, err := s.GetDownload(ctx, "mangabaka", "1677", "suwayomi"); err != nil || got != nil {
		t.Fatalf("missing record: got %+v, %v", got, err)
	}
	retry := time.Unix(1_700_086_400, 0).UTC()
	r := DownloadRecord{Tracker: "mangabaka", TrackerID: "1677", Downloader: "suwayomi", Status: NotFound,
		Attempts: 2, RetryAfter: retry, UpdatedAt: time.Unix(1_700_000_000, 0).UTC()}
	if err := s.PutDownload(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Status, r.CandidateRef, r.Source = Acquired, "130", "Weeb Central (EN)"
	if err := s.PutDownload(ctx, r); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := open(t, path)
	got, err := s2.GetDownload(ctx, "mangabaka", "1677", "suwayomi")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != Acquired || got.CandidateRef != "130" || got.Source != "Weeb Central (EN)" ||
		got.Attempts != 2 || !got.RetryAfter.Equal(retry) {
		t.Fatalf("got %+v", got)
	}
}

func TestMatchedTrackerIDs(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "s.db"))
	put := func(reader, ref, tracker, id string, st MatchStatus) {
		t.Helper()
		if err := s.PutMapping(ctx, SeriesMapping{Reader: reader, ReaderRef: ref, Tracker: tracker, TrackerID: id, Status: st}); err != nil {
			t.Fatal(err)
		}
	}
	put("komga", "K1", "mangabaka", "5", Matched)
	put("komga", "K2", "mangabaka", "", Unmatched)
	put("komga", "K3", "anilist", "77", Matched)
	put("kavita", "V1", "mangabaka", "9", Matched)

	got, err := s.MatchedTrackerIDs(ctx, "komga", "mangabaka")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !slices.Equal(got["5"], []string{"K1"}) {
		t.Fatalf("got %v, want only tracker 5 -> [K1]", got)
	}

	// Several reader series (e.g. after a re-import) can map to one tracker ID: all are returned.
	put("komga", "K9", "mangabaka", "5", Matched)
	got, err = s.MatchedTrackerIDs(ctx, "komga", "mangabaka")
	if err != nil {
		t.Fatal(err)
	}
	refs := slices.Clone(got["5"])
	slices.Sort(refs)
	if len(got) != 1 || !slices.Equal(refs, []string{"K1", "K9"}) {
		t.Fatalf("got %v, want tracker 5 -> [K1 K9]", got)
	}

	empty, err := s.MatchedTrackerIDs(ctx, "komga", "nobody")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("no rows: got %v, %v; want empty non-nil map", empty, err)
	}
}

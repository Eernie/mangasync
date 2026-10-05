package coretest

import (
	"testing"

	"mangasync/internal/core"
)

func TestFakesSatisfyContracts(t *testing.T) {
	series := core.Series{Ref: "S1", Title: "Chainsaw Man"}
	r := &FakeReader{
		ReaderName: "fake-reader",
		Series:     map[string]core.Series{"S1": series},
		Progress:   map[string]core.ReadProgress{"S1": {Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 3}},
		Started:    []string{"S1"},
	}
	ReaderContract(t, r, "S1")

	tr := &FakeTracker{TrackerName: "fake-tracker", IDsByRef: map[string]string{"S1": "1677"}}
	TrackerContract(t, tr, series, "1677")

	d := &FakeDownloader{DownloaderName: "fake-dl", Search: map[string]core.Candidate{"S1": {Ref: "24", Title: "Chainsaw Man"}}}
	DownloaderContract(t, d, series)
}

func TestFakeTrackerAppliesSaves(t *testing.T) {
	tr := &FakeTracker{}
	ch := 5.0
	st := core.StatusReading
	if err := tr.SaveEntry(t.Context(), "1", core.EntryUpdate{Status: &st, Chapter: &ch}); err != nil {
		t.Fatal(err)
	}
	e, _ := tr.GetEntry(t.Context(), "1")
	if e == nil || e.Status != core.StatusReading || *e.Chapter != 5 || len(tr.SavedEntries()) != 1 {
		t.Fatalf("entry=%+v saved=%v", e, tr.SavedEntries())
	}
}

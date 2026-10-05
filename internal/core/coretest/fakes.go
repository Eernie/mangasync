// Package coretest provides in-memory fake adapters and reusable contract checks.
package coretest

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

	"mangasync/internal/core"
)

var (
	_ core.Reader        = (*FakeReader)(nil)
	_ core.Tracker       = (*FakeTracker)(nil)
	_ core.LibraryLister = (*FakeTracker)(nil)
	_ core.Downloader    = (*FakeDownloader)(nil)
)

type FakeReader struct {
	ReaderName string
	Series     map[string]core.Series
	Progress   map[string]core.ReadProgress
	Started    []string // refs returned by ListStartedSeries
}

func (f *FakeReader) Name() string { return f.ReaderName }

func (f *FakeReader) ListStartedSeries(context.Context) ([]core.Series, error) {
	var out []core.Series
	for _, ref := range f.Started {
		out = append(out, f.Series[ref])
	}
	return out, nil
}

func (f *FakeReader) ListAllSeries(context.Context) ([]core.Series, error) {
	out := make([]core.Series, 0, len(f.Series))
	for _, s := range f.Series {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out, nil
}

func (f *FakeReader) GetSeries(_ context.Context, ref string) (core.Series, error) {
	s, ok := f.Series[ref]
	if !ok {
		return core.Series{}, fmt.Errorf("fake reader: no series %q", ref)
	}
	return s, nil
}

func (f *FakeReader) GetProgress(_ context.Context, ref string) (core.ReadProgress, error) {
	p, ok := f.Progress[ref]
	if !ok {
		return core.ReadProgress{}, fmt.Errorf("fake reader: no progress for %q", ref)
	}
	return p, nil
}

type SavedEntry struct {
	ID     string
	Update core.EntryUpdate
}

type FakeTracker struct {
	TrackerName string
	IDsByRef    map[string]string // reader series Ref -> tracker ID
	ResolveErr  error
	Ended       map[string]bool
	Entries     map[string]*core.Entry
	SaveErr     error
	Library     []core.LibraryEntry

	mu           sync.Mutex
	saved        []SavedEntry
	resolveCalls int
}

func (f *FakeTracker) Name() string { return f.TrackerName }

func (f *FakeTracker) Resolve(_ context.Context, s core.Series) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolveCalls++
	if f.ResolveErr != nil {
		return "", false, f.ResolveErr
	}
	id, ok := f.IDsByRef[s.Ref]
	return id, ok, nil
}

func (f *FakeTracker) SeriesEnded(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Ended[id], nil
}

func (f *FakeTracker) GetEntry(_ context.Context, id string) (*core.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.Entries[id]
	if !ok || e == nil {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (f *FakeTracker) SaveEntry(_ context.Context, id string, u core.EntryUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.SaveErr != nil {
		return f.SaveErr
	}
	f.saved = append(f.saved, SavedEntry{ID: id, Update: u})
	if f.Entries == nil {
		f.Entries = map[string]*core.Entry{}
	}
	e := f.Entries[id]
	if e == nil {
		e = &core.Entry{}
		f.Entries[id] = e
	}
	if u.Status != nil {
		e.Status = *u.Status
	}
	if u.Chapter != nil {
		v := *u.Chapter
		e.Chapter = &v
	}
	if u.Volume != nil {
		v := *u.Volume
		e.Volume = &v
	}
	if u.StartDate != nil {
		e.StartDate = *u.StartDate
	}
	if u.FinishDate != nil {
		e.FinishDate = *u.FinishDate
	}
	return nil
}

func (f *FakeTracker) ListLibrary(_ context.Context, statuses []core.Status) ([]core.LibraryEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.LibraryEntry
	for _, e := range f.Library {
		if slices.Contains(statuses, e.Status) {
			out = append(out, e)
		}
	}
	return out, nil
}

// SetLibrary replaces the library under the lock (for tests that change it mid-run).
func (f *FakeTracker) SetLibrary(l []core.LibraryEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Library = l
}

func (f *FakeTracker) SavedEntries() []SavedEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.saved)
}

func (f *FakeTracker) ResolveCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resolveCalls
}

type FakeDownloader struct {
	DownloaderName string
	Library        map[string]core.Candidate // keyed by tracker series Ref
	Search         map[string]core.Candidate // keyed by tracker series Ref
	AcquireErr     error

	mu        sync.Mutex
	findCalls int
	acquired  []core.Candidate
	released  []core.Candidate
}

func (f *FakeDownloader) Name() string { return f.DownloaderName }

func (f *FakeDownloader) FindInLibrary(_ context.Context, s core.Series) (*core.Candidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.Library[s.Ref]; ok {
		return &c, nil
	}
	return nil, nil
}

func (f *FakeDownloader) Find(_ context.Context, s core.Series) (*core.Candidate, []core.Candidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.findCalls++
	if c, ok := f.Search[s.Ref]; ok {
		return &c, nil, nil
	}
	return nil, []core.Candidate{{Ref: "near", Title: "Near Miss", Score: 0.5}}, nil
}

func (f *FakeDownloader) Acquire(_ context.Context, c core.Candidate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.AcquireErr != nil {
		return f.AcquireErr
	}
	f.acquired = append(f.acquired, c)
	return nil
}

func (f *FakeDownloader) Release(_ context.Context, c core.Candidate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, c)
	return nil
}

// SetLibrary replaces the library under the lock.
func (f *FakeDownloader) SetLibrary(l map[string]core.Candidate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Library = l
}

func (f *FakeDownloader) SetAcquireErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.AcquireErr = err
}

func (f *FakeDownloader) FindCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.findCalls
}

func (f *FakeDownloader) AcquiredCandidates() []core.Candidate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.acquired)
}

func (f *FakeDownloader) ReleasedCandidates() []core.Candidate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.released)
}

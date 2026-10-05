// Package core defines the provider-neutral types and the ports that adapters implement.
// It must not import any other package from this module.
package core

import (
	"context"
	"time"
)

// IDKind names an external database a series can be identified by.
type IDKind string

const (
	IDMangaBaka    IDKind = "mangabaka"
	IDAniList      IDKind = "anilist"
	IDMAL          IDKind = "mal"
	IDMangaUpdates IDKind = "mangaupdates"
	IDKitsu        IDKind = "kitsu"
	IDAnimePlanet  IDKind = "animeplanet"
	IDANN          IDKind = "ann"
	IDMangaDex     IDKind = "mangadex"
)

// IDs holds the cross-reference IDs known for a series.
type IDs map[IDKind]string

// Series is a series as seen by one service.
type Series struct {
	Ref        string // the owning service's own ID
	Title      string
	AltTitles  []string
	IDs        IDs
	LibraryRef string // reader only: library the series lives in
}

// Titles returns the main title followed by the non-empty alternative titles.
func (s Series) Titles() []string {
	out := make([]string, 0, 1+len(s.AltTitles))
	if s.Title != "" {
		out = append(out, s.Title)
	}
	for _, t := range s.AltTitles {
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// Unit says whether reader books are chapters or volumes.
type Unit string

const (
	UnitChapter Unit = "chapter"
	UnitVolume  Unit = "volume"
)

// ReadProgress is the reader's view of how far a series has been read.
type ReadProgress struct {
	Unit            Unit
	BooksTotal      int
	BooksRead       int
	BooksInProgress int       // partially read books
	LastReadNumber  float64   // last continuously-read chapter/volume number
	MaxNumber       float64   // highest chapter/volume number present
	FirstReadAt     time.Time // earliest read date of any read or in-progress book; zero = unknown
	LastReadAt      time.Time // latest read date of any read or in-progress book; zero = unknown
}

// AllRead reports whether every book in the series has been read.
func (p ReadProgress) AllRead() bool { return p.BooksTotal > 0 && p.BooksRead >= p.BooksTotal }

// Entry is a series' entry in the user's tracker list.
type Entry struct {
	Status     Status
	Chapter    *float64
	Volume     *float64
	StartDate  string // YYYY-MM-DD, "" = not set
	FinishDate string // YYYY-MM-DD, "" = not set
}

// EntryUpdate is a partial update; nil fields are left unchanged.
type EntryUpdate struct {
	Status     *Status
	Chapter    *float64
	Volume     *float64
	StartDate  *string // YYYY-MM-DD
	FinishDate *string // YYYY-MM-DD
}

// LibraryEntry is one series in the user's tracker list.
type LibraryEntry struct {
	Series Series // Ref = tracker ID, titles and IDs filled
	Status Status
}

// Candidate is a manga in the downloader that may correspond to a series.
type Candidate struct {
	Ref        string // downloader's own manga ID
	SourceName string
	Title      string
	Score      float64
}

// Reader is a service where the user reads manga, e.g. Komga.
type Reader interface {
	Name() string
	ListAllSeries(ctx context.Context) ([]Series, error) // every series, with IDs
	GetSeries(ctx context.Context, ref string) (Series, error)
	GetProgress(ctx context.Context, ref string) (ReadProgress, error)
}

// ProgressWatcher is an optional Reader capability. Readers without it are synced by reconcile only.
type ProgressWatcher interface {
	// WatchProgress emits reader series refs whose progress changed. The channel closes on disconnect.
	WatchProgress(ctx context.Context) (<-chan string, error)
}

// Tracker is a list/tracking service, e.g. MangaBaka.
type Tracker interface {
	Name() string
	// Resolve finds this tracker's series ID for a reader series: s.IDs first, title search last.
	Resolve(ctx context.Context, s Series) (id string, found bool, err error)
	// SeriesEnded reports whether publication has finished (completed or cancelled).
	SeriesEnded(ctx context.Context, id string) (bool, error)
	GetEntry(ctx context.Context, id string) (*Entry, error) // nil, nil if not in the user's list
	// SaveEntry creates the entry if it is not in the user's list yet, otherwise applies a partial update.
	SaveEntry(ctx context.Context, id string, u EntryUpdate) error
}

// LibraryLister is an optional Tracker capability, required for DOWNLOAD_TRACKER.
type LibraryLister interface {
	ListLibrary(ctx context.Context, statuses []Status) ([]LibraryEntry, error)
}

// Downloader fetches manga from online sources, e.g. Suwayomi.
type Downloader interface {
	Name() string
	// FindInLibrary matches s against the downloader's own library only (no source searches).
	// Returns nil, nil when nothing matches.
	FindInLibrary(ctx context.Context, s Series) (*Candidate, error)
	// Find searches the downloader's sources. best is nil when nothing passes the threshold.
	// nearMisses are the closest rejected candidates, for logging.
	Find(ctx context.Context, s Series) (best *Candidate, nearMisses []Candidate, err error)
	// Acquire adds the candidate to the library and queues every not-yet-downloaded chapter. Idempotent.
	Acquire(ctx context.Context, c Candidate) error
	// Release removes the candidate from the library; downloaded files are kept. Idempotent.
	Release(ctx context.Context, c Candidate) error
}

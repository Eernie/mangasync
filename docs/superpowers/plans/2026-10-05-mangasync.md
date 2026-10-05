# MangaSync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go service that syncs Komga read progress to MangaBaka and drives Suwayomi downloads from MangaBaka library statuses, with swappable reader/tracker/downloader adapters.

**Architecture:** Ports and adapters. `internal/core` defines neutral types and interfaces; `internal/sync/progress` and `internal/sync/download` hold all business rules and only see those interfaces; each service is an adapter package under `internal/adapters/`. A single long-running process (Kubernetes Deployment, 1 replica) runs a Komga SSE watcher, an hourly reconcile and a 15-minute download sync, with state in SQLite.

**Tech Stack:** Go 1.27, standard library `net/http` + `log/slog`, `modernc.org/sqlite` (pure Go), `golang.org/x/time/rate`, `golang.org/x/text/unicode/norm`. Tests use the standard `testing` package and `net/http/httptest` only.

**Spec:** `docs/superpowers/specs/2026-10-05-mangasync-design.md` — read it before starting. The *Status lifecycle* and *Download sync* sections are the source of truth for behaviour.

**Conventions for every task:**
- Module path is `mangasync` (imports look like `mangasync/internal/core`).
- Run `gofmt -l .` before committing; it must print nothing.
- Run tests with `go test -race ./...` unless the step says otherwise.
- Commit messages end with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (shown once here, implied in every commit step).

---

## File structure

```
go.mod / go.sum
cmd/mangasync/main.go                 wiring, health server, signal handling
internal/core/core.go                 neutral types + port interfaces
internal/core/status.go               Status constants + ParseStatuses
internal/core/coretest/fakes.go       in-memory fake adapters (thread-safe)
internal/core/coretest/contract.go    reusable adapter contract checks
internal/match/normalize.go           Normalize, Similarity, levenshtein
internal/match/best.go                Score, Best
internal/match/links.go               ParseLink, IDsFromLinks
internal/match/same.go                SameSeries
internal/store/store.go               SQLite open/migrate + both tables
internal/httpx/httpx.go               retrying, rate-limited HTTP client + DoJSON
internal/envutil/envutil.go           env lookup helpers
internal/config/config.go             core config from env
internal/sync/progress/decide.go      Target, ComputeTarget, Decide (pure)
internal/sync/progress/syncer.go      Syncer.SyncSeries
internal/sync/download/syncer.go      Syncer.Run, NotFoundBackoff
internal/adapters/reader/komga/       komga.go, sse.go, env.go
internal/adapters/tracker/mangabaka/  mangabaka.go, status.go, entries.go, series.go, library.go, env.go
internal/adapters/downloader/suwayomi/ suwayomi.go, graphql.go, find.go, acquire.go, env.go
internal/adapters/registry/registry.go name → adapter constructor
internal/app/queue.go                 dedup queue + debouncer
internal/app/app.go                   loops: worker, reconcile, watcher, download
Dockerfile, .dockerignore
deploy/                               kustomization, deployment, pvc, configmap, secret.example
README.md
```

---

### Task 1: Module scaffold and core types

**Files:**
- Create: `go.mod`
- Create: `internal/core/core.go`
- Create: `internal/core/status.go`
- Test: `internal/core/core_test.go`

- [ ] **Step 1: Initialise the module**

```bash
cd /Users/erwin/workspace/private/MangaSync
go mod init mangasync
```
Expected: `go: creating new go.mod: module mangasync`

- [ ] **Step 2: Write the failing test**

`internal/core/core_test.go`:
```go
package core

import (
	"slices"
	"testing"
)

func TestParseStatuses(t *testing.T) {
	got, err := ParseStatuses(" planning, Reading,rereading ,")
	if err != nil {
		t.Fatal(err)
	}
	want := []Status{StatusPlanning, StatusReading, StatusRereading}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if got, err := ParseStatuses(""); err != nil || got != nil {
		t.Fatalf("empty: got %v, %v; want nil, nil", got, err)
	}
	if _, err := ParseStatuses("planning,bogus"); err == nil {
		t.Fatal("expected error for unknown status")
	}
	if _, err := ParseStatuses("unknown"); err == nil {
		t.Fatal("expected error: 'unknown' is not configurable")
	}
}

func TestSeriesTitles(t *testing.T) {
	s := Series{Title: "Chainsaw Man", AltTitles: []string{"", "Chain Saw Man"}}
	if got := s.Titles(); !slices.Equal(got, []string{"Chainsaw Man", "Chain Saw Man"}) {
		t.Fatalf("got %v", got)
	}
	if got := (Series{}).Titles(); len(got) != 0 {
		t.Fatalf("empty series: got %v", got)
	}
}

func TestReadProgressAllRead(t *testing.T) {
	cases := []struct {
		p    ReadProgress
		want bool
	}{
		{ReadProgress{BooksTotal: 0, BooksRead: 0}, false},
		{ReadProgress{BooksTotal: 10, BooksRead: 9}, false},
		{ReadProgress{BooksTotal: 10, BooksRead: 10}, true},
	}
	for _, c := range cases {
		if got := c.p.AllRead(); got != c.want {
			t.Errorf("%+v: got %v, want %v", c.p, got, c.want)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/core/`
Expected: FAIL — `undefined: ParseStatuses` (and other undefined names).

- [ ] **Step 4: Write the implementation**

`internal/core/status.go`:
```go
package core

import (
	"fmt"
	"slices"
	"strings"
)

// Status is the provider-neutral library status of a series in a tracker.
type Status string

const (
	StatusConsidering Status = "considering"
	StatusPlanning    Status = "planning"
	StatusReading     Status = "reading"
	StatusCompleted   Status = "completed"
	StatusPaused      Status = "paused"
	StatusDropped     Status = "dropped"
	StatusRereading   Status = "rereading"
	StatusUnknown     Status = "unknown"
)

var configurableStatuses = []Status{
	StatusConsidering, StatusPlanning, StatusReading, StatusCompleted,
	StatusPaused, StatusDropped, StatusRereading,
}

// ParseStatuses parses a comma-separated status list. Empty input returns nil.
func ParseStatuses(csv string) ([]Status, error) {
	var out []Status
	for _, part := range strings.Split(csv, ",") {
		s := Status(strings.ToLower(strings.TrimSpace(part)))
		if s == "" {
			continue
		}
		if !slices.Contains(configurableStatuses, s) {
			return nil, fmt.Errorf("unknown status %q (valid: %v)", part, configurableStatuses)
		}
		out = append(out, s)
	}
	return out, nil
}
```

`internal/core/core.go`:
```go
// Package core defines the provider-neutral types and the ports that adapters implement.
// It must not import any other package from this module.
package core

import "context"

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
	BooksInProgress int     // partially read books
	LastReadNumber  float64 // last continuously-read chapter/volume number
	MaxNumber       float64 // highest chapter/volume number present
}

// AllRead reports whether every book in the series has been read.
func (p ReadProgress) AllRead() bool { return p.BooksTotal > 0 && p.BooksRead >= p.BooksTotal }

// Entry is a series' entry in the user's tracker list.
type Entry struct {
	Status  Status
	Chapter *float64
	Volume  *float64
}

// EntryUpdate is a partial update; nil fields are left unchanged.
type EntryUpdate struct {
	Status  *Status
	Chapter *float64
	Volume  *float64
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

type Reader interface {
	Name() string
	ListStartedSeries(ctx context.Context) ([]Series, error) // series with any read progress
	ListAllSeries(ctx context.Context) ([]Series, error)     // every series, with IDs
	GetSeries(ctx context.Context, ref string) (Series, error)
	GetProgress(ctx context.Context, ref string) (ReadProgress, error)
}

// ProgressWatcher is an optional Reader capability. Readers without it are synced by reconcile only.
type ProgressWatcher interface {
	// WatchProgress emits reader series refs whose progress changed. The channel closes on disconnect.
	WatchProgress(ctx context.Context) (<-chan string, error)
}

type Tracker interface {
	Name() string
	// Resolve finds this tracker's series ID for a reader series: s.IDs first, title search last.
	Resolve(ctx context.Context, s Series) (id string, found bool, err error)
	// SeriesEnded reports whether publication has finished (completed or cancelled).
	SeriesEnded(ctx context.Context, id string) (bool, error)
	GetEntry(ctx context.Context, id string) (*Entry, error) // nil, nil if not in the user's list
	SaveEntry(ctx context.Context, id string, u EntryUpdate) error
}

// LibraryLister is an optional Tracker capability, required for DOWNLOAD_TRACKER.
type LibraryLister interface {
	ListLibrary(ctx context.Context, statuses []Status) ([]LibraryEntry, error)
}

type Downloader interface {
	Name() string
	// FindInLibrary matches s against the downloader's own library only (no source searches).
	FindInLibrary(ctx context.Context, s Series) (*Candidate, error)
	// Find searches the downloader's sources. best is nil when nothing passes the threshold.
	Find(ctx context.Context, s Series) (best *Candidate, nearMisses []Candidate, err error)
	// Acquire adds the candidate to the library and queues every not-yet-downloaded chapter. Idempotent.
	Acquire(ctx context.Context, c Candidate) error
	// Release removes the candidate from the library; downloaded files are kept. Idempotent.
	Release(ctx context.Context, c Candidate) error
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/core/`
Expected: `ok  	mangasync/internal/core`

- [ ] **Step 6: Commit**

```bash
git add go.mod internal/core
git commit -m "Add core types and port interfaces"
```

---

### Task 2: Title normalization and similarity

**Files:**
- Create: `internal/match/normalize.go`
- Test: `internal/match/normalize_test.go`

- [ ] **Step 1: Add the dependency**

```bash
go get golang.org/x/text@latest
```

- [ ] **Step 2: Write the failing test**

`internal/match/normalize_test.go`:
```go
package match

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga": "naruto sasukes story the uchiha and the heavenly stardust the manga",
		"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga":   "naruto sasukes story the uchiha and the heavenly stardust the manga",
		"Frieren - Beyond Journey's End": "frieren beyond journeys end",
		"Frieren: Beyond Journey’s End":  "frieren beyond journeys end",
		"The Promised Neverland":         "promised neverland",
		"Dr. STONE":                      "dr stone",
		"  Chainsaw   Man ":              "chainsaw man",
		"Ｃｈａｉｎｓａｗ Man":                  "chainsaw man",
		"A":                              "a",
		"[Oshi no Ko]":                   "oshi no ko",
		"":                               "",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSimilarity(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"Frieren - Beyond Journey's End", "Frieren: Beyond Journey’s End", 1},
		{"LOSTEND", "Lost End", 1},
		{"Chainsaw Man", "Chainsaw Men", 1 - 1.0/12},
		{"abc", "xyz", 0},
		{"", "abc", 0},
	}
	for _, c := range cases {
		if got := Similarity(c.a, c.b); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("Similarity(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/match/`
Expected: FAIL — `undefined: Normalize`.

- [ ] **Step 4: Write the implementation**

`internal/match/normalize.go`:
```go
// Package match holds the pure title and ID matching shared by sync logic and adapters.
package match

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalize lowercases s and reduces it to letters, digits and single spaces so that
// differently punctuated titles compare equal. Apostrophes are dropped ("Journey's" ==
// "Journeys"); every other non-alphanumeric rune (including "_" from filesystem names)
// becomes a space. A leading "the" or "a" is dropped.
func Normalize(s string) string {
	s = strings.ToLower(norm.NFKC.String(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\'' || r == '’' || r == '‘' || r == '`':
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			b.WriteRune(r)
		default:
			b.WriteByte(' ')
		}
	}
	fields := strings.Fields(b.String())
	if len(fields) > 1 && (fields[0] == "the" || fields[0] == "a") {
		fields = fields[1:]
	}
	return strings.Join(fields, " ")
}

// Similarity returns 1 − Levenshtein distance / longer length of the normalized strings.
// Titles that are equal after removing spaces ("LOSTEND" / "Lost End") score 1.
func Similarity(a, b string) float64 {
	na, nb := Normalize(a), Normalize(b)
	if na == "" || nb == "" {
		return 0
	}
	if na == nb || strings.ReplaceAll(na, " ", "") == strings.ReplaceAll(nb, " ", "") {
		return 1
	}
	ra, rb := []rune(na), []rune(nb)
	return 1 - float64(levenshtein(ra, rb))/float64(max(len(ra), len(rb)))
}

func levenshtein(a, b []rune) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/match/`
Expected: `ok  	mangasync/internal/match`

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/match
git commit -m "Add title normalization and similarity"
```

---

### Task 3: Best-candidate selection

**Files:**
- Create: `internal/match/best.go`
- Test: `internal/match/best_test.go`

- [ ] **Step 1: Write the failing test**

`internal/match/best_test.go`:
```go
package match

import "testing"

type cand struct {
	id    int
	title string
}

func titlesOf(c cand) []string { return []string{c.title} }

func TestBestPicksHighestAccepted(t *testing.T) {
	candidates := []cand{
		{84642, "Naruto Retsuden"},
		{374014, "Sasuke's Story: The Uchiha and the Heavenly Stardust"},
		{56702, "Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga"},
	}
	want := []string{"Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"}
	best, near := Best(want, candidates, titlesOf, 0.9)
	if best == nil || best.Item.id != 56702 || best.Score != 1 {
		t.Fatalf("best = %+v, want id 56702 with score 1", best)
	}
	if len(near) != 2 || near[0].Score < near[1].Score {
		t.Fatalf("near misses should be the 2 rejected, sorted desc: %+v", near)
	}
}

func TestBestUsesAltTitles(t *testing.T) {
	want := []string{"Sousou no Frieren", "Frieren: Beyond Journey’s End"}
	best, _ := Best(want, []cand{{130, "Frieren - Beyond Journey's End"}}, titlesOf, 0.9)
	if best == nil || best.Item.id != 130 {
		t.Fatalf("expected match via alt title, got %+v", best)
	}
}

func TestBestNoMatchReturnsAtMostThreeNearMisses(t *testing.T) {
	candidates := []cand{{1, "aaaa"}, {2, "bbbb"}, {3, "cccc"}, {4, "dddd"}, {5, "Chainsaw Men"}}
	best, near := Best([]string{"Chainsaw Man"}, candidates, titlesOf, 0.95)
	if best != nil {
		t.Fatalf("expected no match, got %+v", best)
	}
	if len(near) != 3 || near[0].Item.id != 5 {
		t.Fatalf("near misses = %+v, want 3 with id 5 first", near)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/match/ -run Best`
Expected: FAIL — `undefined: Best`.

- [ ] **Step 3: Write the implementation**

`internal/match/best.go`:
```go
package match

import "sort"

// Scored pairs an item with its match score.
type Scored[T any] struct {
	Item  T
	Score float64
}

// Score is the best Similarity between any wanted title and any candidate title.
func Score(want, have []string) float64 {
	best := 0.0
	for _, w := range want {
		for _, h := range have {
			if s := Similarity(w, h); s > best {
				best = s
			}
		}
	}
	return best
}

// Best returns the highest-scoring candidate at or above threshold (first one wins ties),
// plus up to three of the best rejected candidates for logging.
func Best[T any](want []string, candidates []T, titlesOf func(T) []string, threshold float64) (*Scored[T], []Scored[T]) {
	var best *Scored[T]
	var rejected []Scored[T]
	for _, c := range candidates {
		s := Score(want, titlesOf(c))
		if s >= threshold {
			if best == nil || s > best.Score {
				best = &Scored[T]{Item: c, Score: s}
			}
			continue
		}
		rejected = append(rejected, Scored[T]{Item: c, Score: s})
	}
	sort.SliceStable(rejected, func(i, j int) bool { return rejected[i].Score > rejected[j].Score })
	if len(rejected) > 3 {
		rejected = rejected[:3]
	}
	return best, rejected
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/match/`
Expected: `ok  	mangasync/internal/match`

- [ ] **Step 5: Commit**

```bash
git add internal/match
git commit -m "Add best-candidate selection"
```

---

### Task 4: Link parsing

**Files:**
- Create: `internal/match/links.go`
- Test: `internal/match/links_test.go`

- [ ] **Step 1: Write the failing test**

`internal/match/links_test.go` (URLs are real values from the live Komga instance):
```go
package match

import (
	"testing"

	"mangasync/internal/core"
)

func TestParseLink(t *testing.T) {
	cases := []struct {
		url  string
		kind core.IDKind
		id   string
		ok   bool
	}{
		{"https://www.mangaupdates.com/series/ylx5wzn/chainsaw-man", core.IDMangaUpdates, "ylx5wzn", true},
		{"https://anilist.co/manga/105778", core.IDAniList, "105778", true},
		{"https://anilist.co/manga/105778/Chainsaw-Man/", core.IDAniList, "105778", true},
		{"https://mangadex.org/title/a77742b1-befd-49a4-bff5-1ad4e6b0ef7b", core.IDMangaDex, "a77742b1-befd-49a4-bff5-1ad4e6b0ef7b", true},
		{"https://www.anime-planet.com/manga/chainsaw-man", core.IDAnimePlanet, "chainsaw-man", true},
		{"https://kitsu.app/manga/54139", core.IDKitsu, "54139", true},
		{"https://kitsu.io/manga/54139", core.IDKitsu, "54139", true},
		{"https://myanimelist.net/manga/116778", core.IDMAL, "116778", true},
		{"https://mangabaka.org/manga/1677/Chainsaw-Man", core.IDMangaBaka, "1677", true},
		{"https://www.animenewsnetwork.com/encyclopedia/manga.php?id=21271", core.IDANN, "21271", true},
		{"https://www.amazon.co.jp/dp/B07MX551PW", "", "", false},
		{"https://anilist.co/user/someone", "", "", false},
		{"not a url", "", "", false},
	}
	for _, c := range cases {
		kind, id, ok := ParseLink(c.url)
		if kind != c.kind || id != c.id || ok != c.ok {
			t.Errorf("ParseLink(%q) = %q, %q, %v; want %q, %q, %v", c.url, kind, id, ok, c.kind, c.id, c.ok)
		}
	}
}

func TestIDsFromLinksFirstWins(t *testing.T) {
	ids := IDsFromLinks([]string{
		"https://anilist.co/manga/1",
		"https://www.amazon.co.jp/dp/X",
		"https://anilist.co/manga/2",
		"https://myanimelist.net/manga/3",
	})
	if len(ids) != 2 || ids[core.IDAniList] != "1" || ids[core.IDMAL] != "3" {
		t.Fatalf("got %v", ids)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/match/ -run Link`
Expected: FAIL — `undefined: ParseLink`.

- [ ] **Step 3: Write the implementation**

`internal/match/links.go`:
```go
package match

import (
	"net/url"
	"strings"

	"mangasync/internal/core"
)

// ParseLink extracts a cross-reference ID from a series URL. Unknown sites return ok=false.
func ParseLink(raw string) (core.IDKind, string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "", "", false
	}
	host := strings.TrimPrefix(strings.ToLower(u.Host), "www.")
	seg := strings.Split(strings.Trim(u.Path, "/"), "/")
	at := func(i int) string {
		if i < len(seg) {
			return seg[i]
		}
		return ""
	}
	pathID := func(prefix string, kind core.IDKind) (core.IDKind, string, bool) {
		if at(0) == prefix && at(1) != "" {
			return kind, at(1), true
		}
		return "", "", false
	}
	switch host {
	case "mangabaka.org", "mangabaka.dev":
		return pathID("manga", core.IDMangaBaka)
	case "anilist.co":
		return pathID("manga", core.IDAniList)
	case "myanimelist.net":
		return pathID("manga", core.IDMAL)
	case "mangaupdates.com":
		return pathID("series", core.IDMangaUpdates)
	case "kitsu.app", "kitsu.io":
		return pathID("manga", core.IDKitsu)
	case "anime-planet.com":
		return pathID("manga", core.IDAnimePlanet)
	case "mangadex.org":
		return pathID("title", core.IDMangaDex)
	case "animenewsnetwork.com":
		if at(0) == "encyclopedia" && at(1) == "manga.php" {
			if id := u.Query().Get("id"); id != "" {
				return core.IDANN, id, true
			}
		}
	}
	return "", "", false
}

// IDsFromLinks collects the IDs found in urls. The first URL of each kind wins.
func IDsFromLinks(urls []string) core.IDs {
	ids := core.IDs{}
	for _, raw := range urls {
		if kind, id, ok := ParseLink(raw); ok {
			if _, seen := ids[kind]; !seen {
				ids[kind] = id
			}
		}
	}
	return ids
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/match/`
Expected: `ok  	mangasync/internal/match`

- [ ] **Step 5: Commit**

```bash
git add internal/match
git commit -m "Add series link parsing"
```

---

### Task 5: Same-series check

**Files:**
- Create: `internal/match/same.go`
- Test: `internal/match/same_test.go`

- [ ] **Step 1: Write the failing test**

`internal/match/same_test.go`:
```go
package match

import (
	"testing"

	"mangasync/internal/core"
)

func TestSameSeries(t *testing.T) {
	cases := []struct {
		name string
		a, b core.Series
		want bool
	}{
		{"shared ID equal",
			core.Series{Title: "X", IDs: core.IDs{core.IDAniList: "105778"}},
			core.Series{Title: "Totally different", IDs: core.IDs{core.IDAniList: "105778", core.IDMAL: "1"}},
			true},
		{"shared ID case-insensitive",
			core.Series{IDs: core.IDs{core.IDMangaUpdates: "YLX5WZN"}},
			core.Series{IDs: core.IDs{core.IDMangaUpdates: "ylx5wzn"}},
			true},
		{"shared kind but different ID beats title",
			core.Series{Title: "Naruto", IDs: core.IDs{core.IDAniList: "30011"}},
			core.Series{Title: "Naruto", IDs: core.IDs{core.IDAniList: "99999"}},
			false},
		{"no shared IDs, title match",
			core.Series{Title: "Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"},
			core.Series{Title: "Sasuke Shinden", AltTitles: []string{"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga"}, IDs: core.IDs{core.IDAniList: "1"}},
			true},
		{"no shared IDs, different titles",
			core.Series{Title: "Dandadan"},
			core.Series{Title: "Kagurabachi"},
			false},
	}
	for _, c := range cases {
		if got := SameSeries(c.a, c.b, 0.9); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/match/ -run SameSeries`
Expected: FAIL — `undefined: SameSeries`.

- [ ] **Step 3: Write the implementation**

`internal/match/same.go`:
```go
package match

import (
	"strings"

	"mangasync/internal/core"
)

// SameSeries reports whether a and b are the same series. A shared ID kind decides:
// equal IDs mean yes, different IDs mean no. Without a shared kind, titles must
// score at least threshold.
func SameSeries(a, b core.Series, threshold float64) bool {
	sharedKind := false
	for kind, va := range a.IDs {
		vb, ok := b.IDs[kind]
		if !ok || va == "" || vb == "" {
			continue
		}
		if strings.EqualFold(va, vb) {
			return true
		}
		sharedKind = true
	}
	if sharedKind {
		return false
	}
	return Score(a.Titles(), b.Titles()) >= threshold
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/match/`
Expected: `ok  	mangasync/internal/match`

- [ ] **Step 5: Commit**

```bash
git add internal/match
git commit -m "Add same-series check"
```

---

### Task 6: SQLite store

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

- [ ] **Step 1: Add the dependency**

```bash
go get modernc.org/sqlite@latest
```

- [ ] **Step 2: Write the failing test**

`internal/store/store_test.go`:
```go
package store

import (
	"context"
	"path/filepath"
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/store/`
Expected: FAIL — `undefined: Open`.

- [ ] **Step 4: Write the implementation**

`internal/store/store.go`:
```go
// Package store persists sync state in SQLite. Everything in it is a cache that can be
// rebuilt from the external services, so losing the database is safe.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"mangasync/internal/core"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS series_map (
	reader        TEXT NOT NULL,
	reader_ref    TEXT NOT NULL,
	tracker       TEXT NOT NULL,
	tracker_id    TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL,
	last_attempt  INTEGER NOT NULL DEFAULT 0,
	last_status   TEXT NOT NULL DEFAULT '',
	last_progress REAL,
	PRIMARY KEY (reader, reader_ref, tracker)
);
CREATE TABLE IF NOT EXISTS downloads (
	tracker       TEXT NOT NULL,
	tracker_id    TEXT NOT NULL,
	downloader    TEXT NOT NULL,
	status        TEXT NOT NULL,
	candidate_ref TEXT NOT NULL DEFAULT '',
	source        TEXT NOT NULL DEFAULT '',
	attempts      INTEGER NOT NULL DEFAULT 0,
	retry_after   INTEGER NOT NULL DEFAULT 0,
	updated_at    INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (tracker, tracker_id, downloader)
);`

type MatchStatus string

const (
	Matched   MatchStatus = "matched"
	Unmatched MatchStatus = "unmatched"
)

// SeriesMapping links a reader series to a tracker series.
type SeriesMapping struct {
	Reader       string
	ReaderRef    string
	Tracker      string
	TrackerID    string
	Status       MatchStatus
	LastAttempt  time.Time
	LastStatus   core.Status
	LastProgress *float64
}

type DownloadStatus string

const (
	Acquired   DownloadStatus = "acquired"
	Released   DownloadStatus = "released"
	NotFound   DownloadStatus = "not_found"
	InProgress DownloadStatus = "in_progress"
)

// DownloadRecord is what download sync last did for a tracker series.
type DownloadRecord struct {
	Tracker      string
	TrackerID    string
	Downloader   string
	Status       DownloadStatus
	CandidateRef string
	Source       string
	Attempts     int
	RetryAfter   time.Time
	UpdatedAt    time.Time
}

type Store struct{ db *sql.DB }

// Open opens (creating if needed) the database at path and applies the schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite has a single writer; one connection also keeps pragmas.
	for _, stmt := range []string{"PRAGMA busy_timeout = 5000", "PRAGMA journal_mode = WAL", schema} {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("init %s: %w", path, err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// GetMapping returns nil, nil when no mapping exists.
func (s *Store) GetMapping(ctx context.Context, reader, readerRef, tracker string) (*SeriesMapping, error) {
	m := SeriesMapping{Reader: reader, ReaderRef: readerRef, Tracker: tracker}
	var status, lastStatus string
	var lastAttempt int64
	var lastProgress sql.NullFloat64
	err := s.db.QueryRowContext(ctx,
		`SELECT tracker_id, status, last_attempt, last_status, last_progress
		 FROM series_map WHERE reader = ? AND reader_ref = ? AND tracker = ?`,
		reader, readerRef, tracker).Scan(&m.TrackerID, &status, &lastAttempt, &lastStatus, &lastProgress)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get mapping: %w", err)
	}
	m.Status, m.LastStatus, m.LastAttempt = MatchStatus(status), core.Status(lastStatus), fromUnix(lastAttempt)
	if lastProgress.Valid {
		v := lastProgress.Float64
		m.LastProgress = &v
	}
	return &m, nil
}

func (s *Store) PutMapping(ctx context.Context, m SeriesMapping) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO series_map (reader, reader_ref, tracker, tracker_id, status, last_attempt, last_status, last_progress)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (reader, reader_ref, tracker) DO UPDATE SET
		   tracker_id = excluded.tracker_id, status = excluded.status, last_attempt = excluded.last_attempt,
		   last_status = excluded.last_status, last_progress = excluded.last_progress`,
		m.Reader, m.ReaderRef, m.Tracker, m.TrackerID, string(m.Status), toUnix(m.LastAttempt),
		string(m.LastStatus), nullFloat(m.LastProgress))
	if err != nil {
		return fmt.Errorf("put mapping: %w", err)
	}
	return nil
}

// GetDownload returns nil, nil when no record exists.
func (s *Store) GetDownload(ctx context.Context, tracker, trackerID, downloader string) (*DownloadRecord, error) {
	r := DownloadRecord{Tracker: tracker, TrackerID: trackerID, Downloader: downloader}
	var status string
	var retryAfter, updatedAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT status, candidate_ref, source, attempts, retry_after, updated_at
		 FROM downloads WHERE tracker = ? AND tracker_id = ? AND downloader = ?`,
		tracker, trackerID, downloader).Scan(&status, &r.CandidateRef, &r.Source, &r.Attempts, &retryAfter, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get download: %w", err)
	}
	r.Status, r.RetryAfter, r.UpdatedAt = DownloadStatus(status), fromUnix(retryAfter), fromUnix(updatedAt)
	return &r, nil
}

func (s *Store) PutDownload(ctx context.Context, r DownloadRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO downloads (tracker, tracker_id, downloader, status, candidate_ref, source, attempts, retry_after, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (tracker, tracker_id, downloader) DO UPDATE SET
		   status = excluded.status, candidate_ref = excluded.candidate_ref, source = excluded.source,
		   attempts = excluded.attempts, retry_after = excluded.retry_after, updated_at = excluded.updated_at`,
		r.Tracker, r.TrackerID, r.Downloader, string(r.Status), r.CandidateRef, r.Source, r.Attempts,
		toUnix(r.RetryAfter), toUnix(r.UpdatedAt))
	if err != nil {
		return fmt.Errorf("put download: %w", err)
	}
	return nil
}

func toUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func fromUnix(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

func nullFloat(p *float64) sql.NullFloat64 {
	if p == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *p, Valid: true}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/store/`
Expected: `ok  	mangasync/internal/store`

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/store
git commit -m "Add SQLite store"
```

---

### Task 7: Shared HTTP client

**Files:**
- Create: `internal/httpx/httpx.go`
- Test: `internal/httpx/httpx_test.go`

- [ ] **Step 1: Add the dependency**

```bash
go get golang.org/x/time@latest
```

- [ ] **Step 2: Write the failing test**

`internal/httpx/httpx_test.go`:
```go
package httpx

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fastClient() *Client {
	c := New(5*time.Second, 0)
	c.BaseDelay = time.Millisecond
	return c
}

func TestRetriesServerErrorsAndResendsBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != `{"a":1}` {
			t.Errorf("attempt %d got body %q", calls.Load()+1, b)
		}
		if r.Header.Get("X-Api-Key") != "k" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing headers: %v", r.Header)
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	var out struct{ OK bool }
	h := http.Header{}
	h.Set("X-Api-Key", "k")
	if err := fastClient().DoJSON(context.Background(), http.MethodPost, srv.URL, h, map[string]int{"a": 1}, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK || calls.Load() != 3 {
		t.Fatalf("out=%+v calls=%d", out, calls.Load())
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	err := fastClient().DoJSON(context.Background(), http.MethodGet, srv.URL, nil, nil, nil)
	if !IsNotFound(err) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := fastClient()
	c.MaxAttempts = 3
	err := c.DoJSON(context.Background(), http.MethodGet, srv.URL, nil, nil, nil)
	if !IsStatus(err, http.StatusTooManyRequests) || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := map[string]time.Duration{"": 0, "3": 3 * time.Second, "-1": 0, "Wed, 21 Oct 2015 07:28:00 GMT": 0}
	for in, want := range cases {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/httpx/`
Expected: FAIL — `undefined: New`.

- [ ] **Step 4: Write the implementation**

`internal/httpx/httpx.go`:
```go
// Package httpx is the HTTP client every adapter uses: rate limiting, retries with
// backoff on network errors / 429 / 5xx, and JSON helpers.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// StatusError is returned by DoJSON for non-2xx responses.
type StatusError struct {
	Method string
	URL    string
	Code   int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.URL, e.Code, e.Body)
}

func IsStatus(err error, code int) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == code
}

func IsNotFound(err error) bool { return IsStatus(err, http.StatusNotFound) }

type Client struct {
	HTTP        *http.Client
	Limiter     *rate.Limiter // nil = unlimited
	MaxAttempts int
	BaseDelay   time.Duration
}

// New returns a client with the given request timeout. perMinute <= 0 disables rate limiting.
func New(timeout time.Duration, perMinute int) *Client {
	c := &Client{HTTP: &http.Client{Timeout: timeout}, MaxAttempts: 5, BaseDelay: 500 * time.Millisecond}
	if perMinute > 0 {
		c.Limiter = rate.NewLimiter(rate.Limit(float64(perMinute)/60), 1)
	}
	return c
}

// Do sends req, retrying network errors, 429 and 5xx with exponential backoff and jitter.
// Retry-After (seconds) is honoured. The final response is returned as-is, whatever its status.
// Requests with a body must have GetBody set (http.NewRequest does this for bytes.Reader).
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	attempts := max(c.MaxAttempts, 1)
	var lastErr error
	var retryAfter time.Duration
	for i := range attempts {
		if i > 0 {
			if err := sleep(ctx, c.backoff(i, retryAfter)); err != nil {
				return nil, err
			}
		}
		if c.Limiter != nil {
			if err := c.Limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		attempt := req.Clone(ctx)
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attempt.Body = body
		}
		resp, err := c.HTTP.Do(attempt)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr, retryAfter = err, 0
			continue
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		if retryable && i < attempts-1 {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("%s %s: HTTP %d", req.Method, req.URL, resp.StatusCode)
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// DoJSON sends in (if non-nil) as JSON and decodes a 2xx response into out (if non-nil).
// Non-2xx responses become *StatusError.
func (c *Client) DoJSON(ctx context.Context, method, url string, header http.Header, in, out any) error {
	var body io.Reader = http.NoBody
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return &StatusError{Method: method, URL: url, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, url, err)
	}
	return nil
}

func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := c.BaseDelay << (attempt - 1)
	if c.BaseDelay > 0 {
		d += rand.N(c.BaseDelay)
	}
	return max(d, retryAfter)
}

func parseRetryAfter(v string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/httpx/`
Expected: `ok  	mangasync/internal/httpx`

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/httpx
git commit -m "Add retrying, rate-limited HTTP client"
```

---

### Task 8: Fake adapters and contract checks

**Files:**
- Create: `internal/core/coretest/fakes.go`
- Create: `internal/core/coretest/contract.go`
- Test: `internal/core/coretest/contract_test.go`

These fakes are used by the sync and app tests. They are thread-safe because the app tests touch them from several goroutines.

- [ ] **Step 1: Write the failing test**

`internal/core/coretest/contract_test.go`:
```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/core/coretest/`
Expected: FAIL — `undefined: FakeReader`.

- [ ] **Step 3: Write the fakes**

`internal/core/coretest/fakes.go`:
```go
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
```

- [ ] **Step 4: Write the contract checks**

`internal/core/coretest/contract.go`:
```go
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/core/...`
Expected: `ok  	mangasync/internal/core/coretest`

- [ ] **Step 6: Commit**

```bash
git add internal/core/coretest
git commit -m "Add fake adapters and contract checks"
```

---

### Task 9: Progress target and decision rules

**Files:**
- Create: `internal/sync/progress/decide.go`
- Test: `internal/sync/progress/decide_test.go`

Implements the two tables in the spec's *Status lifecycle* section.

- [ ] **Step 1: Write the failing test**

`internal/sync/progress/decide_test.go`:
```go
package progress

import (
	"testing"

	"mangasync/internal/core"
)

func f(v float64) *float64 { return &v }

func TestComputeTarget(t *testing.T) {
	ch := core.UnitChapter
	cases := []struct {
		name  string
		p     core.ReadProgress
		ended bool
		want  *Target
	}{
		{"nothing read", core.ReadProgress{Unit: ch, BooksTotal: 10}, false, nil},
		{"first book partly read", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksInProgress: 1}, false,
			&Target{Status: core.StatusReading, Unit: ch}},
		{"some read", core.ReadProgress{Unit: ch, BooksTotal: 244, BooksRead: 104, LastReadNumber: 104, MaxNumber: 232}, false,
			&Target{Status: core.StatusReading, Progress: f(104), Unit: ch}},
		{"all read, ongoing", core.ReadProgress{Unit: ch, BooksTotal: 85, BooksRead: 85, LastReadNumber: 85, MaxNumber: 85}, false,
			&Target{Status: core.StatusReading, Progress: f(85), Unit: ch}},
		{"all read, ended", core.ReadProgress{Unit: ch, BooksTotal: 81, BooksRead: 81, LastReadNumber: 80, MaxNumber: 80}, true,
			&Target{Status: core.StatusCompleted, Progress: f(80), Unit: ch}},
		{"read but no continuous number", core.ReadProgress{Unit: ch, BooksTotal: 10, BooksRead: 1, LastReadNumber: 0, MaxNumber: 10}, false,
			&Target{Status: core.StatusReading, Unit: ch}},
	}
	for _, c := range cases {
		got := ComputeTarget(c.p, c.ended)
		if !targetEqual(got, c.want) {
			t.Errorf("%s: got %s, want %s", c.name, fmtTarget(got), fmtTarget(c.want))
		}
	}
}

func TestDecide(t *testing.T) {
	reading := func(p float64) Target { return Target{Status: core.StatusReading, Progress: f(p), Unit: core.UnitChapter} }
	completed := func(p float64) Target { return Target{Status: core.StatusCompleted, Progress: f(p), Unit: core.UnitChapter} }
	entry := func(s core.Status, ch *float64) *core.Entry { return &core.Entry{Status: s, Chapter: ch} }

	cases := []struct {
		name       string
		cur        *core.Entry
		target     Target
		wantNil    bool
		wantStatus core.Status // "" = no status change
		wantCh     *float64
		wantVol    *float64
	}{
		{"new entry", nil, reading(5), false, core.StatusReading, f(5), nil},
		{"new entry, no progress", nil, Target{Status: core.StatusReading, Unit: core.UnitChapter}, false, core.StatusReading, nil, nil},
		{"planning -> reading", entry(core.StatusPlanning, nil), reading(5), false, core.StatusReading, f(5), nil},
		{"considering -> completed", entry(core.StatusConsidering, nil), completed(80), false, core.StatusCompleted, f(80), nil},
		{"reading, progress up", entry(core.StatusReading, f(3)), reading(5), false, "", f(5), nil},
		{"reading, progress lower", entry(core.StatusReading, f(10)), reading(5), true, "", nil, nil},
		{"reading, progress equal", entry(core.StatusReading, f(5)), reading(5), true, "", nil, nil},
		{"reading -> completed", entry(core.StatusReading, f(79)), completed(80), false, core.StatusCompleted, f(80), nil},
		{"completed protected", entry(core.StatusCompleted, f(10)), reading(50), true, "", nil, nil},
		{"paused protected", entry(core.StatusPaused, nil), reading(5), true, "", nil, nil},
		{"dropped protected", entry(core.StatusDropped, nil), reading(5), true, "", nil, nil},
		{"rereading protected", entry(core.StatusRereading, nil), reading(5), true, "", nil, nil},
		{"unknown protected", entry(core.StatusUnknown, nil), reading(5), true, "", nil, nil},
		{"volume unit", &core.Entry{Status: core.StatusReading, Chapter: f(100), Volume: f(2)},
			Target{Status: core.StatusReading, Progress: f(3), Unit: core.UnitVolume}, false, "", nil, f(3)},
	}
	for _, c := range cases {
		got := Decide(c.cur, c.target)
		if c.wantNil {
			if got != nil {
				t.Errorf("%s: want nil, got %+v", c.name, *got)
			}
			continue
		}
		if got == nil {
			t.Errorf("%s: got nil", c.name)
			continue
		}
		if (c.wantStatus == "") != (got.Status == nil) || (got.Status != nil && *got.Status != c.wantStatus) {
			t.Errorf("%s: status = %v, want %q", c.name, got.Status, c.wantStatus)
		}
		if !floatPtrEqual(got.Chapter, c.wantCh) || !floatPtrEqual(got.Volume, c.wantVol) {
			t.Errorf("%s: chapter/volume = %v/%v, want %v/%v", c.name, got.Chapter, got.Volume, c.wantCh, c.wantVol)
		}
	}
}

func floatPtrEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func targetEqual(a, b *Target) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Status == b.Status && a.Unit == b.Unit && floatPtrEqual(a.Progress, b.Progress)
}

func fmtTarget(t *Target) string {
	if t == nil {
		return "<nil>"
	}
	if t.Progress == nil {
		return string(t.Status) + "/-"
	}
	return string(t.Status) + "/" + describeFloat(*t.Progress)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/progress/`
Expected: FAIL — `undefined: ComputeTarget`.

- [ ] **Step 3: Write the implementation**

`internal/sync/progress/decide.go`:
```go
// Package progress pushes reader progress to trackers.
package progress

import (
	"strconv"
	"strings"

	"mangasync/internal/core"
)

// Target is the tracker state the reader's progress implies.
type Target struct {
	Status   core.Status
	Progress *float64 // nil = don't send progress
	Unit     core.Unit
}

// ComputeTarget maps reader progress to a target status/progress. ended is the tracker's
// publication status and only matters when every book is read. Returns nil if nothing was read.
func ComputeTarget(p core.ReadProgress, ended bool) *Target {
	t := &Target{Unit: p.Unit}
	switch {
	case p.AllRead():
		t.Status = core.StatusReading
		if ended {
			t.Status = core.StatusCompleted
		}
		t.Progress = positive(p.MaxNumber)
	case p.BooksRead > 0:
		t.Status = core.StatusReading
		t.Progress = positive(p.LastReadNumber)
	case p.BooksInProgress > 0:
		t.Status = core.StatusReading
	default:
		return nil
	}
	return t
}

// positive returns nil for values <= 0: trackers store 0 as "nothing recorded".
func positive(v float64) *float64 {
	if v <= 0 {
		return nil
	}
	return &v
}

// Decide returns the update that moves cur towards t, or nil if nothing should change.
// Protected statuses are never touched, status never moves backwards and progress never goes down.
func Decide(cur *core.Entry, t Target) *core.EntryUpdate {
	if cur != nil {
		switch cur.Status {
		case core.StatusConsidering, core.StatusPlanning, core.StatusReading:
		default:
			return nil
		}
	}
	var u core.EntryUpdate
	changed := false

	var curStatus core.Status
	if cur != nil {
		curStatus = cur.Status
	}
	if rank(t.Status) > rank(curStatus) {
		s := t.Status
		u.Status = &s
		changed = true
	}

	if t.Progress != nil {
		var curProgress *float64
		if cur != nil {
			curProgress = cur.Chapter
			if t.Unit == core.UnitVolume {
				curProgress = cur.Volume
			}
		}
		if curProgress == nil || *t.Progress > *curProgress {
			p := *t.Progress
			if t.Unit == core.UnitVolume {
				u.Volume = &p
			} else {
				u.Chapter = &p
			}
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return &u
}

func rank(s core.Status) int {
	switch s {
	case core.StatusReading:
		return 1
	case core.StatusCompleted:
		return 2
	}
	return 0
}

// describe renders an update for logs, e.g. "status=reading chapter=104".
func describe(u core.EntryUpdate) string {
	var parts []string
	if u.Status != nil {
		parts = append(parts, "status="+string(*u.Status))
	}
	if u.Chapter != nil {
		parts = append(parts, "chapter="+describeFloat(*u.Chapter))
	}
	if u.Volume != nil {
		parts = append(parts, "volume="+describeFloat(*u.Volume))
	}
	return strings.Join(parts, " ")
}

func describeFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sync/progress/`
Expected: `ok  	mangasync/internal/sync/progress`

- [ ] **Step 5: Commit**

```bash
git add internal/sync/progress
git commit -m "Add progress target and decision rules"
```

---

### Task 10: Progress syncer

**Files:**
- Create: `internal/sync/progress/syncer.go`
- Test: `internal/sync/progress/syncer_test.go`

- [ ] **Step 1: Write the failing test**

`internal/sync/progress/syncer_test.go`:
```go
package progress

import (
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/progress/ -run Sync`
Expected: FAIL — `undefined: Syncer`.

- [ ] **Step 3: Write the implementation**

`internal/sync/progress/syncer.go`:
```go
package progress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/store"
)

const defaultUnmatchedRetry = 24 * time.Hour

// Syncer pushes one reader series' progress to every tracker.
type Syncer struct {
	Reader         core.Reader
	Trackers       []core.Tracker
	Store          *store.Store
	DryRun         bool
	Log            *slog.Logger
	Now            func() time.Time // nil = time.Now
	UnmatchedRetry time.Duration    // 0 = 24h
}

// SyncSeries syncs one reader series. A failing tracker is logged and does not stop the others;
// all tracker errors are returned joined.
func (s *Syncer) SyncSeries(ctx context.Context, ref string) error {
	series, err := s.Reader.GetSeries(ctx, ref)
	if err != nil {
		return fmt.Errorf("get series %s: %w", ref, err)
	}
	prog, err := s.Reader.GetProgress(ctx, ref)
	if err != nil {
		return fmt.Errorf("get progress %s: %w", ref, err)
	}
	var errs []error
	for _, tr := range s.Trackers {
		if err := s.syncTracker(ctx, series, prog, tr); err != nil {
			s.Log.Error("progress sync failed", "series", series.Title, "tracker", tr.Name(), "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", tr.Name(), err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) syncTracker(ctx context.Context, series core.Series, prog core.ReadProgress, tr core.Tracker) error {
	id, ok, err := s.resolve(ctx, series, tr)
	if err != nil {
		return fmt.Errorf("resolve: %w", err)
	}
	if !ok {
		return nil
	}
	ended := false
	if prog.AllRead() {
		if ended, err = tr.SeriesEnded(ctx, id); err != nil {
			return fmt.Errorf("series ended: %w", err)
		}
	}
	target := ComputeTarget(prog, ended)
	if target == nil {
		return nil
	}
	cur, err := tr.GetEntry(ctx, id)
	if err != nil {
		return fmt.Errorf("get entry: %w", err)
	}
	log := s.Log.With("series", series.Title, "tracker", tr.Name(), "tracker_id", id, "dry_run", s.DryRun)
	upd := Decide(cur, *target)
	if upd == nil {
		log.Debug("tracker entry up to date")
		return nil
	}
	log.Info("updating tracker entry", "update", describe(*upd))
	if s.DryRun {
		return nil
	}
	if err := tr.SaveEntry(ctx, id, *upd); err != nil {
		return fmt.Errorf("save entry: %w", err)
	}
	return s.Store.PutMapping(ctx, store.SeriesMapping{
		Reader: s.Reader.Name(), ReaderRef: series.Ref, Tracker: tr.Name(), TrackerID: id,
		Status: store.Matched, LastAttempt: s.now(), LastStatus: target.Status, LastProgress: target.Progress,
	})
}

// resolve returns the cached tracker ID, or resolves and caches it. Unmatched series are
// retried at most once per UnmatchedRetry to protect search rate limits.
func (s *Syncer) resolve(ctx context.Context, series core.Series, tr core.Tracker) (string, bool, error) {
	m, err := s.Store.GetMapping(ctx, s.Reader.Name(), series.Ref, tr.Name())
	if err != nil {
		return "", false, err
	}
	if m != nil && m.Status == store.Matched {
		return m.TrackerID, true, nil
	}
	if m != nil && m.Status == store.Unmatched && s.now().Sub(m.LastAttempt) < s.unmatchedRetry() {
		return "", false, nil
	}
	id, found, err := tr.Resolve(ctx, series)
	if err != nil {
		return "", false, err
	}
	nm := store.SeriesMapping{Reader: s.Reader.Name(), ReaderRef: series.Ref, Tracker: tr.Name(), LastAttempt: s.now()}
	if found {
		nm.Status, nm.TrackerID = store.Matched, id
		s.Log.Info("matched series", "series", series.Title, "tracker", tr.Name(), "tracker_id", id)
	} else {
		nm.Status = store.Unmatched
		s.Log.Warn("no tracker match; retrying later", "series", series.Title, "tracker", tr.Name(), "retry_in", s.unmatchedRetry())
	}
	if err := s.Store.PutMapping(ctx, nm); err != nil {
		return "", false, err
	}
	return id, found, nil
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Syncer) unmatchedRetry() time.Duration {
	if s.UnmatchedRetry > 0 {
		return s.UnmatchedRetry
	}
	return defaultUnmatchedRetry
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/sync/progress/`
Expected: `ok  	mangasync/internal/sync/progress`

- [ ] **Step 5: Commit**

```bash
git add internal/sync/progress
git commit -m "Add progress syncer"
```

---

### Task 11: Download syncer

**Files:**
- Create: `internal/sync/download/syncer.go`
- Test: `internal/sync/download/syncer_test.go`

Implements the spec's *Download sync* section.

- [ ] **Step 1: Write the failing test**

`internal/sync/download/syncer_test.go`:
```go
package download

import (
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/download/`
Expected: FAIL — `undefined: Syncer`.

- [ ] **Step 3: Write the implementation**

`internal/sync/download/syncer.go`:
```go
// Package download drives the downloader from tracker library statuses.
package download

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/match"
	"mangasync/internal/store"
)

var notFoundBackoff = []time.Duration{24 * time.Hour, 72 * time.Hour, 168 * time.Hour, 336 * time.Hour, 720 * time.Hour}

// NotFoundBackoff is the wait before searching again after the attempts-th miss (1-based).
func NotFoundBackoff(attempts int) time.Duration {
	i := min(max(attempts, 1), len(notFoundBackoff)) - 1
	return notFoundBackoff[i]
}

type Syncer struct {
	Reader     core.Reader
	Tracker    core.Tracker
	Lister     core.LibraryLister
	Downloader core.Downloader
	Store      *store.Store
	Acquire    []core.Status
	Release    []core.Status
	Threshold  float64
	DryRun     bool
	Log        *slog.Logger
	Now        func() time.Time // nil = time.Now
}

// Run does one download-sync pass over the tracker library.
func (s *Syncer) Run(ctx context.Context) error {
	statuses := slices.Concat(s.Acquire, s.Release)
	if len(statuses) == 0 {
		return nil
	}
	entries, err := s.Lister.ListLibrary(ctx, statuses)
	if err != nil {
		return fmt.Errorf("list tracker library: %w", err)
	}
	var readerSeries []core.Series
	if len(s.Acquire) > 0 {
		if readerSeries, err = s.Reader.ListAllSeries(ctx); err != nil {
			return fmt.Errorf("list reader series: %w", err)
		}
	}
	var errs []error
	for _, e := range entries {
		var err error
		switch {
		case slices.Contains(s.Acquire, e.Status):
			err = s.acquire(ctx, e, readerSeries)
		case slices.Contains(s.Release, e.Status):
			err = s.release(ctx, e)
		}
		if err != nil {
			s.Log.Error("download sync failed", "series", e.Series.Title, "status", e.Status, "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", e.Series.Title, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Syncer) acquire(ctx context.Context, e core.LibraryEntry, readerSeries []core.Series) error {
	log := s.logFor(e)
	rec, err := s.Store.GetDownload(ctx, s.Tracker.Name(), e.Series.Ref, s.Downloader.Name())
	if err != nil {
		return err
	}
	if rec != nil {
		if rec.Status == store.Acquired {
			return nil
		}
		if rec.Status == store.NotFound && s.now().Before(rec.RetryAfter) {
			return nil
		}
	}

	have, err := s.Downloader.FindInLibrary(ctx, e.Series)
	if err != nil {
		return fmt.Errorf("find in downloader library: %w", err)
	}
	if have != nil {
		log.Info("already in downloader library", "candidate", have.Title)
		return s.save(ctx, e, store.Acquired, have, 0, time.Time{})
	}
	for _, rs := range readerSeries {
		if match.SameSeries(rs, e.Series, s.Threshold) {
			log.Debug("already in reader", "reader_series", rs.Title)
			return nil
		}
	}

	best, near, err := s.Downloader.Find(ctx, e.Series)
	if err != nil {
		return fmt.Errorf("search downloader: %w", err)
	}
	if best == nil {
		attempts := 1
		if rec != nil && rec.Status == store.NotFound {
			attempts = rec.Attempts + 1
		}
		retry := s.now().Add(NotFoundBackoff(attempts))
		log.Warn("not found in downloader", "near_misses", describe(near), "attempts", attempts, "retry_after", retry)
		return s.save(ctx, e, store.NotFound, nil, attempts, retry)
	}

	log.Info("acquiring", "candidate", best.Title, "source", best.SourceName, "score", best.Score)
	if s.DryRun {
		return nil
	}
	if err := s.Downloader.Acquire(ctx, *best); err != nil {
		if serr := s.save(ctx, e, store.InProgress, best, 0, time.Time{}); serr != nil {
			err = errors.Join(err, serr)
		}
		return fmt.Errorf("acquire: %w", err)
	}
	return s.save(ctx, e, store.Acquired, best, 0, time.Time{})
}

func (s *Syncer) release(ctx context.Context, e core.LibraryEntry) error {
	log := s.logFor(e)
	rec, err := s.Store.GetDownload(ctx, s.Tracker.Name(), e.Series.Ref, s.Downloader.Name())
	if err != nil {
		return err
	}
	if rec != nil && rec.Status == store.Released {
		return nil
	}
	have, err := s.Downloader.FindInLibrary(ctx, e.Series)
	if err != nil {
		return fmt.Errorf("find in downloader library: %w", err)
	}
	if have == nil {
		log.Debug("not in downloader library; nothing to release")
		return s.save(ctx, e, store.Released, nil, 0, time.Time{})
	}
	log.Info("releasing", "candidate", have.Title)
	if s.DryRun {
		return nil
	}
	if err := s.Downloader.Release(ctx, *have); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return s.save(ctx, e, store.Released, have, 0, time.Time{})
}

// save writes the download record. In dry-run mode nothing is recorded.
func (s *Syncer) save(ctx context.Context, e core.LibraryEntry, status store.DownloadStatus, c *core.Candidate, attempts int, retry time.Time) error {
	if s.DryRun {
		return nil
	}
	r := store.DownloadRecord{
		Tracker: s.Tracker.Name(), TrackerID: e.Series.Ref, Downloader: s.Downloader.Name(),
		Status: status, Attempts: attempts, RetryAfter: retry, UpdatedAt: s.now(),
	}
	if c != nil {
		r.CandidateRef, r.Source = c.Ref, c.SourceName
	}
	return s.Store.PutDownload(ctx, r)
}

func (s *Syncer) logFor(e core.LibraryEntry) *slog.Logger {
	return s.Log.With("series", e.Series.Title, "tracker_id", e.Series.Ref, "status", e.Status,
		"downloader", s.Downloader.Name(), "dry_run", s.DryRun)
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func describe(cs []core.Candidate) string {
	parts := make([]string, 0, len(cs))
	for _, c := range cs {
		parts = append(parts, fmt.Sprintf("%q (%s, %.2f)", c.Title, c.SourceName, c.Score))
	}
	return strings.Join(parts, "; ")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/sync/download/`
Expected: `ok  	mangasync/internal/sync/download`

- [ ] **Step 5: Commit**

```bash
git add internal/sync/download
git commit -m "Add download syncer"
```

---

### Task 12: Komga reader (REST)

**Files:**
- Create: `internal/adapters/reader/komga/komga.go`
- Test: `internal/adapters/reader/komga/komga_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/reader/komga/komga_test.go` (fixtures are trimmed copies of live responses):
```go
package komga

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
	"mangasync/internal/httpx"
)

const seriesS1 = `{"id":"S1","libraryId":"L1","name":"Chainsaw Man","metadata":{"title":"Chainsaw Man",
"alternateTitles":[{"label":"Japanese","title":"チェンソーマン"}],
"links":[{"label":"AniList","url":"https://anilist.co/manga/105778"},
{"label":"MangaUpdates","url":"https://www.mangaupdates.com/series/ylx5wzn/chainsaw-man"},
{"label":"Amazon","url":"https://www.amazon.co.jp/dp/B07MX551PW"}]}}`

const seriesS2 = `{"id":"S2","libraryId":"L2","name":"LOSTEND","metadata":{"title":"","alternateTitles":[],"links":[]}}`

const progressS1 = `{"booksCount":244,"booksReadCount":104,"booksUnreadCount":139,"booksInProgressCount":1,
"lastReadContinuousNumberSort":104.0,"maxNumberSort":232.0}`

type server struct {
	*httptest.Server
	mu         sync.Mutex
	listBodies []string
}

func newServer(t *testing.T) *server {
	t.Helper()
	s := &server{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/series/list", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("unpaged") != "true" {
			t.Errorf("list without unpaged=true: %s", r.URL)
		}
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.listBodies = append(s.listBodies, string(b))
		s.mu.Unlock()
		fmt.Fprintf(w, `{"content":[%s,%s]}`, seriesS1, seriesS2)
	})
	mux.HandleFunc("GET /api/v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "S1":
			io.WriteString(w, seriesS1)
		case "S2":
			io.WriteString(w, seriesS2)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /api/v2/series/{id}/read-progress/tachiyomi", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, progressS1)
	})
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func newClient(url string, volumeLibs ...string) *Client {
	return New(Config{URL: url + "/", APIKey: "secret", VolumeLibraries: volumeLibs}, httpx.New(5*time.Second, 0))
}

func TestContract(t *testing.T) {
	srv := newServer(t)
	coretest.ReaderContract(t, newClient(srv.URL), "S1")
}

func TestListBodies(t *testing.T) {
	srv := newServer(t)
	c := newClient(srv.URL)
	if _, err := c.ListStartedSeries(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListAllSeries(t.Context()); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if !strings.Contains(srv.listBodies[0], `"IN_PROGRESS"`) || !strings.Contains(srv.listBodies[0], `"READ"`) {
		t.Errorf("started body = %s", srv.listBodies[0])
	}
	if srv.listBodies[1] != `{}` {
		t.Errorf("all body = %s", srv.listBodies[1])
	}
}

func TestGetSeriesMapsTitlesAndIDs(t *testing.T) {
	c := newClient(newServer(t).URL)
	s, err := c.GetSeries(t.Context(), "S1")
	if err != nil {
		t.Fatal(err)
	}
	if s.Title != "Chainsaw Man" || s.LibraryRef != "L1" || len(s.AltTitles) != 1 || s.AltTitles[0] != "チェンソーマン" {
		t.Errorf("series = %+v", s)
	}
	if s.IDs[core.IDAniList] != "105778" || s.IDs[core.IDMangaUpdates] != "ylx5wzn" || len(s.IDs) != 2 {
		t.Errorf("ids = %v", s.IDs)
	}
	s2, _ := c.GetSeries(t.Context(), "S2")
	if s2.Title != "LOSTEND" {
		t.Errorf("empty metadata title should fall back to name, got %q", s2.Title)
	}
}

func TestGetProgress(t *testing.T) {
	srv := newServer(t)
	p, err := newClient(srv.URL).GetProgress(t.Context(), "S1")
	if err != nil {
		t.Fatal(err)
	}
	want := core.ReadProgress{Unit: core.UnitChapter, BooksTotal: 244, BooksRead: 104, BooksInProgress: 1, LastReadNumber: 104, MaxNumber: 232}
	if p != want {
		t.Errorf("progress = %+v, want %+v", p, want)
	}
	p, _ = newClient(srv.URL, "L1").GetProgress(t.Context(), "S1")
	if p.Unit != core.UnitVolume {
		t.Errorf("volume library: unit = %q", p.Unit)
	}
}

func TestBadKey(t *testing.T) {
	srv := newServer(t)
	c := New(Config{URL: srv.URL, APIKey: "wrong"}, httpx.New(5*time.Second, 0))
	if _, err := c.GetSeries(t.Context(), "S1"); !httpx.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("err = %v, want 401", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/reader/komga/`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/reader/komga/komga.go`:
```go
// Package komga is the Reader adapter for Komga (REST API + SSE events).
package komga

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
	"mangasync/internal/match"
)

var (
	_ core.Reader          = (*Client)(nil)
	_ core.ProgressWatcher = (*Client)(nil)
)

type Config struct {
	URL             string
	APIKey          string
	VolumeLibraries []string // library IDs whose books are volumes
}

type Client struct {
	cfg    Config
	api    *httpx.Client
	stream *http.Client // no timeout: SSE connections stay open
}

func New(cfg Config, api *httpx.Client) *Client {
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	return &Client{cfg: cfg, api: api, stream: &http.Client{}}
}

func (c *Client) Name() string { return "komga" }

func (c *Client) header() http.Header {
	h := http.Header{}
	h.Set("X-API-Key", c.cfg.APIKey)
	return h
}

type seriesDTO struct {
	ID        string `json:"id"`
	LibraryID string `json:"libraryId"`
	Name      string `json:"name"`
	Metadata  struct {
		Title           string `json:"title"`
		AlternateTitles []struct {
			Title string `json:"title"`
		} `json:"alternateTitles"`
		Links []struct {
			URL string `json:"url"`
		} `json:"links"`
	} `json:"metadata"`
}

func (d seriesDTO) toCore() core.Series {
	s := core.Series{Ref: d.ID, Title: d.Metadata.Title, LibraryRef: d.LibraryID}
	if s.Title == "" {
		s.Title = d.Name
	}
	for _, t := range d.Metadata.AlternateTitles {
		s.AltTitles = append(s.AltTitles, t.Title)
	}
	urls := make([]string, 0, len(d.Metadata.Links))
	for _, l := range d.Metadata.Links {
		urls = append(urls, l.URL)
	}
	s.IDs = match.IDsFromLinks(urls)
	return s
}

func (c *Client) list(ctx context.Context, body any) ([]core.Series, error) {
	var page struct {
		Content []seriesDTO `json:"content"`
	}
	if err := c.api.DoJSON(ctx, http.MethodPost, c.cfg.URL+"/api/v1/series/list?unpaged=true", c.header(), body, &page); err != nil {
		return nil, err
	}
	out := make([]core.Series, 0, len(page.Content))
	for _, d := range page.Content {
		out = append(out, d.toCore())
	}
	return out, nil
}

func readStatus(v string) map[string]any {
	return map[string]any{"readStatus": map[string]any{"operator": "is", "value": v}}
}

func (c *Client) ListStartedSeries(ctx context.Context) ([]core.Series, error) {
	return c.list(ctx, map[string]any{"condition": map[string]any{
		"anyOf": []any{readStatus("IN_PROGRESS"), readStatus("READ")},
	}})
}

func (c *Client) ListAllSeries(ctx context.Context) ([]core.Series, error) {
	return c.list(ctx, map[string]any{})
}

func (c *Client) GetSeries(ctx context.Context, ref string) (core.Series, error) {
	var d seriesDTO
	if err := c.api.DoJSON(ctx, http.MethodGet, c.cfg.URL+"/api/v1/series/"+url.PathEscape(ref), c.header(), nil, &d); err != nil {
		return core.Series{}, err
	}
	return d.toCore(), nil
}

func (c *Client) GetProgress(ctx context.Context, ref string) (core.ReadProgress, error) {
	var d struct {
		BooksCount                   int     `json:"booksCount"`
		BooksReadCount               int     `json:"booksReadCount"`
		BooksInProgressCount         int     `json:"booksInProgressCount"`
		LastReadContinuousNumberSort float64 `json:"lastReadContinuousNumberSort"`
		MaxNumberSort                float64 `json:"maxNumberSort"`
	}
	path := c.cfg.URL + "/api/v2/series/" + url.PathEscape(ref) + "/read-progress/tachiyomi"
	if err := c.api.DoJSON(ctx, http.MethodGet, path, c.header(), nil, &d); err != nil {
		return core.ReadProgress{}, err
	}
	unit := core.UnitChapter
	if len(c.cfg.VolumeLibraries) > 0 {
		s, err := c.GetSeries(ctx, ref)
		if err != nil {
			return core.ReadProgress{}, err
		}
		if slices.Contains(c.cfg.VolumeLibraries, s.LibraryRef) {
			unit = core.UnitVolume
		}
	}
	return core.ReadProgress{
		Unit: unit, BooksTotal: d.BooksCount, BooksRead: d.BooksReadCount, BooksInProgress: d.BooksInProgressCount,
		LastReadNumber: d.LastReadContinuousNumberSort, MaxNumber: d.MaxNumberSort,
	}, nil
}
```

This file does not compile yet: `Client` doesn't implement `WatchProgress` until Task 13. Temporarily add this stub at the bottom of `komga.go` so the package builds; Task 13 replaces it:

```go
func (c *Client) WatchProgress(ctx context.Context) (<-chan string, error) {
	panic("implemented in Task 13")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/reader/komga/`
Expected: `ok  	mangasync/internal/adapters/reader/komga`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/reader/komga
git commit -m "Add Komga reader adapter"
```

---

### Task 13: Komga live events (SSE)

**Files:**
- Create: `internal/adapters/reader/komga/sse.go`
- Modify: `internal/adapters/reader/komga/komga.go` (remove the `WatchProgress` stub)
- Test: `internal/adapters/reader/komga/sse_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/reader/komga/sse_test.go`:
```go
package komga

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

const stream = "event:TaskQueueStatus\ndata:{\"count\":0,\"countByType\":{}}\n\n" +
	": keep-alive comment\n\n" +
	"event:ReadProgressSeriesChanged\ndata:{\"seriesId\":\"S1\",\"userId\":\"U\"}\n\n" +
	"event:ReadProgressChanged\ndata:{\"bookId\":\"B1\",\"userId\":\"U\"}\n\n" +
	"event:ReadProgressSeriesDeleted\ndata:{\"seriesId\":\"S2\",\"userId\":\"U\"}\n\n"

func TestParseEvents(t *testing.T) {
	var got []string
	err := parseEvents(strings.NewReader(stream), func(event, data string) { got = append(got, event+"|"+data) })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[1] != `ReadProgressSeriesChanged|{"seriesId":"S1","userId":"U"}` {
		t.Fatalf("got %q", got)
	}
}

func TestWatchProgress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sse/v1/events" || r.Header.Get("X-API-Key") != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	ch, err := New(Config{URL: srv.URL, APIKey: "secret"}, httpx.New(5*time.Second, 0)).WatchProgress(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	for ref := range ch { // closes when the server ends the stream
		refs = append(refs, ref)
	}
	if !slices.Equal(refs, []string{"S1", "S2"}) {
		t.Fatalf("refs = %v", refs)
	}

	if _, err := New(Config{URL: srv.URL, APIKey: "wrong"}, httpx.New(5*time.Second, 0)).WatchProgress(t.Context()); err == nil {
		t.Fatal("expected error for rejected key")
	}
}
```

- [ ] **Step 2: Remove the stub and run the test to verify it fails**

Delete the `WatchProgress` stub from `komga.go`.

Run: `go test ./internal/adapters/reader/komga/`
Expected: FAIL — `undefined: parseEvents` / `*Client does not implement core.ProgressWatcher`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/reader/komga/sse.go`:
```go
package komga

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// WatchProgress subscribes to Komga's SSE stream and emits series IDs whose read progress
// changed. Komga only sends read-progress events to the user owning the API key.
func (c *Client) WatchProgress(ctx context.Context) (<-chan string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.URL+"/sse/v1/events", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", c.cfg.APIKey)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.stream.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("komga events: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	ch := make(chan string, 16)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		_ = parseEvents(resp.Body, func(event, data string) {
			if event != "ReadProgressSeriesChanged" && event != "ReadProgressSeriesDeleted" {
				return
			}
			var payload struct {
				SeriesID string `json:"seriesId"`
			}
			if json.Unmarshal([]byte(data), &payload) != nil || payload.SeriesID == "" {
				return
			}
			select {
			case ch <- payload.SeriesID:
			case <-ctx.Done():
			}
		})
	}()
	return ch, nil
}

// parseEvents reads a text/event-stream and calls fn for every dispatched event.
func parseEvents(r io.Reader, fn func(event, data string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event != "" || len(data) > 0 {
				fn(event, strings.Join(data, "\n"))
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return sc.Err()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/reader/komga/`
Expected: `ok  	mangasync/internal/adapters/reader/komga`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/reader/komga
git commit -m "Add Komga live progress events"
```

---

### Task 14: MangaBaka tracker — client, status map, entries

**Files:**
- Create: `internal/adapters/tracker/mangabaka/mangabaka.go`
- Create: `internal/adapters/tracker/mangabaka/status.go`
- Create: `internal/adapters/tracker/mangabaka/entries.go`
- Test: `internal/adapters/tracker/mangabaka/entries_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/tracker/mangabaka/entries_test.go`:
```go
package mangabaka

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
)

// recorder captures requests made to a test server.
type recorder struct {
	mu   sync.Mutex
	reqs []string // "METHOD path?query body"
}

func (r *recorder) add(req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req.Method+" "+req.URL.RequestURI()+" "+string(b))
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reqs...)
}

func newTestClient(t *testing.T, h http.Handler) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "mb-test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		rec.add(r)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	fast := func() *httpx.Client { c := httpx.New(5*time.Second, 0); c.BaseDelay = time.Millisecond; return c }
	return New(Config{Token: "mb-test", BaseURL: srv.URL, Threshold: 0.9, EndedTTL: time.Hour}, fast(), fast()), rec
}

func TestStatusMapping(t *testing.T) {
	cases := map[string]core.Status{
		"considering": core.StatusConsidering, "plan_to_read": core.StatusPlanning, "reading": core.StatusReading,
		"completed": core.StatusCompleted, "paused": core.StatusPaused, "on_hold": core.StatusPaused,
		"dropped": core.StatusDropped, "rereading": core.StatusRereading, "weird": core.StatusUnknown,
	}
	for state, want := range cases {
		if got := statusFromState(state); got != want {
			t.Errorf("statusFromState(%q) = %q, want %q", state, got, want)
		}
	}
	if s, ok := stateFromStatus(core.StatusPlanning); !ok || s != "plan_to_read" {
		t.Errorf("stateFromStatus(planning) = %q, %v", s, ok)
	}
	if _, ok := stateFromStatus(core.StatusUnknown); ok {
		t.Error("unknown must not be writable")
	}
}

func TestGetEntry(t *testing.T) {
	c, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/my/library/1677" {
			io.WriteString(w, `{"status":200,"data":{"series_id":1677,"state":"reading","progress_chapter":104,"progress_volume":null}}`)
			return
		}
		http.NotFound(w, r)
	}))
	e, err := c.GetEntry(t.Context(), "1677")
	if err != nil || e == nil || e.Status != core.StatusReading || *e.Chapter != 104 || e.Volume != nil {
		t.Fatalf("entry = %+v, err = %v", e, err)
	}
	if e, err := c.GetEntry(t.Context(), "999"); e != nil || err != nil {
		t.Fatalf("missing entry = %+v, %v; want nil, nil", e, err)
	}
}

func TestSaveEntryPatchesOnlySetFields(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{}}`)
	}))
	ch := 104.5
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{Chapter: &ch}); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	if len(got) != 1 || got[0] != `PATCH /v1/my/library/1677 {"progress_chapter":104.5}` {
		t.Fatalf("requests = %q", got)
	}
}

func TestSaveEntryCreatesWhenMissing(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"status":201,"data":{}}`)
	}))
	st := core.StatusReading
	ch := 5.0
	if err := c.SaveEntry(t.Context(), "1677", core.EntryUpdate{Status: &st, Chapter: &ch}); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	if len(got) != 2 || got[1][:len("POST /v1/my/library/1677")] != "POST /v1/my/library/1677" {
		t.Fatalf("requests = %q", got)
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(got[1][len("POST /v1/my/library/1677 "):]), &body)
	if body["state"] != "reading" || body["progress_chapter"] != 5.0 {
		t.Fatalf("POST body = %v", body)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/tracker/mangabaka/`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/tracker/mangabaka/mangabaka.go`:
```go
// Package mangabaka is the Tracker adapter for MangaBaka (https://api.mangabaka.org).
package mangabaka

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"mangasync/internal/httpx"
)

const DefaultBaseURL = "https://api.mangabaka.org"

type Config struct {
	Token     string // Personal Access Token (mb-...)
	BaseURL   string // default DefaultBaseURL
	Threshold float64
	EndedTTL  time.Duration // cache lifetime for SeriesEnded
}

type Client struct {
	cfg    Config
	api    *httpx.Client // general + /my/* calls
	search *httpx.Client // /v1/series/search (stricter rate limit)
	now    func() time.Time

	mu    sync.Mutex
	ended map[string]endedEntry
}

type endedEntry struct {
	ended bool
	at    time.Time
}

func New(cfg Config, api, search *httpx.Client) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.EndedTTL <= 0 {
		cfg.EndedTTL = time.Hour
	}
	return &Client{cfg: cfg, api: api, search: search, now: time.Now, ended: map[string]endedEntry{}}
}

func (c *Client) Name() string { return "mangabaka" }

func (c *Client) header() http.Header {
	h := http.Header{}
	h.Set("x-api-key", c.cfg.Token)
	return h
}

func (c *Client) url(path string) string { return c.cfg.BaseURL + path }
```

`internal/adapters/tracker/mangabaka/status.go`:
```go
package mangabaka

import "mangasync/internal/core"

var stateToStatus = map[string]core.Status{
	"considering":  core.StatusConsidering,
	"plan_to_read": core.StatusPlanning,
	"reading":      core.StatusReading,
	"completed":    core.StatusCompleted,
	"paused":       core.StatusPaused,
	"on_hold":      core.StatusPaused, // reported by third parties, not in the OpenAPI enum
	"dropped":      core.StatusDropped,
	"rereading":    core.StatusRereading,
}

var statusToState = map[core.Status]string{
	core.StatusConsidering: "considering",
	core.StatusPlanning:    "plan_to_read",
	core.StatusReading:     "reading",
	core.StatusCompleted:   "completed",
	core.StatusPaused:      "paused",
	core.StatusDropped:     "dropped",
	core.StatusRereading:   "rereading",
}

func statusFromState(state string) core.Status {
	if s, ok := stateToStatus[state]; ok {
		return s
	}
	return core.StatusUnknown
}

func stateFromStatus(s core.Status) (string, bool) {
	state, ok := statusToState[s]
	return state, ok
}
```

`internal/adapters/tracker/mangabaka/entries.go`:
```go
package mangabaka

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
)

type entryDTO struct {
	SeriesID        int      `json:"series_id"`
	State           string   `json:"state"`
	ProgressChapter *float64 `json:"progress_chapter"`
	ProgressVolume  *float64 `json:"progress_volume"`
}

func (c *Client) GetEntry(ctx context.Context, id string) (*core.Entry, error) {
	var env struct {
		Data entryDTO `json:"data"`
	}
	err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/my/library/"+url.PathEscape(id)), c.header(), nil, &env)
	if httpx.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &core.Entry{Status: statusFromState(env.Data.State), Chapter: env.Data.ProgressChapter, Volume: env.Data.ProgressVolume}, nil
}

// SaveEntry PATCHes the entry, creating it with POST if it is not in the library yet.
func (c *Client) SaveEntry(ctx context.Context, id string, u core.EntryUpdate) error {
	body := map[string]any{}
	if u.Status != nil {
		state, ok := stateFromStatus(*u.Status)
		if !ok {
			return fmt.Errorf("mangabaka: cannot write status %q", *u.Status)
		}
		body["state"] = state
	}
	if u.Chapter != nil {
		body["progress_chapter"] = *u.Chapter
	}
	if u.Volume != nil {
		body["progress_volume"] = *u.Volume
	}
	if len(body) == 0 {
		return nil
	}
	path := c.url("/v1/my/library/" + url.PathEscape(id))
	err := c.api.DoJSON(ctx, http.MethodPatch, path, c.header(), body, nil)
	if !httpx.IsNotFound(err) {
		return err
	}
	if _, ok := body["state"]; !ok {
		body["state"] = "reading"
	}
	return c.api.DoJSON(ctx, http.MethodPost, path, c.header(), body, nil)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/tracker/mangabaka/`
Expected: `ok  	mangasync/internal/adapters/tracker/mangabaka`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/tracker/mangabaka
git commit -m "Add MangaBaka client, status map and entries"
```

---

### Task 15: MangaBaka tracker — Resolve and SeriesEnded

**Files:**
- Create: `internal/adapters/tracker/mangabaka/series.go`
- Test: `internal/adapters/tracker/mangabaka/series_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/tracker/mangabaka/series_test.go`:
```go
package mangabaka

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
)

const chainsaw = `{"id":1677,"state":"active","merged_with":null,"title":"Chainsaw Man","native_title":"チェンソーマン",
"romanized_title":"Chainsaw Man","secondary_titles":{"en":[{"type":"alternative","title":"Chain Saw Man"}],
"ko":[{"type":"official","title":"체인소 맨"}]},"status":"releasing",
"source":{"anilist":{"id":105778},"manga_updates":{"id":"ylx5wzn"},"my_anime_list":{"id":116778},"kitsu":{"id":null}}}`

const boruto = `{"id":2000,"state":"active","title":"Boruto: Naruto Next Generations","status":"completed"}`
const merged = `{"id":10,"state":"merged","merged_with":1677,"title":"Chainsaw Man (dup)"}`

const sasukeSearch = `{"status":200,"data":[
{"id":84642,"state":"active","title":"Naruto Retsuden","status":"completed"},
{"id":56702,"state":"active","title":"Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga","status":"completed"},
{"id":374014,"state":"active","title":"Sasuke's Story: The Uchiha and the Heavenly Stardust","status":"completed"}]}`

func seriesHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/series/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch r.PathValue("id") {
		case "1677":
			io.WriteString(w, `{"status":200,"data":`+chainsaw+`}`)
		case "2000":
			io.WriteString(w, `{"status":200,"data":`+boruto+`}`)
		case "10":
			io.WriteString(w, `{"status":200,"data":`+merged+`}`)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("GET /v1/source/anilist/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") != "105778" {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, `{"status":200,"data":{"source_response":null,"series":[`+chainsaw+`]}}`)
	})
	mux.HandleFunc("GET /v1/source/manga-updates/{id}", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"status":200,"data":{"series":[`+boruto+`]}}`)
	})
	mux.HandleFunc("GET /v1/series/search", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("q"), "Sasuke") {
			io.WriteString(w, sasukeSearch)
			return
		}
		io.WriteString(w, `{"status":200,"data":[]}`)
	})
	mux.HandleFunc("GET /v1/my/library/{id}", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	return mux
}

func TestSeriesToCore(t *testing.T) {
	c, _ := newTestClient(t, seriesHandler())
	d, err := c.getSeries(t.Context(), "1677")
	if err != nil {
		t.Fatal(err)
	}
	s := d.toCore()
	wantAlt := []string{"Chain Saw Man", "체인소 맨", "チェンソーマン"}
	if s.Ref != "1677" || s.Title != "Chainsaw Man" || strings.Join(s.AltTitles, "|") != strings.Join(wantAlt, "|") {
		t.Errorf("series = %+v", s)
	}
	if s.IDs[core.IDMangaBaka] != "1677" || s.IDs[core.IDAniList] != "105778" || s.IDs[core.IDMangaUpdates] != "ylx5wzn" ||
		s.IDs[core.IDMAL] != "116778" {
		t.Errorf("ids = %v", s.IDs)
	}
	if _, ok := s.IDs[core.IDKitsu]; ok {
		t.Error("null source id must be skipped")
	}
}

func TestResolve(t *testing.T) {
	c, rec := newTestClient(t, seriesHandler())
	cases := []struct {
		name   string
		s      core.Series
		wantID string
		found  bool
	}{
		{"direct mangabaka id", core.Series{IDs: core.IDs{core.IDMangaBaka: "1677"}}, "1677", true},
		{"merged mangabaka id", core.Series{IDs: core.IDs{core.IDMangaBaka: "10"}}, "1677", true},
		{"anilist lookup", core.Series{Title: "x", IDs: core.IDs{core.IDAniList: "105778"}}, "1677", true},
		{"anilist 404 falls through to mangaupdates", core.Series{IDs: core.IDs{core.IDAniList: "1", core.IDMangaUpdates: "abc"}}, "2000", true},
		{"title search picks best", core.Series{Title: "Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga"}, "56702", true},
		{"title search no match", core.Series{Title: "Nothing Like It"}, "", false},
	}
	for _, c2 := range cases {
		id, found, err := c.Resolve(t.Context(), c2.s)
		if err != nil || id != c2.wantID || found != c2.found {
			t.Errorf("%s: got %q, %v, %v; want %q, %v", c2.name, id, found, err, c2.wantID, c2.found)
		}
	}
	for _, r := range rec.all() {
		if strings.Contains(r, "/v1/series/search") && strings.Contains(r, "Chainsaw") {
			t.Errorf("ID-based resolve must not search: %s", r)
		}
	}
}

func TestSeriesEndedIsCached(t *testing.T) {
	c, rec := newTestClient(t, seriesHandler())
	for range 2 {
		ended, err := c.SeriesEnded(t.Context(), "2000")
		if err != nil || !ended {
			t.Fatalf("boruto ended = %v, %v", ended, err)
		}
	}
	if n := len(rec.all()); n != 1 {
		t.Fatalf("requests = %d, want 1 (cached)", n)
	}
	if ended, _ := c.SeriesEnded(t.Context(), "1677"); ended {
		t.Fatal("releasing series must not be ended")
	}
}

func TestContract(t *testing.T) {
	c, _ := newTestClient(t, seriesHandler())
	coretest.TrackerContract(t, c, core.Series{Ref: "K1", Title: "Chainsaw Man", IDs: core.IDs{core.IDAniList: "105778"}}, "1677")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/tracker/mangabaka/`
Expected: FAIL — `c.getSeries undefined`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/tracker/mangabaka/series.go`:
```go
package mangabaka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"mangasync/internal/core"
	"mangasync/internal/httpx"
	"mangasync/internal/match"
)

var (
	_ core.Tracker       = (*Client)(nil)
	_ core.LibraryLister = (*Client)(nil)
)

type titleDTO struct {
	Title string `json:"title"`
}

type sourceDTO struct {
	ID json.RawMessage `json:"id"`
}

type seriesDTO struct {
	ID              int                   `json:"id"`
	State           string                `json:"state"`
	MergedWith      *int                  `json:"merged_with"`
	Title           string                `json:"title"`
	NativeTitle     *string               `json:"native_title"`
	RomanizedTitle  *string               `json:"romanized_title"`
	SecondaryTitles map[string][]titleDTO `json:"secondary_titles"`
	Status          *string               `json:"status"`
	Source          map[string]sourceDTO  `json:"source"`
}

var sourceKinds = map[string]core.IDKind{
	"anilist":            core.IDAniList,
	"my_anime_list":      core.IDMAL,
	"manga_updates":      core.IDMangaUpdates,
	"kitsu":              core.IDKitsu,
	"anime_planet":       core.IDAnimePlanet,
	"anime_news_network": core.IDANN,
}

// toCore maps a MangaBaka series. Alt titles: romanized, English secondary titles, other
// languages (sorted), then native — so Latin-script titles come first.
func (d seriesDTO) toCore() core.Series {
	id := strconv.Itoa(d.ID)
	s := core.Series{Ref: id, Title: d.Title, IDs: core.IDs{core.IDMangaBaka: id}}
	add := func(t string) {
		if t != "" && t != d.Title && !slices.Contains(s.AltTitles, t) {
			s.AltTitles = append(s.AltTitles, t)
		}
	}
	if d.RomanizedTitle != nil {
		add(*d.RomanizedTitle)
	}
	langs := make([]string, 0, len(d.SecondaryTitles))
	for l := range d.SecondaryTitles {
		langs = append(langs, l)
	}
	sort.Slice(langs, func(i, j int) bool {
		if (langs[i] == "en") != (langs[j] == "en") {
			return langs[i] == "en"
		}
		return langs[i] < langs[j]
	})
	for _, l := range langs {
		for _, t := range d.SecondaryTitles[l] {
			add(t.Title)
		}
	}
	if d.NativeTitle != nil {
		add(*d.NativeTitle)
	}
	for key, src := range d.Source {
		if kind, ok := sourceKinds[key]; ok {
			if v := rawID(src.ID); v != "" {
				s.IDs[kind] = v
			}
		}
	}
	return s
}

func rawID(r json.RawMessage) string {
	v := strings.Trim(strings.TrimSpace(string(r)), `"`)
	if v == "null" {
		return ""
	}
	return v
}

func (d seriesDTO) currentID() string {
	if d.State == "merged" && d.MergedWith != nil {
		return strconv.Itoa(*d.MergedWith)
	}
	return strconv.Itoa(d.ID)
}

func (c *Client) getSeries(ctx context.Context, id string) (seriesDTO, error) {
	var env struct {
		Data seriesDTO `json:"data"`
	}
	err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/series/"+url.PathEscape(id)), c.header(), nil, &env)
	return env.Data, err
}

var sourcePaths = []struct {
	kind core.IDKind
	path string
}{
	{core.IDAniList, "anilist"},
	{core.IDMangaUpdates, "manga-updates"},
	{core.IDMAL, "my-anime-list"},
	{core.IDKitsu, "kitsu"},
	{core.IDAnimePlanet, "anime-planet"},
}

// Resolve: MangaBaka ID → cross-reference lookup → title search.
func (c *Client) Resolve(ctx context.Context, s core.Series) (string, bool, error) {
	if id := s.IDs[core.IDMangaBaka]; id != "" {
		return c.followMerged(ctx, id)
	}
	for _, sp := range sourcePaths {
		v := s.IDs[sp.kind]
		if v == "" {
			continue
		}
		var env struct {
			Data struct {
				Series []seriesDTO `json:"series"`
			} `json:"data"`
		}
		err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v1/source/"+sp.path+"/"+url.PathEscape(v)), c.header(), nil, &env)
		if httpx.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", false, fmt.Errorf("source lookup %s/%s: %w", sp.path, v, err)
		}
		if len(env.Data.Series) > 0 {
			return env.Data.Series[0].currentID(), true, nil
		}
	}
	return c.searchTitle(ctx, s)
}

func (c *Client) followMerged(ctx context.Context, id string) (string, bool, error) {
	for range 3 {
		d, err := c.getSeries(ctx, id)
		if httpx.IsNotFound(err) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		next := d.currentID()
		if next == id {
			return id, true, nil
		}
		id = next
	}
	return id, true, nil
}

func (c *Client) searchTitle(ctx context.Context, s core.Series) (string, bool, error) {
	if s.Title == "" {
		return "", false, nil
	}
	q := url.Values{"q": {s.Title}, "limit": {"10"}}
	var env struct {
		Data []seriesDTO `json:"data"`
	}
	if err := c.search.DoJSON(ctx, http.MethodGet, c.url("/v1/series/search?"+q.Encode()), c.header(), nil, &env); err != nil {
		return "", false, fmt.Errorf("search %q: %w", s.Title, err)
	}
	best, _ := match.Best(s.Titles(), env.Data, func(d seriesDTO) []string { return d.toCore().Titles() }, c.cfg.Threshold)
	if best == nil {
		return "", false, nil
	}
	return best.Item.currentID(), true, nil
}

// SeriesEnded reports whether publication is completed or cancelled. Cached for EndedTTL.
func (c *Client) SeriesEnded(ctx context.Context, id string) (bool, error) {
	c.mu.Lock()
	e, ok := c.ended[id]
	c.mu.Unlock()
	if ok && c.now().Sub(e.at) < c.cfg.EndedTTL {
		return e.ended, nil
	}
	d, err := c.getSeries(ctx, id)
	if err != nil {
		return false, err
	}
	ended := d.Status != nil && (*d.Status == "completed" || *d.Status == "cancelled")
	c.mu.Lock()
	c.ended[id] = endedEntry{ended: ended, at: c.now()}
	c.mu.Unlock()
	return ended, nil
}
```

The `var _ core.LibraryLister` assertion will fail to compile until Task 16. Temporarily add this stub at the bottom of `series.go` (Task 16 removes it):

```go
func (c *Client) ListLibrary(ctx context.Context, statuses []core.Status) ([]core.LibraryEntry, error) {
	panic("implemented in Task 16")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/tracker/mangabaka/`
Expected: `ok  	mangasync/internal/adapters/tracker/mangabaka`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/tracker/mangabaka
git commit -m "Add MangaBaka resolve and publication status"
```

---

### Task 16: MangaBaka tracker — ListLibrary

**Files:**
- Create: `internal/adapters/tracker/mangabaka/library.go`
- Modify: `internal/adapters/tracker/mangabaka/series.go` (remove the `ListLibrary` stub)
- Test: `internal/adapters/tracker/mangabaka/library_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/tracker/mangabaka/library_test.go`:
```go
package mangabaka

import (
	"io"
	"net/http"
	"slices"
	"testing"

	"mangasync/internal/core"
)

func TestListLibraryPagesAndMaps(t *testing.T) {
	c, rec := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/v2/my/library" || !slices.Equal(q["state"], []string{"plan_to_read", "dropped"}) || q.Get("limit") != "100" {
			t.Errorf("unexpected request %s", r.URL)
		}
		switch q.Get("page") {
		case "1":
			io.WriteString(w, `{"status":200,"pagination":{"next":"page2"},"data":[
				{"entry":{"series_id":1677,"state":"plan_to_read"},"lists":[],"series":`+chainsaw+`}]}`)
		case "2":
			io.WriteString(w, `{"status":200,"pagination":{"next":null},"data":[
				{"entry":{"series_id":2000,"state":"dropped"},"lists":[],"series":`+boruto+`}]}`)
		default:
			t.Errorf("unexpected page %q", q.Get("page"))
		}
	}))

	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusPlanning, core.StatusDropped})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(rec.all()) != 2 {
		t.Fatalf("entries = %+v, requests = %d", got, len(rec.all()))
	}
	if got[0].Status != core.StatusPlanning || got[0].Series.Ref != "1677" || got[0].Series.IDs[core.IDAniList] != "105778" ||
		len(got[0].Series.AltTitles) == 0 {
		t.Errorf("first = %+v", got[0])
	}
	if got[1].Status != core.StatusDropped || got[1].Series.Ref != "2000" {
		t.Errorf("second = %+v", got[1])
	}
}

func TestListLibraryNothingRequested(t *testing.T) {
	c, rec := newTestClient(t, http.NotFoundHandler())
	got, err := c.ListLibrary(t.Context(), []core.Status{core.StatusUnknown})
	if err != nil || got != nil || len(rec.all()) != 0 {
		t.Fatalf("got %v, %v, %d requests", got, err, len(rec.all()))
	}
}
```

- [ ] **Step 2: Remove the stub and run the test to verify it fails**

Delete the `ListLibrary` stub from `series.go`.

Run: `go test ./internal/adapters/tracker/mangabaka/`
Expected: FAIL — `*Client does not implement core.LibraryLister (missing method ListLibrary)`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/tracker/mangabaka/library.go`:
```go
package mangabaka

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strconv"

	"mangasync/internal/core"
)

// ListLibrary returns the user's library entries with one of the given statuses.
// Uses /v2/my/library: v1 list items do not carry the series ID.
func (c *Client) ListLibrary(ctx context.Context, statuses []core.Status) ([]core.LibraryEntry, error) {
	q := url.Values{"limit": {"100"}}
	for _, st := range statuses {
		if state, ok := stateFromStatus(st); ok {
			q.Add("state", state)
		}
	}
	if len(q["state"]) == 0 {
		return nil, nil
	}
	var out []core.LibraryEntry
	for page := 1; ; page++ {
		q.Set("page", strconv.Itoa(page))
		var env struct {
			Data []struct {
				Entry  entryDTO  `json:"entry"`
				Series seriesDTO `json:"series"`
			} `json:"data"`
			Pagination struct {
				Next *string `json:"next"`
			} `json:"pagination"`
		}
		if err := c.api.DoJSON(ctx, http.MethodGet, c.url("/v2/my/library?"+q.Encode()), c.header(), nil, &env); err != nil {
			return nil, err
		}
		for _, it := range env.Data {
			st := statusFromState(it.Entry.State)
			if !slices.Contains(statuses, st) {
				continue
			}
			out = append(out, core.LibraryEntry{Series: it.Series.toCore(), Status: st})
		}
		if env.Pagination.Next == nil || len(env.Data) == 0 {
			return out, nil
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/tracker/mangabaka/`
Expected: `ok  	mangasync/internal/adapters/tracker/mangabaka`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/tracker/mangabaka
git commit -m "Add MangaBaka library listing"
```

---

### Task 17: Suwayomi downloader — GraphQL client, auth, sources

**Files:**
- Create: `internal/adapters/downloader/suwayomi/suwayomi.go`
- Create: `internal/adapters/downloader/suwayomi/graphql.go`
- Create: `internal/adapters/downloader/suwayomi/fake_test.go` (test server shared by Tasks 17–19)
- Test: `internal/adapters/downloader/suwayomi/suwayomi_test.go`

- [ ] **Step 1: Write the shared fake GraphQL server**

`internal/adapters/downloader/suwayomi/fake_test.go`:
```go
package suwayomi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

type gqlReq struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// fakeServer answers the GraphQL operations this adapter uses. Fields are set by tests.
type fakeServer struct {
	*httptest.Server
	mu        sync.Mutex
	calls     []gqlReq
	authCheck func(r *http.Request) bool                 // nil = accept everything
	search    map[string]map[string]string               // source ID -> query -> mangas JSON array
	library   string                                     // nodes JSON array
	chapters  string                                     // chapters JSON array
	errorFor  map[string]string                          // source ID -> GraphQL error message
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{search: map[string]map[string]string{}, library: `[]`, chapters: `[]`, errorFor: map[string]string{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	var req gqlReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()
	q := req.Query
	if strings.Contains(q, "login(") {
		fmt.Fprint(w, `{"data":{"login":{"accessToken":"fresh","refreshToken":"r1"}}}`)
		return
	}
	if strings.Contains(q, "refreshToken(") {
		fmt.Fprint(w, `{"data":{"refreshToken":{"accessToken":"fresh"}}}`)
		return
	}
	if f.authCheck != nil && !f.authCheck(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case strings.Contains(q, "sources"):
		fmt.Fprint(w, `{"data":{"sources":{"nodes":[{"id":"0","displayName":"Local source"},
			{"id":"2131019126180322627","displayName":"Weeb Central (EN)"},{"id":"1903782575226230108","displayName":"ManhuaTop (EN)"}]}}}`)
	case strings.Contains(q, "fetchSourceManga"):
		src, _ := req.Variables["source"].(string)
		query, _ := req.Variables["q"].(string)
		if msg, ok := f.errorFor[src]; ok {
			fmt.Fprintf(w, `{"data":null,"errors":[{"message":%q}]}`, msg)
			return
		}
		mangas := f.search[src][query]
		if mangas == "" {
			mangas = `[]`
		}
		fmt.Fprintf(w, `{"data":{"fetchSourceManga":{"mangas":%s}}}`, mangas)
	case strings.Contains(q, "mangas(condition"):
		fmt.Fprintf(w, `{"data":{"mangas":{"nodes":%s}}}`, f.library)
	case strings.Contains(q, "updateManga"):
		fmt.Fprint(w, `{"data":{"updateManga":{"manga":{"id":1}}}}`)
	case strings.Contains(q, "fetchMangaAndChapters"):
		fmt.Fprintf(w, `{"data":{"fetchMangaAndChapters":{"chapters":%s}}}`, f.chapters)
	case strings.Contains(q, "enqueueChapterDownloads"):
		fmt.Fprint(w, `{"data":{"enqueueChapterDownloads":{"clientMutationId":null}}}`)
	default:
		fmt.Fprintf(w, `{"data":null,"errors":[{"message":"unexpected query: %s"}]}`, strings.ReplaceAll(q, `"`, `'`))
	}
}

func (f *fakeServer) callsMatching(substr string) []gqlReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []gqlReq
	for _, c := range f.calls {
		if strings.Contains(c.Query, substr) {
			out = append(out, c)
		}
	}
	return out
}

func newTestClient(t *testing.T, f *fakeServer, cfg Config) *Client {
	t.Helper()
	cfg.URL = f.URL
	if cfg.Auth == "" {
		cfg.Auth = "none"
	}
	if cfg.Threshold == 0 {
		cfg.Threshold = 0.9
	}
	if cfg.Sources == nil {
		cfg.Sources = []string{"Weeb Central (EN)", "ManhuaTop (EN)"}
	}
	hc := httpx.New(5*time.Second, 0)
	hc.BaseDelay = time.Millisecond
	c, err := New(cfg, hc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c
}
```

- [ ] **Step 2: Write the failing test**

`internal/adapters/downloader/suwayomi/suwayomi_test.go`:
```go
package suwayomi

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"mangasync/internal/httpx"
)

func TestResolvesSourcesByNameAndID(t *testing.T) {
	f := newFakeServer(t)
	c := newTestClient(t, f, Config{Sources: []string{"manhuatop (en)", "2131019126180322627"}})
	if len(c.sources) != 2 || c.sources[0].ID != "1903782575226230108" || c.sources[1].Name != "Weeb Central (EN)" {
		t.Fatalf("sources = %+v", c.sources)
	}
}

func TestUnknownSourceFailsInit(t *testing.T) {
	f := newFakeServer(t)
	c, err := New(Config{URL: f.URL, Auth: "none", Sources: []string{"MangaDex (EN)"}, Threshold: 0.9}, httpx.New(5*time.Second, 0))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Init(t.Context())
	if err == nil || !strings.Contains(err.Error(), "MangaDex (EN)") || !strings.Contains(err.Error(), "Weeb Central (EN)") {
		t.Fatalf("err = %v; want it to name the missing and available sources", err)
	}
}

func TestBasicAuth(t *testing.T) {
	f := newFakeServer(t)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	f.authCheck = func(r *http.Request) bool { return r.Header.Get("Authorization") == want }
	newTestClient(t, f, Config{Auth: "basic", User: "u", Pass: "p"}) // Init fails if auth is wrong
}

func TestUILoginRefreshesOn401(t *testing.T) {
	f := newFakeServer(t)
	f.authCheck = func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer fresh" }
	c := newTestClient(t, f, Config{Auth: "ui_login", User: "u", Pass: "p"})

	c.mu.Lock()
	c.access = "expired"
	c.mu.Unlock()
	if _, err := c.library(t.Context()); err != nil {
		t.Fatalf("expected transparent refresh, got %v", err)
	}
	if len(f.callsMatching("refreshToken(")) != 1 {
		t.Fatalf("refresh calls = %d, want 1", len(f.callsMatching("refreshToken(")))
	}
}

func TestInvalidAuthMode(t *testing.T) {
	if _, err := New(Config{URL: "http://x", Auth: "oauth"}, httpx.New(time.Second, 0)); err == nil {
		t.Fatal("expected error")
	}
	if _, err := New(Config{URL: "http://x", Auth: "basic"}, httpx.New(time.Second, 0)); err == nil {
		t.Fatal("expected error for missing credentials")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/adapters/downloader/suwayomi/`
Expected: FAIL — `undefined: New`.

- [ ] **Step 4: Write the implementation**

`internal/adapters/downloader/suwayomi/suwayomi.go`:
```go
// Package suwayomi is the Downloader adapter for Suwayomi-Server (GraphQL API).
package suwayomi

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"mangasync/internal/httpx"
)

type Config struct {
	URL       string
	Auth      string // none | basic | ui_login
	User      string
	Pass      string
	Sources   []string // ordered display names or numeric IDs
	Threshold float64
}

type source struct {
	ID   string
	Name string
}

type Client struct {
	cfg  Config
	http *httpx.Client
	now  func() time.Time

	mu      sync.Mutex
	access  string
	refresh string
	sources []source
	lib     []mangaDTO
	libAt   time.Time
}

func New(cfg Config, hc *httpx.Client) (*Client, error) {
	switch cfg.Auth {
	case "none":
	case "basic", "ui_login":
		if cfg.User == "" || cfg.Pass == "" {
			return nil, fmt.Errorf("suwayomi: auth %q needs a user and password", cfg.Auth)
		}
	default:
		return nil, fmt.Errorf("suwayomi: unknown auth mode %q (none, basic, ui_login)", cfg.Auth)
	}
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	return &Client{cfg: cfg, http: hc, now: time.Now}, nil
}

func (c *Client) Name() string { return "suwayomi" }

// Init logs in (ui_login) and resolves the configured sources. Call once before use.
func (c *Client) Init(ctx context.Context) error {
	if c.cfg.Auth == "ui_login" {
		if err := c.login(ctx); err != nil {
			return fmt.Errorf("suwayomi login: %w", err)
		}
	}
	return c.resolveSources(ctx)
}

func (c *Client) resolveSources(ctx context.Context) error {
	if len(c.cfg.Sources) == 0 {
		return fmt.Errorf("suwayomi: no sources configured")
	}
	var out struct {
		Sources struct {
			Nodes []struct {
				ID          string `json:"id"`
				DisplayName string `json:"displayName"`
			} `json:"nodes"`
		} `json:"sources"`
	}
	if err := c.gql(ctx, `{ sources { nodes { id displayName } } }`, nil, &out); err != nil {
		return fmt.Errorf("list sources: %w", err)
	}
	var resolved []source
	for _, want := range c.cfg.Sources {
		found := false
		for _, n := range out.Sources.Nodes {
			if n.ID == want || strings.EqualFold(n.DisplayName, want) {
				resolved = append(resolved, source{ID: n.ID, Name: n.DisplayName})
				found = true
				break
			}
		}
		if !found {
			names := make([]string, 0, len(out.Sources.Nodes))
			for _, n := range out.Sources.Nodes {
				names = append(names, n.DisplayName)
			}
			return fmt.Errorf("suwayomi source %q is not installed (available: %s)", want, strings.Join(names, ", "))
		}
	}
	c.mu.Lock()
	c.sources = resolved
	c.mu.Unlock()
	return nil
}
```

`internal/adapters/downloader/suwayomi/graphql.go`:
```go
package suwayomi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"mangasync/internal/httpx"
)

var errUnauthorized = errors.New("suwayomi: unauthorized")

const (
	loginMutation   = `mutation($u: String!, $p: String!) { login(input: {username: $u, password: $p}) { accessToken refreshToken } }`
	refreshMutation = `mutation($t: String!) { refreshToken(input: {refreshToken: $t}) { accessToken } }`
)

// gql runs a GraphQL operation. With ui_login, an unauthorized response triggers one
// token refresh (or a fresh login) and a retry.
func (c *Client) gql(ctx context.Context, query string, vars map[string]any, out any) error {
	err := c.gqlOnce(ctx, query, vars, out)
	if c.cfg.Auth == "ui_login" && errors.Is(err, errUnauthorized) {
		if rerr := c.reauth(ctx); rerr != nil {
			return fmt.Errorf("reauthenticate: %w", rerr)
		}
		err = c.gqlOnce(ctx, query, vars, out)
	}
	return err
}

func (c *Client) gqlOnce(ctx context.Context, query string, vars map[string]any, out any) error {
	var resp struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	body := map[string]any{"query": query, "variables": vars}
	err := c.http.DoJSON(ctx, http.MethodPost, c.cfg.URL+"/api/graphql", c.authHeader(), body, &resp)
	if httpx.IsStatus(err, http.StatusUnauthorized) {
		return fmt.Errorf("%w: %v", errUnauthorized, err)
	}
	if err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		msgs := make([]string, 0, len(resp.Errors))
		for _, e := range resp.Errors {
			msgs = append(msgs, e.Message)
		}
		joined := strings.Join(msgs, "; ")
		if strings.Contains(strings.ToLower(joined), "unauthorized") {
			return fmt.Errorf("%w: %s", errUnauthorized, joined)
		}
		return fmt.Errorf("graphql: %s", joined)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Data, out)
}

func (c *Client) authHeader() http.Header {
	h := http.Header{}
	switch c.cfg.Auth {
	case "basic":
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.cfg.User+":"+c.cfg.Pass)))
	case "ui_login":
		c.mu.Lock()
		if c.access != "" {
			h.Set("Authorization", "Bearer "+c.access)
		}
		c.mu.Unlock()
	}
	return h
}

func (c *Client) login(ctx context.Context) error {
	var out struct {
		Login struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"login"`
	}
	if err := c.gqlOnce(ctx, loginMutation, map[string]any{"u": c.cfg.User, "p": c.cfg.Pass}, &out); err != nil {
		return err
	}
	if out.Login.AccessToken == "" {
		return errors.New("login returned no access token")
	}
	c.mu.Lock()
	c.access, c.refresh = out.Login.AccessToken, out.Login.RefreshToken
	c.mu.Unlock()
	return nil
}

func (c *Client) reauth(ctx context.Context) error {
	c.mu.Lock()
	rt := c.refresh
	c.access = ""
	c.mu.Unlock()
	if rt != "" {
		var out struct {
			RefreshToken struct {
				AccessToken string `json:"accessToken"`
			} `json:"refreshToken"`
		}
		if err := c.gqlOnce(ctx, refreshMutation, map[string]any{"t": rt}, &out); err == nil && out.RefreshToken.AccessToken != "" {
			c.mu.Lock()
			c.access = out.RefreshToken.AccessToken
			c.mu.Unlock()
			return nil
		}
	}
	return c.login(ctx)
}
```

The test calls `c.library`, defined in Task 18. Add this temporary `find.go` so the package compiles (Task 18 replaces the whole file):

```go
package suwayomi

import "context"

type mangaDTO struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func (c *Client) library(ctx context.Context) ([]mangaDTO, error) {
	var out struct {
		Mangas struct {
			Nodes []mangaDTO `json:"nodes"`
		} `json:"mangas"`
	}
	err := c.gql(ctx, `{ mangas(condition: {inLibrary: true}) { nodes { id title } } }`, nil, &out)
	return out.Mangas.Nodes, err
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/adapters/downloader/suwayomi/`
Expected: `ok  	mangasync/internal/adapters/downloader/suwayomi`

- [ ] **Step 6: Commit**

```bash
git add internal/adapters/downloader/suwayomi
git commit -m "Add Suwayomi GraphQL client, auth and sources"
```

---

### Task 18: Suwayomi downloader — Find and FindInLibrary

**Files:**
- Replace: `internal/adapters/downloader/suwayomi/find.go`
- Test: `internal/adapters/downloader/suwayomi/find_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/downloader/suwayomi/find_test.go`:
```go
package suwayomi

import (
	"testing"

	"mangasync/internal/core"
)

const weeb = "2131019126180322627"
const manhuatop = "1903782575226230108"

func TestFindFallsThroughSourcesInOrder(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Kagurabachi": `[{"id":1,"title":"Kagura Bachi Gaiden","inLibrary":false}]`}
	f.search[manhuatop] = map[string]string{"Kagurabachi": `[{"id":7,"title":"Kagurabachi","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, _, err := c.Find(t.Context(), core.Series{Title: "Kagurabachi"})
	if err != nil || best == nil || best.Ref != "7" || best.SourceName != "ManhuaTop (EN)" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}
}

func TestFindUsesLatinAltTitles(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{
		"Frieren: Beyond Journey’s End": `[{"id":130,"title":"Frieren - Beyond Journey's End","inLibrary":false}]`,
	}
	c := newTestClient(t, f, Config{})
	s := core.Series{Title: "Sousou no Frieren", AltTitles: []string{"葬送のフリーレン", "Frieren: Beyond Journey’s End"}}

	best, _, err := c.Find(t.Context(), s)
	if err != nil || best == nil || best.Ref != "130" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}
	for _, call := range f.callsMatching("fetchSourceManga") {
		if call.Variables["q"] == "葬送のフリーレン" {
			t.Fatal("non-Latin titles must not be used as queries")
		}
	}
}

func TestFindNoMatchReturnsNearMisses(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Obscure": `[{"id":1,"title":"Obscura","inLibrary":false},{"id":2,"title":"Zzz","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, near, err := c.Find(t.Context(), core.Series{Title: "Obscure"})
	if err != nil || best != nil || len(near) == 0 || near[0].Title != "Obscura" {
		t.Fatalf("best = %+v, near = %+v, err = %v", best, near, err)
	}
}

func TestFindSourceErrorDoesNotHideOtherSources(t *testing.T) {
	f := newFakeServer(t)
	f.errorFor[weeb] = "cloudflare challenge"
	f.search[manhuatop] = map[string]string{"Kagurabachi": `[{"id":7,"title":"Kagurabachi","inLibrary":false}]`}
	c := newTestClient(t, f, Config{})

	best, _, err := c.Find(t.Context(), core.Series{Title: "Kagurabachi"})
	if err != nil || best == nil || best.Ref != "7" {
		t.Fatalf("best = %+v, err = %v", best, err)
	}

	// With no match anywhere, the source error is returned so the caller retries instead of recording not_found.
	_, _, err = c.Find(t.Context(), core.Series{Title: "Nothing"})
	if err == nil {
		t.Fatal("expected source error when nothing matched")
	}
}

func TestFindInLibraryIsCached(t *testing.T) {
	f := newFakeServer(t)
	f.library = `[{"id":24,"title":"Witch Hat Atelier"},{"id":33,"title":"Chainsaw Man"}]`
	c := newTestClient(t, f, Config{})

	got, err := c.FindInLibrary(t.Context(), core.Series{Title: "Chainsaw Man"})
	if err != nil || got == nil || got.Ref != "33" {
		t.Fatalf("got %+v, %v", got, err)
	}
	got, _ = c.FindInLibrary(t.Context(), core.Series{Title: "Frieren"})
	if got != nil {
		t.Fatalf("unexpected match %+v", got)
	}
	if n := len(f.callsMatching("mangas(condition")); n != 1 {
		t.Fatalf("library queries = %d, want 1 (cached)", n)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/downloader/suwayomi/`
Expected: FAIL — `c.Find undefined`.

- [ ] **Step 3: Replace `find.go` with the implementation**

`internal/adapters/downloader/suwayomi/find.go`:
```go
package suwayomi

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"
	"unicode"

	"mangasync/internal/core"
	"mangasync/internal/match"
)

const (
	searchMutation = `mutation($source: LongString!, $q: String!) {
  fetchSourceManga(input: {source: $source, type: SEARCH, page: 1, query: $q}) { mangas { id title inLibrary } } }`
	libraryQuery    = `{ mangas(condition: {inLibrary: true}) { nodes { id title } } }`
	libraryCacheTTL = time.Minute
)

type mangaDTO struct {
	ID        int    `json:"id"`
	Title     string `json:"title"`
	InLibrary bool   `json:"inLibrary"`
}

func mangaTitles(m mangaDTO) []string { return []string{m.Title} }

// searchQueries: the main title, then up to two Latin-script alt titles.
func searchQueries(s core.Series) []string {
	var qs []string
	if s.Title != "" {
		qs = append(qs, s.Title)
	}
	for _, t := range s.AltTitles {
		if len(qs) >= 3 {
			break
		}
		if isLatin(t) && !slices.Contains(qs, t) {
			qs = append(qs, t)
		}
	}
	return qs
}

func isLatin(s string) bool {
	hasLetter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			if !unicode.Is(unicode.Latin, r) {
				return false
			}
			hasLetter = true
		}
	}
	return hasLetter
}

// Find searches the configured sources in priority order, query by query. The first accepted
// match wins. Source errors are only returned if nothing matched anywhere.
func (c *Client) Find(ctx context.Context, s core.Series) (*core.Candidate, []core.Candidate, error) {
	c.mu.Lock()
	sources := slices.Clone(c.sources)
	c.mu.Unlock()

	var near []core.Candidate
	var errs []error
	for _, q := range searchQueries(s) {
		for _, src := range sources {
			var out struct {
				FetchSourceManga struct {
					Mangas []mangaDTO `json:"mangas"`
				} `json:"fetchSourceManga"`
			}
			if err := c.gql(ctx, searchMutation, map[string]any{"source": src.ID, "q": q}, &out); err != nil {
				errs = append(errs, fmt.Errorf("search %s for %q: %w", src.Name, q, err))
				continue
			}
			best, misses := match.Best(s.Titles(), out.FetchSourceManga.Mangas, mangaTitles, c.cfg.Threshold)
			if best != nil {
				return &core.Candidate{Ref: strconv.Itoa(best.Item.ID), SourceName: src.Name, Title: best.Item.Title, Score: best.Score}, nil, nil
			}
			for _, m := range misses {
				near = append(near, core.Candidate{Ref: strconv.Itoa(m.Item.ID), SourceName: src.Name, Title: m.Item.Title, Score: m.Score})
			}
		}
	}
	sort.SliceStable(near, func(i, j int) bool { return near[i].Score > near[j].Score })
	if len(near) > 3 {
		near = near[:3]
	}
	return nil, near, errors.Join(errs...)
}

// FindInLibrary matches s against the Suwayomi library (cached for a minute).
func (c *Client) FindInLibrary(ctx context.Context, s core.Series) (*core.Candidate, error) {
	lib, err := c.library(ctx)
	if err != nil {
		return nil, err
	}
	best, _ := match.Best(s.Titles(), lib, mangaTitles, c.cfg.Threshold)
	if best == nil {
		return nil, nil
	}
	return &core.Candidate{Ref: strconv.Itoa(best.Item.ID), Title: best.Item.Title, Score: best.Score}, nil
}

func (c *Client) library(ctx context.Context) ([]mangaDTO, error) {
	c.mu.Lock()
	if c.lib != nil && c.now().Sub(c.libAt) < libraryCacheTTL {
		lib := c.lib
		c.mu.Unlock()
		return lib, nil
	}
	c.mu.Unlock()

	var out struct {
		Mangas struct {
			Nodes []mangaDTO `json:"nodes"`
		} `json:"mangas"`
	}
	if err := c.gql(ctx, libraryQuery, nil, &out); err != nil {
		return nil, fmt.Errorf("list library: %w", err)
	}
	lib := out.Mangas.Nodes
	if lib == nil {
		lib = []mangaDTO{}
	}
	c.mu.Lock()
	c.lib, c.libAt = lib, c.now()
	c.mu.Unlock()
	return lib, nil
}

func (c *Client) invalidateLibrary() {
	c.mu.Lock()
	c.lib = nil
	c.mu.Unlock()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/downloader/suwayomi/`
Expected: `ok  	mangasync/internal/adapters/downloader/suwayomi`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/downloader/suwayomi
git commit -m "Add Suwayomi source search and library matching"
```

---

### Task 19: Suwayomi downloader — Acquire and Release

**Files:**
- Create: `internal/adapters/downloader/suwayomi/acquire.go`
- Test: `internal/adapters/downloader/suwayomi/acquire_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/downloader/suwayomi/acquire_test.go`:
```go
package suwayomi

import (
	"fmt"
	"testing"

	"mangasync/internal/core"
	"mangasync/internal/core/coretest"
)

func TestAcquireAddsAndQueuesMissingChapters(t *testing.T) {
	f := newFakeServer(t)
	f.chapters = `[{"id":1,"isDownloaded":true},{"id":2,"isDownloaded":false},{"id":3,"isDownloaded":false}]`
	c := newTestClient(t, f, Config{})
	c.lib, c.libAt = []mangaDTO{}, c.now() // warm cache that Acquire must invalidate

	if err := c.Acquire(t.Context(), core.Candidate{Ref: "130"}); err != nil {
		t.Fatal(err)
	}
	upd := f.callsMatching("updateManga")
	if len(upd) != 1 || upd[0].Variables["id"] != 130.0 || upd[0].Variables["in"] != true {
		t.Fatalf("updateManga calls = %+v", upd)
	}
	fetch := f.callsMatching("fetchMangaAndChapters")
	if len(fetch) != 1 || fetch[0].Variables["id"] != 130.0 {
		t.Fatalf("fetch calls = %+v", fetch)
	}
	enq := f.callsMatching("enqueueChapterDownloads")
	if len(enq) != 1 || fmt.Sprint(enq[0].Variables["ids"]) != "[2 3]" {
		t.Fatalf("enqueue calls = %+v", enq)
	}
	if c.lib != nil {
		t.Fatal("library cache not invalidated")
	}
}

func TestAcquireAllDownloadedSkipsEnqueue(t *testing.T) {
	f := newFakeServer(t)
	f.chapters = `[{"id":1,"isDownloaded":true}]`
	c := newTestClient(t, f, Config{})
	if err := c.Acquire(t.Context(), core.Candidate{Ref: "5"}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.callsMatching("enqueueChapterDownloads")); n != 0 {
		t.Fatalf("enqueue calls = %d, want 0", n)
	}
}

func TestRelease(t *testing.T) {
	f := newFakeServer(t)
	c := newTestClient(t, f, Config{})
	if err := c.Release(t.Context(), core.Candidate{Ref: "50"}); err != nil {
		t.Fatal(err)
	}
	upd := f.callsMatching("updateManga")
	if len(upd) != 1 || upd[0].Variables["id"] != 50.0 || upd[0].Variables["in"] != false {
		t.Fatalf("updateManga calls = %+v", upd)
	}
}

func TestBadRef(t *testing.T) {
	c := newTestClient(t, newFakeServer(t), Config{})
	if err := c.Acquire(t.Context(), core.Candidate{Ref: "abc"}); err == nil {
		t.Fatal("expected error for non-numeric ref")
	}
}

func TestContract(t *testing.T) {
	f := newFakeServer(t)
	f.search[weeb] = map[string]string{"Chainsaw Man": `[{"id":33,"title":"Chainsaw Man","inLibrary":true}]`}
	coretest.DownloaderContract(t, newTestClient(t, f, Config{}), core.Series{Title: "Chainsaw Man"})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/downloader/suwayomi/`
Expected: FAIL — `c.Acquire undefined`.

- [ ] **Step 3: Write the implementation**

`internal/adapters/downloader/suwayomi/acquire.go`:
```go
package suwayomi

import (
	"context"
	"fmt"
	"strconv"

	"mangasync/internal/core"
)

var _ core.Downloader = (*Client)(nil)

const (
	setInLibraryMutation = `mutation($id: Int!, $in: Boolean!) {
  updateManga(input: {id: $id, patch: {inLibrary: $in}}) { manga { id } } }`
	fetchChaptersMutation = `mutation($id: Int!) {
  fetchMangaAndChapters(input: {id: $id, fetchManga: true, fetchChapters: true}) { chapters { id isDownloaded } } }`
	enqueueMutation = `mutation($ids: [Int!]!) { enqueueChapterDownloads(input: {ids: $ids}) { clientMutationId } }`
)

// Acquire adds the manga to the library, refreshes its chapter list and queues every
// chapter that is not downloaded yet.
func (c *Client) Acquire(ctx context.Context, cand core.Candidate) error {
	id, err := strconv.Atoi(cand.Ref)
	if err != nil {
		return fmt.Errorf("suwayomi: bad manga ref %q", cand.Ref)
	}
	if err := c.setInLibrary(ctx, id, true); err != nil {
		return fmt.Errorf("add to library: %w", err)
	}
	var out struct {
		FetchMangaAndChapters struct {
			Chapters []struct {
				ID           int  `json:"id"`
				IsDownloaded bool `json:"isDownloaded"`
			} `json:"chapters"`
		} `json:"fetchMangaAndChapters"`
	}
	if err := c.gql(ctx, fetchChaptersMutation, map[string]any{"id": id}, &out); err != nil {
		return fmt.Errorf("fetch chapters: %w", err)
	}
	var ids []int
	for _, ch := range out.FetchMangaAndChapters.Chapters {
		if !ch.IsDownloaded {
			ids = append(ids, ch.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := c.gql(ctx, enqueueMutation, map[string]any{"ids": ids}, nil); err != nil {
		return fmt.Errorf("enqueue %d chapters: %w", len(ids), err)
	}
	return nil
}

// Release removes the manga from the library. Downloaded chapters stay on disk.
func (c *Client) Release(ctx context.Context, cand core.Candidate) error {
	id, err := strconv.Atoi(cand.Ref)
	if err != nil {
		return fmt.Errorf("suwayomi: bad manga ref %q", cand.Ref)
	}
	return c.setInLibrary(ctx, id, false)
}

func (c *Client) setInLibrary(ctx context.Context, id int, in bool) error {
	defer c.invalidateLibrary()
	return c.gql(ctx, setInLibraryMutation, map[string]any{"id": id, "in": in}, nil)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/adapters/downloader/suwayomi/`
Expected: `ok  	mangasync/internal/adapters/downloader/suwayomi`

- [ ] **Step 5: Commit**

```bash
git add internal/adapters/downloader/suwayomi
git commit -m "Add Suwayomi acquire and release"
```

---

### Task 20: Env helpers and core config

**Files:**
- Create: `internal/envutil/envutil.go`
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

`internal/config/config_test.go`:
```go
package config

import (
	"slices"
	"testing"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

func TestDefaults(t *testing.T) {
	c, err := Load(envutil.MapLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Reader != "komga" || !slices.Equal(c.Trackers, []string{"mangabaka"}) || c.DownloadTracker != "" || c.Downloader != "" {
		t.Errorf("adapters = %+v", c)
	}
	if !slices.Equal(c.Acquire, []core.Status{core.StatusPlanning, core.StatusReading, core.StatusRereading}) ||
		!slices.Equal(c.Release, []core.Status{core.StatusDropped}) {
		t.Errorf("statuses = %v / %v", c.Acquire, c.Release)
	}
	if c.DownloadInterval != 15*time.Minute || c.ReconcileInterval != time.Hour || c.SSEDebounce != 10*time.Second {
		t.Errorf("intervals = %v %v %v", c.DownloadInterval, c.ReconcileInterval, c.SSEDebounce)
	}
	if c.MatchThreshold != 0.9 || c.DryRun || c.DBPath != "/data/mangasync.db" || c.LogLevel != "info" || c.HTTPAddr != ":8080" {
		t.Errorf("misc = %+v", c)
	}
}

func TestCustom(t *testing.T) {
	c, err := Load(envutil.MapLookup(map[string]string{
		"TRACKERS": "mangabaka, anilist", "DOWNLOAD_TRACKER": "mangabaka", "DOWNLOADER": "suwayomi",
		"ACQUIRE_STATUSES": "", "RELEASE_STATUSES": "dropped,paused", "DOWNLOAD_INTERVAL": "5m",
		"MATCH_THRESHOLD": "0.85", "DRY_RUN": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Trackers, []string{"mangabaka", "anilist"}) || c.Acquire != nil || len(c.Release) != 2 ||
		c.DownloadInterval != 5*time.Minute || c.MatchThreshold != 0.85 || !c.DryRun {
		t.Errorf("config = %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	cases := map[string]map[string]string{
		"downloader without tracker": {"DOWNLOADER": "suwayomi"},
		"tracker without downloader": {"DOWNLOAD_TRACKER": "mangabaka"},
		"status in both sets":        {"ACQUIRE_STATUSES": "planning,dropped"},
		"bad status":                 {"RELEASE_STATUSES": "gone"},
		"bad duration":               {"RECONCILE_INTERVAL": "soon"},
		"zero duration":              {"DOWNLOAD_INTERVAL": "0s"},
		"bad threshold":              {"MATCH_THRESHOLD": "1.5"},
		"bad bool":                   {"DRY_RUN": "maybe"},
		"no trackers":                {"TRACKERS": " , "},
	}
	for name, env := range cases {
		if _, err := Load(envutil.MapLookup(env)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/`
Expected: FAIL — `package mangasync/internal/envutil is not in std` / `undefined: Load`.

- [ ] **Step 3: Write the implementation**

`internal/envutil/envutil.go`:
```go
// Package envutil reads configuration from environment-style lookups.
package envutil

import "strings"

// Lookup has the signature of os.LookupEnv.
type Lookup func(key string) (string, bool)

// Get returns the trimmed value of key, or "" if unset.
func Get(l Lookup, key string) string {
	v, _ := l(key)
	return strings.TrimSpace(v)
}

// GetOr returns the trimmed value of key if it is set (even to ""), else def.
func GetOr(l Lookup, key, def string) string {
	if v, ok := l(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

// List splits a comma-separated value, trimming items and dropping empty ones.
func List(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// MapLookup adapts a map for tests.
func MapLookup(m map[string]string) Lookup {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}
```

`internal/config/config.go`:
```go
// Package config loads the core (non-adapter) configuration.
package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

type Config struct {
	Reader            string
	Trackers          []string
	DownloadTracker   string
	Downloader        string
	Acquire           []core.Status
	Release           []core.Status
	DownloadInterval  time.Duration
	ReconcileInterval time.Duration
	SSEDebounce       time.Duration
	MatchThreshold    float64
	DryRun            bool
	DBPath            string
	LogLevel          string
	HTTPAddr          string
}

func Load(l envutil.Lookup) (Config, error) {
	c := Config{
		Reader:          cmp.Or(envutil.Get(l, "READER"), "komga"),
		Trackers:        envutil.List(envutil.GetOr(l, "TRACKERS", "mangabaka")),
		DownloadTracker: envutil.Get(l, "DOWNLOAD_TRACKER"),
		Downloader:      envutil.Get(l, "DOWNLOADER"),
		DBPath:          cmp.Or(envutil.Get(l, "DB_PATH"), "/data/mangasync.db"),
		LogLevel:        cmp.Or(envutil.Get(l, "LOG_LEVEL"), "info"),
		HTTPAddr:        cmp.Or(envutil.Get(l, "HTTP_ADDR"), ":8080"),
	}
	var err error
	if c.Acquire, err = core.ParseStatuses(envutil.GetOr(l, "ACQUIRE_STATUSES", "planning,reading,rereading")); err != nil {
		return c, fmt.Errorf("ACQUIRE_STATUSES: %w", err)
	}
	if c.Release, err = core.ParseStatuses(envutil.GetOr(l, "RELEASE_STATUSES", "dropped")); err != nil {
		return c, fmt.Errorf("RELEASE_STATUSES: %w", err)
	}
	for _, d := range []struct {
		key string
		def string
		dst *time.Duration
	}{
		{"DOWNLOAD_INTERVAL", "15m", &c.DownloadInterval},
		{"RECONCILE_INTERVAL", "1h", &c.ReconcileInterval},
		{"SSE_DEBOUNCE", "10s", &c.SSEDebounce},
	} {
		v, err := time.ParseDuration(cmp.Or(envutil.Get(l, d.key), d.def))
		if err != nil || v <= 0 {
			return c, fmt.Errorf("%s: must be a positive duration like %q", d.key, d.def)
		}
		*d.dst = v
	}
	if c.MatchThreshold, err = strconv.ParseFloat(cmp.Or(envutil.Get(l, "MATCH_THRESHOLD"), "0.9"), 64); err != nil ||
		c.MatchThreshold <= 0 || c.MatchThreshold > 1 {
		return c, errors.New("MATCH_THRESHOLD: must be a number in (0, 1]")
	}
	if c.DryRun, err = strconv.ParseBool(cmp.Or(envutil.Get(l, "DRY_RUN"), "false")); err != nil {
		return c, errors.New("DRY_RUN: must be true or false")
	}

	if len(c.Trackers) == 0 {
		return c, errors.New("TRACKERS: at least one tracker is required")
	}
	if (c.DownloadTracker == "") != (c.Downloader == "") {
		return c, errors.New("DOWNLOAD_TRACKER and DOWNLOADER must be set together (or both empty to disable download sync)")
	}
	for _, s := range c.Acquire {
		if slices.Contains(c.Release, s) {
			return c, fmt.Errorf("status %q is in both ACQUIRE_STATUSES and RELEASE_STATUSES", s)
		}
	}
	return c, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/`
Expected: `ok  	mangasync/internal/config`

- [ ] **Step 5: Commit**

```bash
git add internal/envutil internal/config
git commit -m "Add env helpers and core config"
```

---

### Task 21: Adapter env constructors and registry

**Files:**
- Create: `internal/adapters/reader/komga/env.go`
- Create: `internal/adapters/tracker/mangabaka/env.go`
- Create: `internal/adapters/downloader/suwayomi/env.go`
- Create: `internal/adapters/registry/registry.go`
- Test: `internal/adapters/registry/registry_test.go`

- [ ] **Step 1: Write the failing test**

`internal/adapters/registry/registry_test.go`:
```go
package registry

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mangasync/internal/envutil"
)

func TestUnknownNames(t *testing.T) {
	env := envutil.MapLookup(nil)
	if _, err := Reader("kavita", env); err == nil || !strings.Contains(err.Error(), "komga") {
		t.Errorf("reader err = %v", err)
	}
	if _, err := Tracker("anilist", env, 0.9, time.Hour); err == nil || !strings.Contains(err.Error(), "mangabaka") {
		t.Errorf("tracker err = %v", err)
	}
	if _, err := Downloader(t.Context(), "kaizoku", env, 0.9); err == nil || !strings.Contains(err.Error(), "suwayomi") {
		t.Errorf("downloader err = %v", err)
	}
}

func TestMissingEnv(t *testing.T) {
	env := envutil.MapLookup(nil)
	if _, err := Reader("komga", env); err == nil || !strings.Contains(err.Error(), "KOMGA_URL") {
		t.Errorf("komga err = %v", err)
	}
	if _, err := Tracker("mangabaka", env, 0.9, time.Hour); err == nil || !strings.Contains(err.Error(), "MANGABAKA_TOKEN") {
		t.Errorf("mangabaka err = %v", err)
	}
	if _, err := Downloader(t.Context(), "suwayomi", env, 0.9); err == nil || !strings.Contains(err.Error(), "SUWAYOMI_URL") {
		t.Errorf("suwayomi err = %v", err)
	}
}

func TestBuildsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"sources":{"nodes":[{"id":"1","displayName":"Weeb Central (EN)"}]}}}`)
	}))
	defer srv.Close()
	env := envutil.MapLookup(map[string]string{
		"KOMGA_URL": "http://komga", "KOMGA_API_KEY": "k", "KOMGA_VOLUME_LIBRARIES": "L1",
		"MANGABAKA_TOKEN": "mb-x",
		"SUWAYOMI_URL":    srv.URL, "SUWAYOMI_SOURCES": "Weeb Central (EN)",
	})
	if r, err := Reader("komga", env); err != nil || r.Name() != "komga" {
		t.Errorf("reader = %v, %v", r, err)
	}
	if tr, err := Tracker("mangabaka", env, 0.9, time.Hour); err != nil || tr.Name() != "mangabaka" {
		t.Errorf("tracker = %v, %v", tr, err)
	}
	if d, err := Downloader(t.Context(), "suwayomi", env, 0.9); err != nil || d.Name() != "suwayomi" {
		t.Errorf("downloader = %v, %v", d, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/adapters/registry/`
Expected: FAIL — `undefined: Reader`.

- [ ] **Step 3: Write the env constructors**

`internal/adapters/reader/komga/env.go`:
```go
package komga

import (
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads KOMGA_URL, KOMGA_API_KEY and KOMGA_VOLUME_LIBRARIES.
func FromEnv(l envutil.Lookup) (*Client, error) {
	cfg := Config{
		URL:             envutil.Get(l, "KOMGA_URL"),
		APIKey:          envutil.Get(l, "KOMGA_API_KEY"),
		VolumeLibraries: envutil.List(envutil.Get(l, "KOMGA_VOLUME_LIBRARIES")),
	}
	if cfg.URL == "" || cfg.APIKey == "" {
		return nil, errors.New("komga: KOMGA_URL and KOMGA_API_KEY are required")
	}
	return New(cfg, httpx.New(30*time.Second, 0)), nil
}
```

`internal/adapters/tracker/mangabaka/env.go`:
```go
package mangabaka

import (
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads MANGABAKA_TOKEN and optional MANGABAKA_URL. Rate limits stay below the
// API's per-IP limits (search 30/min, other 180/min).
func FromEnv(l envutil.Lookup, threshold float64, endedTTL time.Duration) (*Client, error) {
	cfg := Config{
		Token:     envutil.Get(l, "MANGABAKA_TOKEN"),
		BaseURL:   envutil.Get(l, "MANGABAKA_URL"),
		Threshold: threshold,
		EndedTTL:  endedTTL,
	}
	if cfg.Token == "" {
		return nil, errors.New("mangabaka: MANGABAKA_TOKEN is required")
	}
	return New(cfg, httpx.New(30*time.Second, 150), httpx.New(30*time.Second, 25)), nil
}
```

`internal/adapters/downloader/suwayomi/env.go`:
```go
package suwayomi

import (
	"cmp"
	"errors"
	"time"

	"mangasync/internal/envutil"
	"mangasync/internal/httpx"
)

// FromEnv reads SUWAYOMI_URL, SUWAYOMI_AUTH, SUWAYOMI_USER, SUWAYOMI_PASS and SUWAYOMI_SOURCES.
// Call Init on the result before use.
func FromEnv(l envutil.Lookup, threshold float64) (*Client, error) {
	cfg := Config{
		URL:       envutil.Get(l, "SUWAYOMI_URL"),
		Auth:      cmp.Or(envutil.Get(l, "SUWAYOMI_AUTH"), "none"),
		User:      envutil.Get(l, "SUWAYOMI_USER"),
		Pass:      envutil.Get(l, "SUWAYOMI_PASS"),
		Sources:   envutil.List(envutil.Get(l, "SUWAYOMI_SOURCES")),
		Threshold: threshold,
	}
	if cfg.URL == "" || len(cfg.Sources) == 0 {
		return nil, errors.New("suwayomi: SUWAYOMI_URL and SUWAYOMI_SOURCES are required")
	}
	// Source searches go through the source website and can be slow.
	return New(cfg, httpx.New(2*time.Minute, 0))
}
```

- [ ] **Step 4: Write the registry**

`internal/adapters/registry/registry.go`:
```go
// Package registry maps adapter names to constructors. Adding an adapter means adding
// one case here.
package registry

import (
	"context"
	"fmt"
	"time"

	"mangasync/internal/adapters/downloader/suwayomi"
	"mangasync/internal/adapters/reader/komga"
	"mangasync/internal/adapters/tracker/mangabaka"
	"mangasync/internal/core"
	"mangasync/internal/envutil"
)

func Reader(name string, l envutil.Lookup) (core.Reader, error) {
	switch name {
	case "komga":
		c, err := komga.FromEnv(l)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown reader %q (available: komga)", name)
}

func Tracker(name string, l envutil.Lookup, threshold float64, endedTTL time.Duration) (core.Tracker, error) {
	switch name {
	case "mangabaka":
		c, err := mangabaka.FromEnv(l, threshold, endedTTL)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown tracker %q (available: mangabaka)", name)
}

// Downloader builds and initialises the named downloader (this contacts the service).
func Downloader(ctx context.Context, name string, l envutil.Lookup, threshold float64) (core.Downloader, error) {
	switch name {
	case "suwayomi":
		c, err := suwayomi.FromEnv(l, threshold)
		if err != nil {
			return nil, err
		}
		if err := c.Init(ctx); err != nil {
			return nil, fmt.Errorf("suwayomi: %w", err)
		}
		return c, nil
	}
	return nil, fmt.Errorf("unknown downloader %q (available: suwayomi)", name)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/adapters/...`
Expected: all four packages `ok`.

- [ ] **Step 6: Commit**

```bash
git add internal/adapters
git commit -m "Add adapter env constructors and registry"
```

---

### Task 22: Work queue and debouncer

**Files:**
- Create: `internal/app/queue.go`
- Test: `internal/app/queue_test.go`

- [ ] **Step 1: Write the failing test**

`internal/app/queue_test.go`:
```go
package app

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestQueueDedupsAndKeepsOrder(t *testing.T) {
	q := NewQueue()
	q.Push("a")
	q.Push("b")
	q.Push("a")
	for _, want := range []string{"a", "b"} {
		got, ok := q.Pop(t.Context())
		if !ok || got != want {
			t.Fatalf("Pop = %q, %v; want %q", got, ok, want)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("Pop on empty queue should block until ctx is done")
	}
}

func TestQueuePopWakesOnPush(t *testing.T) {
	q := NewQueue()
	go func() {
		time.Sleep(10 * time.Millisecond)
		q.Push("x")
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if got, ok := q.Pop(ctx); !ok || got != "x" {
		t.Fatalf("Pop = %q, %v", got, ok)
	}
}

func TestDebouncerCoalesces(t *testing.T) {
	var mu sync.Mutex
	fired := map[string]int{}
	d := NewDebouncer(30*time.Millisecond, func(ref string) {
		mu.Lock()
		fired[ref]++
		mu.Unlock()
	})
	defer d.Stop()
	for range 5 {
		d.Trigger("s1")
		time.Sleep(5 * time.Millisecond)
	}
	d.Trigger("s2")
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if fired["s1"] != 1 || fired["s2"] != 1 {
		t.Fatalf("fired = %v; want each once", fired)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/app/`
Expected: FAIL — `undefined: NewQueue`.

- [ ] **Step 3: Write the implementation**

`internal/app/queue.go`:
```go
// Package app runs the sync loops.
package app

import (
	"context"
	"sync"
	"time"
)

// Queue is a FIFO of series refs that ignores refs already waiting.
type Queue struct {
	mu      sync.Mutex
	pending []string
	queued  map[string]bool
	notify  chan struct{}
}

func NewQueue() *Queue {
	return &Queue{queued: map[string]bool{}, notify: make(chan struct{}, 1)}
}

func (q *Queue) Push(ref string) {
	q.mu.Lock()
	if !q.queued[ref] {
		q.queued[ref] = true
		q.pending = append(q.pending, ref)
	}
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Pop blocks until a ref is available or ctx is done.
func (q *Queue) Pop(ctx context.Context) (string, bool) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			ref := q.pending[0]
			q.pending = q.pending[1:]
			delete(q.queued, ref)
			q.mu.Unlock()
			return ref, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-q.notify:
		}
	}
}

// Debouncer calls fire(ref) once a ref has been quiet for delay.
type Debouncer struct {
	delay  time.Duration
	fire   func(string)
	mu     sync.Mutex
	timers map[string]*time.Timer
}

func NewDebouncer(delay time.Duration, fire func(string)) *Debouncer {
	return &Debouncer{delay: delay, fire: fire, timers: map[string]*time.Timer{}}
}

func (d *Debouncer) Trigger(ref string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.timers[ref]; ok {
		t.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		if d.timers[ref] == t {
			delete(d.timers, ref)
		}
		d.mu.Unlock()
		d.fire(ref)
	})
	d.timers[ref] = t
}

// Stop cancels all pending timers.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for ref, t := range d.timers {
		t.Stop()
		delete(d.timers, ref)
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/app/`
Expected: `ok  	mangasync/internal/app`

- [ ] **Step 5: Commit**

```bash
git add internal/app
git commit -m "Add work queue and debouncer"
```

---

### Task 23: App loops and main

**Files:**
- Create: `internal/app/app.go`
- Test: `internal/app/app_test.go`
- Create: `cmd/mangasync/main.go`

- [ ] **Step 1: Write the failing test**

`internal/app/app_test.go`:
```go
package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
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
			},
			Progress: map[string]core.ReadProgress{
				"S1": {Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 2, LastReadNumber: 2},
				"S2": {Unit: core.UnitChapter, BooksTotal: 10, BooksRead: 3, LastReadNumber: 3},
			},
			Started: []string{"S1"}, // S2 only arrives via a live event
		},
		events: make(chan string, 1),
	}
	tr := &coretest.FakeTracker{
		TrackerName: "mangabaka",
		IDsByRef:    map[string]string{"S1": "1", "S2": "2"},
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
	eventually(t, func() bool { return savedIDs()["1"] })                  // reconcile at startup
	eventually(t, func() bool { return len(dl.AcquiredCandidates()) == 1 }) // download sync at startup
	reader.events <- "S2"
	eventually(t, func() bool { return savedIDs()["2"] }) // live event

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/app/ -run App`
Expected: FAIL — `undefined: App`.

- [ ] **Step 3: Write the app**

`internal/app/app.go`:
```go
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
			a.Log.Warn("series sync incomplete", "ref", ref, "err", err)
		}
	}
}

func (a *App) reconcile(ctx context.Context) {
	series, err := a.Reader.ListStartedSeries(ctx)
	if err != nil {
		if ctx.Err() == nil {
			a.Log.Error("reconcile: list started series", "err", err)
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
		a.Log.Warn("download sync incomplete", "err", err)
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
			backoff = time.Second
			for ref := range ch {
				deb.Trigger(ref)
			}
			if ctx.Err() != nil {
				return
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/app/`
Expected: `ok  	mangasync/internal/app`

- [ ] **Step 5: Write main**

`cmd/mangasync/main.go`:
```go
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
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", "err", err)
			stop()
		}
	}()

	log.Info("mangasync started", "reader", cfg.Reader, "trackers", cfg.Trackers,
		"download_tracker", cfg.DownloadTracker, "downloader", cfg.Downloader, "dry_run", cfg.DryRun)
	a.Run(ctx)
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func healthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}
```

- [ ] **Step 6: Build and run the whole suite**

Run: `go build ./... && go vet ./... && go test -race ./...`
Expected: no build or vet output; every package `ok`.

- [ ] **Step 7: Check the binary fails fast without config**

Run: `go run ./cmd/mangasync`
Expected: exits 1 with `mangasync: init /data/mangasync.db: ...` or `mangasync: komga: KOMGA_URL and KOMGA_API_KEY are required` (whichever comes first on this machine).

- [ ] **Step 8: Commit**

```bash
git add internal/app cmd
git commit -m "Add app loops and main"
```

---

### Task 24: Container image, Kubernetes manifests, README

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `deploy/kustomization.yaml`
- Create: `deploy/deployment.yaml`
- Create: `deploy/pvc.yaml`
- Create: `deploy/configmap.yaml`
- Create: `deploy/secret.example.yaml`
- Create: `README.md`

- [ ] **Step 1: Write the Dockerfile**

`Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mangasync ./cmd/mangasync

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/mangasync /mangasync
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/mangasync"]
```

`.dockerignore`:
```
.git
.env
*.db
*.db-*
docs
deploy
```

- [ ] **Step 2: Build the image**

Run: `docker build -t mangasync:dev .`
Expected: `naming to docker.io/library/mangasync:dev` and a successful build.

- [ ] **Step 3: Write the manifests**

`deploy/kustomization.yaml`:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - pvc.yaml
  - configmap.yaml
  - deployment.yaml
images:
  - name: mangasync
    newName: mangasync # set to your registry, e.g. ghcr.io/<you>/mangasync
    newTag: dev
```

`deploy/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mangasync
  labels:
    app.kubernetes.io/name: mangasync
spec:
  replicas: 1 # SQLite + SSE: exactly one instance
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: mangasync
  template:
    metadata:
      labels:
        app.kubernetes.io/name: mangasync
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: mangasync
          image: mangasync
          envFrom:
            - configMapRef:
                name: mangasync
            - secretRef:
                name: mangasync
          ports:
            - name: http
              containerPort: 8080
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            periodSeconds: 10
          resources:
            requests:
              cpu: 10m
              memory: 32Mi
            limits:
              memory: 128Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: data
              mountPath: /data
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: mangasync-data
        - name: tmp
          emptyDir: {}
```

`deploy/pvc.yaml`:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mangasync-data
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 100Mi
```

`deploy/configmap.yaml`:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mangasync
data:
  # Start in dry-run: check the logs, then set to "false".
  DRY_RUN: "true"
  READER: komga
  TRACKERS: mangabaka
  DOWNLOAD_TRACKER: mangabaka
  DOWNLOADER: suwayomi
  ACQUIRE_STATUSES: planning,reading,rereading
  RELEASE_STATUSES: dropped
  KOMGA_URL: https://komga.example.com
  SUWAYOMI_URL: https://suwayomi.example.com
  SUWAYOMI_AUTH: none
  SUWAYOMI_SOURCES: Weeb Central (EN),ManhuaTop (EN),Webdex Scans (EN)
  DOWNLOAD_INTERVAL: 15m
  RECONCILE_INTERVAL: 1h
  LOG_LEVEL: info
```

`deploy/secret.example.yaml`:
```yaml
# Not applied by kustomize. Create the real secret with:
#   kubectl create secret generic mangasync \
#     --from-literal=KOMGA_API_KEY=... --from-literal=MANGABAKA_TOKEN=mb-...
# (add SUWAYOMI_USER / SUWAYOMI_PASS if SUWAYOMI_AUTH is basic or ui_login)
apiVersion: v1
kind: Secret
metadata:
  name: mangasync
type: Opaque
stringData:
  KOMGA_API_KEY: replace-me
  MANGABAKA_TOKEN: mb-replace-me
```

- [ ] **Step 4: Validate the manifests**

Run: `kustomize build deploy`
Expected: a Deployment, PVC and ConfigMap rendered; the Deployment's image is `mangasync:dev`.

- [ ] **Step 5: Write the README**

`README.md`:
````markdown
# MangaSync

Syncs Komga read progress to MangaBaka, and drives Suwayomi downloads from your MangaBaka
library statuses. Design: `docs/superpowers/specs/2026-10-05-mangasync-design.md`.

| MangaBaka status | Komga sets it? | Suwayomi action |
|---|---|---|
| plan to read / reading / rereading | reading from Komga progress | add + download all, unless already in Suwayomi or Komga |
| completed | when everything is read and publication ended | none |
| dropped | never (yours) | removed from library, files kept |
| considering / paused | never | none |

## Run locally

```bash
set -a; source .env; set +a
DRY_RUN=true DB_PATH=./mangasync.db HTTP_ADDR=:18080 \
DOWNLOAD_TRACKER=mangabaka DOWNLOADER=suwayomi \
SUWAYOMI_SOURCES="Weeb Central (EN),ManhuaTop (EN),Webdex Scans (EN)" \
go run ./cmd/mangasync
```

## Deploy

```bash
docker build -t <registry>/mangasync:<tag> . && docker push <registry>/mangasync:<tag>
kubectl create secret generic mangasync --from-literal=KOMGA_API_KEY=... --from-literal=MANGABAKA_TOKEN=mb-...
kustomize edit set image mangasync=<registry>/mangasync:<tag>   # run inside deploy/
kubectl apply -k deploy
```

The ConfigMap starts with `DRY_RUN: "true"`. Watch `kubectl logs deploy/mangasync`, then set it to `"false"`.

## Configuration

Core: `READER`, `TRACKERS`, `DOWNLOAD_TRACKER`, `DOWNLOADER`, `ACQUIRE_STATUSES`, `RELEASE_STATUSES`,
`DOWNLOAD_INTERVAL`, `RECONCILE_INTERVAL`, `SSE_DEBOUNCE`, `MATCH_THRESHOLD`, `DRY_RUN`, `DB_PATH`,
`LOG_LEVEL`, `HTTP_ADDR`.

Komga: `KOMGA_URL`, `KOMGA_API_KEY`, `KOMGA_VOLUME_LIBRARIES`.
MangaBaka: `MANGABAKA_TOKEN`, `MANGABAKA_URL` (optional).
Suwayomi: `SUWAYOMI_URL`, `SUWAYOMI_AUTH` (`none`/`basic`/`ui_login`), `SUWAYOMI_USER`, `SUWAYOMI_PASS`, `SUWAYOMI_SOURCES` (priority order).

## Adding a service

Implement the port in `internal/core/core.go` in a new package under `internal/adapters/<kind>/<name>`,
run the matching `coretest.*Contract` in its tests, and add a case to `internal/adapters/registry`.
````

- [ ] **Step 6: Commit**

```bash
git add Dockerfile .dockerignore deploy README.md
git commit -m "Add container image, Kubernetes manifests and README"
```

---

### Task 25: Dry-run smoke test against the live services

This task needs the user. It exercises the parts no unit test can: real response shapes (MangaBaka v2 library, single-entry GET), real title matching on the user's 28 series, and the Komga event stream.

- [ ] **Step 1: Fix the Suwayomi URL in `.env`**

The `.env` value is `https://suwayomi.example` (no `.nl`); it must be `https://suwayomi.example.com`. Ask the user to fix it (the file holds their secrets; don't print it).

- [ ] **Step 2: Start a dry run**

```bash
set -a; source .env; set +a
DRY_RUN=true DB_PATH=./mangasync.db HTTP_ADDR=:18080 LOG_LEVEL=debug \
DOWNLOAD_TRACKER=mangabaka DOWNLOADER=suwayomi \
SUWAYOMI_SOURCES="Weeb Central (EN),ManhuaTop (EN),Webdex Scans (EN)" \
go run ./cmd/mangasync 2>&1 | tee smoke.log
```
Run it in the background, let it run ~2 minutes, then stop it with Ctrl-C.

Expected in `smoke.log`:
- `mangasync started` with `"dry_run":true`
- `event stream connected`
- `reconcile queued` with `"series":17`
- `matched series` lines (one per resolved series) and `no tracker match` warnings for the rest
- `updating tracker entry` lines, e.g. Chainsaw Man `status=reading chapter=104`; Boruto - Naruto Next Generations `status=completed chapter=80` (ended); Heavenly Delusion `status=reading chapter=85` (caught up, still publishing)
- no `download sync` errors (the MangaBaka library is currently empty, so nothing to acquire)

- [ ] **Step 3: Review the matches with the user**

```bash
grep -E '"msg":"(matched series|no tracker match)' smoke.log | jq -c '{msg, series, tracker_id}'
```
Show the user the list. Any wrong match → lower-risk fix is adding the right link in Komga (Komf / series metadata). Any miss that should match → note it; tuning `MATCH_THRESHOLD` is the user's call.

- [ ] **Step 4: Exercise download sync**

Ask the user to mark one manga they don't have yet (e.g. Frieren) as *Plan to read* on MangaBaka. Re-run Step 2 for ~1 minute.

Expected: an `acquiring` line naming a Suwayomi candidate and source, with `"dry_run":true`. If instead `not found in downloader` appears, check its `near_misses`.

- [ ] **Step 5: First real write (user approval required)**

With the user's explicit go-ahead, run once with `DRY_RUN=false` and the same env, then verify one entry:

```bash
set -a; source .env; set +a
curl -sS -H "x-api-key: $MANGABAKA_TOKEN" https://api.mangabaka.org/v1/my/library/1677 | jq '.data | {state, progress_chapter}'
```
Expected: `{"state":"reading","progress_chapter":104}`. This confirms the single-entry GET shape and PATCH/POST against the real API. If the shape differs, fix `entryDTO` in `internal/adapters/tracker/mangabaka/entries.go` and its test fixture.

- [ ] **Step 6: Clean up and commit any fixes**

```bash
rm -f smoke.log mangasync.db mangasync.db-*
git status --short
```
Commit any fixes made during the smoke test with a message describing what the live API differed in.

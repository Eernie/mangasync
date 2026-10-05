# MangaSync — Design

Date: 2026-10-05
Status: Approved for planning (rev 4: status lifecycle + download sync)

## Goal

A small always-on service on Kubernetes that connects three kinds of services through swappable adapters:

1. **Read sync:** pushes read history from a **Reader** (Komga) to one or more **Trackers** (MangaBaka) — status + progress, one-way.
2. **Download sync:** reacts to the status of each series in a designated Tracker. Want-to-read and reading series that you don't have yet are found with a **Downloader** (Suwayomi), added to its library and fully downloaded; dropped series are removed from the downloader's library (files kept). See *Status lifecycle*.

v1 ships exactly one adapter per port: `komga`, `mangabaka`, `suwayomi`. The ports are shaped so that Kavita (reader), AniList / MyAnimeList (trackers) and other downloaders can be added later as new adapter packages without changing the sync logic.

Non-goals (v1): two-way sync (Tracker → Reader), syncing ratings/notes, multi-user, a UI, implementing any adapter beyond the three above.

## Architecture: ports and adapters

```
              ┌──────────────── core (types + ports) ────────────────┐
 Reader ──▶   │  sync/progress  ──▶  Tracker(s)                      │
 (komga)      │  sync/download  ◀──  Tracker      ──▶  Downloader    │
              └──────────────────────────────────────────────────────┘
                     ▲ match (pure)        ▲ store (SQLite)
```

- `internal/core` holds provider-neutral types and port interfaces. It imports nothing from the project.
- `internal/sync/progress` and `internal/sync/download` contain all business rules and depend only on `core`, `match` and `store`.
- Each adapter lives in `internal/adapters/<port>/<name>` and implements a port. Adapters never import each other or the sync packages.
- `internal/adapters/registry` maps adapter names to constructors that read their own env vars. Adding an adapter = new package + one registry line.

### Core types

```go
// IDKind names an external database a series can be identified by.
type IDKind string // "mangabaka", "anilist", "mal", "mangaupdates", "kitsu", "animeplanet", "ann", "mangadex"

type IDs map[IDKind]string

type Series struct {
    Ref        string   // the owning service's own ID (reader series ID, tracker ID, ...)
    Title      string
    AltTitles  []string
    IDs        IDs      // cross-reference IDs known for this series
    LibraryRef string   // reader only: library/collection the series lives in
}

type Unit string // "chapter" | "volume"

type ReadProgress struct {
    Unit            Unit
    BooksTotal      int
    BooksRead       int
    BooksInProgress int     // partially read books
    LastReadNumber float64 // last continuously-read chapter/volume number
    MaxNumber      float64 // highest chapter/volume number present
    FirstReadAt    time.Time // earliest read date of any read/in-progress book; zero = unknown
    LastReadAt     time.Time // latest read date of any read/in-progress book; zero = unknown
}

type Status string // "considering" | "planning" | "reading" | "completed" | "paused" | "dropped" | "rereading" | "unknown"

type Entry struct {
    Status     Status
    Chapter    *float64
    Volume     *float64
    StartDate  string // YYYY-MM-DD, "" = not set
    FinishDate string // YYYY-MM-DD, "" = not set
}

type EntryUpdate struct { // nil fields are left unchanged
    Status     *Status
    Chapter    *float64
    Volume     *float64
    StartDate  *string // YYYY-MM-DD
    FinishDate *string // YYYY-MM-DD
}
```

### Ports

```go
type Reader interface {
    Name() string
    ListAllSeries(ctx context.Context) ([]Series, error) // every series, with IDs; reconcile queues all of them, and used for "already have it"
    GetSeries(ctx context.Context, ref string) (Series, error)
    GetProgress(ctx context.Context, ref string) (ReadProgress, error)
}

// Optional Reader capability. Readers without it are synced by reconcile only.
type ProgressWatcher interface {
    WatchProgress(ctx context.Context) (<-chan string, error) // emits reader series refs; closes on disconnect
}

type Tracker interface {
    Name() string
    // Resolve finds this tracker's series ID for a reader series, using s.IDs first
    // (direct ID, then cross-reference lookup) and title search last.
    Resolve(ctx context.Context, s Series) (id string, found bool, err error)
    // SeriesEnded reports whether publication has finished (completed or cancelled).
    // Used to tell "finished the series" apart from "caught up on an ongoing series".
    SeriesEnded(ctx context.Context, id string) (bool, error)
    GetEntry(ctx context.Context, id string) (*Entry, error) // nil, nil if not in the user's list
    SaveEntry(ctx context.Context, id string, u EntryUpdate) error // create or partial update
}

type LibraryEntry struct {
    Series Series // Ref = tracker ID, with titles + IDs filled
    Status Status
}

// Optional Tracker capability, required for the tracker named in DOWNLOAD_TRACKER.
type LibraryLister interface {
    ListLibrary(ctx context.Context, statuses []Status) ([]LibraryEntry, error)
}

type Candidate struct {
    Ref        string // downloader's own manga ID
    SourceName string
    Title      string
    Score      float64
}

type Downloader interface {
    Name() string
    // FindInLibrary matches s against the downloader's own library only (no source searches).
    FindInLibrary(ctx context.Context, s Series) (*Candidate, error)
    // Find searches the downloader for s. best is nil when nothing passes the threshold;
    // nearMisses are the closest rejected candidates, for logging.
    Find(ctx context.Context, s Series) (best *Candidate, nearMisses []Candidate, err error)
    // Acquire adds the candidate to the downloader's library and queues every
    // not-yet-downloaded chapter. Must be idempotent.
    Acquire(ctx context.Context, c Candidate) error
    // Release removes the manga from the downloader's library so it stops updating.
    // Downloaded files are kept. Must be idempotent.
    Release(ctx context.Context, c Candidate) error
}
```

Status mapping lives in each tracker adapter (e.g. MangaBaka `plan_to_read` → `planning`, `considering` → `considering`; AniList `CURRENT` → `reading`, `REPEATING` → `rereading`; MAL `on_hold` → `paused`). Values an adapter doesn't recognize map to `unknown`.

## Status lifecycle

Normalized statuses, what sets them, and what the downloader does (defaults):

| Status (MangaBaka) | Set from the reader? | Overwritten from the reader? | Downloader action |
|---|---|---|---|
| `considering` | Never | Yes → `reading`/`completed` once reading starts | None |
| `planning` (`plan_to_read`) | Created for a series that is in the reader but not in the tracker list yet and has nothing read or opened; an existing entry is never touched | Yes → `reading`/`completed` once reading starts | **Acquire** (if not already had) |
| `reading` | `IN_PROGRESS`, or all read while still publishing | Progress only goes up; → `completed` when finished | **Acquire** (if not already had) |
| `rereading` | Never | Never (protected) | **Acquire** (if not already had) |
| `completed` | All read and publication ended | Never (protected) | None |
| `paused` | Never | Never (protected) | None |
| `dropped` | Never | Never (protected) | **Release** (remove from library, keep files) |
| not in list | Created as `reading`/`completed` once the reader shows progress; created as `planning` while nothing is read or opened | — | None |

Reader → tracker:

| Reader state | Tracker status | Progress |
|---|---|---|
| nothing read or opened (`BooksRead == 0`, `BooksInProgress == 0`) | `planning` (`plan_to_read`), **only if the series is not in the tracker list yet**; an existing entry of any status is left untouched | none, no dates |
| first book(s) only partly read (`BooksRead == 0`, `BooksInProgress > 0`) | `reading` | none |
| some books read | `reading` | `LastReadNumber` |
| all read, still publishing | `reading` | `MaxNumber` |
| all read, publication ended | `completed` | `MaxNumber` |

Typical flow: mark WTR → acquired and downloaded → appears in Komga → reading sets `reading` (downloader: already had, no-op) → finishing sets `completed` (or stays `reading` while ongoing). Manually setting `dropped` releases it; setting it back to WTR/reading acquires it again.

"Already had" means either of:
- found in the downloader's library (`FindInLibrary`), or
- present in the reader: a reader series (`ListAllSeries`, fetched once per poll) shares any cross-reference ID with the tracker series, or matches its titles at ≥ `MATCH_THRESHOLD`. This prevents duplicate downloads of series added to Komga outside Suwayomi.
  - or the tracker series is the one progress sync matched to a reader series (`series_map` row with status `matched`, loaded once per poll via `MatchedTrackerIDs`) that is still in the reader. This covers matches made by title search where the shared IDs conflict, and merged tracker IDs. A mapping to a reader series that no longer exists is ignored.
  - Both reader checks are skipped when our own download record shows we managed the series (kept files of a released series must not block re-acquiring).

The acquire/release status sets are configurable (`ACQUIRE_STATUSES`, `RELEASE_STATUSES`).

## Runtime

Single Go binary, Kubernetes Deployment (1 replica), three loops:

1. **Watcher** — only if the reader implements `ProgressWatcher`. Each emitted series ref is debounced (`SSE_DEBOUNCE`, default 10s) then queued for progress sync. On channel close: reconnect with backoff (1s → 60s cap).
2. **Reconcile** (`RECONCILE_INTERVAL`, default 1h, plus once at startup) — queues every series from `ListAllSeries` (unstarted ones included, so they can be planned).
3. **Download sync** (`DOWNLOAD_INTERVAL`, default 15m, plus once at startup) — only if `DOWNLOAD_TRACKER` and `DOWNLOADER` are set.

Progress sync runs on one worker goroutine fed by a deduplicating queue, so watcher and reconcile never sync the same series concurrently.

## Progress sync (Reader → each Tracker)

For one reader series, for each tracker in `TRACKERS` (failures in one tracker don't affect the others):

1. **Resolve** the tracker ID: cached in store → else `tracker.Resolve(series)`. Not found → store `unmatched`, retry at most once per 24h.
2. **Target** from `ReadProgress` (see the reader → tracker table in *Status lifecycle*):
   - `BooksRead == BooksTotal > 0` and `tracker.SeriesEnded` → `completed`, progress = `MaxNumber`.
   - `BooksRead == BooksTotal > 0`, series still publishing → `reading`, progress = `MaxNumber` (caught up).
   - `BooksRead > 0` → `reading`, progress = `LastReadNumber`.
   - `BooksRead == 0`, `BooksInProgress > 0` → `reading`, no progress.
   - Publication status comes from the tracker, not the reader: Komga's `metadata.status` defaults to `ONGOING` for series without metadata (seen live on finished series), so it can't be trusted.
   - `BooksTotal` can exceed `MaxNumber` (duplicate scanlations, chapter 0), so "all read" uses the book counts and progress uses the numbers.
   - `BooksRead == 0`, `BooksInProgress == 0` (and `BooksTotal > 0`) → `planning`, no progress, no dates. A series without any books gives no target.
   - Progress goes in `Chapter` or `Volume` per `ReadProgress.Unit`.
3. **Decide** against the current entry (`GetEntry`):
   - A `planning` target only creates a missing entry: with no current entry the update is `status = planning` and nothing else; with any current entry (whatever its status, progress or dates) the result is nil. It never modifies an existing MangaBaka entry. This is an early branch, so the rules below only apply to `reading`/`completed` targets.
   - Protected: if current status is `paused`, `dropped`, `completed`, `rereading` or `unknown` → do nothing.
   - Status may move: none/`considering`/`planning` → `reading`/`completed`; `reading` → `completed`. Never backwards.
   - Never lower progress: only send progress if target > current (or current is nil).
   - Nothing to change → no write.
4. **Write** with `SaveEntry` (only changed fields). Record last pushed status/progress in store.

The decision function (step 3) is a pure function `Decide(current *Entry, target Target) *EntryUpdate`, independent of any adapter.

### Dates

- **Start date** = the calendar date of the earliest `readProgress.readDate` of any read or in-progress book in the series.
- **Finish date** = the calendar date of the latest `readProgress.readDate` of a read book, and only when the target status is `completed`.
- Dates are civil dates (`YYYY-MM-DD`; `""` = no date), computed in the process time zone (`time.Local`, set with the standard `TZ` env var). `ComputeTarget(p, ended, loc)` does the conversion and fills `Target.StartDate` / `Target.FinishDate`.
- `Decide` writes a date only when the tracker entry has no value for it yet. A date the user set is never overwritten. Protected statuses are never touched at all, dates included.
- A finish date earlier than a start date already on the entry is skipped, so the tracker never ends up with finish < start.
- The Komga date lookups are best effort: if they fail, progress sync continues with unknown (zero) dates.
- A date alone is a valid reason to write: a `reading` entry with up-to-date progress but no start date gets its start date filled.
- Dates are not stored in the local store (`pushedState` is unchanged); the tracker entry is the source of truth.

## Download sync (Tracker → Downloader)

Each poll: `ListLibrary(ACQUIRE_STATUSES ∪ RELEASE_STATUSES)`, `reader.ListAllSeries()` once, then per entry, using the `downloads` record for (tracker, tracker_id, downloader):

**Status in `ACQUIRE_STATUSES`:**
1. Record is `acquired` → skip.
2. Record is `not_found` and `retry_after` not reached → skip.
3. `FindInLibrary` hit → store `acquired` (no download; you already have it).
4. Present in the reader (see *Status lifecycle*) → skip, store nothing (re-checked each poll, cheap and local), unless the record shows this series was managed in the downloader by us (`released`/`in_progress` with a candidate ref), since released files stay in the reader.
5. `Find` → no match → store `not_found` with backoff 1d → 3d → 7d → 14d → 30d (cap); log up to 3 near misses.
6. Match → `Acquire` → store `acquired` with candidate ref + source. On error store `in_progress`, retry next poll.

**Status in `RELEASE_STATUSES`:**
1. Record is `released` → skip.
2. `FindInLibrary` hit → `Release` → store `released`. Miss → store `released` (nothing to remove).

Unstarted reader series show up in the tracker as `planning` (see *Status lifecycle*), which is an acquire status, but they are skipped here because they are already in the downloader library or the reader (steps 3 and 4), so no download is started for them.

Any other status → nothing. A record flips between `acquired` and `released` as the tracker status changes, so dropped → WTR acquires again, and WTR → dropped releases.

## Matching (`internal/match`, shared by adapters)

- `Normalize`: lowercase, Unicode NFKC, map `_`, `-`, `–`, `—`, `:` to spaces, drop apostrophes (`'`, `’`), strip remaining punctuation/brackets, collapse whitespace, drop leading "the"/"a". Komga folder names replace `:` with `_` (e.g. `Naruto_ Sasuke's Story - The Uchiha and the Heavenly Stardust_ The Manga` must match MangaBaka's `Naruto: Sasuke’s Story—The Uchiha and the Heavenly Stardust: The Manga`).
- `Similarity`: 1 − Levenshtein/maxLen on normalized strings; exact match = 1.0; equal after removing spaces = 1.0 (`LOSTEND` ↔ `Lost End`).
- `Best(titles []string, candidates) (best, nearMisses)`: max similarity of each candidate against all titles; accept if ≥ `MATCH_THRESHOLD` (default 0.9).
- `ParseLink(url) (IDKind, id, ok)`: recognizes URLs for mangabaka.org, anilist.co, myanimelist.net, mangaupdates.com (base36 ID, e.g. `/series/ylx5wzn/…`), kitsu.app/kitsu.io, anime-planet.com (slug), animenewsnetwork.com, mangadex.org (UUID; `IDKind` `mangadex`, not used by MangaBaka but kept for future adapters). Unrecognized links (Amazon, BookWalker, official sites) are ignored.

## State (SQLite at `DB_PATH`)

- `series_map(reader, reader_ref, tracker, tracker_id NULL, status [matched|unmatched], last_attempt, last_status, last_progress, PK(reader, reader_ref, tracker))`
- `downloads(tracker, tracker_id, downloader, status [acquired|released|not_found|in_progress], candidate_ref NULL, source NULL, attempts, retry_after, updated_at, PK(tracker, tracker_id, downloader))`

Keys include the adapter names, so switching or adding adapters never collides with old rows. Losing the DB is safe: everything is re-derivable.

## Error handling

- Shared `internal/httpx`: client with per-adapter rate limiter and retries on 429/5xx/network errors (exponential backoff + jitter, honors `Retry-After`, max 5 attempts). Every adapter uses it.
- One series/tracker failing is logged and skipped.
- Config errors (unknown adapter name, missing env, unknown Suwayomi source, `DOWNLOAD_TRACKER` not a tracker that implements `LibraryLister`, unknown status names in `ACQUIRE_STATUSES`/`RELEASE_STATUSES`, a status in both sets) → fail fast at startup.
- `DRY_RUN=true`: sync logic computes everything, logs intended `SaveEntry`/`Acquire`/`Release` calls with payloads, and skips them. Store records nothing as `acquired`/`released`. Implemented once in the sync layer, not per adapter.

## Configuration

Core:

| Var | Default | Notes |
|---|---|---|
| `READER` | `komga` | adapter name |
| `TRACKERS` | `mangabaka` | comma-separated adapter names |
| `DOWNLOAD_TRACKER` | empty | tracker whose statuses drive the downloader; empty disables download sync |
| `DOWNLOADER` | empty | adapter name; empty disables download sync |
| `ACQUIRE_STATUSES` | `planning,reading,rereading` | empty disables acquiring |
| `RELEASE_STATUSES` | `dropped` | empty disables releasing |
| `DOWNLOAD_INTERVAL` | `15m` | |
| `RECONCILE_INTERVAL` | `1h` | |
| `SSE_DEBOUNCE` | `10s` | |
| `MATCH_THRESHOLD` | `0.9` | |
| `DRY_RUN` | `false` | |
| `DB_PATH` | `/data/mangasync.db` | |
| `LOG_LEVEL` | `info` | |
| `TZ` | system zone (UTC in the container) | time zone for start/finish dates; the container embeds `time/tzdata` |
| `HTTP_ADDR` | `:8080` | `/healthz` |

Adapter-specific vars are prefixed with the adapter name and only read when that adapter is selected (see each adapter below).

## v1 adapters

### Reader `komga` (implements `Reader`, `ProgressWatcher`)

Env: `KOMGA_URL`, `KOMGA_API_KEY`, `KOMGA_VOLUME_LIBRARIES` (comma-separated library IDs whose books are volumes; default empty = all chapters).

- Auth header `X-API-Key`.
- `ListAllSeries`: `POST /api/v1/series/list?unpaged=true` with `{}`; the list response already includes `metadata.title`, `alternateTitles` and `links`, so no per-series calls.
- `GetSeries`: `GET /api/v1/series/{id}` → `metadata.title`, `metadata.alternateTitles`, `libraryId`; `IDs` from `metadata.links[].url` via `match.ParseLink`. Parse by URL, not label: live labels are `AniList`, `MangaUpdates`, `MyAnimeList`, `Kitsu`, `Anime-Planet`, `MangaDex`, plus shop/official links to ignore. No `MangaBaka` links exist on the live instance, and 16 of 28 series have no links at all, so title search is a primary path, not an edge case.
- `GetProgress`: `GET /api/v2/series/{id}/read-progress/tachiyomi` → `lastReadContinuousNumberSort`, `maxNumberSort`, `booksCount`, `booksReadCount`, `booksInProgressCount`. Unit = volume if library in `KOMGA_VOLUME_LIBRARIES`, else chapter. When `booksReadCount + booksInProgressCount > 0`, two more calls fetch the dates: `POST /api/v1/books/list?size=1&sort=readProgress.readDate,asc` and `…,desc` with condition `allOf [seriesId is <ref>, anyOf [readStatus is READ, readStatus is IN_PROGRESS]]`; `content[0].readProgress.readDate` (RFC 3339) gives `FirstReadAt` (asc) and `LastReadAt` (desc). No extra calls when nothing is read or in progress.
- `WatchProgress`: SSE `GET /sse/v1/events` with `X-API-Key` (verified: 200, `text/event-stream`). Format is `event:<Type>\ndata:<json>\n\n`; ignore other types (e.g. `TaskQueueStatus`). Emit `seriesId` from `ReadProgressSeriesChanged` / `ReadProgressSeriesDeleted`. Events are delivered only to the owning user, so the API key must belong to the reading user.

### Tracker `mangabaka` (implements `Tracker`, `LibraryLister`)

Env: `MANGABAKA_TOKEN` (Personal Access Token, `mb-…`).

- Base `https://api.mangabaka.org`, header `x-api-key`, responses `{status, data, pagination?}`.
- `Resolve`: `IDs["mangabaka"]` → done; else first of anilist / mangaupdates / mal / kitsu / animeplanet → `GET /v1/source/{anilist|manga-updates|my-anime-list|kitsu|anime-planet}/{id}` (verified: returns `data.series[]`, take the first `active` one); else `GET /v1/series/search?q=` + `match.Best` against `title`, `romanized_title` and `secondary_titles.*[].title`. Search returns near-duplicates (e.g. two Sasuke's Story entries), so `Best` must pick the highest score, not the first hit. Follow `merged_with` on merged series.
- `SeriesEnded`: `GET /v1/series/{id}` → `status` in {`completed`, `cancelled`}. Enum: `cancelled, completed, hiatus, releasing, unknown, upcoming`. Cached in memory for the reconcile interval.
- `GetEntry`: `GET /v1/my/library/{id}` (404 → nil). `SaveEntry`: `PATCH` (or `POST` if absent) with `state`, `progress_chapter`, `progress_volume`, `start_date`, `finish_date` (`YYYY-MM-DD`). `GetEntry` reads the first 10 characters of `start_date` / `finish_date` (the API may return `2026-06-06` or a full timestamp).
- `ListLibrary`: `GET /v2/my/library?state=<s>&limit=100` (paged), once per requested MangaBaka state (or unfiltered and filtered locally if the API rejects repeated `state`). Must be v2: v1 list items carry no series ID. v2 items are `{entry, lists, series}` with the full series object (`id`, titles, `secondary_titles`, `source`).
- Progress fields are JSON numbers with no `multipleOf` in the spec, so decimals (e.g. 10.5) are sent as-is. A submitted `0` is stored as null.
- Status map: `considering` → considering; `plan_to_read` → planning; `reading` → reading; `completed` → completed; `paused`, `on_hold` → paused; `dropped` → dropped; `rereading` → rereading; else unknown. Reverse is the same table (only `reading`/`completed`/`planning` are ever written, and `planning` only to create a missing entry).
- Rate limits: search 25/min, others 150/min (API limits are 30 / 180 per IP).

### Downloader `suwayomi` (implements `Downloader`)

Env: `SUWAYOMI_URL`, `SUWAYOMI_AUTH` (`none` | `basic` | `ui_login`), `SUWAYOMI_USER`, `SUWAYOMI_PASS`, `SUWAYOMI_SOURCES` (ordered, comma-separated source display names like `MangaDex (EN)` or numeric IDs).

- GraphQL at `/api/graphql`, typed structs over `net/http`. `ui_login`: `login` mutation → Bearer access token (~5 min), refreshed via `refreshToken` mutation.
- Startup: resolve `SUWAYOMI_SOURCES` names → IDs via `sources` query; fail on unknown names.
- `Find`: for each source in order, `fetchSourceManga(type: SEARCH, query: title, page: 1)`; score with `match.Best` against title + all alt titles; first source with an accepted candidate wins. Queries: the main title first (all sources), then up to two Latin-script alt titles. Alt titles are essential: sources use English titles (Weeb Central returns `Frieren - Beyond Journey's End`) while MangaBaka's main title may be romaji.
- `FindInLibrary`: `mangas(condition:{inLibrary:true})` (cached per poll) scored with `match.Best` against title + alt titles.
- `Acquire`: `updateManga(patch:{inLibrary:true})` → `fetchMangaAndChapters(input:{id, fetchManga:true, fetchChapters:true})` (there is no `fetchChapters` mutation in v2.4.2378) → `enqueueChapterDownloads` for chapters with `isDownloaded == false`.
- `Release`: `updateManga(patch:{inLibrary:false})`. Chapters and files on disk are untouched.
- Live instance: v2.4.2378 (Preview), auth `none`, installed sources `Weeb Central (EN)`, `ManhuaTop (EN)`, `Webdex Scans (EN)`. The existing 28-series library all comes from Weeb Central.

## Future adapters (shape check only, not built in v1)

Expected shapes, to confirm when built: Kavita (REST + JWT/API key, SignalR hub for live events → `ProgressWatcher`), AniList (GraphQL, OAuth token, `MediaList` status/progress/progressVolumes, IDs via `idMal`), MyAnimeList (REST v2, OAuth2 PKCE, `num_chapters_read`/`num_volumes_read`). None of them require changes to the ports above (AniList/MAL have no `considering`; they simply never produce it).

## Package layout

| Package | Responsibility |
|---|---|
| `cmd/mangasync` | Config, wiring, loops, `/healthz` |
| `internal/core` | Types + port interfaces |
| `internal/core/coretest` | Reusable contract tests + in-memory fake adapters |
| `internal/match` | Normalization, similarity, link parsing |
| `internal/sync/progress` | Resolve → target → `Decide` → write |
| `internal/sync/download` | Tracker status → acquire/release, with "already had" checks and backoff |
| `internal/store` | SQLite (`modernc.org/sqlite`, no CGO) |
| `internal/httpx` | Shared HTTP client: rate limit + retry |
| `internal/adapters/registry` | Name → constructor |
| `internal/adapters/reader/komga` | Komga adapter |
| `internal/adapters/tracker/mangabaka` | MangaBaka adapter |
| `internal/adapters/downloader/suwayomi` | Suwayomi adapter |

## Deployment

- Multi-stage `Dockerfile` → `gcr.io/distroless/static:nonroot`, `CGO_ENABLED=0`.
- `deploy/` Kustomize base: Deployment (replicas 1, `strategy: Recreate`, non-root, read-only root FS, `/data` from PVC), PVC (100Mi, RWO), ConfigMap, example Secret with placeholders, liveness/readiness on `/healthz`. Requests ~10m CPU / 32Mi memory.
- Logs: JSON via `log/slog` with `series`, `reader`, `tracker`, `downloader`, `action`, `dry_run` fields.

## Testing

- **Pure units** (table-driven): `match`, `progress.Decide` (every row of both lifecycle tables), not-found backoff schedule.
- **Sync logic** with in-memory fakes from `coretest`: multi-tracker fan-out with one tracker failing, protected statuses, never-lower progress; download sync for every status, already-in-downloader, already-in-reader (by ID and by title), dropped → release → back to WTR → re-acquire, idempotent re-run, partial failure, dry run.
- **Adapter tests**: `httptest` servers with JSON fixtures shaped like real responses; each adapter also runs the `coretest` contract suite for its port, which future adapters reuse.
- **Manual smoke**: `DRY_RUN=true` against the real instances before enabling writes.

## Validation log (2026-10-05)

Read-only checks against the live instances; no writes were made.

| Item | Result |
|---|---|
| Komga SSE with `X-API-Key` | Works (200, `text/event-stream`). Session fallback dropped. |
| Komga tachiyomi progress endpoint | Works; shape as documented. |
| Komga links | No MangaBaka links; AniList/MU/MAL/Kitsu/Anime-Planet/MangaDex on 12 of 28 series. |
| Komga `metadata.status` | Unreliable (defaults to `ONGOING`); use tracker's publication status. |
| Komga titles | Filesystem-mangled (`_` for `:`); handled in `Normalize`. |
| MangaBaka list shape | v1 items lack series ID; use `/v2/my/library` (`{entry, lists, series}`). |
| MangaBaka decimals | Allowed per OpenAPI (`number`, no `multipleOf`). Confirm on first real write. |
| MangaBaka source lookup | `/v1/source/anilist/105778` → `data.series[0].id = 1677` (Chainsaw Man). |
| Suwayomi auth / search / chapters | `none`; `fetchSourceManga` SEARCH works; `isDownloaded`, `downloadCount` present. |

Remaining: the MangaBaka library was empty, so a real list response and a real `PATCH` are first exercised during the dry-run/first-write smoke test.

# MangaSync — Design

Date: 2026-10-05
Status: Approved for planning (rev 2: pluggable providers)

## Goal

A small always-on service on Kubernetes that connects three kinds of services through swappable adapters:

1. **Read sync:** pushes read history from a **Reader** (Komga) to one or more **Trackers** (MangaBaka) — status + progress, one-way.
2. **WTR → download:** when a series is marked "plan to read" in a designated Tracker, finds it with a **Downloader** (Suwayomi), adds it to the downloader's library and downloads all chapters.

v1 ships exactly one adapter per port: `komga`, `mangabaka`, `suwayomi`. The ports are shaped so that Kavita (reader), AniList / MyAnimeList (trackers) and other downloaders can be added later as new adapter packages without changing the sync logic.

Non-goals (v1): two-way sync (Tracker → Reader), syncing ratings/notes/dates, multi-user, a UI, implementing any adapter beyond the three above.

## Architecture: ports and adapters

```
              ┌──────────────── core (types + ports) ────────────────┐
 Reader ──▶   │  sync/progress  ──▶  Tracker(s)                      │
 (komga)      │  sync/wtr       ◀──  WTR Tracker  ──▶  Downloader    │
              └──────────────────────────────────────────────────────┘
                     ▲ match (pure)        ▲ store (SQLite)
```

- `internal/core` holds provider-neutral types and port interfaces. It imports nothing from the project.
- `internal/sync/progress` and `internal/sync/wtr` contain all business rules and depend only on `core`, `match` and `store`.
- Each adapter lives in `internal/adapters/<port>/<name>` and implements a port. Adapters never import each other or the sync packages.
- `internal/adapters/registry` maps adapter names to constructors that read their own env vars. Adding an adapter = new package + one registry line.

### Core types

```go
// IDKind names an external database a series can be identified by.
type IDKind string // "mangabaka", "anilist", "mal", "mangaupdates", "kitsu", "animeplanet", "ann"

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
    Unit           Unit
    BooksTotal     int
    BooksRead      int
    LastReadNumber float64 // last continuously-read chapter/volume number
    MaxNumber      float64 // highest chapter/volume number present
}

type Status string // "planning" | "reading" | "completed" | "paused" | "dropped" | "rereading" | "unknown"

type Entry struct {
    Status  Status
    Chapter *float64
    Volume  *float64
}

type EntryUpdate struct { // nil fields are left unchanged
    Status  *Status
    Chapter *float64
    Volume  *float64
}
```

### Ports

```go
type Reader interface {
    Name() string
    ListStartedSeries(ctx context.Context) ([]Series, error) // series with any read progress
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
    GetEntry(ctx context.Context, id string) (*Entry, error) // nil, nil if not in the user's list
    SaveEntry(ctx context.Context, id string, u EntryUpdate) error // create or partial update
}

// Optional Tracker capability, required for the tracker named in WTR_TRACKER.
type PlanningLister interface {
    ListPlanning(ctx context.Context) ([]Series, error) // Ref = tracker ID, with titles + IDs filled
}

type Candidate struct {
    Ref        string // downloader's own manga ID
    SourceName string
    Title      string
    Score      float64
}

type Downloader interface {
    Name() string
    // Find searches the downloader for s. best is nil when nothing passes the threshold;
    // nearMisses are the closest rejected candidates, for logging.
    Find(ctx context.Context, s Series) (best *Candidate, nearMisses []Candidate, err error)
    // Acquire adds the candidate to the downloader's library and queues every
    // not-yet-downloaded chapter. Must be idempotent.
    Acquire(ctx context.Context, c Candidate) error
}
```

Status mapping lives in each tracker adapter (e.g. MangaBaka `plan_to_read`/`considering` → `planning`; AniList `CURRENT` → `reading`, `REPEATING` → `rereading`; MAL `on_hold` → `paused`). Values an adapter doesn't recognize map to `unknown`.

## Runtime

Single Go binary, Kubernetes Deployment (1 replica), three loops:

1. **Watcher** — only if the reader implements `ProgressWatcher`. Each emitted series ref is debounced (`SSE_DEBOUNCE`, default 10s) then queued for progress sync. On channel close: reconnect with backoff (1s → 60s cap).
2. **Reconcile** (`RECONCILE_INTERVAL`, default 1h, plus once at startup) — queues every series from `ListStartedSeries`.
3. **WTR poller** (`WTR_INTERVAL`, default 15m, plus once at startup) — only if `WTR_TRACKER` and `DOWNLOADER` are set.

Progress sync runs on one worker goroutine fed by a deduplicating queue, so watcher and reconcile never sync the same series concurrently.

## Progress sync (Reader → each Tracker)

For one reader series, for each tracker in `TRACKERS` (failures in one tracker don't affect the others):

1. **Resolve** the tracker ID: cached in store → else `tracker.Resolve(series)`. Not found → store `unmatched`, retry at most once per 24h.
2. **Target** from `ReadProgress`:
   - `BooksRead == BooksTotal > 0` → `completed`, progress = `MaxNumber`.
   - `BooksRead > 0` → `reading`, progress = `LastReadNumber`.
   - else → no action.
   - Progress goes in `Chapter` or `Volume` per `ReadProgress.Unit`.
3. **Decide** against the current entry (`GetEntry`):
   - Protected: if current status is `paused`, `dropped`, `completed`, `rereading` or `unknown` → do nothing.
   - Status may move: none/`planning` → `reading`/`completed`; `reading` → `completed`. Never backwards.
   - Never lower progress: only send progress if target > current (or current is nil).
   - Nothing to change → no write.
4. **Write** with `SaveEntry` (only changed fields). Record last pushed status/progress in store.

The decision function (step 3) is a pure function `Decide(current *Entry, target Target) *EntryUpdate`, independent of any adapter.

## WTR flow (WTR Tracker → Downloader)

For each series from `ListPlanning` that isn't `done` and isn't in `not_found` backoff:

1. `downloader.Find(series)`.
2. `best == nil` → store `not_found` with `retry_after` backoff 1d → 3d → 7d → 14d → 30d (cap); log up to 3 near misses.
3. Otherwise `downloader.Acquire(best)` → store `done` with candidate ref + source. On error store `in_progress` and retry next poll (Acquire is idempotent).

Series that leave planning are ignored; `done` records stay.

## Matching (`internal/match`, shared by adapters)

- `Normalize`: lowercase, Unicode NFKC, strip punctuation/brackets, collapse whitespace, drop leading "the"/"a".
- `Similarity`: 1 − Levenshtein/maxLen on normalized strings; exact match = 1.0.
- `Best(titles []string, candidates) (best, nearMisses)`: max similarity of each candidate against all titles; accept if ≥ `MATCH_THRESHOLD` (default 0.9).
- `ParseLink(url) (IDKind, id, ok)`: recognizes URLs for mangabaka.org, anilist.co, myanimelist.net, mangaupdates.com, kitsu.app/kitsu.io, anime-planet.com, animenewsnetwork.com.

## State (SQLite at `DB_PATH`)

- `series_map(reader, reader_ref, tracker, tracker_id NULL, status [matched|unmatched], last_attempt, last_status, last_progress, PK(reader, reader_ref, tracker))`
- `wtr(tracker, tracker_id, downloader, status [done|not_found|in_progress], candidate_ref NULL, source NULL, attempts, retry_after, updated_at, PK(tracker, tracker_id, downloader))`

Keys include the adapter names, so switching or adding adapters never collides with old rows. Losing the DB is safe: everything is re-derivable.

## Error handling

- Shared `internal/httpx`: client with per-adapter rate limiter and retries on 429/5xx/network errors (exponential backoff + jitter, honors `Retry-After`, max 5 attempts). Every adapter uses it.
- One series/tracker failing is logged and skipped.
- Config errors (unknown adapter name, missing env, unknown Suwayomi source, `WTR_TRACKER` not in a tracker that implements `PlanningLister`) → fail fast at startup.
- `DRY_RUN=true`: sync logic computes everything, logs intended `SaveEntry`/`Acquire` calls with payloads, and skips them. Store records nothing as `done`. Implemented once in the sync layer, not per adapter.

## Configuration

Core:

| Var | Default | Notes |
|---|---|---|
| `READER` | `komga` | adapter name |
| `TRACKERS` | `mangabaka` | comma-separated adapter names |
| `WTR_TRACKER` | empty | tracker to read planning list from; empty disables WTR |
| `DOWNLOADER` | empty | adapter name; empty disables WTR |
| `WTR_INTERVAL` | `15m` | |
| `RECONCILE_INTERVAL` | `1h` | |
| `SSE_DEBOUNCE` | `10s` | |
| `MATCH_THRESHOLD` | `0.9` | |
| `DRY_RUN` | `false` | |
| `DB_PATH` | `/data/mangasync.db` | |
| `LOG_LEVEL` | `info` | |
| `HTTP_ADDR` | `:8080` | `/healthz` |

Adapter-specific vars are prefixed with the adapter name and only read when that adapter is selected (see each adapter below).

## v1 adapters

### Reader `komga` (implements `Reader`, `ProgressWatcher`)

Env: `KOMGA_URL`, `KOMGA_API_KEY`, optional `KOMGA_USER`/`KOMGA_PASS` (SSE fallback), `KOMGA_VOLUME_LIBRARIES` (comma-separated library IDs whose books are volumes).

- Auth header `X-API-Key`.
- `ListStartedSeries`: `POST /api/v1/series/list?unpaged=true` with condition `anyOf readStatus is IN_PROGRESS / READ`.
- `GetSeries`: `GET /api/v1/series/{id}` → title, `metadata.alternateTitles`, `libraryId`; `IDs` from `metadata.links[].url` via `match.ParseLink` (Komf writes `MangaBaka`, `AniList`, `MangaUpdates`, `MyAnimeList`, `Kitsu`, `AnimePlanet`, `AnimeNewsNetwork` links).
- `GetProgress`: `GET /api/v2/series/{id}/read-progress/tachiyomi` → `lastReadContinuousNumberSort`, `maxNumberSort`, `booksCount`, `booksReadCount`. Unit = volume if library in `KOMGA_VOLUME_LIBRARIES`, else chapter.
- `WatchProgress`: SSE `GET /sse/v1/events`; emit `seriesId` from `ReadProgressSeriesChanged` / `ReadProgressSeriesDeleted`. Events are delivered only to the owning user. If the API key is rejected on SSE, log in with basic auth and use the `X-Auth-Token` session.

### Tracker `mangabaka` (implements `Tracker`, `PlanningLister`)

Env: `MANGABAKA_TOKEN` (Personal Access Token, `mb-…`).

- Base `https://api.mangabaka.org`, header `x-api-key`, responses `{status, data, pagination?}`.
- `Resolve`: `IDs["mangabaka"]` → done; else first of anilist / mangaupdates / mal / kitsu / animeplanet → `GET /v1/source/{anilist|manga-updates|my-anime-list|kitsu|anime-planet}/{id}`; else `GET /v1/series/search?q=` + `match.Best`. Follow `merged_with` on merged series.
- `GetEntry`: `GET /v1/my/library/{id}` (404 → nil). `SaveEntry`: `PATCH` (or `POST` if absent) with `state`, `progress_chapter`, `progress_volume`.
- `ListPlanning`: `GET /v1/my/library?state=plan_to_read&limit=100` (paged) + series titles/alt titles/`source` IDs (`/v1/series/batch` if not embedded).
- Status map: `plan_to_read`, `considering` → planning; `reading` → reading; `completed` → completed; `paused`, `on_hold` → paused; `dropped` → dropped; `rereading` → rereading; else unknown. Reverse: planning → `plan_to_read`.
- Rate limits: search 25/min, others 150/min (API limits are 30 / 180 per IP).

### Downloader `suwayomi` (implements `Downloader`)

Env: `SUWAYOMI_URL`, `SUWAYOMI_AUTH` (`none` | `basic` | `ui_login`), `SUWAYOMI_USER`, `SUWAYOMI_PASS`, `SUWAYOMI_SOURCES` (ordered, comma-separated source display names like `MangaDex (EN)` or numeric IDs).

- GraphQL at `/api/graphql`, typed structs over `net/http`. `ui_login`: `login` mutation → Bearer access token (~5 min), refreshed via `refreshToken` mutation.
- Startup: resolve `SUWAYOMI_SOURCES` names → IDs via `sources` query; fail on unknown names.
- `Find`: for each source in order, `fetchSourceManga(type: SEARCH, query: title, page: 1)`; score with `match.Best` against title + alt titles; first source with an accepted candidate wins. If nothing matches on the main title, repeat once with the first English alt title.
- `Acquire`: `updateManga(patch:{inLibrary:true})` if not in library → `fetchChapters` → `enqueueChapterDownloads` for chapters with `isDownloaded == false`.

## Future adapters (shape check only, not built in v1)

Expected shapes, to confirm when built: Kavita (REST + JWT/API key, SignalR hub for live events → `ProgressWatcher`), AniList (GraphQL, OAuth token, `MediaList` status/progress/progressVolumes, IDs via `idMal`), MyAnimeList (REST v2, OAuth2 PKCE, `num_chapters_read`/`num_volumes_read`). None of them require changes to the ports above.

## Package layout

| Package | Responsibility |
|---|---|
| `cmd/mangasync` | Config, wiring, loops, `/healthz` |
| `internal/core` | Types + port interfaces |
| `internal/core/coretest` | Reusable contract tests + in-memory fake adapters |
| `internal/match` | Normalization, similarity, link parsing |
| `internal/sync/progress` | Resolve → target → `Decide` → write |
| `internal/sync/wtr` | Planning → Find → Acquire with backoff |
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

- **Pure units** (table-driven): `match`, `progress.Decide`, WTR backoff schedule.
- **Sync logic** with in-memory fakes from `coretest`: multi-tracker fan-out with one tracker failing, protected statuses, never-lower progress, WTR idempotent re-run, partial failure, not-found backoff, dry run.
- **Adapter tests**: `httptest` servers with JSON fixtures shaped like real responses; each adapter also runs the `coretest` contract suite for its port, which future adapters reuse.
- **Manual smoke**: `DRY_RUN=true` against the real instances before enabling writes.

## Open items to verify early in implementation

1. Komga SSE accepts `X-API-Key` (else session fallback).
2. Shape of MangaBaka `/v1/my/library` list items (where `series_id` / series object sit).
3. Whether MangaBaka progress fields accept decimals (else floor).

# MangaSync — Design

Date: 2026-10-05
Status: Approved for planning

## Goal

A small always-on service on Kubernetes that:

1. **Read sync:** pushes read history from Komga to the user's MangaBaka library (status + progress), one-way.
2. **WTR → download:** when a series is marked `plan_to_read` in MangaBaka, finds it on Suwayomi via a priority list of sources, adds it to the Suwayomi library and enqueues all chapters for download.

Non-goals: two-way sync (MangaBaka → Komga), syncing ratings/notes/dates, multi-user support, a UI.

## External APIs (verified facts)

### Komga (`/api/v1`, `/api/v2`)
- Auth: `X-API-Key` header (user API key).
- List series: `POST /api/v1/series/list` with condition body, e.g. `{"condition":{"readStatus":{"operator":"is","value":"IN_PROGRESS"}}}`. ReadStatus: `UNREAD | READ | IN_PROGRESS`. `SeriesDto` has `libraryId`, `booksCount`, `booksReadCount`, `metadata.title`, `metadata.links[] {label,url}`.
- Progress shortcut: `GET /api/v2/series/{id}/read-progress/tachiyomi` → `lastReadContinuousNumberSort`, `maxNumberSort`, `booksCount`, `booksReadCount`.
- Live events: SSE at `GET /sse/v1/events` (not in OpenAPI). Relevant: `ReadProgressSeriesChanged {seriesId,userId}`, `ReadProgressSeriesDeleted`. Read-progress events are only delivered to the owning user.
- Links written by Komf's MangaBaka provider use labels `MangaBaka` (URL `https://mangabaka.org/manga/{id}/{slug}`), `AniList`, `MangaUpdates`, `MyAnimeList`, `Kitsu`, `AnimePlanet`, `AnimeNewsNetwork`.

### MangaBaka (`https://api.mangabaka.org`)
- Responses wrapped as `{status, data, pagination?, message?}`. OpenAPI: `https://mangabaka.org/api.json`.
- Auth: Personal Access Token (`mb-…`) in `x-api-key` header.
- `GET /v1/my/library?state=plan_to_read&page=&limit=100` — library filtered by library state (`state`, not `status`).
- `GET|POST|PATCH /v1/my/library/{series_id}` — writable: `state`, `progress_chapter`, `progress_volume` (others untouched by us).
- States: `considering, completed, dropped, paused, plan_to_read, reading, rereading`. Unknown states must be tolerated (`on_hold`, `planned` reported by third parties).
- `GET /v1/series/{id}` (title + alt titles), `GET /v1/series/search?q=`, reverse lookup `GET /v1/source/{anilist|manga-updates|my-anime-list|kitsu|anime-planet}/{id}`.
- Series may have `state: merged` with `merged_with` → follow and update stored mapping.
- Rate limits (per IP): search 30/min, other GET 180/min, `/my/*` 180/min. 429 on exceed.

### Suwayomi-Server (GraphQL at `/api/graphql`)
- Auth modes: `none`, `basic_auth`, `ui_login` (mutation `login` → `{accessToken, refreshToken}`, Bearer token, ~5 min expiry, refresh via `refreshToken` mutation).
- `sources` query (resolve names → IDs), `fetchSourceManga(input:{source, type: SEARCH, page, query})` → `{mangas, hasNextPage}`. No global search; loop sources.
- `updateManga(input:{id, patch:{inLibrary:true}})`, `fetchChapters(input:{mangaId})`, `enqueueChapterDownloads(input:{ids})`.

## Architecture

Single Go binary running as a Kubernetes Deployment (1 replica) with three concurrent loops:

1. **Komga listener** — subscribes to Komga SSE. On `ReadProgressSeriesChanged`, debounces per series (default 10s) then runs progress sync for that series. Reconnects with exponential backoff.
2. **Reconcile timer** (`RECONCILE_INTERVAL`, default 1h, also once at startup) — runs progress sync over every Komga series with readStatus `IN_PROGRESS` or `READ`. Same code path as the listener.
3. **WTR poller** (`WTR_INTERVAL`, default 15m, also once at startup) — lists MangaBaka `plan_to_read` entries and runs the WTR flow for each one not yet handled (or whose retry time has passed).

Progress sync calls are serialized through one worker so the listener and reconcile never write the same series concurrently.

### Packages

| Package | Responsibility |
|---|---|
| `internal/komga` | REST client (list series, tachiyomi progress, series detail) + SSE subscriber |
| `internal/mangabaka` | REST client (library list/get/patch, series get/search, source lookup), rate limiter |
| `internal/suwayomi` | GraphQL client over `net/http` with typed structs; auth incl. token refresh |
| `internal/match` | Pure functions: Komga link parsing, title normalization, similarity, best-candidate selection |
| `internal/progress` | Komga series → MangaBaka entry decision + write |
| `internal/wtr` | MangaBaka WTR entry → Suwayomi flow |
| `internal/store` | SQLite state (`modernc.org/sqlite`, no CGO) |
| `cmd/mangasync` | Config, wiring, goroutines, `/healthz` |

`progress` and `wtr` depend on small interfaces (defined in those packages), not on concrete clients, so they can be tested with fakes.

## Progress sync (Komga → MangaBaka)

### Resolving the MangaBaka series ID (cached in store)
1. `metadata.links` entry labeled `MangaBaka` → parse ID from URL.
2. Else an AniList / MangaUpdates / MyAnimeList / Kitsu / AnimePlanet link → parse ID → `/v1/source/{source}/{id}`.
3. Else title search (`/v1/series/search?q=<komga title>`) → accept only if a result's title or alt title passes `MATCH_THRESHOLD`.
4. Else record `unmatched`; retry at most once per 24h.

If a resolved series is `merged`, follow `merged_with` and update the cache.

### Computing the target
- Unit: `progress_volume` if the series' Komga library ID is in `KOMGA_VOLUME_LIBRARIES`, otherwise `progress_chapter`.
- `booksReadCount == booksCount` (and > 0) → target state `completed`, progress = `maxNumberSort`.
- `booksReadCount > 0` → target state `reading`, progress = `lastReadContinuousNumberSort`.
- Nothing read → no action.
- Progress is sent as-is if MangaBaka accepts decimals (e.g. 10.5); if it only accepts integers, it is floored (see open item 3).

### Write rules
- Fetch the current MangaBaka entry (404 → none).
- **Never lower progress:** if current progress ≥ target progress, don't change progress.
- **State transitions allowed:** none/`plan_to_read`/`considering`/`reading` → `reading` or `completed`; `reading` → `completed`. Entries in `dropped`, `paused`, `completed`, `rereading`, or any unknown state are left completely untouched (neither state nor progress).
- If nothing changes, no write. Otherwise `PATCH` (or `POST` when no entry exists) with only `state` and/or the progress field.
- Record last pushed state/progress in store for logging and skipping no-op API calls.

## WTR flow (MangaBaka → Suwayomi)

For each `plan_to_read` entry not marked `done` (and not in `not_found` backoff):

1. Get series title + alt titles from MangaBaka.
2. For each source in `SUWAYOMI_SOURCES` order: `fetchSourceManga(SEARCH, query=title)` (page 1). Pick the best candidate whose normalized title matches the main title or any alt title with similarity ≥ `MATCH_THRESHOLD`. First source with a match wins. If the main title yields nothing on all sources, retry with the English alt title, if any.
3. No match → store `not_found` with `retry_after` backoff 1d → 3d → 7d → 14d → 30d (cap). Log the top 3 near-misses per source.
4. Match → `updateManga(inLibrary:true)` (skip if already in library) → `fetchChapters` → `enqueueChapterDownloads` for all chapters where `isDownloaded == false`.
5. Store `done` with the Suwayomi manga ID and source. Partial failure stores the step reached; next poll resumes (all steps idempotent).

Entries leaving `plan_to_read` are not acted upon; `done` records stay.

## Matching

- Normalize: lowercase, Unicode NFKC, strip punctuation/brackets, collapse whitespace, drop leading articles ("the", "a").
- Similarity: normalized Levenshtein ratio (1 − distance/maxLen). Exact normalized match = 1.0.
- `MATCH_THRESHOLD` default 0.9. Ties broken by source priority, then highest similarity.

## State (SQLite at `DB_PATH`)

- `series_map(komga_series_id PK, mangabaka_id NULL, status [matched|unmatched], last_attempt, last_state, last_progress)`
- `wtr(mangabaka_id PK, status [done|not_found|in_progress], step, suwayomi_manga_id NULL, source_id NULL, attempts, retry_after, updated_at)`

Losing the DB is safe: everything is re-derivable (with extra API calls).

## Error handling

- Each client: rate limiter (MangaBaka search 25/min, other 150/min to stay under limits), retries on 429/5xx/network errors with exponential backoff + jitter, honors `Retry-After`. Max 5 attempts.
- One series failing is logged and skipped; the loop continues.
- Config errors (missing env, unknown Suwayomi source name) → fail fast at startup with a clear message.
- SSE: reconnect with backoff (1s → 60s cap). If SSE rejects API-key auth (unverified), fall back to a session: log in with basic auth via `KOMGA_USER`/`KOMGA_PASS`, use the `X-Auth-Token` session. Reconcile covers any gap regardless.
- `DRY_RUN=true`: all writes to MangaBaka and Suwayomi are logged with their payload and skipped; store records nothing as `done`.

## Configuration

| Var | Default | Notes |
|---|---|---|
| `KOMGA_URL` | — | required |
| `KOMGA_API_KEY` | — | required (Secret) |
| `KOMGA_USER`, `KOMGA_PASS` | — | optional, SSE fallback only |
| `KOMGA_VOLUME_LIBRARIES` | empty | comma-separated library IDs |
| `MANGABAKA_TOKEN` | — | required (Secret) |
| `SUWAYOMI_URL` | — | required |
| `SUWAYOMI_AUTH` | `none` | `none` \| `basic` \| `ui_login` |
| `SUWAYOMI_USER`, `SUWAYOMI_PASS` | — | required unless `none` (Secret) |
| `SUWAYOMI_SOURCES` | — | required, ordered, comma-separated display names (e.g. `MangaDex (EN)`) or numeric IDs |
| `WTR_INTERVAL` | `15m` | |
| `RECONCILE_INTERVAL` | `1h` | |
| `SSE_DEBOUNCE` | `10s` | |
| `MATCH_THRESHOLD` | `0.9` | |
| `DRY_RUN` | `false` | |
| `DB_PATH` | `/data/mangasync.db` | |
| `LOG_LEVEL` | `info` | |
| `HTTP_ADDR` | `:8080` | `/healthz` |

## Deployment

- Multi-stage `Dockerfile` → `gcr.io/distroless/static:nonroot`, `CGO_ENABLED=0`.
- `deploy/` Kustomize base: Deployment (replicas 1, `strategy: Recreate`, non-root, read-only root FS, `/data` from PVC), PVC (100Mi, RWO), ConfigMap, Secret (example with placeholders only), liveness/readiness on `/healthz`. Resource requests ~10m CPU / 32Mi memory.
- Logs: JSON via `log/slog`, with `series`, `action`, `dry_run` fields.

## Testing

- Unit (table-driven): `match` (normalization, similarity, link parsing), `progress` decision rules (never lower, protected states, completed vs reading, volume libraries), `wtr` flow with fakes (idempotent re-run, partial failure resume, backoff schedule, no-match path).
- Client tests: `httptest` servers with JSON fixtures shaped like real responses; Suwayomi token refresh; MangaBaka 429 retry.
- Manual smoke: run with `DRY_RUN=true` against the real instances before enabling writes.

## Open items to verify early in implementation

1. Komga SSE accepts `X-API-Key` (else use the session fallback).
2. Shape of MangaBaka `/v1/my/library` list items (where `series_id` / series object sit).
3. Whether MangaBaka progress fields accept decimals (e.g. chapter 10.5).

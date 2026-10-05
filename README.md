# MangaSync

MangaSync keeps three self-hosted / online manga services in step:

- **Komga → MangaBaka (read sync).** What you read in [Komga](https://komga.org) is written to your
  [MangaBaka](https://mangabaka.org) library: status, chapter (or volume) progress, and start/finish dates.
- **MangaBaka → Suwayomi (download sync).** When you mark a series *Plan to read* (or *Reading*) on
  MangaBaka and you don't have it yet, MangaSync finds it on one of your
  [Suwayomi](https://github.com/Suwayomi/Suwayomi-Server) sources, adds it to the Suwayomi library and
  downloads every chapter. Mark it *Dropped* and it is removed from the Suwayomi library again (files stay).

It is a single small Go binary (~15 MB distroless image, ~20 MB RAM) meant to run as one pod next to
Komga and Suwayomi. It reacts to Komga read events live, re-checks everything hourly, and polls
MangaBaka every 15 minutes.

---

## Contents

- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Getting credentials](#getting-credentials)
- [Configuration reference](#configuration-reference)
- [Deploying to Kubernetes](#deploying-to-kubernetes)
- [Running with Docker](#running-with-docker)
- [First run: dry run, then go live](#first-run-dry-run-then-go-live)
- [Operations and troubleshooting](#operations-and-troubleshooting)
- [Container image](#container-image)
- [Development](#development)

---

## How it works

### Read sync (Komga → MangaBaka)

MangaSync looks at each Komga series and decides what its MangaBaka entry should be:

| What you did in Komga | MangaBaka status | Progress | Dates |
|---|---|---|---|
| Nothing read or opened yet | *Plan to read* — **only if the series is not in your MangaBaka list yet, and only once** | – | – |
| Opened the first chapter, not finished | *Reading* | – | start date |
| Read some chapters | *Reading* | last chapter read without gaps from chapter 1 | start date |
| Read everything, series still publishing | *Reading* | highest chapter | start date |
| Read everything, series finished publishing | *Completed* | highest chapter | start + finish date |

Rules that protect what you set yourself on MangaBaka:

- **Progress never goes down.** If MangaBaka already says chapter 120 and Komga says 100, nothing changes.
- **Status only moves forward:** (none) / *Considering* / *Plan to read* → *Reading* → *Completed*.
- **Hands off:** entries you set to *Completed*, *Paused*, *Dropped* or *Rereading* are never touched.
- **Dates are only filled in when empty.** A date you set on MangaBaka is never overwritten.
- **Plan to read is added once.** Existing entries are never changed by it, and if you delete an
  auto-added *Plan to read* entry, it is not re-added.
- "Finished publishing" comes from MangaBaka (Komga's own series status is often wrong).
- Dates are calendar dates in the `TZ` time zone, taken from Komga's read timestamps. Note that Komga
  stores when a chapter was *marked* read, so bulk-marking chapters moves those dates.

**Matching a Komga series to MangaBaka.** In order:
1. A MangaBaka link in the Komga series metadata.
2. An AniList, MangaUpdates, MyAnimeList, Kitsu or Anime-Planet link (e.g. written by
   [Komf](https://github.com/Snd-R/komf)), looked up on MangaBaka.
3. A title search. Titles are compared after normalising punctuation, accents and spacing
   (`Naruto_ Sasuke's Story` matches `Naruto: Sasuke’s Story`), numbers must match
   (`Kaiju No. 8` never matches `Kaiju No. 9`), and colour-edition suffixes are ignored
   (`Naruto (Color)` matches `Naruto`).

A series without a match is logged as a warning and retried once a day. The fastest fix is adding a
MangaBaka or AniList link to the series in Komga.

### Download sync (MangaBaka → Suwayomi)

| MangaBaka status | Suwayomi action (defaults) |
|---|---|
| *Plan to read*, *Reading*, *Rereading* | **Acquire**: add to the Suwayomi library and download all chapters — unless you already have it |
| *Dropped* | **Release**: remove from the Suwayomi library; downloaded files are kept |
| *Considering*, *Paused*, *Completed* | nothing |

"Already have it" means: it is in the Suwayomi library, **or** it is already in Komga (so series you
added to Komga by hand are never downloaded twice).

Finding a series on Suwayomi: your sources are searched in the priority order you configure
(`SUWAYOMI_SOURCES`), first with the main title, then with up to two Latin-script alternative titles.
The first source with a confident title match wins. If nothing matches, MangaSync waits 1 day, then
3, 7, 14 and 30 days before searching again (the closest near-misses are logged).

MangaSync remembers what it acquired and released in a small SQLite database, so a series you remove
from Suwayomi by hand is not added back, and moving a dropped series back to *Plan to read* acquires
it again.

---

## Requirements

- **Komga** with API key support (recent 1.x). MangaSync uses the REST API and the live event stream (`/sse/v1/events`).
- **A MangaBaka account** with a personal access token.
- **Suwayomi-Server** v2.x (GraphQL API) with at least one source extension installed — only needed
  for download sync.
- Network access from the pod to Komga, Suwayomi and `https://api.mangabaka.org`.
- A small persistent volume (100 Mi is plenty) for the SQLite state.

Komga and Suwayomi typically share a library folder (Suwayomi downloads into the folder Komga scans),
so downloaded chapters show up in Komga automatically. MangaSync does not move files.

---

## Getting credentials

**Komga API key.** Log in to Komga *as the user whose reading you want to sync* → your account
settings → *API Keys* → create a key. Read events are only sent to the user that owns the key, so it
must be your own user, not a separate service account.

**MangaBaka token.** On mangabaka.org, create a *personal access token* in your account settings. It
starts with `mb-`. MangaSync uses it to read your library and write library entries.

**Suwayomi.** Nothing is needed if Suwayomi has no authentication (`SUWAYOMI_AUTH=none`). With basic
auth or the web-UI login enabled, use that username and password.

**Suwayomi source names.** `SUWAYOMI_SOURCES` takes the source *display names* exactly as Suwayomi
shows them (e.g. `MangaDex (EN)`, `Weeb Central (EN)`) or their numeric IDs. List them with:

```bash
curl -s -H 'Content-Type: application/json' -d '{"query":"{ sources { nodes { id displayName } } }"}' \
  https://suwayomi.example.com/api/graphql | jq -r '.data.sources.nodes[] | "\(.id)  \(.displayName)"'
```

MangaSync refuses to start if a configured source is not installed, and prints the installed ones.

---

## Configuration reference

All configuration is through environment variables. Secrets are marked 🔒 — put those in a
Kubernetes Secret.

### General

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `false` | `true` = read everything and log every change it *would* make (`"dry_run":true`), but write nothing to MangaBaka or Suwayomi and record nothing. Use this for the first run. |
| `TZ` | `UTC` | Time zone for start/finish dates, e.g. `Europe/Amsterdam`. Time zone data is built into the image. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. `debug` also logs why things were skipped. Logs are JSON on stdout. |
| `DB_PATH` | `/data/mangasync.db` | SQLite state file. Put `/data` on a persistent volume. |
| `HTTP_ADDR` | `:8080` | Listen address for the health endpoint `GET /healthz`. |

### Services and sync behaviour

| Variable | Default | Description |
|---|---|---|
| `READER` | `komga` | Where you read. Only `komga` exists today. |
| `TRACKERS` | `mangabaka` | Comma-separated list of trackers to push reading progress to. Only `mangabaka` exists today. |
| `DOWNLOAD_TRACKER` | *(empty)* | Tracker whose library statuses drive downloads, e.g. `mangabaka`. Empty disables download sync. Must be set together with `DOWNLOADER`. |
| `DOWNLOADER` | *(empty)* | Download service, e.g. `suwayomi`. Empty disables download sync. |
| `ACQUIRE_STATUSES` | `planning,reading,rereading` | Statuses that make MangaSync acquire a series you don't have. Set to an empty value to never download. |
| `RELEASE_STATUSES` | `dropped` | Statuses that remove a series from the downloader library (files kept). Set to an empty value to never remove anything. |
| `RECONCILE_INTERVAL` | `1h` | How often every Komga series is re-checked (catches anything missed while live events were down). Go duration syntax: `30m`, `2h`. |
| `DOWNLOAD_INTERVAL` | `15m` | How often the MangaBaka library is checked for series to acquire or release. |
| `SSE_DEBOUNCE` | `10s` | After a Komga read event, wait this long for more events of the same series before syncing (avoids one update per page). |
| `MATCH_THRESHOLD` | `0.9` | How similar titles must be (0–1] to count as a match. Lower finds more but risks wrong matches. |

Status names for `ACQUIRE_STATUSES` / `RELEASE_STATUSES`: `considering`, `planning` (= Plan to read),
`reading`, `completed`, `paused`, `dropped`, `rereading`. A status may not be in both lists.

### Komga

| Variable | Default | Description |
|---|---|---|
| `KOMGA_URL` | **required** | Base URL, e.g. `http://komga.media.svc.cluster.local:25600` or `https://komga.example.com`. |
| `KOMGA_API_KEY` 🔒 | **required** | API key of *your* Komga user (see [Getting credentials](#getting-credentials)). |
| `KOMGA_VOLUME_LIBRARIES` | *(empty)* | Comma-separated Komga library IDs whose books are *volumes* instead of chapters. Progress for those is written as volume progress. Find a library ID in the Komga URL when you open the library. |

### MangaBaka

| Variable | Default | Description |
|---|---|---|
| `MANGABAKA_TOKEN` 🔒 | **required** | Personal access token (`mb-…`). |
| `MANGABAKA_URL` | `https://api.mangabaka.org` | API base URL. Only change for testing. |

MangaSync stays below MangaBaka's rate limits on its own (25 searches and 150 other requests per minute).

### Suwayomi (only with `DOWNLOADER=suwayomi`)

| Variable | Default | Description |
|---|---|---|
| `SUWAYOMI_URL` | **required** | Base URL, e.g. `http://suwayomi.media.svc.cluster.local:4567`. |
| `SUWAYOMI_SOURCES` | **required** | Comma-separated sources in **priority order** (display names or IDs), e.g. `Weeb Central (EN),MangaDex (EN),MangaFire (EN)`. The first source with a confident match wins. |
| `SUWAYOMI_AUTH` | `none` | `none`, `basic` (HTTP basic auth) or `ui_login` (Suwayomi's login with tokens, refreshed automatically). |
| `SUWAYOMI_USER` 🔒 | – | Username, required for `basic` and `ui_login`. |
| `SUWAYOMI_PASS` 🔒 | – | Password, required for `basic` and `ui_login`. |

Sources behind Cloudflare only work if Suwayomi has a Cloudflare bypass (e.g. FlareSolverr) configured;
otherwise put them last or leave them out.

MangaSync checks its configuration at startup and exits with a clear message on anything missing or
invalid (unknown status, unknown source, threshold out of range, …).

---

## Deploying to Kubernetes

MangaSync has no Helm chart of its own; use your own chart or the generic
[bjw-s app-template](https://bjw-s-labs.github.io/helm-charts/docs/app-template/). Whatever you use,
the workload needs:

| Requirement | Why |
|---|---|
| **Exactly 1 replica, `Recreate` strategy** | One SQLite writer, one Komga event stream. Two pods would fight over the database. |
| **Persistent volume at `/data`** (RWO, 100 Mi) | Series matches and download records. Losing it is safe but causes one round of re-matching. Avoid NFS-backed storage for SQLite. |
| **`emptyDir` at `/tmp`** | Scratch space when the root filesystem is read-only. |
| Run as UID/GID `65532`, `fsGroup: 65532` | The image is distroless `nonroot`. |
| Probes on `GET /healthz` port `8080` | Startup can take a minute while Suwayomi sources are checked; give the startup probe room. |
| No Service/Ingress needed | MangaSync only makes outgoing requests. A Service is only useful for scraping `/healthz`. |

### 1. Create the secret

```bash
kubectl create namespace media   # or use your existing namespace
kubectl -n media create secret generic mangasync \
  --from-literal=KOMGA_API_KEY='<komga api key>' \
  --from-literal=MANGABAKA_TOKEN='mb-<token>'
# add --from-literal=SUWAYOMI_USER=... --from-literal=SUWAYOMI_PASS=... if Suwayomi has auth
```

(With GitOps, create the same keys through SOPS, Sealed Secrets or External Secrets instead.)

### 2. Values for bjw-s app-template

Checked with `helm template` against app-template 5.2.1 (renders a PVC and a single-replica `Recreate` Deployment):

```yaml
# values.yaml for oci://ghcr.io/bjw-s-labs/helm/app-template
controllers:
  mangasync:
    strategy: Recreate
    containers:
      app:
        image:
          repository: ghcr.io/<github-user>/mangasync
          tag: latest   # or pin a release tag / sha-xxxxxxx
        env:
          TZ: Europe/Amsterdam
          DRY_RUN: "true"          # start in dry run; set to "false" after checking the logs
          KOMGA_URL: http://komga.media.svc.cluster.local:25600
          DOWNLOAD_TRACKER: mangabaka
          DOWNLOADER: suwayomi
          SUWAYOMI_URL: http://suwayomi.media.svc.cluster.local:4567
          SUWAYOMI_SOURCES: Weeb Central (EN),MangaDex (EN),MangaFire (EN)
        envFrom:
          - secretRef:
              name: mangasync
        probes:
          liveness: &probe
            enabled: true
            custom: true
            spec:
              httpGet:
                path: /healthz
                port: 8080
              periodSeconds: 30
          readiness: *probe
          startup:
            enabled: true
            custom: true
            spec:
              httpGet:
                path: /healthz
                port: 8080
              periodSeconds: 10
              failureThreshold: 30
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop: ["ALL"]
        resources:
          requests:
            cpu: 10m
            memory: 32Mi
          limits:
            memory: 128Mi

defaultPodOptions:
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    fsGroup: 65532
    fsGroupChangePolicy: OnRootMismatch
    seccompProfile:
      type: RuntimeDefault

persistence:
  data:
    type: persistentVolumeClaim
    accessMode: ReadWriteOnce
    size: 100Mi
    globalMounts:
      - path: /data
  tmp:
    type: emptyDir
    globalMounts:
      - path: /tmp
```

```bash
helm install mangasync oci://ghcr.io/bjw-s-labs/helm/app-template -n media -f values.yaml
```

Or the equivalent Flux `HelmRelease` / Argo CD `Application` with the same values.

### 3. Pulling the image

Images are published to `ghcr.io/<github-user>/mangasync` (see [Container image](#container-image)).
If the GitHub package is private, either make it public (GitHub → *Packages* → *mangasync* → *Package
settings* → *Change visibility*) or add an image pull secret:

```bash
kubectl -n media create secret docker-registry ghcr \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<PAT with read:packages>
```

and reference it with `defaultPodOptions.imagePullSecrets: [{name: ghcr}]`.

### Writing your own manifests or chart

Translate the table above: a `Deployment` (replicas 1, `strategy.type: Recreate`), a
`PersistentVolumeClaim` mounted at `/data`, an `emptyDir` at `/tmp`, the pod and container security
contexts from the values above, `/healthz` probes on 8080, and the environment variables from a
ConfigMap/`env` plus the Secret via `envFrom`.

---

## Running with Docker

```bash
docker run -d --name mangasync --restart unless-stopped \
  -v mangasync-data:/data \
  -e TZ=Europe/Amsterdam -e DRY_RUN=true \
  -e KOMGA_URL=http://komga:25600 -e KOMGA_API_KEY=... \
  -e MANGABAKA_TOKEN=mb-... \
  -e DOWNLOAD_TRACKER=mangabaka -e DOWNLOADER=suwayomi \
  -e SUWAYOMI_URL=http://suwayomi:4567 -e "SUWAYOMI_SOURCES=MangaDex (EN)" \
  ghcr.io/<github-user>/mangasync:latest
```

---

## First run: dry run, then go live

1. Deploy with `DRY_RUN=true`.
2. Read the logs (`kubectl -n media logs deploy/mangasync -f`). Useful lines:
   - `matched series` — which MangaBaka entry each Komga series was matched to (check these!).
   - `no tracker match; retrying later` — series that need a link in Komga.
   - `updating tracker entry` with `update` — exactly what would be written, e.g.
     `status=reading chapter=104 start=2026-06-09`.
   - `acquiring` / `releasing` — what would be downloaded / removed, with the chosen source and score.
   - `already in downloader library` — series it recognised as already downloaded.
3. Fix wrong or missing matches by adding a MangaBaka/AniList link to the series in Komga.
4. Set `DRY_RUN=false` and redeploy. Later runs only log changes.

Dry run writes nothing to MangaBaka, Suwayomi or the download records. It does cache which MangaBaka
entry each Komga series matched (so the live run doesn't search again).

---

## Operations and troubleshooting

**Logs.** JSON on stdout. Every line has `series`, and where relevant `tracker`, `tracker_id`,
`downloader`, `candidate`, `source`, `dry_run`. Errors are logged once at `error` level with the
failing step. Set `LOG_LEVEL=debug` to see why something was skipped (`tracker entry up to date`,
`already in reader`, …).

**Health.** `GET /healthz` returns `ok` once the process is running. The process exits non-zero on
invalid configuration, an unreachable Suwayomi at startup, or if the health port can't be bound — let
Kubernetes restart it.

**A series isn't matched.** Add a MangaBaka or AniList link to the series in Komga (Komga → series →
edit → Links), or let Komf do it. MangaSync retries unmatched series once a day; the retry time is
stored in the database, so a restart does not retry sooner. To force it, reset the state (below).

**Wrong progress chapter.** Progress is the last chapter read *without gaps* from chapter 1. A
missing read mark on one chapter in Komga holds progress back.

**Something isn't downloaded.** Check for `not found in downloader` (with `near_misses`) or
`download sync failed` lines. Typical causes: the series isn't on any configured source, the title
differs too much (lower `MATCH_THRESHOLD` slightly or add the right source first in the list), or a
source behind Cloudflare without a bypass.

**Komga live events keep reconnecting.** An ingress or proxy with a short idle timeout cuts the
event stream. Use the in-cluster Komga service URL, or raise the proxy's read timeout. Nothing is
lost either way: the hourly reconcile catches up.

**Resetting state.** Stop the pod and delete `/data/mangasync.db`. MangaSync re-matches everything.
Download records are rebuilt as *already in downloader library*; a series you previously removed from
Suwayomi by hand may be acquired again if it is still *Plan to read*/*Reading*.

**Upgrading.** Pull the new image and restart; the database schema is created/updated automatically.

---

## Container image

GitHub Actions (`.github/workflows/build.yml`) tests every push and pull request, and builds a
multi-arch image (`linux/amd64`, `linux/arm64`) that is pushed to the GitHub Container Registry of the
repository owner:

| Trigger | Tags pushed to `ghcr.io/<owner>/<repo>` |
|---|---|
| Push to the default branch (`master`/`main`) | `latest`, `master` (branch name), `sha-<short sha>` |
| Tag `v1.2.3` | `1.2.3`, `1.2`, `sha-<short sha>` |
| Pull request | built and tested, not pushed |
| Manual (*Run workflow*) | same as a push to that branch |

It uses the built-in `GITHUB_TOKEN` (workflow permission `packages: write`), so there are no secrets
to configure. To cut a release: `git tag v1.0.0 && git push origin v1.0.0`.

Build it yourself:

```bash
docker build -t mangasync .
```

---

## Development

Requirements: Go 1.27+.

```bash
go test -race ./...          # all tests (no network needed)
go vet ./... && gofmt -l .   # what CI checks
```

Run locally against your real services, safely:

```bash
touch .env   # git-ignored; put the variables from the configuration reference in it
set -a; source .env; set +a
DRY_RUN=true DB_PATH=./mangasync.db HTTP_ADDR=:18080 go run ./cmd/mangasync
```

### Architecture

Ports and adapters: the sync rules never talk to a specific service.

```
cmd/mangasync              wiring, loops, health endpoint
internal/core              neutral types and the Reader / Tracker / Downloader interfaces
internal/sync/progress     read sync rules (Komga → trackers)
internal/sync/download     download sync rules (tracker → downloader)
internal/match             title normalisation, similarity, link parsing
internal/store             SQLite state
internal/adapters/...      komga, mangabaka, suwayomi + registry
```

**Adding a service** (e.g. Kavita, AniList, MyAnimeList): implement the matching interface from
`internal/core/core.go` in a new package under `internal/adapters/<reader|tracker|downloader>/<name>`,
run the shared contract test from `internal/core/coretest` in its tests, and add one case to
`internal/adapters/registry`. The design document is in `docs/superpowers/specs/`.

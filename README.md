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
`LOG_LEVEL`, `HTTP_ADDR`, `TZ`.

Start and finish reading dates are copied from Komga to MangaBaka (only when MangaBaka has none yet).
They are calendar dates in the `TZ` time zone (default: the system zone, UTC in the container; the
ConfigMap sets `Europe/Amsterdam`).

Komga: `KOMGA_URL`, `KOMGA_API_KEY`, `KOMGA_VOLUME_LIBRARIES`.
MangaBaka: `MANGABAKA_TOKEN`, `MANGABAKA_URL` (optional).
Suwayomi: `SUWAYOMI_URL`, `SUWAYOMI_AUTH` (`none`/`basic`/`ui_login`), `SUWAYOMI_USER`, `SUWAYOMI_PASS`, `SUWAYOMI_SOURCES` (priority order).

## Adding a service

Implement the port in `internal/core/core.go` in a new package under `internal/adapters/<kind>/<name>`,
run the matching `coretest.*Contract` in its tests, and add a case to `internal/adapters/registry`.

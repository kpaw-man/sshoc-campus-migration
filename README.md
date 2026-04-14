# SSHOC / DARIAH Campus patch pipeline

Three scripts run sequentially to fetch resources from two APIs, match them by title, and update SSHOC training materials with a source reference via PATCH requests.

## Requirements

- Docker
- Docker Compose v2+


Edit `.env` and fill in the required values (see section below).

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `BEARER_TOKEN` | yes | — | Bearer token for SSHOC Marketplace API |
| `SSHOC_BASE_URL` | no | `https://sshoc-marketplace-api.acdh-dev.oeaw.ac.at` | Base URL of the SSHOC API |
| `SOURCE_ID` | no | `253` | Source ID used in fetch and patch |
| `SOURCE_LABEL` | no | `DARIAH-CAMPUS` | Label of the static source object sent in PATCH body |
| `SOURCE_URL` | no | `https://campus.dariah.eu/` | URL of the static source object |
| `SOURCE_URL_TEMPLATE` | no | `https://hdl.handle.net/21.11159/{source-item-id}` | URL template of the static source object |
| `MAX_ITEMS` | no | `0` | Limit number of PATCH requests (0 = no limit) |
| `DATA_DIR` | no | `/data` | Directory inside the container where JSON files are written |

## Running

```bash
docker compose up --build
```

On first run Docker builds the image. Subsequent runs without code changes can skip `--build`.

## What each script does

**Script1__fetch_items_from_source_id.py**
Fetches all items from `GET /api/sources/{SOURCE_ID}/items` using offset pagination. Normalizes each record to: `id`, `category`, `label`, `persistentId`, `lastInfoUpdate`. Deduplicates by id. Writes result to `data/sshoc_source_items.json`.

**Script2__fetch_uuid_from_v1_and_v2.py**
Fetches all resources from DARIAH Campus API v1 and v2 using offset pagination. v1 provides `uuid`, v2 provides `pid` and `title`. Merges both by id. Writes result to `data/dariah_resources.json`.

**Script3__patching_persitant_Ids.py**
Reads both JSON files produced above. For each SSHOC item:
1. Normalizes the `label` field (lowercase, strip, collapse whitespace).
2. Looks up a matching DARIAH record by normalized title.
3. Extracts `sourceItemId` from the DARIAH `pid` handle URL (last path segment).
4. Sends `PATCH /api/training-materials/{persistentId}` with the static source definition and extracted `sourceItemId`.

Stops immediately on any network error or non-2xx HTTP response. Writes a full execution log to `data/patch_execution_log.txt`.

## Testing before a full run

Set `MAX_ITEMS=1` in `.env` (or inline) to process only one record:

```bash
MAX_ITEMS=1 docker compose up --build
```

Check `data/patch_execution_log.txt` to verify the PATCH request and response before running without a limit.

## Notes

- If a SSHOC item has no matching DARIAH record by title, it is skipped and logged as `[SKIP]`.
- If a matched DARIAH record has no `pid`, it is also skipped.
- All skipped and failed records are visible in the log file with a reason.
- The pipeline stops on the first failed PATCH. Already-applied patches are not rolled back.

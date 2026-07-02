# target-cbx1

Singer **target** (destination connector) for syncing CRM data (accounts, contacts) into the CBX1 platform. Built on the Meltano Singer SDK + HotGlue's `target-hotglue` base classes. It is the write side of the HotGlue CRM → CBX1 sync:

```
CRM tap (Salesforce/HubSpot/Marketo)
    → hotglue-transformation-scripts (etl.py)   # maps CRM fields → CBX1 shape
        → target-cbx1 (this repo)
            → CBX1 integration records API
```

End-to-end pipeline documentation lives in the `hotglue-transformation-scripts` repo (`docs/architecture.md`).

## Quickstart

```bash
poetry install
cp .env.example .env        # fill in BASE_URL (+ CONNECTOR_ID)
```

Create a `config.json` (see [Config](#config-configjson)), then feed Singer messages on stdin:

```bash
cat input.singer | poetry run target-cbx1 --config config.json
```

## Branches

| Branch | Purpose |
|---|---|
| `main` | Development |
| `production` | Production deployment |

## Layout

| Path | Role |
|---|---|
| `target_api/target.py` | `TargetApi` — CLI entry point, sink selection, sequential drain + state assembly |
| `target_api/sinks.py` | `RecordSink` (single) / `BatchSink` (default) + UTF-8 sanitization (`ftfy`) |
| `target_api/client.py` | `ApiSink` — endpoint construction, auth headers, retry/validation, cURL-on-error |
| `target_api/auth.py` | `Cbx1Authenticator` — access-key → JWT session token against CBX1 IDM |
| `target_api/constants.py` | Config key names |
| `tests/test_core.py` | pytest: batch draining, sequential drain, UTF-8/mojibake/lone-surrogate sanitization |

CLI entry point (pyproject): `target-cbx1 = 'target_api.target:TargetApi.cli'` (package name `target-api`).

## Config (`config.json`)

```json
{
    "Code": "…",
    "OrgId": "…",
    "process_as_batch": true,
    "batch_size": 50
}
```

| Config | Description | Default |
|---|---|---|
| `Code` / `OrgId` | Access-key credentials for CBX1 IDM (same auth flow as the tap). The JWT session token and `expires_in` are **written back into the config file** (`AccessToken` key) — never commit a used `config.json`. | required |
| `process_as_batch` | Use `BatchSink` (bulk request per batch) instead of `RecordSink` | `true` |
| `batch_size` | Records per bulk request | `50` |
| `max_size_in_bytes` | Also flush a batch when its JSON size approaches this many bytes | unset |
| `enforce_order` | Force `MAX_PARALLELISM = 1` (otherwise 10) | unset |
| `add_stream_key` | Add `stream` field to each record | `false` |
| `metadata` | Extra metadata object (or JSON string) merged into each record's `metadata` | unset |
| `inject_batch_ids` | Add `hgBatchId` (md5 of `JOB_ROOT:stream:index`) to each record + state, for batch tracking | `false` |
| `post_empty_record` | Post an empty record for streams that had schema but no records | `false` |
| `custom_headers` | List of `{name, value}` extra HTTP headers | unset |

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `BASE_URL` | yes | CBX1 Java backend base URL, **with trailing slash**. e.g. `http://java-backend.api.qa.cbx1.internal/` |
| `CONNECTOR_ID` | no (default `HUBSPOT`) | Fallback CRM source when a record has no `source` field. One of `SALESFORCE`, `HUBSPOT`, `MARKETO`. Determines the endpoint path. |
| `JOB_ROOT` | no | HotGlue job identifier; used in `hgBatchId` generation when `inject_batch_ids` is on. |

Copy `.env.example` to `.env` for local runs.

## API surface

- Auth: `GET {BASE_URL}api/g/v1/auth/tokens?authenticationType=ACCESS_KEY&code=…&orgId=…` → `data.sessionToken` (Bearer) — plus `x-organisation-id: {OrgId}` header on every request.
- Ingestion (single **and** bulk — same endpoint): `POST {BASE_URL}api/t/v1/targets/integrations/{SOURCE}/{OBJECT_TYPE}/records` with payload `{"records": […]}`, where `SOURCE` comes from the record's `source` field or `CONNECTOR_ID`, and `OBJECT_TYPE` is derived from the stream name (`account|company|companies` → `ACCOUNT`; `contact|lead` → `CONTACT`).

### Response shape (`GenericResponse<RecordIngestionResponse>`)

```json
{
  "status": { "code": "CM000", "message": "Success" },
  "data": {
    "totalProcessed": 2, "successful": 1, "failed": 1,
    "results": [
      { "status": "SUCCESS", "entityId": "uuid", "lookupKey": "example.com" },
      { "status": "FAILED",  "entityId": null,  "lookupKey": "bad.com", "error": "…" }
    ]
  }
}
```

Per-record `status` is an enum: `SUCCESS`, `FAILED`, or `SKIPPED`. Results live at `response["data"]["results"]` — CBX1 wraps everything in `{status, data}`.

## Key patterns

### Record contract (what the ETL must produce)

- `lookupKey` — **required**; domain (accounts) or email (contacts). Records without it are skipped with a warning, never sent.
- `sourceRecordId` — the CRM-side record id; becomes `externalId` in state (what the HotGlue UI shows).
- `source` — optional per-record CRM source override (`SALESFORCE`/`HUBSPOT`/`MARKETO`).

### Batch processing (three-function pattern, `BatchSink`)

```python
make_batch_request(records)                     # POST the batch, return parsed JSON
handle_batch_response(response, raw_records, batch_external_id)  # build per-record state
process_batch(context)                          # orchestrate: chunk, post, update_state()
```

**Correlation strategy:** per-record results are matched back to inputs by `lookupKey` (never array order). State per record: `success` (bool, from the status enum), `id` (`entityId` as string), `externalId` (from `sourceRecordId`), `lookupKey`, `error` (failures only), `hgBatchId` (when injected). A failed batch request produces a `{"error": …, "batch_failed": true}` state entry.

### UTF-8 sanitization

Every record passes through `sanitize_record_utf8` before send: `ftfy` repairs mojibake ("Ã©" → "é") and lone surrogates (typically to U+FFFD, **preserving the field**); only a leaf still unencodable as UTF-8 *after* repair — a rare case — is **dropped at the leaf** (siblings survive). Repairs and drops are logged separately, including `sourceRecordId`. In the rare event a `lookupKey` is dropped rather than repaired, the record is then skipped by the lookupKey guard.

### Sequential drain

`drain_all` drains sinks one at a time (`_drain_all(…, 1)`) so final partial batches cannot race, and final state is assembled from each `BatchSink`'s `latest_state`.

### Error handling / retries

`validate_response`: 429 and 5xx → `RetriableAPIError` (backoff, max 2 tries); 4xx → `FatalAPIError`. Both log a **replayable cURL command** with `Authorization`/`x-organisation-id` masked — grab it from the logs to reproduce a failure by hand.

## Running locally

The target reads Singer messages on **stdin** and writes final state to **stdout**:

```bash
poetry install
set -a; source .env; set +a
cat input.singer | poetry run target-cbx1 --config config.json > state_out.json
```

⚠️ The target performs **real writes** — point `config.json` at a **QA tenant**; never run a local experiment against prod credentials.

Where to get `input.singer`:

- **Real shape (recommended):** `etl-output/data.singer` from a `hotglue-transformation-scripts` write-job run (see that repo's `local-job-debugging` skill) — this is exactly what the target receives in production.
- **From the sibling tap** (CBX1-shaped, useful for plumbing checks): `cbx1-tap-hotglue` sync output.
- **Hand-crafted minimal fixture:** one SCHEMA + a few RECORD lines with `lookupKey`/`sourceRecordId` — see `tests/test_core.py` for realistic record shapes.

The target prints its final STATE (per-record success/failure map) to stdout — that's what HotGlue surfaces in its UI.

### Verifying a run

1. **stderr logs**: `Making bulk request: <stream> with N records`, then `Batch complete: X/Y succeeded`.
2. **Skipped records**: count `Skipping … without lookupKey` warnings — the most common "records silently missing" cause.
3. **stdout state**: in batch mode (the default), per-record `{success, id, externalId, lookupKey, error?}` — `externalId` is what the HotGlue UI displays; `id` is the CBX1 entity UUID. In single-record mode (`process_as_batch: false`), the state carries only `{externalId, lookupKey}` (success/id are handled separately by the SDK).
4. **On HTTP errors** the logs contain a **masked replayable cURL** — rerun it by hand against QA to isolate target-vs-backend.

## Tests

```bash
poetry run pytest        # or: tox
```

CI (GitHub Actions) runs the suite on every push across Python 3.9–3.11 (3.7/3.8 are EOL and unavailable on current runner images; `tox.ini` still lists them for local use on older interpreters).

Covers batch draining, sequential drain, and the UTF-8 sanitization matrix (mojibake repair, lone-surrogate drop, bad keys).

## Debugging playbook

| Symptom | Likely cause / where to look |
|---|---|
| `Failed OAuth login` at startup | Bad `Code`/`OrgId` or `BASE_URL` unset/missing trailing slash (`auth.py` concatenates). |
| `Invalid CONNECTOR_ID` ValueError | `CONNECTOR_ID` env not one of SALESFORCE/HUBSPOT/MARKETO. |
| `Unsupported stream type` ValueError | Stream name doesn't contain account/company/contact/lead — check what the ETL named the stream. |
| Records silently not written | Missing `lookupKey` (skip is logged: "Skipping N record(s) without lookupKey"). Count the warnings. |
| externalId missing in HotGlue UI | State must carry `externalId` (mapped from `sourceRecordId`), not `crmAssociationId` — check `handle_batch_response`. |
| Partial batch failures | Per-record `error` in `data.results`; correlate by `lookupKey`. |
| 4xx/5xx from the API | Logs include a masked, replayable cURL — rerun it by hand against QA. 5xx retries twice; 4xx is fatal. |
| Weird characters / encode errors | Look for the sanitizer warnings (repaired vs dropped field paths, with `sourceRecordId`). |
| Order-sensitive failures | Try `enforce_order: true` (parallelism 1) to rule out interleaving. |

Quick first checks for the three highest-frequency issues:

| Quick check | Rules out |
|---|---|
| `grep -c 'without lookupKey'` on stderr | records dropped before the API |
| Per-record `status`/`error` in `data.results` (correlate by `lookupKey`, never index) | backend-side validation failures |
| Sanitizer warnings (`Repaired`/`Dropped … field(s)`) | encoding mutations you didn't expect |

To isolate a single record: set `process_as_batch: false` (RecordSink, one API call per record) or `batch_size: 1`.

## Conventions

- `meltano.yml` exists for SDK plumbing only; config truth is this file + `config.json`. Don't add settings there without updating here.
- Response parsing must always go through `response["data"]` — never assume a flat body.
- Never correlate batch results by array index; always `lookupKey`.

## Related repos

- [`cbx1-tap-hotglue`](https://github.com/CBX1/cbx1-tap-hotglue) — Singer tap (read side)
- [`hotglue-transformation-scripts`](https://github.com/CBX1/hotglue-transformation-scripts) — ETL between tap and target; contains the end-to-end architecture doc

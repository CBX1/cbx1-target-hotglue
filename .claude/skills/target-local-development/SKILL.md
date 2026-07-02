---
name: target-local-development
description: Run, test, and debug target-cbx1 locally — fixture-driven runs, piping a live tap, batch vs record mode, interpreting bulk API responses and state output, UTF-8 sanitization behavior. Use when running the target, investigating records not landing in CBX1, externalId/state issues, or batch failures.
---

# target-cbx1 Local Development

Authoritative reference: `AGENTS.md` at the repo root (config, endpoints, response shape, record contract, debugging playbook). This skill is the operational workflow.

## Setup (once)

```bash
poetry install
cp .env.example .env    # fill in BASE_URL (+ CONNECTOR_ID)
```

Create `config.json` with `Code`, `OrgId` (+ optional `process_as_batch`, `batch_size` — full table in `AGENTS.md`). Point it at a **QA tenant** — the target performs real writes; never run a local experiment against prod credentials. `config.json` is gitignored (a run writes the JWT back into it).

## Run

The target reads Singer messages on **stdin** and writes final state to **stdout**:

```bash
set -a; source .env; set +a
cat input.singer | poetry run target-cbx1 --config config.json > state_out.json
```

Getting an `input.singer` (in order of realism):

1. `etl-output/data.singer` from a `hotglue-transformation-scripts` write-job run — exactly the production input shape.
2. Sync output from `../cbx1-tap-hotglue` (plumbing checks).
3. Hand-crafted: one SCHEMA line + RECORD lines. Minimum viable record fields: `lookupKey` (domain/email — **required or the record is skipped**), `sourceRecordId`. Copy shapes from `tests/test_core.py`.

Mode selection: stream name must contain `account`/`company`/`companies` or `contact`/`lead` (that picks the CBX1 entity type); the CRM source comes from each record's `source` field, else `CONNECTOR_ID`.

## Verify a run

1. **stderr logs**: `Making bulk request: <stream> with N records`, then `Batch complete: X/Y succeeded`.
2. **Skipped records**: count `Skipping … without lookupKey` warnings — the most common "records silently missing" cause.
3. **stdout state**: per-record `{success, id, externalId, lookupKey, error?}` — `externalId` is what the HotGlue UI displays; `id` is the CBX1 entity UUID.
4. **On HTTP errors** the logs contain a **masked replayable cURL** — rerun it by hand against QA to isolate target-vs-backend.

## Test

```bash
poetry run pytest        # or: tox
```

## Debug

Work the symptom table in `AGENTS.md` → "Debugging playbook". The three highest-frequency issues:

| Quick check | Rules out |
|---|---|
| `grep -c 'without lookupKey'` on stderr | records dropped before the API |
| Per-record `status`/`error` in `data.results` (correlate by `lookupKey`, never index) | backend-side validation failures |
| Sanitizer warnings (`Repaired`/`Dropped … field(s)`) | encoding mutations you didn't expect |

To isolate a single record: set `process_as_batch: false` (RecordSink, one API call per record) or `batch_size: 1`.

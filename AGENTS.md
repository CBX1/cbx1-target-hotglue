# Agent Guide — target-cbx1

**`README.md` is the authoritative documentation** for this repo: layout, config, env vars, API surface, response shape, record contract, batch semantics, tests, and the debugging playbook. Read it first; don't duplicate its content here.

Agent-specific ground rules:

- **Response parsing:** always go through `response["data"]` (CBX1 wraps everything in `{status, data}`); per-record `status` is an enum (`SUCCESS`/`FAILED`/`SKIPPED`), not a boolean. Never correlate batch results by array index — always `lookupKey`.
- **`meltano.yml` is SDK plumbing only.** Config truth is README + `config.json`; keep the settings list in sync when adding config options.
- Never commit `config.json` or `.env` — a run writes the JWT session token back into `config.json`. Both are gitignored.
- Run `poetry run pytest` before and after touching `sinks.py` or `target.py` — the suite encodes batch draining and the UTF-8 sanitization contract (a prod-incident regression suite; don't "generalize" the sanitizer).
- Local writes go to a real backend — use **QA tenant credentials only**, never prod, for local experiments.
- Local run/debug workflow: README → "Running locally" (incl. "Verifying a run") and "Debugging playbook".
- End-to-end pipeline context: `docs/architecture.md` in `hotglue-transformation-scripts`.

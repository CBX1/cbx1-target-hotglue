# target-cbx1

Singer **target** that receives CRM records (accounts, contacts) from HotGlue taps and writes them to the CBX1 platform's integration records API. Built with the Meltano Singer SDK + `target-hotglue`. Runs inside HotGlue as the write side of the CRM → CBX1 sync pipeline.

> **Agents / detailed reference:** see [`AGENTS.md`](AGENTS.md) for architecture, endpoints, batch semantics, and debugging guidance.

## Quickstart

```bash
poetry install
```

Create a `config.json`:

```json
{
    "Code": "<access key code from CBX1 IDM>",
    "OrgId": "<tenant organization id>",
    "process_as_batch": true,
    "batch_size": 50
}
```

Set environment variables (see `.env.example`):

```bash
export BASE_URL="http://java-backend.api.qa.cbx1.internal/"   # trailing slash required
export CONNECTOR_ID="HUBSPOT"                                 # SALESFORCE | HUBSPOT | MARKETO
```

Run against a Singer input stream:

```bash
# From a saved fixture (e.g. etl-output/data.singer from hotglue-transformation-scripts)
cat input.singer | poetry run target-cbx1 --config config.json

# Or pipe a live tap
tap-hubspot --config tap_config.json --catalog catalog.json | poetry run target-cbx1 --config config.json
```

## Tests

```bash
poetry run pytest        # or: tox
```

## Branches

| Branch | Purpose |
|---|---|
| `main` | Development |
| `production` | Production deployment |

## Related repos

- [`cbx1-tap-hotglue`](https://github.com/CBX1/cbx1-tap-hotglue) — Singer tap (read side)
- [`hotglue-transformation-scripts`](https://github.com/CBX1/hotglue-transformation-scripts) — ETL between tap and target; contains the end-to-end architecture doc

# CBX1 Target HotGlue

HotGlue Singer target for syncing CRM data (accounts, contacts) to the CBX1 platform.

## Project Overview

This is a Singer target built with the Meltano Singer SDK. It receives data from HotGlue taps (Salesforce, HubSpot, etc.) and writes to the CBX1 bulk upsert API.

**Key Components:**
- `target_api/target.py` - Main target class (`TargetApi`)
- `target_api/sinks.py` - Sink classes for data processing (`RecordSink`, `BatchSink`)
- `target_api/client.py` - Base API client (`ApiSink`)
- `target_api/auth.py` - Authentication handling

## Architecture

```
HotGlue Tap (Salesforce/HubSpot)
    ↓ Singer messages (RECORD, STATE, SCHEMA)
TargetApi (target.py)
    ↓ Routes to appropriate sink
BatchSink / RecordSink (sinks.py)
    ↓ API calls
CBX1 Bulk Upsert API (/api/t/v1/targets/{stream}/bulk)
```

## Processing Modes

| Mode | Sink | Endpoint | Default |
|------|------|----------|---------|
| Batch | `BatchSink` | `/bulk` | Yes (`process_as_batch: true`) |
| Single | `RecordSink` | `/upsert` | No |

**Batch size:** 20 records (configurable via `batch_size`)

## Key Patterns

### External ID Mapping

CRM sync requires mapping between CRM IDs and internal IDs:

```python
# Input field (from ETL): crmAssociationId
# Output field (for HotGlue UI): externalId
# Lookup field: domain (accounts) or email (contacts)
```

### Bulk API Response Structure

```json
{
  "status": { "code": "CM000", "message": "Success" },
  "data": {
    "totalProcessed": 2,
    "successful": 2,
    "failed": 0,
    "results": [
      { "success": true, "id": "uuid", "lookupKey": "example.com" },
      { "success": false, "id": null, "lookupKey": "bad.com", "error": "..." }
    ]
  }
}
```

### Batch Processing Pattern

The `BatchSink` follows the HotGlue-recommended three-function pattern:

```python
# 1. Post batch to API, return full response
def make_batch_request(self, records: List[dict]) -> dict

# 2. Parse response, build per-record state with externalId mapping
def handle_batch_response(self, response: dict, raw_records: List[dict]) -> dict

# 3. Orchestrate batch, call update_state() per record
def process_batch(self, context: dict) -> None
```

**Correlation strategy**: Use `lookupKey` (domain/email) to map `crmAssociationId` ↔ `internal id` rather than relying on array order.

### State Output

Each record in state includes:
- `success` - Boolean success status
- `id` - Internal CBX1 UUID (null if failed)
- `externalId` - CRM record ID (from `crmAssociationId`)
- `lookupKey` - Domain or email used for correlation
- `error` - Error message (only for failures)

## Configuration

| Config | Description | Default |
|--------|-------------|---------|
| `process_as_batch` | Use batch processing | `true` |
| `batch_size` | Records per batch | `20` |
| `add_stream_key` | Add stream name to records | `false` |
| `metadata` | Additional metadata to inject | `null` |
| `inject_batch_ids` | Add batch tracking IDs | `false` |

## Development

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run target locally
poetry run target-cbx1 --config config.json
```

## Branches

| Branch | Purpose |
|--------|---------|
| `main` | Development |
| `production` | Production deployment |

## Related Repositories

- `hotglue-transformation-scripts/` - ETL scripts that transform CRM data before this target
- Backend bulk API: `POST /api/t/v1/targets/accounts/bulk`, `POST /api/t/v1/targets/contacts/bulk`

## Common Issues

1. **External ID not showing in HotGlue UI**: Ensure state outputs `externalId` field (not `crmAssociationId`)
2. **Domain/email required errors**: Records without lookup key fail validation
3. **Batch failures**: Check `data.results` for per-record error messages
4. **Response parsing errors**: Results are in `response["data"]["results"]`, not `response["results"]` - CBX1 API wraps responses in `{ status: {...}, data: {...} }`

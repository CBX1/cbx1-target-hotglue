"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
import logging
from typing import Any, List, Tuple

import ftfy
from target_hotglue.client import HotglueBatchSink, HotglueSink

from target_api.client import ApiSink
import os
import math
import hashlib


_DROP = object()


def _repair_string(value: str) -> Tuple[str, bool]:
    """Run ftfy.fix_text on a string. Returns (fixed, changed)."""
    fixed = ftfy.fix_text(value)
    return fixed, fixed != value


def _scrub_utf8(
    value: Any,
    path: str,
    dropped: List[str],
    repaired: List[str],
) -> Any:
    """Repair mojibake/lone-surrogate strings with ftfy, then drop any that
    still cannot be UTF-8 encoded.

    Mojibake (e.g. "Ã©" instead of "é") is valid UTF-8 but semantically
    wrong; ftfy repairs it. Lone surrogates from errors='surrogateescape'
    decoding fail .encode("utf-8") outright; ftfy.fix_text re-interprets the
    surrogate bytes when possible. Anything still unencodable after repair
    is dropped at the leaf — siblings survive.
    """
    if isinstance(value, str):
        fixed, changed = _repair_string(value)
        if changed:
            repaired.append(path or "<root>")
        try:
            fixed.encode("utf-8")
        except UnicodeEncodeError:
            dropped.append(path or "<root>")
            return _DROP
        return fixed
    if isinstance(value, dict):
        cleaned: dict = {}
        for k, v in value.items():
            fixed_k = k
            if isinstance(k, str):
                fixed_k, changed = _repair_string(k)
                if changed:
                    repaired.append(
                        f"{path}.<key>" if path else "<key>"
                    )
                try:
                    fixed_k.encode("utf-8")
                except UnicodeEncodeError:
                    # Mask the bad key in the path — the raw key cannot be
                    # safely formatted into log handlers.
                    dropped.append(f"{path}.<bad-key>" if path else "<bad-key>")
                    continue
            child = f"{path}.{fixed_k}" if path else str(fixed_k)
            new_v = _scrub_utf8(v, child, dropped, repaired)
            if new_v is _DROP:
                continue
            cleaned[fixed_k] = new_v
        return cleaned
    if isinstance(value, list):
        cleaned_list: list = []
        for i, v in enumerate(value):
            child = f"{path}[{i}]"
            new_v = _scrub_utf8(v, child, dropped, repaired)
            if new_v is _DROP:
                continue
            cleaned_list.append(new_v)
        return cleaned_list
    return value


def sanitize_record_utf8(
    record: dict,
    stream_name: str,
    logger: logging.Logger,
) -> dict:
    """Repair mojibake/lone-surrogate strings via ftfy, then drop anything
    still unencodable as UTF-8. Logs repairs and drops separately."""
    dropped: List[str] = []
    repaired: List[str] = []
    cleaned = _scrub_utf8(record, "", dropped, repaired)
    if repaired:
        logger.warning(
            "Repaired %d mojibake/non-UTF-8 field(s) in %s record (sourceRecordId=%s): %s",
            len(repaired),
            stream_name,
            record.get("sourceRecordId"),
            ", ".join(repaired),
        )
    if dropped:
        logger.warning(
            "Dropped %d non-UTF-8 field(s) from %s record (sourceRecordId=%s) after repair attempt: %s",
            len(dropped),
            stream_name,
            record.get("sourceRecordId"),
            ", ".join(dropped),
        )
    return cleaned


class RecordSink(ApiSink, HotglueSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = sanitize_record_utf8(record, self.stream_name, self.logger)

        if self.config.get("add_stream_key"):
            record["stream"] = self.stream_name

        if self.config.get("metadata", None):
            metadata = record.get("metadata") or {}

            try:
                metadata.update(json.loads(self.config.get("metadata")))
            except:
                metadata.update(self.config.get("metadata"))

            record["metadata"] = metadata
        return record

    def upsert_record(self, record: dict, context: dict):
        self.logger.info(f"Making request: {self.stream_name}")

        # Only process if lookupKey exists
        if record.get("lookupKey") is None:
            self.logger.warning(f"Skipping record without lookupKey")
            return None, False, {}

        request_payload = {"records": [record]}
        
        endpoint = self.get_endpoint(record)

        response = self.request_api(
            self._config.get("method", "POST").upper(),
            endpoint=endpoint,
            request_data=request_payload,
            headers=self.custom_headers,
            verify=False
        )

        # Parse GenericResponse<RecordIngestionResponse> format
        # Response structure: {"status": {...}, "data": {"results": [...]}}
        id = None
        try:
            data = response.json().get("data", {})
            results = data.get("results", [])
            if results:
                result = results[0]
                if result.get("status") == "SUCCESS":
                    id = result.get("entityId")
        except Exception as e:
            self.logger.warning(f"Unable to parse response: {e}")

        # Build state with externalId mapping for HotGlue UI
        state = {
            "externalId": record.get("sourceRecordId"),
            "lookupKey": record.get("lookupKey"),
        }

        return id, response.ok, state


class BatchSink(ApiSink, HotglueBatchSink):

    send_empty_record = False

    @property
    def max_size(self):
        if self.config.get("process_as_batch"):
            batch_size = self.config.get("batch_size", 10)
            if batch_size:
                return int(batch_size)
        return 10

    def process_batch_record(self, record: dict, index: int) -> dict:
        record = sanitize_record_utf8(record, self.stream_name, self.logger)

        if self.config.get("add_stream_key"):
            record["stream"] = self.stream_name

        if self.config.get("metadata", None):
            metadata = record.get("metadata") or {}

            try:
                metadata.update(json.loads(self.config.get("metadata")))
            except:
                metadata.update(self.config.get("metadata"))

            record["metadata"] = metadata
        return record

    def make_batch_request(self, records: List[dict]):
        """
        Post batch of records to new integration endpoint.

        Returns:
            dict: API response with RecordIngestionResponse format
        """
        self.logger.info(f"Making bulk request: {self.stream_name} with {len(records)} records")

        ingestion_records = [
            record
            for record in records
            if record.get("lookupKey") is not None
        ]
        skipped = len(records) - len(ingestion_records)
        if skipped:
            self.logger.warning(
                "Skipping %d %s record(s) without lookupKey (e.g. dropped during UTF-8 sanitization)",
                skipped,
                self.stream_name,
            )
        request_payload = {"records": ingestion_records}
        
        endpoint = self.get_bulk_endpoint(ingestion_records[0] if ingestion_records else None)

        response = self.request_api(
            "POST",
            endpoint=endpoint,
            request_data=request_payload,
            headers=self.custom_headers,
            verify=False
        )
        return response.json()
    
    def generate_batch_id(self):
        index = math.ceil(self._total_records_read/self.max_size)
        external_id = f"{os.environ.get('JOB_ROOT', 'job_Example')}:{self.name}:{index}"
        external_id = hashlib.md5(external_id.encode()).hexdigest()
        return external_id

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]
        batch_external_id = None

        for i in range(0, len(raw_records), self.max_size):
            batch_records = raw_records[i:i+self.max_size]
            processed_records = batch_records

            if not self.send_empty_record:
                processed_records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(batch_records)))

                inject_batch_ids = self.config.get("inject_batch_ids", False)
                if inject_batch_ids:
                    batch_external_id = self.generate_batch_id()
                    [record.update({"hgBatchId": batch_external_id}) for record in processed_records]

            try:
                response = self.make_batch_request(processed_records)
                result = self.handle_batch_response(response, batch_records, batch_external_id)

                for state in result.get("state_updates", []):
                    self.update_state(state)

                summary = result.get("summary", {})
                self.logger.info(
                    f"Batch complete: {summary.get('successful', 0)}/{summary.get('totalProcessed', 0)} succeeded"
                )
            except Exception as e:
                self.logger.error(f"Batch request failed: {e}")
                state = {"error": str(e), "batch_failed": True}
                if batch_external_id:
                    state["hgBatchId"] = batch_external_id
                self.update_state(state)
                
    def handle_batch_response(self, response: dict, raw_records: List[dict], batch_external_id=None) -> dict:
        """
        Parse new IntegrationRecordsController response format.

        Args:
            response: Response with RecordIngestionResponse format
            raw_records: Original input records (for externalId lookup)
            batch_external_id: Optional batch ID for tracking

        Returns:
            dict with state_updates list containing per-record states
        """
        state_updates = []
        
        # Extract data from nested structure
        data = response.get("data", {})
        results = data.get("results", [])

        # Build lookup map: lookupKey -> sourceRecordId from input records
        external_id_by_lookup = {
            record.get("lookupKey"): record.get("sourceRecordId")
            for record in raw_records
            if record.get("lookupKey")
        }

        for result in results:
            lookup_key = result.get("lookupKey")
            external_id = external_id_by_lookup.get(lookup_key)

            # Map new status enum to boolean
            status = result.get("status")  # SUCCESS, FAILED, or SKIPPED
            success = (status == "SUCCESS")

            state = {
                "success": success,
                "id": str(result.get("entityId")) if result.get("entityId") else None,
                "externalId": external_id,
                "lookupKey": lookup_key,
            }

            if batch_external_id:
                state["hgBatchId"] = batch_external_id

            if not success:
                state["error"] = result.get("error")

            state_updates.append(state)

        return {
            "state_updates": state_updates,
            "summary": {
                "totalProcessed": data.get("totalProcessed"),
                "successful": data.get("successful"),
                "failed": data.get("failed"),
            }
        }

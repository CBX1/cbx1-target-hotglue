"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
from typing import List

from target_hotglue.client import HotglueBatchSink, HotglueSink

from target_api.client import ApiSink
import os
import math
import hashlib


class RecordSink(ApiSink, HotglueSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
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
        response = self.request_api(
            self._config.get("method", "POST").upper(), request_data=record, headers=self.custom_headers, verify=False
        )

        id = None

        try:
            id = response.json().get("data").get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()


class BatchSink(ApiSink, HotglueBatchSink):

    send_empty_record = False

    @property
    def max_size(self):
        if self.config.get("process_as_batch"):
            batch_size = self.config.get("batch_size", 100)
            if batch_size:
                return int(batch_size)
        return 100

    def process_batch_record(self, record: dict, index: int) -> dict:
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
        Post batch of records to bulk endpoint and return response.

        Returns:
            dict: Full API response with results array for per-record handling
        """
        self.logger.info(f"Making bulk request: {self.stream_name} with {len(records)} records")
        response = self.request_api(
            "POST",
            endpoint=self.bulk_endpoint,
            request_data=records,
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
                
    def _get_lookup_field(self) -> str:
        """Return the lookup field based on stream name."""
        stream_lower = self.stream_name.lower()
        if "account" in stream_lower:
            return "domain"
        elif "contact" in stream_lower:
            return "email"
        return "id"

    def handle_batch_response(self, response: dict, raw_records: List[dict], batch_external_id=None) -> dict:
        """
        Parse bulk upsert response and build state payloads.

        Args:
            response: Bulk upsert API response with results array
            raw_records: Original input records (for externalId lookup)
            batch_external_id: Optional batch ID for tracking

        Returns:
            dict with state_updates list containing per-record states
        """
        state_updates = []
        results = response.get("results", [])

        # Build lookup map: lookupKey -> externalId from input records
        lookup_field = self._get_lookup_field()
        external_id_by_lookup = {
            record.get(lookup_field): record.get("externalId")
            for record in raw_records
            if record.get(lookup_field)
        }

        for result in results:
            lookup_key = result.get("lookupKey")
            external_id = external_id_by_lookup.get(lookup_key)

            state = {
                "success": result.get("success"),
                "id": result.get("id"),
                "externalId": external_id,
                "lookupKey": lookup_key,
            }

            if batch_external_id:
                state["hgBatchId"] = batch_external_id

            if not result.get("success"):
                state["error"] = result.get("error")

            state_updates.append(state)

        return {
            "state_updates": state_updates,
            "summary": {
                "totalProcessed": response.get("totalProcessed"),
                "successful": response.get("successful"),
                "failed": response.get("failed"),
            }
        }

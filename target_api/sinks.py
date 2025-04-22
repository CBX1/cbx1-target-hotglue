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
        self.logger.info(f"Making request: {self.stream_name}")
        response = self.request_api(
            self._config.get("method", "POST").upper(), request_data=records, headers=self.custom_headers, verify=False
        )

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id
    
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
            records = raw_records[i:i+self.max_size]

            if not self.send_empty_record:
                records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(records)))

                inject_batch_ids = self.config.get("inject_batch_ids", False)
                if inject_batch_ids:
                    batch_external_id = self.generate_batch_id()
                    # add batch_external_id to each record
                    [record.update({"hgBatchId": batch_external_id}) for record in records]

            try:
                id = self.make_batch_request(records)
                result = self.handle_batch_response(id, batch_external_id)
                for state in result.get("state_updates", list()):
                    self.update_state(state)
            except Exception as e:
                state = {"error": str(e)}
                if inject_batch_ids:
                    state.update({"hgBatchId": batch_external_id})
                self.update_state(state)
                
    def handle_batch_response(self, id, batch_external_id=None) -> dict:
        state = {"id": id, "success": True}
        if batch_external_id:
            state.update({"hgBatchId": batch_external_id})
        return {"state_updates": [state]}

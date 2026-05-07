"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import json
import typing as t

import pytest

try:
    from singer_sdk.testing import get_target_test_class
except ImportError:
    get_target_test_class = None

from target_api.target import TargetApi

# TODO: Initialize minimal target config
SAMPLE_CONFIG: dict[str, t.Any] = {}


if get_target_test_class:
    # Run standard built-in target tests from SDK versions that expose them:
    StandardTargetTests = get_target_test_class(
        target_class=TargetApi,
        config=SAMPLE_CONFIG,
    )

    class TestTargetApi(StandardTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
        """Standard Target Tests."""

        @pytest.fixture(scope="class")
        def resource(self):  # noqa: ANN201
            """Generic external resource.

            This fixture is useful for setup and teardown of external resources,
            such output folders, tables, buckets etc. for use during testing.

            Example usage can be found in the SDK samples test suite:
            https://github.com/meltano/sdk/tree/main/tests/samples
            """
            return "resource"


# TODO: Create additional tests as appropriate for your target.


def test_batch_sink_drains_when_full(tmp_path, monkeypatch):
    """Regression coverage for the custom record processing override."""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"process_as_batch": True, "batch_size": 2, "OrgId": "org-id"})
    )

    target = TargetApi(config=[str(config_path)], validate_config=False)
    drain_calls = []

    def fake_drain_one(sink):
        drain_calls.append((sink.stream_name, sink.current_size))
        draining_status = sink.start_drain()
        assert len(draining_status["records"]) == 2
        sink.mark_drained()

    monkeypatch.setattr(target, "drain_one", fake_drain_one)

    target._process_schema_message(
        {
            "type": "SCHEMA",
            "stream": "contacts",
            "schema": {
                "type": "object",
                "properties": {
                    "sourceRecordId": {"type": ["string", "null"]},
                    "lookupKey": {"type": ["string", "null"]},
                    "source": {"type": ["string", "null"]},
                    "data": {"type": ["string", "null"]},
                },
            },
            "key_properties": [],
        }
    )

    for index in range(2):
        target._process_record_message(
            {
                "type": "RECORD",
                "stream": "contacts",
                "record": {
                    "sourceRecordId": str(index),
                    "lookupKey": f"user-{index}@example.com",
                    "source": "HUBSPOT",
                    "data": "{}",
                },
            }
        )

    assert drain_calls == [("contacts", 2)]

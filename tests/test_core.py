"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import json
import logging
import typing as t

import pytest

try:
    from singer_sdk.testing import get_target_test_class
except ImportError:
    get_target_test_class = None

from target_api.sinks import sanitize_record_utf8
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


def test_drain_all_drains_active_sinks_sequentially(tmp_path, monkeypatch):
    """Final drain should not process multiple stream sinks concurrently."""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"process_as_batch": True, "batch_size": 20, "OrgId": "org-id"})
    )

    target = TargetApi(config=[str(config_path)], validate_config=False)
    calls = []

    def fake_drain_all(sinks, parallelism):
        calls.append(([sink.stream_name for sink in sinks], parallelism))

    monkeypatch.setattr(target, "_drain_all", fake_drain_all)
    target._sinks_active = {
        "accounts": t.cast(t.Any, type("Sink", (), {"stream_name": "accounts"})()),
        "contacts": t.cast(t.Any, type("Sink", (), {"stream_name": "contacts"})()),
    }
    monkeypatch.setattr(target, "_write_state_message", lambda state: None)
    monkeypatch.setattr(target, "_reset_max_record_age", lambda: None)

    target.drain_all()

    assert calls == [
        ([], 1),
        (["accounts", "contacts"], 1),
    ]


def test_sanitize_record_utf8_drops_only_offending_field(caplog):
    """Non-UTF-8 fields are dropped at the leaf; sibling fields and the
    surrounding record/structure survive. JSON-serializable result."""
    bad = "lone-surrogate-\udce9"  # cannot encode as UTF-8
    record = {
        "sourceRecordId": "rec-1",
        "lookupKey": "user@example.com",
        "firstName": bad,
        "data": {
            "company": "Acme",
            "notes": bad,
            "tags": ["clean", bad, "also-clean"],
        },
    }

    with caplog.at_level(logging.WARNING):
        cleaned = sanitize_record_utf8(record, "contacts", logging.getLogger("test"))

    assert cleaned == {
        "sourceRecordId": "rec-1",
        "lookupKey": "user@example.com",
        "data": {
            "company": "Acme",
            "tags": ["clean", "also-clean"],
        },
    }
    # Sanitized record must round-trip through json without errors.
    json.dumps(cleaned).encode("utf-8")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "firstName" in msg
    assert "data.notes" in msg
    assert "data.tags[1]" in msg
    assert "rec-1" in msg


def test_sanitize_record_utf8_passthrough_for_clean_record():
    record = {
        "sourceRecordId": "rec-2",
        "lookupKey": "héllo@example.com",
        "data": {"name": "Zoë", "tags": ["α", "β"]},
    }
    cleaned = sanitize_record_utf8(record, "contacts", logging.getLogger("test"))
    assert cleaned == record


def test_sanitize_record_utf8_drops_lookupkey_when_invalid():
    """If the lookupKey itself is malformed, the field is dropped — the
    existing 'no lookupKey' guard then skips the record at request time."""
    record = {
        "sourceRecordId": "rec-3",
        "lookupKey": "bad-\udce9-domain.com",
        "domain": "bad-\udce9-domain.com",
    }
    cleaned = sanitize_record_utf8(record, "accounts", logging.getLogger("test"))
    assert "lookupKey" not in cleaned
    assert "domain" not in cleaned
    assert cleaned["sourceRecordId"] == "rec-3"

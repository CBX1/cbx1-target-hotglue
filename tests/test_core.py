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

from target_api.client import ApiSink
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


def test_sanitize_record_utf8_repairs_mojibake(caplog):
    """Mojibake — UTF-8 bytes that were decoded as latin-1 and re-encoded as
    UTF-8 — is salvageable. ftfy reverses it, so the original name reaches
    CBX1 instead of being silently dropped."""
    record = {
        "sourceRecordId": "rec-mojibake",
        "lookupKey": "user@example.com",
        "firstName": "CafÃ©",  # mojibake of "Café"
        "lastName": "MÃ¼ller",  # mojibake of "Müller"
        "data": {
            "company": "Acme",
            "notes": "naÃ¯ve",  # mojibake of "naïve"
        },
    }

    with caplog.at_level(logging.WARNING):
        cleaned = sanitize_record_utf8(record, "contacts", logging.getLogger("test"))

    assert cleaned == {
        "sourceRecordId": "rec-mojibake",
        "lookupKey": "user@example.com",
        "firstName": "Café",
        "lastName": "Müller",
        "data": {
            "company": "Acme",
            "notes": "naïve",
        },
    }
    json.dumps(cleaned).encode("utf-8")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "Repaired" in msg
    assert "firstName" in msg
    assert "lastName" in msg
    assert "data.notes" in msg


def test_sanitize_record_utf8_repairs_lone_surrogates(caplog):
    """Lone surrogates (from errors='surrogateescape' decoding) get replaced
    with U+FFFD via ftfy — the field is preserved so any remaining valid
    context survives, instead of dropping the whole leaf."""
    bad = "lone-surrogate-\udce9"
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

    repaired_value = "lone-surrogate-�"
    assert cleaned == {
        "sourceRecordId": "rec-1",
        "lookupKey": "user@example.com",
        "firstName": repaired_value,
        "data": {
            "company": "Acme",
            "notes": repaired_value,
            "tags": ["clean", repaired_value, "also-clean"],
        },
    }
    json.dumps(cleaned).encode("utf-8")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "Repaired" in msg
    assert "firstName" in msg
    assert "data.notes" in msg
    assert "data.tags[1]" in msg
    assert "rec-1" in msg


def test_sanitize_record_utf8_passthrough_for_clean_record(caplog):
    """Clean UTF-8 strings — including non-ASCII names like Zoë and the
    intentionally weird 'X Æ A-Xii' / 'John 3rd' — pass through unchanged
    with no log output."""
    record = {
        "sourceRecordId": "rec-2",
        "lookupKey": "héllo@example.com",
        "firstName": "X Æ A-Xii",
        "middleName": "John 3rd",
        "data": {"name": "Zoë", "tags": ["α", "β"]},
    }
    with caplog.at_level(logging.WARNING):
        cleaned = sanitize_record_utf8(
            record, "contacts", logging.getLogger("test")
        )
    assert cleaned == record
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_sanitize_record_utf8_repairs_invalid_dict_keys():
    """Lone-surrogate dict keys are repaired (replaced with U+FFFD) so
    json.dumps stops shipping malformed keys to CBX1 — both at the top
    level and nested."""
    bad_key = "key-\udce9"
    repaired_key = "key-�"
    record = {
        "sourceRecordId": "rec-4",
        bad_key: "outer-value",
        "data": {
            "good": "ok",
            bad_key: "inner-value",
        },
    }

    cleaned = sanitize_record_utf8(record, "contacts", logging.getLogger("test"))

    assert bad_key not in cleaned
    assert bad_key not in cleaned["data"]
    assert cleaned == {
        "sourceRecordId": "rec-4",
        repaired_key: "outer-value",
        "data": {
            "good": "ok",
            repaired_key: "inner-value",
        },
    }
    json.dumps(cleaned).encode("utf-8")


def test_sanitize_record_utf8_repairs_lookupkey_with_lone_surrogate():
    """A malformed lookupKey is now repaired instead of dropped. The record
    no longer falls through the no-lookupKey skip path; CBX1 sees a string
    with a replacement char and can match or reject as appropriate."""
    record = {
        "sourceRecordId": "rec-3",
        "lookupKey": "bad-\udce9-domain.com",
        "domain": "bad-\udce9-domain.com",
    }
    cleaned = sanitize_record_utf8(record, "accounts", logging.getLogger("test"))
    assert cleaned["lookupKey"] == "bad-�-domain.com"
    assert cleaned["domain"] == "bad-�-domain.com"
    assert cleaned["sourceRecordId"] == "rec-3"


def _sink_with_stream(stream_name: str) -> ApiSink:
    """Build a bare ApiSink for stream-name routing checks.

    _get_object_type / _get_lookup_field read only self.stream_name, so the sink
    is constructed without going through the SDK's __init__ (which would need a
    target, schema and live config).
    """
    sink = ApiSink.__new__(ApiSink)
    sink.stream_name = stream_name
    return sink


@pytest.mark.parametrize(
    ("stream", "object_type", "lookup_field"),
    [
        ("deals", "DEAL", "id"),
        ("associations_deals_companies", "DEAL_COMPANY_LINK", "lookupKey"),
        ("associations_deals_contacts", "DEAL_CONTACT_LINK", "lookupKey"),
        ("forms", "FORM", "lookupKey"),
        ("form_submissions", "FORM_SUBMISSION", "lookupKey"),
    ],
)
def test_exact_name_streams_route_correctly(stream, object_type, lookup_field):
    """Exact-name streams resolve via the table, not substring matching."""
    sink = _sink_with_stream(stream)
    assert sink._get_object_type() == object_type
    assert sink._get_lookup_field() == lookup_field


@pytest.mark.parametrize("stream", ["associations_deals_companies", "associations_deals_contacts"])
def test_link_streams_are_not_misrouted_to_account_or_contact(stream):
    """Regression guard for the substring-matching trap.

    "associations_deals_companies" contains "company" and "associations_deals_contacts" contains
    "contact", so if the exact-name table is ever removed or consulted after the
    substring branches, both streams would POST edge records to the existing
    ACCOUNT/CONTACT ingestion endpoints and silently corrupt AccountV2/ContactV2.
    """
    sink = _sink_with_stream(stream)
    assert sink._get_object_type() not in {"ACCOUNT", "CONTACT"}
    assert sink._get_lookup_field() not in {"domain", "email"}


@pytest.mark.parametrize(
    ("stream", "object_type", "lookup_field"),
    [
        ("accounts", "ACCOUNT", "domain"),
        ("companies", "ACCOUNT", "domain"),
        ("contacts", "CONTACT", "email"),
        ("leads", "CONTACT", "email"),
    ],
)
def test_existing_streams_keep_substring_routing(stream, object_type, lookup_field):
    """The account/contact sync must be untouched by the exact-name table."""
    sink = _sink_with_stream(stream)
    assert sink._get_object_type() == object_type
    assert sink._get_lookup_field() == lookup_field


def test_unknown_stream_still_raises():
    with pytest.raises(ValueError, match="Unsupported stream type"):
        _sink_with_stream("invoices")._get_object_type()

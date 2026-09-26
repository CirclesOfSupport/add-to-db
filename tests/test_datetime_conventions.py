"""Date/time columns of the responses target are stored in the table's own convention."""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta

import pytest
from google.cloud import bigquery

import config
from bq_writer import coerce_payload_to_schema, coerce_value_to_bq_type
from conftest import FEEDBACK_SCHEMA_WITH_DATETIME, RESPONSE_DATA_SCHEMA

RESP = config.DATETIME_CONVENTIONS["responses"]


def coerce_responses(payload):
    row, errors = coerce_payload_to_schema(payload, RESPONSE_DATA_SCHEMA, RESP)
    assert errors == []
    return row


@pytest.mark.parametrize("column", [
    "checkinDateTime", "checkinReplyDateTime", "resourceOfferReplyDatetime",
    "referralFollowUpUtilizedDateTime",
])
def test_timestamps_are_stored_as_utc(column):
    # url-encoded, as TextIt sends it; -04:00 is Eastern daylight time
    row = coerce_responses({column: "2026-09-25T08%3A30%3A21.956197-04%3A00"})
    assert row[column] == datetime(2026, 9, 25, 12, 30, 21, 956197)


def test_utc_conversion_crosses_midnight():
    row = coerce_responses({"checkinDateTime": "2026-09-25T21:00:00-04:00"})
    assert row["checkinDateTime"] == datetime(2026, 9, 26, 1, 0, 0)


def test_standard_time_offset():
    row = coerce_responses({"checkinReplyDateTime": "2026-12-01T09:15:00-05:00"})
    assert row["checkinReplyDateTime"] == datetime(2026, 12, 1, 14, 15, 0)


def test_other_zone_offset_and_z_suffix():
    row = coerce_responses({"checkinDateTime": "2026-09-25T08:00:00-07:00",
                            "checkinReplyDateTime": "2026-09-25T15:00:00Z"})
    assert row["checkinDateTime"] == datetime(2026, 9, 25, 15, 0, 0)
    assert row["checkinReplyDateTime"] == datetime(2026, 9, 25, 15, 0, 0)


def test_naive_value_is_stored_unchanged():
    row = coerce_responses({"checkinDateTime": "2026-09-25T12:30:21.956197"})
    assert row["checkinDateTime"] == datetime(2026, 9, 25, 12, 30, 21, 956197)


def test_aware_python_datetime_is_converted():
    aware = datetime(2026, 9, 25, 8, 30, tzinfo=timezone(timedelta(hours=-4)))
    row = coerce_responses({"checkinDateTime": aware})
    assert row["checkinDateTime"] == datetime(2026, 9, 25, 12, 30)


def test_checkin_reply_date_is_midnight_of_the_local_date():
    # 21:00 Eastern on the 25th is already the 26th in UTC; the stored value is the local date
    row = coerce_responses({"checkinReplyDate": "2026-09-25T21:00:00-04:00"})
    assert row["checkinReplyDate"] == datetime(2026, 9, 25, 0, 0, 0)


@pytest.mark.parametrize("column", ["checkinDate", "resourceOfferReplyDate"])
def test_date_columns_keep_the_local_date(column):
    row = coerce_responses({column: "2026-09-25T21:00:00-04:00"})
    assert row[column] == date(2026, 9, 25)
    row = coerce_responses({column: "2026-09-25"})
    assert row[column] == date(2026, 9, 25)


def test_unlisted_datetime_column_of_responses_is_utc():
    schema = RESPONSE_DATA_SCHEMA + [bigquery.SchemaField("someNewDateTime", "DATETIME")]
    row, errors = coerce_payload_to_schema({"someNewDateTime": "2026-09-25T08:00:00-04:00"}, schema, RESP)
    assert errors == [] and row["someNewDateTime"] == datetime(2026, 9, 25, 12, 0, 0)


def test_blank_still_becomes_null():
    row = coerce_responses({"checkinDateTime": "", "checkinReplyDate": "  "})
    assert row == {"checkinDateTime": None, "checkinReplyDate": None}


def test_other_targets_keep_the_local_wall_clock():
    # No convention for the target: the historical behaviour, exactly.
    row, errors = coerce_payload_to_schema(
        {"local_seen_at": "2026-09-25T08:30:00-04:00"}, FEEDBACK_SCHEMA_WITH_DATETIME,
        config.DATETIME_CONVENTIONS.get("feedback"))
    assert errors == [] and row["local_seen_at"] == datetime(2026, 9, 25, 8, 30, 0)
    assert config.DATETIME_CONVENTIONS.get("feedback") is None
    assert config.DATETIME_CONVENTIONS.get("triage_data") is None
    assert config.DATETIME_CONVENTIONS.get("users") is None


def test_default_convention_is_unchanged():
    field = bigquery.SchemaField("x", "DATETIME")
    assert coerce_value_to_bq_type("2026-09-25T08:30:00-04:00", field) == datetime(2026, 9, 25, 8, 30)


def test_unknown_convention_is_an_error():
    field = bigquery.SchemaField("x", "DATETIME")
    with pytest.raises(ValueError):
        coerce_value_to_bq_type("2026-09-25T08:30:00-04:00", field, "eastern")

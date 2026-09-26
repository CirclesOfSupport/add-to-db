"""
Triage and testimonial writes must be exactly what the base revision sends.

For each caller's payload shape, the statement and parameters the branch sends
to BigQuery are compared with what the frozen base module
(tests/baseline/bq_writer_b4ac22f.py) builds for the same payload.
"""
from __future__ import annotations

import pytest
from google.cloud import bigquery

import config
from baseline import bq_writer_b4ac22f as base
from conftest import FEEDBACK_SCHEMA, TRIAGE_SCHEMA

UNRECOGNIZED = {  # Unrecognized Message
    "message_id": "20260922083245510926-842596",
    "uuid": "5a2ac9de-5f3e-4ccb-897a-10a935c0721f",
    "sessionid": "5a2ac9de-5f3e-4ccb-897a-10a935c0721f2026-09-22T14:00:43.711342-04:00",
    "message": "Sorry you are?", "classification": "Other",
    "message_time": "2026-09-22T20:32:45.510853-04:00", "message_day": "2026-09-22",
    "classifier_details": {}, "referring_flow_id": "", "referring_flow_name": "",
}
INITIATE = {  # Initiate Triage Review
    "message_id": "20260922080726887653-773772",
    "triage_request_id": "2026-09-22_20:07:27_3873bb4c-f59f-4245-88df-ad837baf6aae",
    "triage_request_time": "2026-09-22T20:07:27.088721-04:00",
}
DETERMINATION = {  # Triage Determination
    "message_id": "20260922082426968367-673930", "determination": "LowConcern",
    "determination_time": "2026-09-22T20:27:38.145038-04:00",
}
TESTIMONIAL = {  # Testimonial: Approved
    "testimonial_id": "20260920030132819873-351788",
    "uuid": "eee6f4ff-24b2-4413-8e8a-5af9dda094b0",
    "sessionid": "eee6f4ff-24b2-4413-8e8a-5af9dda094b02026-09-20T14:36:29.555454-04:00",
    "origin_flow": "LIVE: NPS + Testimonial + Refer-A-Friend", "nps": 6,
    "testimonial_raw": "", "testimonial_edited": "", "dislike": "", "grade": "",
    "referral_intent": "", "timestamp": "2026-09-20T15:01:32.820325-04:00",
    "approved_timestamp": "2026-09-23T06:37:55.434356-04:00",
}

CASES = [
    ("triage_data", TRIAGE_SCHEMA, UNRECOGNIZED),
    ("triage_data", TRIAGE_SCHEMA, INITIATE),
    ("triage_data", TRIAGE_SCHEMA, DETERMINATION),
    ("feedback", FEEDBACK_SCHEMA, TESTIMONIAL),
]


def base_statement(target, schema, data):
    """The base worker's path: normalize, coerce, filter, build (no partition column)."""
    normalized, _ = base.normalize_payload_to_schema(data, schema)
    coerced, errors = base.coerce_payload_to_schema(normalized, schema)
    assert errors == []
    names = {f.name for f in schema}
    row = {k: v for k, v in coerced.items() if k in names}
    keys, _ = base.resolve_key_columns(config.UPSERT_KEYS[target], schema)
    sql = base.build_upsert_query(config.ALLOWED_TARGETS[target], row, keys, None)
    struct = base.build_struct_param(row, schema, "placeholder")
    return sql, struct


def dump(struct):
    return [(n, t, repr(struct.struct_values[n])) for n, t in struct.struct_types.items()]


@pytest.mark.parametrize("target,schema,data", CASES)
def test_statement_and_parameters_identical_to_base(svc, target, schema, data):
    body, status = svc.perform_upsert(target, dict(data))
    assert status == 200 and body["status"] == "ok", body
    job = svc.fake.statements[-1]
    sql, struct = base_statement(target, schema, dict(data))
    assert job.sql == sql
    assert list(job.params) == ["rows"]
    assert dump(job.params["rows"].values[0]) == dump(struct)


def test_triage_and_feedback_have_none_of_the_new_behaviour():
    for target in ("triage_data", "feedback", "users_copy", "responses_copy"):
        assert target not in config.DATETIME_CONVENTIONS
        assert target not in config.PRESERVE_ON_BLANK
        assert target not in config.KEYLESS_INSERT_TARGETS
        assert target not in config.PARTITION_COLUMNS
    assert "users" not in config.DATETIME_CONVENTIONS
    assert "users" not in config.KEYLESS_INSERT_TARGETS


def test_triage_timestamps_still_stored_as_utc_instants(svc):
    svc.perform_upsert("triage_data", dict(DETERMINATION))
    struct = svc.fake.statements[-1].params["rows"].values[0]
    value = struct.struct_values["determination_time"]
    assert struct.struct_types["determination_time"] == "TIMESTAMP"
    assert value.utcoffset().total_seconds() == 0 and value.hour == 0 and value.minute == 27


def test_message_day_date_unchanged(svc):
    svc.perform_upsert("triage_data", dict(UNRECOGNIZED))
    struct = svc.fake.statements[-1].params["rows"].values[0]
    assert str(struct.struct_values["message_day"]) == "2026-09-22"

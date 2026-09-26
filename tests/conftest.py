from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime

import pytest
from google.cloud import bigquery

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "src"))
sys.path.insert(0, HERE)

from fake_bq import FakeClient  # noqa: E402

F = bigquery.SchemaField

# Column sets (names, types, modes) of the live tables, trimmed to what the tests use.
RESPONSE_DATA_SCHEMA = [
    F("SessionID", "STRING"), F("checkinDateTime", "DATETIME"), F("checkinDate", "DATE"),
    F("uuid", "STRING"), F("userWeek", "INTEGER"), F("contactType", "STRING"),
    F("orgID", "STRING"), F("orgCode", "STRING"), F("class", "INTEGER"),
    F("wellnessDomain", "STRING"), F("checkinReply", "STRING"),
    F("checkinReplyDateTime", "DATETIME"), F("checkinReplyDate", "DATETIME"),
    F("checkinReplyNumerical", "FLOAT"), F("resourceOfferReplyDatetime", "DATETIME"),
    F("resourceOfferReplyDate", "DATE"), F("referralFollowUpUtilizedDateTime", "DATETIME"),
    F("referralFollowUpAttempts_str", "STRING"), F("subscribed", "STRING"),
    F("zipcode", "STRING"),
]
USERS_SCHEMA = [
    F("uuid", "STRING"), F("orgID", "STRING"), F("orgCode", "STRING"),
    F("userWeek", "INTEGER"), F("class", "INTEGER"), F("checkinrepliestotal", "INTEGER"),
    F("subscribed", "STRING"), F("testaccount", "STRING"),
]
TRIAGE_SCHEMA = [
    F("message_id", "STRING", mode="REQUIRED"), F("uuid", "STRING"), F("sessionid", "STRING"),
    F("message", "STRING"), F("classification", "STRING"), F("classifier_details", "JSON"),
    F("message_time", "TIMESTAMP"), F("message_day", "DATE"), F("triage_request_id", "STRING"),
    F("triage_request_time", "TIMESTAMP"), F("determination", "STRING"),
    F("determination_time", "TIMESTAMP"), F("triage_interaction_initiated_datetime", "STRING"),
    F("referring_flow_id", "STRING"), F("referring_flow_name", "STRING"),
]
FEEDBACK_SCHEMA = [
    F("uuid", "STRING", mode="REQUIRED"), F("sessionid", "STRING"), F("origin_flow", "STRING"),
    F("nps", "INTEGER"), F("testimonial_raw", "STRING"), F("testimonial_edited", "STRING"),
    F("dislike", "STRING"), F("grade", "STRING"), F("timestamp", "TIMESTAMP"),
    F("referral_intent", "STRING"), F("approved_timestamp", "TIMESTAMP"),
    F("testimonial_id", "STRING"),
]
# A DATETIME column on a target with no convention, to prove the default is untouched.
FEEDBACK_SCHEMA_WITH_DATETIME = FEEDBACK_SCHEMA + [F("local_seen_at", "DATETIME")]


@pytest.fixture
def svc(monkeypatch):
    """The service's main module with BigQuery replaced by DuckDB and Cloud Tasks by a list."""
    fake = FakeClient()
    monkeypatch.setattr(bigquery, "Client", lambda *a, **k: fake)
    for name in ("main", "tasks"):
        sys.modules.pop(name, None)
    main = importlib.import_module("main")
    import config
    for target, schema in (("responses", RESPONSE_DATA_SCHEMA), ("users", USERS_SCHEMA),
                           ("triage_data", TRIAGE_SCHEMA), ("feedback", FEEDBACK_SCHEMA)):
        fake.create(config.ALLOWED_TARGETS[target], schema)
    main._SCHEMA_CACHE.clear()
    queued = []
    monkeypatch.setattr(main, "enqueue_write", lambda path, target, data: queued.append((path, target, data)) or f"task-{len(queued)}")
    monkeypatch.setattr(main.time_module, "sleep", lambda s: None)
    main.fake = fake
    main.queued = queued
    main.table = lambda target: config.ALLOWED_TARGETS[target]
    return main


def utc(*args):
    return datetime(*args)

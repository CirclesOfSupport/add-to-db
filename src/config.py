from __future__ import annotations
import os
from datetime import date, datetime, time
from decimal import Decimal

PROJECT_ID = "early-alert-responses"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Cloud Tasks queue that the /ingest and /upsert webhook endpoints enqueue
# onto, so the webhook can respond as soon as work is durably queued instead
# of waiting for the BigQuery write to finish.
TASKS_PROJECT = os.getenv("TASKS_PROJECT", PROJECT_ID)
TASKS_LOCATION = os.getenv("TASKS_LOCATION", "us-east1")
TASKS_QUEUE = os.getenv("TASKS_QUEUE", "add-to-db-writes")

# Base URL of this Cloud Run service (e.g. https://add-to-db-xxxx-ue.a.run.app).
# Used both as the Cloud Tasks callback target and as the expected OIDC
# audience when verifying that a /tasks/* request really came from Cloud Tasks.
SERVICE_URL = os.getenv("SERVICE_URL", "")

# Service account Cloud Tasks uses to sign the OIDC token on its callback.
# Requests to /tasks/* are rejected unless the token's email matches this.
TASKS_INVOKER_SERVICE_ACCOUNT = os.getenv("TASKS_INVOKER_SERVICE_ACCOUNT", "")

# Only allow approved destinations.
ALLOWED_TARGETS: dict[str, str] = {
    "users": f"{PROJECT_ID}.RESPONSES.users",
    "responses": f"{PROJECT_ID}.RESPONSES.response_data",
    "triage_data": f"{PROJECT_ID}.RESPONSES.triage-message-data",
    "feedback": f"{PROJECT_ID}.RESPONSES.subscriber_feedback",
    "users_copy": f"{PROJECT_ID}.COPY.users",
    "responses_copy": f"{PROJECT_ID}.COPY.response_data",
}

UPSERT_KEYS: dict[str, list[str]] = {
    "users": ["uuid"],
    "responses": ["SessionID"],
    "triage_data": ["message_id"],
    "feedback": ["testimonial_id"],
    "users_copy": ["uuid"],
    "responses_copy": ["SessionID"],
}

# Partition column per target. When set, the MERGE gets an extra
# `AND T.<col> BETWEEN @min_dt AND @max_dt` predicate so BigQuery can prune
# to the partition(s) the incoming row belongs to. Only DATETIME columns are
# supported; the value comes from the row itself (one row per MERGE, so
# min == max). Targets not listed here get the plain key-only MERGE.
# Harmless on an unpartitioned table (predicate is redundant), so this can
# ship before the table is rebuilt.
PARTITION_COLUMNS: dict[str, str] = {
    "responses": "checkinDateTime",
}

# How each date/time column of a target is stored, where it differs from the
# plain "drop the offset, keep the local wall clock" default. Only targets
# listed here change; every other target keeps the default exactly.
#
# response_data stores its timestamps as the UTC wall clock of the instant the
# payload names (2026-05-26T11:36:27-04:00 is stored as 2026-05-26 15:36:27),
# and its one date-only DATETIME column as midnight of the payload's own local
# date. DATE columns keep the payload's own local date (the default).
#   "utc"            -> convert to UTC, then drop the offset
#   "local_midnight" -> midnight of the payload's local calendar date
# "*" is the convention for any DATETIME column of the target not listed.
DATETIME_CONVENTIONS: dict[str, dict[str, str]] = {
    "responses": {
        "checkinDateTime": "utc",
        "checkinReplyDateTime": "utc",
        "resourceOfferReplyDatetime": "utc",
        "referralFollowUpUtilizedDateTime": "utc",
        "checkinReplyDate": "local_midnight",
        "*": "utc",
    },
}

# Columns a blank payload value must never overwrite. A MERGE that matches a
# stored row keeps the stored value when the payload's is NULL.
PRESERVE_ON_BLANK: dict[str, list[str]] = {
    "responses": ["checkinDateTime"],
}

# Targets that accept a row with no key. Such a row cannot be matched to
# anything, so it is inserted as a new row instead of being rejected
# (sign-up events carry no session ID).
KEYLESS_INSERT_TARGETS: set[str] = {"responses"}

TYPE_CHECKERS = {
    "STRING": lambda v: isinstance(v, str),
    "JSON": lambda v: isinstance(v, (dict, list, str)),
    "INTEGER": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "INT64": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "FLOAT": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "FLOAT64": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "NUMERIC": lambda v: isinstance(v, (int, float, Decimal)) and not isinstance(v, bool),
    "BIGNUMERIC": lambda v: isinstance(v, (int, float, Decimal)) and not isinstance(v, bool),
    "BOOLEAN": lambda v: isinstance(v, bool),
    "BOOL": lambda v: isinstance(v, bool),
    "DATETIME": lambda v: isinstance(v, datetime) and v.tzinfo is None,
    "TIMESTAMP": lambda v: isinstance(v, datetime) and v.tzinfo is not None,
    "DATE": lambda v: isinstance(v, date) and not isinstance(v, datetime),
    "TIME": lambda v: isinstance(v, time),
    "BYTES": lambda v: isinstance(v, (bytes, str))
}
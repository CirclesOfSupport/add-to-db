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
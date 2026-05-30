from __future__ import annotations
import os
from datetime import date, datetime, time
from decimal import Decimal

PROJECT_ID = "early-alert-responses"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

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
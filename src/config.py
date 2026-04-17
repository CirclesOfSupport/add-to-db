from __future__ import annotations
import os

PROJECT_ID = "early-alert-responses"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Only allow approved destinations.
ALLOWED_TARGETS: dict[str, str] = {
    "users": f"{PROJECT_ID}.RESPONSES.users",
    "responses": f"{PROJECT_ID}.RESPONSES.response_data",
    #"users_and_responses": f"{PROJECT_ID}.RESPONSES.users_and_responses",
    "triage_data": f"{PROJECT_ID}.RESPONSES.triage-message-data",
    "users_copy": f"{PROJECT_ID}.COPY.users",
    "responses_copy": f"{PROJECT_ID}.COPY.response_data",
}

UPSERT_KEYS: dict[str, list[str]] = {
    "users": ["uuid"],
    "responses": ["SessionID"], #TODO check if this is the right key to upsert on
    #"users_and_responses": ["uuid", "session_id"],
    "triage_data": ["message_id"],
    "users_copy": ["uuid"],
    "responses_copy": ["SessionID"],
}

TYPE_CHECKERS = {
    "STRING": lambda v: isinstance(v, str),
    "JSON": lambda v: isinstance(v, (dict, list)),
    "INTEGER": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "FLOAT": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "BOOLEAN": lambda v: isinstance(v, bool),
    "DATETIME": lambda v: isinstance(v, str),
    "TIMESTAMP": lambda v: isinstance(v, str),
    "DATE": lambda v: isinstance(v, str),
    "TIME": lambda v: isinstance(v, str),
}
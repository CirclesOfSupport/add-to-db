from __future__ import annotations
import os

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Only allow approved destinations.
ALLOWED_TARGETS: dict[str, str] = {
    "users": f"{PROJECT_ID}.RESPONSES.users",
    "responses": f"{PROJECT_ID}.RESPONSES.responses",
    "users_and_responses": f"{PROJECT_ID}.RESPONSES.users_and_responses",
    "triage_data": f"{PROJECT_ID}.RESPONSES.triage-message-data",
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
from __future__ import annotations
from flask import Request
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token
from config import WEBHOOK_SECRET, SERVICE_URL, TASKS_INVOKER_SERVICE_ACCOUNT

_google_auth_request = google_auth_requests.Request()


def is_authorized(request: Request) -> bool:
    if not WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Webhook-Secret", "") == WEBHOOK_SECRET


def is_task_request_authorized(request: Request) -> bool:
    """
    Verifies /tasks/* callbacks actually came from our Cloud Tasks queue by
    checking the OIDC token Cloud Tasks attaches, rather than trusting the
    caller. Without this, anyone who finds the worker URL could trigger
    BigQuery writes without going through webhook auth or validation.
    """
    if not SERVICE_URL or not TASKS_INVOKER_SERVICE_ACCOUNT:
        return False

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False

    token = auth_header[len("Bearer "):]

    try:
        claims = google_id_token.verify_oauth2_token(
            token, _google_auth_request, audience=SERVICE_URL
        )
    except Exception:
        return False

    return (
        claims.get("email_verified") is True
        and claims.get("email") == TASKS_INVOKER_SERVICE_ACCOUNT
    )
from __future__ import annotations
from flask import Request
from config import WEBHOOK_SECRET


def is_authorized(request: Request) -> bool:
    if not WEBHOOK_SECRET:
        return True
    return request.headers.get("X-Webhook-Secret", "") == WEBHOOK_SECRET
"""
Send recorded check-in calls to a deployed add-to-db (a STAGING service) over HTTP, at their
real spacing, in 8 lanes keyed by contact so each contact's calls keep their order. Used to
walk the deployed single-writer path end to end: /upsert -> staging table -> named flush task
on the flush queue -> /tasks/flush -> DEV tables. Never point it at the production service.

    python tools/post_calls.py --url https://add-to-db-staging-xxxx.a.run.app --minutes 15
    (set ADD_TO_DB_SECRET if the service has WEBHOOK_SECRET)
Reads the calls from the webhook log (read-only); prints status counts.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests
from _harness import make_client
from replay_webhook_log import fetch_calls


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--start", default="2026-08-31 19:00:00", help="UTC; default: the busiest hour")
    ap.add_argument("--minutes", type=float, default=15)
    args = ap.parse_args()
    if "add-to-db-staging" not in args.url:
        raise SystemExit("refusing: --url must be the add-to-db-staging service")
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    calls = fetch_calls(make_client(), start, start + timedelta(minutes=args.minutes))
    headers = {"Content-Type": "application/json"}
    if os.getenv("ADD_TO_DB_SECRET"):
        headers["X-Webhook-Secret"] = os.environ["ADD_TO_DB_SECRET"]
    lanes = [[] for _ in range(8)]
    for c in calls:
        uid = next((str(d.get("uuid")) for _, d in c["items"] if d.get("uuid")), str(c["id"]))
        lanes[int(hashlib.md5(uid.encode()).hexdigest(), 16) % 8].append(c)
    statuses, lock = Counter(), threading.Lock()
    t0 = time.monotonic()
    print(f"posting {len(calls)} calls over {args.minutes:.0f} minutes to {args.url}/upsert", flush=True)

    def lane(items):
        s = requests.Session()
        for c in items:
            wait = t0 + (c["fired_at"] - start).total_seconds() - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                r = s.post(f"{args.url}/upsert", headers=headers, timeout=30,
                           json={"tables": [{"table": t, "data": d} for t, d in c["items"]]})
                key = str(r.status_code)
            except requests.RequestException as exc:
                key = f"error {type(exc).__name__}"
            with lock:
                statuses[key] += 1

    threads = [threading.Thread(target=lane, args=(items,)) for items in lanes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"done in {time.monotonic() - t0:.0f} s; responses: {dict(statuses)}")
    print(f"last call posted at {datetime.now(timezone.utc):%H:%M:%S} UTC")


if __name__ == "__main__":
    main_()

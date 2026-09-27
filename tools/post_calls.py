"""
Send recorded check-in calls to a deployed add-to-db (a STAGING service) over HTTP, at their
real spacing, in 8 lanes keyed by contact so each contact's calls keep their order. Used to
walk the deployed single-writer path end to end: /upsert -> staging table -> named flush task
on the flush queue -> /tasks/flush -> DEV tables. Never point it at the production service.

    python tools/post_calls.py --url https://add-to-db-staging-xxxx.a.run.app --minutes 15
    python tools/post_calls.py --url https://add-to-db-staging-xxxx.a.run.app --triage-negative
    (set ADD_TO_DB_SECRET if the service has WEBHOOK_SECRET)
The staging service does not allow unauthenticated calls: every request carries an identity
token from `gcloud auth print-identity-token` (the caller needs run.invoker on the service).
Reads the calls from the webhook log (read-only); prints status counts.

--triage-negative posts ONE synthetic triage payload (message_id staging-negative-<stamp>),
which the staging service's account has no right to write, waits 60 s, and reads (read-only)
RESPONSES.triage-message-data for that message_id. PASS = a non-2xx answer and 0 rows.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests
from _harness import PROJECT, make_client
from google.cloud import bigquery
from replay_webhook_log import fetch_calls


class IdentityToken:
    """Identity token of the active gcloud account, refreshed every 40 minutes."""

    def __init__(self):
        self.gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud")
        self.lock, self.value, self.at = threading.Lock(), None, 0.0

    def get(self):
        with self.lock:
            if self.value is None or time.monotonic() - self.at > 2400:
                out = subprocess.run([self.gcloud, "auth", "print-identity-token"], capture_output=True, text=True)
                if out.returncode != 0:
                    raise SystemExit("gcloud could not print an identity token; run: gcloud auth login")
                self.value, self.at = out.stdout.strip(), time.monotonic()
            return self.value


def triage_negative(url, headers, token):
    """One triage payload to staging must fail (no rights on RESPONSES) and land nowhere."""
    mid = f"staging-negative-{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
    body = {"table": "triage_data", "data": {"message_id": mid, "determination": "LowConcern",
                                             "determination_time": datetime.now(timezone.utc).isoformat()}}
    r = requests.post(f"{url}/upsert", json=body, timeout=60,
                      headers={**headers, "Authorization": f"Bearer {token.get()}"})
    print(f"triage payload {mid}: HTTP {r.status_code} {r.text[:300]!r}")
    print("waiting 60 s for any queued write to be attempted ...", flush=True)
    time.sleep(60)
    client = make_client()
    n = list(client.query(
        f"SELECT COUNT(*) n FROM `{PROJECT}.RESPONSES.triage-message-data` WHERE message_id = @m",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("m", "STRING", mid)])
    ).result())[0]["n"]
    ok = r.status_code >= 400 and n == 0
    print(f"rows in RESPONSES.triage-message-data with that message_id: {n}")
    print("NEGATIVE CHECK:", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--start", default="2026-08-31 19:00:00", help="UTC; default: the busiest hour")
    ap.add_argument("--minutes", type=float, default=15)
    ap.add_argument("--triage-negative", action="store_true", help="post one triage payload; it must fail")
    args = ap.parse_args()
    if "add-to-db-staging" not in args.url:
        raise SystemExit("refusing: --url must be the add-to-db-staging service")
    args.url = args.url.rstrip("/")
    headers = {"Content-Type": "application/json"}
    if os.getenv("ADD_TO_DB_SECRET"):
        headers["X-Webhook-Secret"] = os.environ["ADD_TO_DB_SECRET"]
    token = IdentityToken()
    if args.triage_negative:
        triage_negative(args.url, headers, token)
    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    calls = fetch_calls(make_client(), start, start + timedelta(minutes=args.minutes))
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
                r = s.post(f"{args.url}/upsert", headers={**headers, "Authorization": f"Bearer {token.get()}"}, timeout=30,
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

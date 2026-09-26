"""
Replay real check-in webhook calls through the service's own worker code,
in-process, against DEV copies of response_data and users, at production
concurrency. Nothing goes through Cloud Tasks.

Calls come from OPS.webhook_log_detail (the old writer's request bodies; each
body's Users and Responses halves become the users and responses items the
add-to-db flow sends, in that order). They are released at their real
fired_at spacing (--speed compresses it) into a worker pool shaped like the
queue: FIFO, --workers at a time, a failed item retried up to --max-attempts
times with backoff doubling from 0.1 s.

Variants:
  concurrent   the production shape: any worker takes the next item
  per_session  every item for one SessionID (responses) or uuid (users) runs on
               one lane, in order; the per-session serialization candidate

Sessions whose first logged call falls before the window are seeded with their
current RESPONSES row, so later calls must MATCH it (as in production).

Report per variant: sessions whose final row differs from the last call's
values (per column), sessions with more than one row, the same among sessions
whose first two calls were under a second apart, keyless rows vs keyless calls,
users write failures and checkinrepliestotal type/value, items failed after all
attempts, conflict retries, bytes billed, item latency.

    python tools/replay_webhook_log.py
    python tools/replay_webhook_log.py --start "2026-09-24 16:00:00" --end "2026-09-24 17:00:00" --variant both
Writes replay_report_<variant>_<stamp>.json next to where it runs.
"""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import statistics
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from urllib.parse import unquote

from _harness import PROJECT, JobLog, load_service, make_client, stamp

from google.cloud import bigquery

LOG = f"{PROJECT}.OPS.webhook_log_detail"
PATH = "/get-responses_v2/v2/add"
BODIES_FROM = "2026-08-24"   # request bodies are complete in the log from this week on


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def fetch_calls(client, start, end):
    sql = (f"SELECT httplog_id, fired_at, request_body FROM `{LOG}` "
           f"WHERE request_path = @p AND fired_at >= @s AND fired_at < @e ORDER BY fired_at, httplog_id")
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("p", "STRING", PATH),
        bigquery.ScalarQueryParameter("s", "TIMESTAMP", start),
        bigquery.ScalarQueryParameter("e", "TIMESTAMP", end)])
    calls = []
    for r in client.query(sql, job_config=cfg).result():
        try:
            body = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        items = [(t, body[k]) for k, t in (("Users", "users"), ("Responses", "responses"))
                 if isinstance(body.get(k), dict)]
        if items:
            calls.append({"id": r["httplog_id"], "fired_at": r["fired_at"], "items": items})
    return calls


def session_of(data):
    return unquote(str(data.get("sessionID") or data.get("SessionID") or "")).strip()


def first_seen(client, sids, start):
    """SessionIDs (decoded) whose first logged call is before the window."""
    if not sids:
        return set()
    sql = (f"SELECT sid FROM (SELECT REPLACE(REPLACE(JSON_VALUE(request_body, '$.Responses.sessionID'), '%3A', ':'), '%2B', '+') sid, "
           f"MIN(fired_at) first_at FROM `{LOG}` WHERE request_path = @p AND fired_at >= TIMESTAMP(@b) AND fired_at < @s "
           f"GROUP BY 1) WHERE sid IN UNNEST(@ids)")
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("p", "STRING", PATH),
        bigquery.ScalarQueryParameter("b", "STRING", BODIES_FROM),
        bigquery.ScalarQueryParameter("s", "TIMESTAMP", start),
        bigquery.ArrayQueryParameter("ids", "STRING", sorted(sids))])
    return {r["sid"] for r in client.query(sql, job_config=cfg).result()}


def make_tables(client, run, variant, prior_sids, uuids):
    rd = f"{PROJECT}.DEV.adb_replay_{variant}_{run}_response_data"
    us = f"{PROJECT}.DEV.adb_replay_{variant}_{run}_users"
    client.query(f"CREATE TABLE `{rd}` LIKE `{PROJECT}.RESPONSES.response_data`").result()
    client.query(f"CREATE TABLE `{us}` LIKE `{PROJECT}.RESPONSES.users`").result()
    client.query(f"INSERT INTO `{rd}` SELECT * FROM `{PROJECT}.RESPONSES.response_data` WHERE SessionID IN UNNEST(@ids)",
                 job_config=bigquery.QueryJobConfig(query_parameters=[
                     bigquery.ArrayQueryParameter("ids", "STRING", sorted(prior_sids))])).result()
    client.query(f"INSERT INTO `{us}` SELECT * FROM `{PROJECT}.RESPONSES.users` WHERE uuid IN UNNEST(@ids)",
                 job_config=bigquery.QueryJobConfig(query_parameters=[
                     bigquery.ArrayQueryParameter("ids", "STRING", sorted(uuids))])).result()
    return rd, us


class Runner:
    """Queue-shaped dispatcher: FIFO, N workers (or N ordered lanes), retries with doubling backoff."""

    def __init__(self, svc, workers, max_attempts, per_session):
        self.svc, self.workers, self.max_attempts, self.per_session = svc, workers, max_attempts, per_session
        self.cv = threading.Condition()
        self.ready = defaultdict(list) if per_session else []   # heap of (due, seq, item)
        self.seq = 0
        self.pending = 0
        self.outcomes = []
        self.done = False

    def lane(self, item):
        target, data = item["target"], item["data"]
        key = session_of(data) if target == "responses" else str(data.get("uuid", ""))
        key = key or str(data.get("uuid", ""))
        return int(hashlib.md5(f"{target}:{key}".encode()).hexdigest(), 16) % self.workers

    def push(self, item, due):
        with self.cv:
            self.seq += 1
            q = self.ready[self.lane(item)] if self.per_session else self.ready
            heapq.heappush(q, (due, self.seq, item))
            self.cv.notify_all()

    def take(self, lane):
        with self.cv:
            while True:
                q = self.ready[lane] if self.per_session else self.ready
                now = time.monotonic()
                if q and q[0][0] <= now:
                    return heapq.heappop(q)[2]
                if self.done and not q:
                    return None
                self.cv.wait(timeout=(q[0][0] - now) if q else 0.5)

    def attempt(self, item):
        item["attempts"] += 1
        t0 = time.monotonic()
        try:
            body, status = self.svc.perform_upsert(item["target"], dict(item["data"]))
        except Exception as exc:   # a crash is a 500 to the queue
            body, status = {"details": repr(exc)}, 500
        item["ms"].append(int((time.monotonic() - t0) * 1000))
        detail = str(body.get("details") or body.get("errors") or "")
        if "Could not serialize" in detail:
            item["conflicts"] += 1
        return body, status

    def backoff(self, item):
        return min(0.1 * 2 ** (item["attempts"] - 1), 3600)

    def finish(self, item, body, status):
        item["final_status"], item["final_body"] = status, body
        with self.cv:
            self.outcomes.append(item)
            self.pending -= 1
            self.cv.notify_all()

    def work(self, lane):
        while True:
            item = self.take(lane)
            if item is None:
                return
            body, status = self.attempt(item)
            if self.per_session:
                # the lane retries in place, so nothing behind it can overtake
                while status >= 500 and item["attempts"] < self.max_attempts:
                    time.sleep(self.backoff(item))
                    body, status = self.attempt(item)
            elif status >= 500 and item["attempts"] < self.max_attempts:
                self.push(item, time.monotonic() + self.backoff(item))
                continue
            self.finish(item, body, status)

    def run(self, calls, start, speed):
        self.done = False
        self.pending = sum(len(c["items"]) for c in calls)
        threads = [threading.Thread(target=self.work, args=(i,), daemon=True) for i in range(self.workers)]
        for t in threads:
            t.start()
        t0 = time.monotonic()
        base = start
        for c in calls:
            due = t0 + (c["fired_at"] - base).total_seconds() / speed
            wait = due - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            for target, data in c["items"]:   # users then responses, as the flow sends them
                self.push({"target": target, "data": data, "call": c["id"], "fired_at": c["fired_at"],
                           "attempts": 0, "conflicts": 0, "ms": []}, time.monotonic())
        with self.cv:
            while self.pending > 0:
                self.cv.wait(timeout=1)
            self.done = True
            self.cv.notify_all()
        for t in threads:
            t.join()


def expected_rows(svc, calls):
    """Per SessionID and per users uuid: each column's value from the LAST call carrying it."""
    import config
    from bq_writer import coerce_payload_to_schema, normalize_payload_to_schema
    rd_schema = svc.client.get_table(config.ALLOWED_TARGETS["responses"]).schema
    us_schema = svc.client.get_table(config.ALLOWED_TARGETS["users"]).schema
    names_rd, names_us = {f.name for f in rd_schema}, {f.name for f in us_schema}
    preserve = {c for c in config.PRESERVE_ON_BLANK.get("responses", [])}
    resp, users, first_two = defaultdict(dict), defaultdict(dict), defaultdict(list)
    keyless_calls = 0
    for c in calls:
        for target, data in c["items"]:
            schema = rd_schema if target == "responses" else us_schema
            norm, _ = normalize_payload_to_schema(dict(data), schema)
            row, errs = coerce_payload_to_schema(norm, schema, config.DATETIME_CONVENTIONS.get(target))
            row = {k: v for k, v in row.items() if k in (names_rd if target == "responses" else names_us)}
            if target == "responses":
                sid = row.get("SessionID")
                if not sid:
                    keyless_calls += 1
                    continue
                for k, v in row.items():
                    if k in preserve and v is None and k in resp[sid]:
                        continue
                    resp[sid][k] = v
                if len(first_two[sid]) < 2:
                    first_two[sid].append(c["fired_at"])
            else:
                if row.get("uuid"):
                    users[row["uuid"]].update(row)
    return resp, users, first_two, keyless_calls


def same(a, b):
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(float(a) - float(b)) < 1e-9
    return a == b


def report(client, svc, variant, runner, calls, prior, rd, us, billed, statements, window):
    resp_exp, users_exp, first_two, keyless_calls = expected_rows(svc, calls)
    rows = defaultdict(list)
    for r in client.query(f"SELECT * FROM `{rd}` WHERE SessionID IN UNNEST(@ids)", job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", sorted(resp_exp))])).result():
        rows[r["SessionID"]].append(dict(r))
    keyless_rows = list(client.query(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID IS NULL").result())[0]["n"]

    wrong_sessions, col_mismatch, examples = 0, Counter(), []
    dup_sessions, dup_new_fast, new_fast = 0, 0, 0
    missing = 0
    for sid, exp in resp_exp.items():
        got = rows.get(sid, [])
        fast = sid not in prior and len(first_two[sid]) == 2 and (first_two[sid][1] - first_two[sid][0]).total_seconds() < 1
        new_fast += fast
        if not got:
            missing += 1
            continue
        if len(got) > 1:
            dup_sessions += 1
            dup_new_fast += fast
        bad = [k for k, v in exp.items() if not any(same(g.get(k), v) for g in got[:1])] if len(got) == 1 else \
              [k for k, v in exp.items() if not all(same(g.get(k), v) for g in got)]
        if bad:
            wrong_sessions += 1
            col_mismatch.update(bad)
            if len(examples) < 15:
                examples.append({"SessionID": sid, "columns": bad[:6]})

    users_rows = defaultdict(list)
    for r in client.query(f"SELECT uuid, checkinrepliestotal FROM `{us}`").result():
        users_rows[r["uuid"]].append(r["checkinrepliestotal"])
    us_type = {f.name: f.field_type for f in client.get_table(us).schema}.get("checkinrepliestotal")
    users_wrong = sum(1 for u, e in users_exp.items()
                      if "checkinrepliestotal" in e and e["checkinrepliestotal"] is not None
                      and not all(v == e["checkinrepliestotal"] for v in users_rows.get(u, [None])))

    failed = [o for o in runner.outcomes if o["final_status"] >= 400 or o["final_body"].get("status") != "ok"]
    fail_reasons = Counter(f"{o['target']} {o['final_status']} {str(o['final_body'].get('error') or o['final_body'].get('errors') or o['final_body'].get('details'))[:160]}" for o in failed)
    ms = [m for o in runner.outcomes for m in o["ms"]]
    out = {
        "variant": variant, "window": window, "calls": len(calls), "items": len(runner.outcomes),
        "sessions": len(resp_exp), "sessions_seeded_from_production": len(prior & set(resp_exp)),
        "sessions_final_row_differs_from_last_call": wrong_sessions,
        "column_mismatch_counts": dict(col_mismatch.most_common()),
        "mismatch_examples": examples,
        "sessions_with_no_row": missing,
        "sessions_with_more_than_one_row": dup_sessions,
        "new_sessions_first_two_calls_under_1s": new_fast,
        "  of_which_more_than_one_row": dup_new_fast,
        "keyless_calls": keyless_calls, "keyless_rows": keyless_rows,
        "users_uuids": len(users_exp), "users_checkinrepliestotal_wrong": users_wrong,
        "users_checkinrepliestotal_column_type": us_type,
        "items_failed_after_all_attempts": len(failed), "failure_reasons": dict(fail_reasons.most_common(10)),
        "items_retried": sum(1 for o in runner.outcomes if o["attempts"] > 1),
        "conflict_errors_seen": sum(o["conflicts"] for o in runner.outcomes),
        "statements": statements, "bytes_billed_total": billed,
        "item_ms_p50": statistics.median(ms) if ms else None,
        "item_ms_p95": sorted(ms)[int(0.95 * (len(ms) - 1))] if ms else None,
        "dev_tables": [rd, us],
    }
    return out


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-09-24 16:00:00", help="UTC")
    ap.add_argument("--end", default="2026-09-24 17:00:00", help="UTC")
    ap.add_argument("--workers", type=int, default=15)
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--speed", type=float, default=1.0, help="1.0 = real time")
    ap.add_argument("--variant", choices=["concurrent", "per_session", "both"], default="both")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    start, end = ts(args.start), ts(args.end)
    client = make_client()
    run = stamp()
    calls = fetch_calls(client, start, end)
    sids = {session_of(d) for c in calls for t, d in c["items"] if t == "responses"} - {""}
    uuids = {str(d.get("uuid")) for c in calls for t, d in c["items"] if d.get("uuid")}
    prior = first_seen(client, sids, start)
    print(f"{len(calls)} calls, {len(sids)} sessions ({len(prior)} began before the window), {len(uuids)} contacts")

    variants = ["concurrent", "per_session"] if args.variant == "both" else [args.variant]
    svc = None
    for variant in variants:
        rd, us = make_tables(client, run, variant, prior, uuids)
        if svc is None:
            svc = load_service(client, {"responses": rd, "users": us})
            jobs = JobLog(client)
        else:
            import config
            config.ALLOWED_TARGETS["responses"], config.ALLOWED_TARGETS["users"] = rd, us
        svc.client = client
        jobs.jobs.clear()
        print(f"[{variant}] replaying into {rd} and {us} ...", flush=True)
        runner = Runner(svc, args.workers, args.max_attempts, per_session=(variant == "per_session"))
        runner.run(calls, start, args.speed)
        billed = sum((j.total_bytes_billed or 0) for j in jobs.jobs)
        statements = len(jobs.jobs)
        out = report(client, svc, variant, runner, calls, prior, rd, us, billed, statements, [args.start, args.end])
        path = f"replay_report_{variant}_{run}.json"
        with open(path, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(json.dumps({k: v for k, v in out.items() if k not in ("mismatch_examples",)}, indent=2, default=str))
        print(f"report: {path}")
        if not args.keep:
            client.delete_table(rd)
            client.delete_table(us)
            print(f"dropped {rd} and {us}")


if __name__ == "__main__":
    main_()

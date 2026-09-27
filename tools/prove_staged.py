"""
Prove the live single-writer hookup on real BigQuery, against DEV copies.

Creates in DEV: response_data and users copies (LIKE, empty) and the four
single-writer tables (tools/staged_ddl.py), points the service at them, and
runs the real /upsert handler (Flask test client; the flush kick is recorded,
not sent) and the real run_flush_cycle. Drops everything unless --keep.

Scenarios:
  1  a staged call is readable in staging right after /upsert returns 202
  2  a flush writes the last call per session and advances the watermark
  3  a stale carry-over reply (reply time before check-in) is written as not replied
  4  calls without a session are inserted; a bad call is dead-lettered, the rest written
  5  two writers flushing at once: exactly one commits, no duplicate rows
  6  flushes while another writer keeps updating users (the nightly contacts sync):
     a failed flush writes nothing and a retry commits
  7  the health check sees backlog, failures and dead letters

    python tools/prove_staged.py
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from _harness import PROJECT, JobLog, load_service, make_client, stamp
from staged_ddl import ddl

from google.cloud import bigquery

UUID = "00000000-0000-4000-8000-000000000002"   # not a real contact
CHECKIN = "2026-09-18T16%3A01%3A25.006790-04%3A00"
CHECKIN_UTC = datetime(2026, 9, 18, 20, 1, 25, 6790)


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    client = make_client()
    run = stamp()
    prefix = f"adb_proof_{run}_"
    ds = f"{PROJECT}.DEV"
    rd, us = f"{ds}.{prefix}response_data", f"{ds}.{prefix}users"
    client.query(f"CREATE TABLE `{rd}` LIKE `{PROJECT}.RESPONSES.response_data`").result()
    client.query(f"CREATE TABLE `{us}` LIKE `{PROJECT}.RESPONSES.users`").result()
    for stmt in ddl("DEV", prefix):
        client.query(stmt).result()
    print(f"DEV tables: {prefix}*")

    import config
    config.STAGING_TABLE = f"{ds}.{prefix}staging"
    config.DEAD_LETTER_TABLE = f"{ds}.{prefix}dead_letter"
    config.FLUSH_LOG_TABLE = f"{ds}.{prefix}flush_log"
    config.FLUSH_STATE_TABLE = f"{ds}.{prefix}flush_state"
    config.STAGED_TARGETS = {"users", "responses"}
    config.FLUSH_SAFETY_S = 5     # production default is 20; shorter here so the proof runs in minutes
    svc = load_service(client, {"responses": rd, "users": us})
    kicks = []
    svc.enqueue_flush = lambda bucket, when: kicks.append(bucket)
    web = svc.app.test_client()
    results = []

    def check(name, ok, detail=""):
        results.append((name, bool(ok), detail))
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"\n      {detail}" if not ok and detail else ""), flush=True)

    def sid(n):
        return f"staged-proof-{run}-{n}"

    def body(n, reply, **extra):
        d = {"uuid": UUID, "orgID": "0", "orgCode": "staged-proof", "sessionID": sid(n), "contactType": "CheckIn",
             "checkinDateTime": CHECKIN, "wellnessDomain": "Relational", "checkinReply": reply, "userWeek": "1"}
        d.update(extra)
        return d

    def post(*tables):
        r = web.post("/upsert", json={"tables": [{"table": t, "data": d} for t, d in tables]})
        return r.status_code, r.get_json()

    def rows(sql, **p):
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in p.items()])
        return [dict(r) for r in client.query(sql, job_config=cfg).result()]

    timing = {}

    def flush_now():
        # real clock, as in production: wait out the safety window so every call staged so far is due
        time.sleep(config.FLUSH_SAFETY_S + 1)
        t = time.monotonic()
        out = svc.run_flush_cycle()
        timing["last"] = time.monotonic() - t
        return out

    # 1. durable and readable right after the 202
    status, out = post(("users", {"uuid": UUID, "checkInRepliesTotal": "1"}), ("responses", body(1, "No")))
    staged_now = rows(f"SELECT COUNT(*) n FROM `{config.STAGING_TABLE}` WHERE request_id = @r",
                      r=out["results"][0]["staged_request_id"]) if status == 202 else [{"n": 0}]
    check("1 staged and readable immediately after 202", status == 202 and staged_now[0]["n"] == 2 and len(kicks) == 1,
          f"status {status}, staged rows {staged_now[0]['n']}, kicks {kicks}")

    # 2. last call per session wins; watermark advances
    post(("users", {"uuid": UUID, "checkInRepliesTotal": "2"}), ("responses", body(1, "Yes")))
    out = flush_now()
    secs = timing["last"]
    r = rows(f"SELECT checkinReply, checkinDateTime FROM `{rd}` WHERE SessionID = @s", s=sid(1))
    u = rows(f"SELECT checkinrepliestotal FROM `{us}` WHERE uuid = @u", u=UUID)
    st = rows(f"SELECT version FROM `{config.FLUSH_STATE_TABLE}`")
    check("2 last call wins, one row, watermark advanced",
          out["status"] == "ok" and len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["checkinDateTime"] == CHECKIN_UTC
          and u == [{"checkinrepliestotal": 2}] and st[0]["version"] == 1,
          f"flush {out}, rows {r}, users {u}, state {st}")
    print(f"      flush of {out.get('items')} calls in {secs:.1f} s, {out.get('statements')} statements")

    # 3. stale carry-over
    post(("responses", body(3, "Yes", checkinReplyDateTime="2026-09-11T10%3A00%3A00-04%3A00", checkinReplyNumerical="7")))
    flush_now()
    r = rows(f"SELECT checkinReply, checkinReplyDateTime, checkinReplyNumerical, wellnessDomain FROM `{rd}` WHERE SessionID = @s", s=sid(3))
    check("3 stale reply written as not replied", len(r) == 1 and r[0]["checkinReply"] is None
          and r[0]["checkinReplyDateTime"] is None and r[0]["checkinReplyNumerical"] is None and r[0]["wellnessDomain"] == "Relational", r)

    # 4. keyless inserted; bad call dead-lettered (staged directly: /upsert would reject it with a 400)
    marker = f"staged-proof-{run}-keyless"
    post(("responses", {"uuid": UUID, "orgCode": marker, "sessionID": "", "contactType": "Subscribe", "checkinDateTime": CHECKIN}))
    svc.stage_calls([("responses", body(4, "Yes", userWeek="not-a-number"))], datetime.now(timezone.utc))
    out = flush_now()
    k = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID IS NULL AND orgCode = @m", m=marker)
    bad = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID = @s", s=sid(4))
    dl = rows(f"SELECT stage, target FROM `{config.DEAD_LETTER_TABLE}`")
    check("4 keyless inserted, bad call dead-lettered", k[0]["n"] == 1 and bad[0]["n"] == 0
          and {"stage": "flush", "target": "responses"} in dl, f"keyless {k}, bad {bad}, dead {dl}, flush {out}")

    # 5. two writers at once
    for n in range(5, 10):
        post(("users", {"uuid": UUID, "checkInRepliesTotal": str(n)}), ("responses", body(n, f"r{n}")))
    outcomes = []

    def writer():
        try:
            outcomes.append(("ok", flush_now()))
        except Exception as exc:
            outcomes.append(("failed", repr(exc)[:200]))

    th = [threading.Thread(target=writer) for _ in range(2)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    dups = rows(f"SELECT COUNT(*) n FROM (SELECT SessionID FROM `{rd}` WHERE SessionID IS NOT NULL GROUP BY 1 HAVING COUNT(*) > 1)")
    have = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID LIKE @p", p=f"staged-proof-{run}-%")
    committed = [o for o in outcomes if o[0] == "ok" and o[1].get("status") == "ok" and o[1].get("items", 0) > 0]
    if not committed:     # both lost: a retry must commit everything
        flush_now()
        have = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID LIKE @p", p=f"staged-proof-{run}-%")
    check("5 two writers: at most one commits, no duplicates", len(committed) <= 1 and dups[0]["n"] == 0 and have[0]["n"] == 7,
          f"outcomes {outcomes}, duplicate sessions {dups}, session rows {have}")

    # 6. a competing users writer (contacts-sync shape) while flushing; failed flushes retried
    stop = threading.Event()
    competing = {"n": 0, "errors": 0}

    def contacts_sync():
        while not stop.is_set():
            try:
                client.query(f"UPDATE `{us}` SET testaccount = testaccount WHERE TRUE").result()
                competing["n"] += 1
            except Exception:
                competing["errors"] += 1

    cs = threading.Thread(target=contacts_sync)
    cs.start()
    fails, commits = 0, 0
    for n in range(10, 16):
        post(("users", {"uuid": UUID, "checkInRepliesTotal": str(n)}), ("responses", body(n, f"r{n}")))
        for attempt in range(6):
            try:
                flush_now()
                commits += 1
                break
            except Exception:
                fails += 1
                time.sleep(0.5 * 2 ** attempt)
    stop.set()
    cs.join()
    have = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID LIKE @p", p=f"staged-proof-{run}-%")
    u = rows(f"SELECT checkinrepliestotal FROM `{us}` WHERE uuid = @u", u=UUID)
    check("6 competing users writer: every flush committed after retries", commits == 6 and have[0]["n"] == 13
          and u == [{"checkinrepliestotal": 15}],
          f"commits {commits}, failed attempts {fails}, competing updates {competing}, session rows {have}, users {u}")
    print(f"      failed flush attempts during the competing writer: {fails}; competing updates: {competing['n']}")

    # 7. health
    h = svc.flush_health()
    check("7 health check reads backlog, failures and dead letters",
          h["backlog_calls"] == 0 and h["dead_letters_24h"] >= 1 and h["consecutive_failed_flushes"] == 0, h)

    print()
    if args.keep:
        print(f"kept DEV tables {prefix}*")
    else:
        for name in ("response_data", "users", "staging", "dead_letter", "flush_log", "flush_state"):
            client.delete_table(f"{ds}.{prefix}{name}", not_found_ok=True)
        print(f"dropped DEV tables {prefix}*")
    failed = sum(1 for _, ok, _ in results if not ok)
    print(f"{len(results) - failed} of {len(results)} scenarios passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main_()

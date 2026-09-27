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
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from _harness import PROJECT, JobLog, load_service, make_client, stamp
from staged_ddl import ddl, dry_run

from google.cloud import bigquery

UUID = "00000000-0000-4000-8000-000000000002"   # not a real contact
NIGHTLY_ACCOUNT = "853176470965-compute@developer.gserviceaccount.com"   # runs contacts-sync and the vamc syncs
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
    statements = ddl("DEV", prefix)
    creates = [s_ for s_ in statements if s_.startswith("CREATE")]
    failed = dry_run(client, creates)          # BigQuery validates the DDL before anything is created
    if failed:
        raise SystemExit("DDL does not validate on BigQuery (nothing was created):\n  " + "\n  ".join(failed))
    print(f"DDL: {len(creates)} of {len(creates)} CREATE statements valid on BigQuery")
    names = ("response_data", "users", "staging", "dead_letter", "flush_log", "flush_state",
             "night_users", "night_response_data")
    try:
        _run(args, client, run, prefix, ds, rd, us, statements)
    finally:
        if args.keep:
            print(f"kept DEV tables {prefix}*")
        else:
            for name in names:
                client.delete_table(f"{ds}.{prefix}{name}", not_found_ok=True)
            print(f"dropped DEV tables {prefix}*")


def _run(args, client, run, prefix, ds, rd, us, statements):
    client.query(f"CREATE TABLE `{rd}` LIKE `{PROJECT}.RESPONSES.response_data`").result()
    client.query(f"CREATE TABLE `{us}` LIKE `{PROJECT}.RESPONSES.users`").result()
    for stmt in statements:
        if not stmt.startswith("CREATE"):
            bad = dry_run(client, [stmt])
            if bad:
                raise SystemExit("DDL INSERT does not validate: " + bad[0])
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
          and u == [{"checkinrepliestotal": 2}] and sorted(x["version"] for x in st) == [1, 1],
          f"flush {out}, rows {r}, users {u}, state {st}")
    print(f"      flush of {out.get('items')} calls in {secs:.1f} s, "
          + ", ".join(f"{t} {v.get('statements')} statements" for t, v in out.get("targets", {}).items()))

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
    # per target, the calls are committed by exactly one of the two writers
    per_target = {t: sum(o[1]["targets"][t]["items"] for o in outcomes if o[0] == "ok" and t in o[1].get("targets", {}))
                  for t in ("responses", "users")}
    if any(o[0] == "failed" for o in outcomes):
        flush_now()
        have = rows(f"SELECT COUNT(*) n FROM `{rd}` WHERE SessionID LIKE @p", p=f"staged-proof-{run}-%")
    u = rows(f"SELECT checkinrepliestotal FROM `{us}` WHERE uuid = @u", u=UUID)
    check("5 two writers: each call committed once, no duplicates",
          dups[0]["n"] == 0 and have[0]["n"] == 7 and u == [{"checkinrepliestotal": 9}]
          and all(v in (0, 5) for v in per_target.values()),
          f"outcomes {outcomes}, committed per target {per_target}, duplicate sessions {dups}, session rows {have}, users {u}")

    replay_errors_out = []
    # 6. the real nightly pattern on users and response_data, replayed against clones, while calls keep coming
    night = nightly_statements(client)
    print(f"      nightly pattern {night['day']}: {len(night['statements'])} statements replayed "
          f"({', '.join(f'+{x[0]:.0f}s {x[1]} {x[2]} {x[3]} ms' for x in night['statements'])})")
    for skipped in night["skipped"]:
        print(f"      not replayed (writes outside users/response_data or uses parameters): {skipped}")
    n_users, n_rd = f"{ds}.{prefix}night_users", f"{ds}.{prefix}night_response_data"
    for clone, src in ((n_users, "users"), (n_rd, "response_data")):
        client.query(f"CREATE TABLE `{clone}` CLONE `{PROJECT}.RESPONSES.{src}` "
                     f"FOR SYSTEM_TIME AS OF TIMESTAMP('{night['as_of']}')").result()
    config.ALLOWED_TARGETS["users"], config.ALLOWED_TARGETS["responses"] = n_users, n_rd
    svc._SCHEMA_CACHE.clear()
    cycles, staged_n, last_total = run_with_traffic(
        svc, config, post, body, UUID, start_n=100,
        background=lambda: replay_statements(client, night["statements"], n_users, n_rd, replay_errors_out),
        settle_cycles=2)
    replay_errors = replay_errors_out
    resp_fail = [c for c in cycles if c["responses"] != "ok"]
    users_fail = [c for c in cycles if c["users"] != "ok"]
    streak = max_streak([c["users"] != "ok" for c in cycles])
    last = cycles[-1]
    landed_rd = rows(f"SELECT COUNT(*) n FROM `{n_rd}` WHERE SessionID LIKE @p", p=f"staged-proof-{run}-%")
    u = rows(f"SELECT checkinrepliestotal FROM `{n_users}` WHERE uuid = @u", u=UUID)
    check("6a nightly pattern: check-in rows never blocked, users recovers within one cycle, nothing lost",
          not resp_fail and streak <= 1 and last["users"] == "ok" and not replay_errors
          and landed_rd[0]["n"] == staged_n and u == [{"checkinrepliestotal": last_total}],
          f"cycles {cycles}, replay errors {replay_errors}, rows {landed_rd}, users {u}")
    for c in cycles:
        print(f"      cycle {c['n']:>2} {c['phase']:<7}: responses {c['responses']} ({c['attempts_responses']} attempts), "
              f"users {c['users']} ({c['attempts_users']} attempts), {c['seconds']:.0f} s")

    # 6b. a continuous users writer (worst case): check-in rows keep flushing; users catches up when it stops
    stop = threading.Event()

    def hammer():
        while not stop.is_set():
            try:
                client.query(f"UPDATE `{n_users}` SET testaccount = testaccount WHERE uuid = @u",
                             job_config=bigquery.QueryJobConfig(query_parameters=[
                                 bigquery.ScalarQueryParameter("u", "STRING", UUID)])).result()
            except Exception:
                pass
    cycles_h, _, _ = run_with_traffic(svc, config, post, body, UUID, start_n=1000,
                                      hammer=(hammer, stop), hammer_cycles=3, settle_cycles=2)
    resp_fail = [c for c in cycles_h if c["responses"] != "ok"]
    after = cycles_h[-1]
    check("6b continuous users writer: check-in rows flush every cycle; users commits once it stops",
          not resp_fail and after["users"] == "ok" and after["phase"] == "after",
          f"cycles {cycles_h}")
    for c in cycles_h:
        print(f"      cycle {c['n']:>2} {c['phase']:<7}: responses {c['responses']} ({c['attempts_responses']} attempts), "
              f"users {c['users']} ({c['attempts_users']} attempts), {c['seconds']:.0f} s")

    # 7. health on a clean system: ok; alert only while failing (a failed users cycle above cleared on success)
    h = svc.flush_health()
    check("7 health ok when nothing is failing",
          h["status"] == "ok" and all(t["consecutive_failed_flushes"] == 0 and t["backlog_calls"] == 0
                                      for t in h["targets"].values()) and h["dead_letters_24h"] >= 1, h)

    print()
    failed = sum(1 for _, ok, _ in results if not ok)
    print(f"{len(results) - failed} of {len(results)} scenarios passed")
    if failed:
        raise SystemExit(1)


def nightly_statements(client) -> dict:
    """
    Last night's writes to users and response_data by the nightly jobs (from the job log), with offsets.
    Only statements whose every write target is users or response_data and that take no parameters are
    replayed; the rest are listed as skipped.
    """
    latest = list(client.query(
        f"SELECT MAX(creation_time) t FROM `{PROJECT}.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
        f"WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 DAY) "
        f"AND user_email = '{NIGHTLY_ACCOUNT}' AND destination_table.table_id = 'users' "
        f"AND query LIKE '%contacts_sync_textit_staging%'").result())[0]["t"]
    if latest is None:
        raise SystemExit("no nightly contacts-sync write to users found in the last 6 days")
    start, end = latest - timedelta(minutes=10), latest + timedelta(minutes=15)
    jobs = list(client.query(
        f"SELECT creation_time, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) ms, statement_type, query "
        f"FROM `{PROJECT}.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
        f"WHERE creation_time BETWEEN @s AND @e AND user_email = '{NIGHTLY_ACCOUNT}' AND parent_job_id IS NULL "
        f"AND statement_type NOT IN ('SELECT') AND state = 'DONE' AND error_result IS NULL ORDER BY creation_time",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("s", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("e", "TIMESTAMP", end)])).result())
    targets_re = re.compile(r"(?:INSERT\s+INTO|MERGE(?:\s+INTO)?|UPDATE|DELETE\s+FROM|TRUNCATE\s+TABLE)\s+`([^`]+)`", re.I)
    keep, skipped, first = [], [], None
    for j in jobs:
        targets = {t.split(".")[-1] for t in targets_re.findall(j["query"])}
        if not targets & {"users", "response_data"}:
            continue
        label = f"{j['statement_type']} -> {','.join(sorted(targets))}"
        if targets - {"users", "response_data"} or re.search(r"@\w", j["query"]):
            skipped.append(label)
            continue
        first = first or j["creation_time"]
        keep.append(((j["creation_time"] - first).total_seconds(), j["statement_type"], ",".join(sorted(targets)),
                     j["ms"], j["query"]))
    return {"day": latest.strftime("%Y-%m-%d"), "as_of": (start).strftime("%Y-%m-%d %H:%M:%S+00"),
            "statements": keep, "skipped": skipped}


def replay_statements(client, statements, n_users, n_rd, errors_out):
    t0 = time.monotonic()
    for offset, kind, target, _ms, sql in statements:
        wait = t0 + offset - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        text = (sql.replace(f"`{PROJECT}.RESPONSES.users`", f"`{n_users}`")
                   .replace(f"`{PROJECT}.RESPONSES.response_data`", f"`{n_rd}`"))
        if f"{PROJECT}.RESPONSES.users`" in text or f"{PROJECT}.RESPONSES.response_data`" in text:
            errors_out.append(f"{kind} {target}: a production reference survived the rewrite; not run")
            continue
        try:
            client.query(text).result()
        except Exception as exc:
            errors_out.append(f"{kind} {target}: {str(exc).splitlines()[0][:200]}")


def max_streak(flags) -> int:
    best = cur = 0
    for f in flags:
        cur = cur + 1 if f else 0
        best = max(best, cur)
    return best


def run_with_traffic(svc, config, post, body, uuid, start_n, background=None, hammer=None,
                     hammer_cycles=0, settle_cycles=2):
    """
    Production cadence: a check-in call (users + responses) every 2 s and a flush cycle every
    FLUSH_BUCKET_S. Active while `background` runs (started 10 s in) or for `hammer_cycles`
    cycles with the hammer on; then the traffic stops and `settle_cycles` more cycles run.
    Returns per-cycle outcomes.
    """
    state = {"n": start_n, "stop": False}

    def traffic():
        while not state["stop"]:
            state["n"] += 1
            n = state["n"]
            post(("users", {"uuid": uuid, "checkInRepliesTotal": str(n)}), ("responses", body(n, f"r{n}")))
            time.sleep(2)

    tt = threading.Thread(target=traffic, daemon=True)
    tt.start()
    bg = None
    if background:
        time.sleep(10)
        bg = threading.Thread(target=background)
        bg.start()
    if hammer:
        hammer_fn, hammer_stop = hammer
        threading.Thread(target=hammer_fn, daemon=True).start()

    cycles = []

    def cycle(label):
        t0 = time.monotonic()
        rec = {"n": len(cycles) + 1, "phase": label}
        try:
            out = svc.run_flush_cycle()
            res, failed = out["targets"], {}
        except svc.FlushFailed as exc:
            res, failed = exc.results, exc.errors
        for t in ("responses", "users"):
            rec[t] = "ok" if t in res else "FAILED"
            rec[f"attempts_{t}"] = res.get(t, {}).get("attempts")
        rec["seconds"] = time.monotonic() - t0
        cycles.append(rec)

    def active():
        if bg is not None:
            return bg.is_alive()
        return hammer is not None and len(cycles) < hammer_cycles

    while active():
        time.sleep(config.FLUSH_BUCKET_S)
        cycle("hammer" if hammer else "nightly")
    if hammer:
        hammer_stop.set()
    state["stop"] = True
    tt.join()
    for _ in range(settle_cycles):
        time.sleep(config.FLUSH_SAFETY_S + 1)
        cycle("after")
    return cycles, state["n"] - start_n, state["n"]


if __name__ == "__main__":
    main_()

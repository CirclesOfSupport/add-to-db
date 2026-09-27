"""
Regression replay for the live triage and testimonial writes: every logged
/upsert call for triage_data and feedback in the window is written twice, into
two DEV copies of each table -- once exactly as the live revision writes it
(the frozen base module tests/baseline/bq_writer_b4ac22f.py builds the SQL) and
once through this branch's worker (perform_upsert) -- then the tables are
compared row for row. In-process, sequential, in fired_at order; never the
live queue.

Resumable: progress is saved after every call to regress_progress_<run>.json
(the call window is fixed at the start). If it stops -- e.g. the gcloud sign-in
expires -- sign in again and run the same command: it resumes the unfinished
run (both writes of a call are upserts, so redoing the last call is harmless).
Drops the DEV tables when the run completes, unless --keep.

    python tools/replay_triage_testimonial.py                  # last 7 days, or resume
    python tools/replay_triage_testimonial.py --days 14
    python tools/replay_triage_testimonial.py --new            # abandon an unfinished run, start over
For every call either side rejects, it prints the status the live service
actually returned for that call (from the webhook log).
Exit code 0 = identical tables and identical accept/reject decisions.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import _harness
from _harness import PROJECT, load_service, make_client, stamp

from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from baseline import bq_writer_b4ac22f as base  # noqa: E402

LOG = f"{PROJECT}.OPS.webhook_log_detail"
SOURCES = {"triage_data": "triage-message-data", "feedback": "subscriber_feedback"}


def fetch(client, since, until):
    """[(target, data, fired_at, live status line)] in fired_at order; fixed for a run by (since, until)."""
    sql = (f"SELECT httplog_id, fired_at, request_body, response_status_line FROM `{LOG}` "
           f"WHERE request_path = '/upsert' AND fired_at >= @s AND fired_at < @u ORDER BY fired_at, httplog_id")
    items = []
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("s", "TIMESTAMP", since),
                                                    bigquery.ScalarQueryParameter("u", "TIMESTAMP", until)])
    for r in client.query(sql, job_config=cfg).result():
        try:
            b = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        pairs = [(b.get("table"), b.get("data"))] if "table" in b else [(t.get("table"), t.get("data")) for t in b.get("tables") or []]
        items += [(t, d, r["fired_at"].isoformat(), (r["response_status_line"] or "").strip())
                  for t, d in pairs if t in SOURCES and isinstance(d, dict)]
    return items


_SCHEMAS = {}


def base_write(client, config, svc, target, table_id, data):
    """The live revision's worker path, statement for statement."""
    if table_id not in _SCHEMAS:
        _SCHEMAS[table_id] = client.get_table(table_id).schema
    schema = _SCHEMAS[table_id]
    normalized, nerr = base.normalize_payload_to_schema(dict(data), schema)
    coerced, cerr = base.coerce_payload_to_schema(normalized, schema)
    errors, _ = svc.validate_payload(coerced, schema)     # unchanged in main.py since the live revision
    errors += nerr + cerr
    names = {f.name for f in schema}
    row = {k: v for k, v in coerced.items() if k in names}
    keys, kerr = base.resolve_key_columns(config.UPSERT_KEYS[target], schema)
    errors += kerr + base.validate_upsert_keys(keys, schema, row)
    if errors:
        return "rejected"
    sql = base.build_upsert_query(table_id, row, keys, None)
    struct = base.build_struct_param(row, schema, "placeholder")
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("rows", "RECORD", [struct])])).result()
    return "ok"


def snapshot(client, table_id):
    return sorted(json.dumps(dict(r), default=str, sort_keys=True)
                  for r in client.query(f"SELECT * FROM `{table_id}`").result())


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--new", action="store_true", help="start a new run even if one is unfinished")
    args = ap.parse_args()
    _harness.RESUMABLE_HINT = True
    client = make_client()

    unfinished = sorted(glob.glob("regress_progress_*.json"))
    if unfinished and not args.new:
        path = unfinished[-1]
        with open(path) as f:
            prog = json.load(f)
        print(f"resuming {path}: {prog['next']} of {prog['total']} calls done")
    else:
        run = stamp()
        until = datetime.now(timezone.utc)
        since = until - timedelta(days=args.days)
        prog = {"run": run, "since": since.isoformat(), "until": until.isoformat(), "next": 0, "total": None,
                "decisions": {}, "rejected": [], "differ": [],
                "tables": {f"{side}|{t}": f"{PROJECT}.DEV.adb_regress_{run}_{side}_{t}"
                           for t in SOURCES for side in ("base", "branch")}}
        for key, tid in prog["tables"].items():
            client.query(f"CREATE TABLE `{tid}` LIKE `{PROJECT}.RESPONSES.{SOURCES[key.split('|')[1]]}`").result()
        path = f"regress_progress_{run}.json"
    items = fetch(client, datetime.fromisoformat(prog["since"]), datetime.fromisoformat(prog["until"]))
    if prog["total"] is None:
        prog["total"] = len(items)
        print(f"{len(items)} calls {prog['since'][:16]} -> {prog['until'][:16]} UTC: "
              f"{dict(Counter(t for t, *_ in items))}; progress file {path}")
    elif prog["total"] != len(items):
        raise SystemExit(f"the logged call set changed ({prog['total']} -> {len(items)}); start over with --new")

    tables = {tuple(k.split("|")): v for k, v in prog["tables"].items()}
    svc = load_service(client, {t: tables[("branch", t)] for t in SOURCES})
    import config

    def save():
        with open(path, "w") as f:
            json.dump(prog, f, indent=1)

    day = None
    for i in range(prog["next"], len(items)):
        target, data, fired_at, live = items[i]
        if fired_at[:10] != day:
            day = fired_at[:10]
            print(f"  {day}: from call {i + 1} of {len(items)}", flush=True)
        b = base_write(client, config, svc, target, tables[("base", target)], data)
        body, status = svc.perform_upsert(target, dict(data))
        n = "ok" if status == 200 and body.get("status") == "ok" else "rejected"
        key = f"{target} base={b} branch={n}"
        prog["decisions"][key] = prog["decisions"].get(key, 0) + 1
        if "rejected" in (b, n):
            prog["rejected"].append({"call": i + 1, "fired_at": fired_at, "target": target, "base": b, "branch": n,
                                     "live_status": live, "errors": body.get("errors")})
        if b != n:
            prog["differ"].append({"call": i + 1, "target": target, "base": b, "branch": n})
        prog["next"] = i + 1
        save()

    ok = not prog["differ"]
    for target in SOURCES:
        a, c = snapshot(client, tables[("base", target)]), snapshot(client, tables[("branch", target)])
        only_a, only_c = Counter(a) - Counter(c), Counter(c) - Counter(a)
        same = not only_a and not only_c
        ok &= same
        print(f"{target}: base {len(a)} rows, branch {len(c)} rows -> {'IDENTICAL' if same else 'DIFFERENT'}"
              + ("" if same else f" ({sum(only_a.values())} rows only in base, {sum(only_c.values())} only in branch)"))
    print("accept/reject decisions:", prog["decisions"])
    for r in prog["rejected"]:
        print(f"  rejected call {r['call']} ({r['fired_at'][:19]}Z, {r['target']}): base {r['base']}, branch {r['branch']}, "
              f"live service returned '{r['live_status']}'; errors {r['errors']}")
    if prog["differ"]:
        print("calls decided differently:", prog["differ"][:20])
    if not args.keep:
        for tid in tables.values():
            client.delete_table(tid, not_found_ok=True)
        print("dropped the DEV tables")
    os.replace(path, path.replace("regress_progress_", "regress_done_"))
    print("RESULT:", "IDENTICAL" if ok else "DIFFERENT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_()

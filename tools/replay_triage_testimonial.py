"""
Regression replay for the live triage and testimonial writes: every logged
/upsert call for triage_data and feedback in the window is written twice, into
two DEV copies of each table -- once exactly as the live revision writes it
(the frozen base module tests/baseline/bq_writer_b4ac22f.py builds the SQL) and
once through this branch's worker (perform_upsert) -- then the tables are
compared row for row. In-process, sequential, in fired_at order; never the
live queue. Drops the DEV tables unless --keep.

    python tools/replay_triage_testimonial.py                  # last 7 days
    python tools/replay_triage_testimonial.py --days 14
Exit code 0 = identical tables and identical accept/reject decisions.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

from _harness import PROJECT, load_service, make_client, stamp

from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
from baseline import bq_writer_b4ac22f as base  # noqa: E402

LOG = f"{PROJECT}.OPS.webhook_log_detail"
SOURCES = {"triage_data": "triage-message-data", "feedback": "subscriber_feedback"}


def fetch(client, since):
    sql = (f"SELECT fired_at, request_body FROM `{LOG}` WHERE request_path = '/upsert' AND fired_at >= @s "
           f"ORDER BY fired_at")
    items = []
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("s", "TIMESTAMP", since)])
    for r in client.query(sql, job_config=cfg).result():
        try:
            b = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        pairs = [(b.get("table"), b.get("data"))] if "table" in b else [(t.get("table"), t.get("data")) for t in b.get("tables") or []]
        items += [(t, d) for t, d in pairs if t in SOURCES and isinstance(d, dict)]
    return items


def base_write(client, config, svc, target, table_id, data):
    """The live revision's worker path, statement for statement."""
    schema = client.get_table(table_id).schema
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
    args = ap.parse_args()
    client = make_client()
    run = stamp()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    items = fetch(client, since)
    print(f"{len(items)} calls since {since:%Y-%m-%d %H:%M} UTC: {dict(Counter(t for t, _ in items))}")

    tables = {}
    for target, src in SOURCES.items():
        for side in ("base", "branch"):
            tid = f"{PROJECT}.DEV.adb_regress_{run}_{side}_{target}"
            client.query(f"CREATE TABLE `{tid}` LIKE `{PROJECT}.RESPONSES.{src}`").result()
            tables[(side, target)] = tid
    svc = load_service(client, {t: tables[("branch", t)] for t in SOURCES})
    import config

    decisions = Counter()
    differ = []
    for i, (target, data) in enumerate(items):
        b = base_write(client, config, svc, target, tables[("base", target)], data)
        body, status = svc.perform_upsert(target, dict(data))
        n = "ok" if status == 200 and body.get("status") == "ok" else "rejected"
        decisions[(target, b, n)] += 1
        if b != n:
            differ.append({"index": i, "target": target, "base": b, "branch": n})
        if (i + 1) % 200 == 0:
            print(f"  {i + 1} of {len(items)}", flush=True)

    ok = not differ
    for target in SOURCES:
        a, c = snapshot(client, tables[("base", target)]), snapshot(client, tables[("branch", target)])
        only_a, only_c = Counter(a) - Counter(c), Counter(c) - Counter(a)
        same = not only_a and not only_c
        ok &= same
        print(f"{target}: base {len(a)} rows, branch {len(c)} rows -> {'IDENTICAL' if same else 'DIFFERENT'}"
              + ("" if same else f" ({sum(only_a.values())} rows only in base, {sum(only_c.values())} only in branch)"))
    print("accept/reject decisions:", {f"{t} base={b} branch={n}": v for (t, b, n), v in decisions.items()})
    if differ:
        print("calls decided differently:", differ[:20])
    if not args.keep:
        for tid in tables.values():
            client.delete_table(tid, not_found_ok=True)
        print("dropped the DEV tables")
    print("RESULT:", "IDENTICAL" if ok else "DIFFERENT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_()

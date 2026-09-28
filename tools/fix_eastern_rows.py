"""
Correct response_data rows that add-to-db's 2026 load tests stored in the
payload's LOCAL wall clock (Eastern) instead of UTC, before cutover.

A row qualifies when its checkinDateTime equals the local wall-clock time in
its own SessionID (within 5 s) and not that time in UTC. Its checkinDateTime
moves to UTC by the SessionID's own offset. checkinReplyDateTime and
resourceOfferReplyDatetime move only when the stored value equals the LOCAL
form of a value some logged call carried for that session and not the UTC
form, or -- when no logged call carries the column -- when the stored value is
earlier than the corrected check-in time but its UTC form is not (a reply
cannot precede its check-in, so only the local reading is possible).
Otherwise they are left as they are, and the list says why. Each row is also
marked with whether its session appears in add-to-db's own logged request
bodies (the log starts 2026-08-25).

    python tools/fix_eastern_rows.py list [--table T]
        read-only. Prints a summary; writes the full list (every row, every proposed change, the
        reason) to eastern_fix_list_<stamp>.txt and the plan (which names its table) to
        eastern_fix_plan_<stamp>.json. A reply time still before its own check-in after conversion
        is stale carry-over from an earlier session: converted like the rest, but flagged
        "stale reply, value to the repair unit" in the list and the plan.
    python tools/fix_eastern_rows.py rehearse
        the whole correction on a DEV clone of today's RESPONSES.response_data: clone, list, apply
        (timed, bytes billed, committed), check that no local-time row is left, roll back (timed),
        check they are all back; then drops the clone and its backup (--keep keeps them). Writes
        nothing outside DEV.
    python tools/fix_eastern_rows.py apply PLAN [--production]
        copies the plan's rows to DEV.adb_eastern_fix_backup_<stamp>, then makes every change in ONE
        transaction. Per column: an ASSERT that every change still matches exactly its listed row
        count on its old value, then one UPDATE (only rows still holding the old value), then an
        ASSERT that the UPDATE touched exactly the listed total. Any mismatch aborts everything.
    python tools/fix_eastern_rows.py rollback PLAN [--production]
        the reverse, the same way, for rows still holding the new value.

apply and rollback refuse a plan whose table is not in DEV unless --production is given (the
cutover's correction step on RESPONSES.response_data is the one run that passes it).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from _harness import PROJECT, make_client, stamp

from google.cloud import bigquery

TABLE = f"{PROJECT}.RESPONSES.response_data"          # the production table; list's default
DEV_PREFIX = f"{PROJECT}.DEV."
LOG = f"{PROJECT}.OPS.webhook_log_detail"
REPLY_COLS = ["checkinReplyDateTime", "resourceOfferReplyDatetime"]

CANDIDATES_SQL = """
WITH r AS (
  SELECT SessionID, uuid, contactType, checkinDateTime, checkinReplyDateTime, resourceOfferReplyDatetime,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SUBSTR(SessionID, 37)) sid_ts,
    SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', REGEXP_EXTRACT(SUBSTR(SessionID, 37), r'^([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9:.]+)')) sid_local
  FROM `{table}` WHERE SessionID IS NOT NULL AND checkinDateTime IS NOT NULL
)
SELECT SessionID, uuid, contactType, checkinDateTime, checkinReplyDateTime, resourceOfferReplyDatetime,
  DATETIME_DIFF(sid_local, DATETIME(sid_ts, 'UTC'), MINUTE) offset_min
FROM r
WHERE sid_ts IS NOT NULL AND sid_local IS NOT NULL
  AND ABS(DATETIME_DIFF(checkinDateTime, sid_local, SECOND)) <= 5
  AND ABS(DATETIME_DIFF(checkinDateTime, DATETIME(sid_ts, 'UTC'), SECOND)) > 60
ORDER BY checkinDateTime, SessionID
"""


def candidates_sql(table: str) -> str:
    return CANDIDATES_SQL.format(table=table)


CANDIDATES = candidates_sql(TABLE)


def parse_aware(text):
    try:
        v = datetime.fromisoformat(unquote(str(text)).replace("Z", "+00:00"))
    except ValueError:
        return None
    return v if v.tzinfo else None


def logged_values(client, sessions):
    """
    ({(SessionID, column): {(local naive, utc naive)}}, {SessionIDs seen in add-to-db's own bodies})
    from every logged call body since 2026-08-24.
    """
    uuids = sorted({s[:36] for s in sessions})
    sql = (f"SELECT request_body FROM `{LOG}` WHERE fired_at >= TIMESTAMP('2026-08-24') "
           f"AND REGEXP_CONTAINS(request_body, @pat)")
    pat = "|".join(re.escape(u) for u in uuids)
    out = defaultdict(set)
    in_addtodb = set()
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("pat", "STRING", pat)])
    for r in client.query(sql, job_config=cfg).result():
        try:
            body = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        datas = []
        if isinstance(body.get("Responses"), dict):
            datas.append((body["Responses"], False))
        for t in body.get("tables") or []:
            if isinstance(t, dict) and t.get("table") == "responses" and isinstance(t.get("data"), dict):
                datas.append((t["data"], True))
        if body.get("table") == "responses" and isinstance(body.get("data"), dict):
            datas.append((body["data"], True))
        for d, addtodb in datas:
            sid = unquote(str(d.get("sessionID") or d.get("SessionID") or ""))
            if sid not in sessions:
                continue
            if addtodb:
                in_addtodb.add(sid)
            for col in REPLY_COLS:
                v = parse_aware(d.get(col)) if d.get(col) else None
                if v:
                    out[(sid, col)].add((v.replace(tzinfo=None), v.astimezone(timezone.utc).replace(tzinfo=None)))
    return out, in_addtodb


def cmd_list(client, out=None, table: str = TABLE) -> str:
    """Read-only. Writes the list and the plan; returns the plan's path."""
    rows = [dict(r) for r in client.query(candidates_sql(table)).result()]
    sessions = {r["SessionID"] for r in rows}
    logged, in_addtodb = logged_values(client, sessions) if rows else ({}, set())
    changes, lines = [], []
    summary, stale_rows = defaultdict(int), 0
    run = stamp()
    list_path, plan_path = f"eastern_fix_list_{run}.txt", f"eastern_fix_plan_{run}.json"
    lines.append(f"{len(rows)} rows stored in local time ({len(sessions)} sessions)\n")
    lines.append(f"{'#':>4}  {'SessionID':<70} {'contactType':<11} {'column':<27} {'stored':<27} proposed")
    for i, r in enumerate(rows, 1):
        off = timedelta(minutes=r["offset_min"])
        new = r["checkinDateTime"] - off
        prov = "in add-to-db bodies" if r["SessionID"] in in_addtodb else "not in logged add-to-db bodies"
        summary[(r["checkinDateTime"].strftime("%Y-%m"), prov)] += 1
        changes.append({"SessionID": r["SessionID"], "column": "checkinDateTime",
                        "old": r["checkinDateTime"].isoformat(), "new": new.isoformat()})
        lines.append(f"{i:>4}  {r['SessionID']:<70} {str(r['contactType']):<11} {'checkinDateTime':<27} "
                     f"{r['checkinDateTime'].isoformat():<27} {new.isoformat()}")
        row_stale = False
        for col in REPLY_COLS:
            v = r[col]
            if v is None:
                continue
            pairs = logged.get((r["SessionID"], col), set())
            locals_, utcs = {p[0] for p in pairs}, {p[1] for p in pairs}
            target, why = None, None
            if v in locals_ and v not in utcs:
                target, why = next(p[1] for p in pairs if p[0] == v), "local form of a logged value"
            elif not pairs and v < new <= v - off:
                target, why = v - off, "before the check-in unless read as local"
            if target is not None:
                change = {"SessionID": r["SessionID"], "column": col, "old": v.isoformat(), "new": target.isoformat()}
                if target < new:        # still before its own check-in once converted: an earlier session's reply
                    change["flag"] = "stale reply, value to the repair unit"
                    why += "; STALE REPLY, value to the repair unit"
                    row_stale = True
                changes.append(change)
                lines.append(f"{'':>4}  {'':<70} {'':<11} {col:<27} {v.isoformat():<27} {target.isoformat()}  ({why})")
            else:
                why = ("matches a logged UTC value" if v in utcs else
                       "logged values do not match it" if pairs else "no logged value; consistent either way")
                if v < r["checkinDateTime"] - off and v in utcs:
                    why += "; STALE REPLY, value to the repair unit"
                    row_stale = True
                lines.append(f"{'':>4}  {'':<70} {'':<11} {col:<27} {v.isoformat():<27} unchanged ({why})")
        stale_rows += row_stale
    counts = defaultdict(int)
    for c in changes:
        counts[(c["SessionID"], c["column"], c["old"])] += 1
    uniq = {}
    for c in changes:
        uniq[(c["SessionID"], c["column"], c["old"])] = dict(c, rows=counts[(c["SessionID"], c["column"], c["old"])])
    flagged = sum(1 for c in uniq.values() if c.get("flag"))
    summary_lines = [f"{len(rows)} rows stored in local time ({len(sessions)} sessions)", "",
                     "rows by check-in month and provenance:"]
    summary_lines += [f"  {month}  {prov:<32} {n:>5}" for (month, prov), n in sorted(summary.items())]
    summary_lines += ["", f"{len(uniq)} changes on {len(rows)} rows; {flagged} reply-time changes on {stale_rows} rows are "
                      f"flagged 'stale reply, value to the repair unit' (format converted, value not trusted)",
                      f"full list: {list_path}", f"plan:      {plan_path}", "Nothing was written to BigQuery."]
    with open(list_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines + [""] + summary_lines) + "\n")
    with open(plan_path, "w") as f:
        json.dump({"table": table, "changes": list(uniq.values())}, f, indent=2)
    print("\n".join(summary_lines), file=out)
    return plan_path


def _map_param(name: str, changes: list[dict], forward: bool) -> "bigquery.ArrayQueryParameter":
    structs = []
    for c in changes:
        old, new = (c["old"], c["new"]) if forward else (c["new"], c["old"])
        structs.append(bigquery.StructQueryParameter(
            "placeholder",
            bigquery.ScalarQueryParameter("fix_sid", "STRING", c["SessionID"]),
            bigquery.ScalarQueryParameter("fix_old", "DATETIME", datetime.fromisoformat(old)),
            bigquery.ScalarQueryParameter("fix_new", "DATETIME", datetime.fromisoformat(new)),
            bigquery.ScalarQueryParameter("fix_rows", "INT64", int(c["rows"]))))
    return bigquery.ArrayQueryParameter(name, "RECORD", structs)


def build_script(table: str, changes: list[dict], forward: bool) -> tuple[str, list]:
    """
    One transaction. Per column, three statements:
      ASSERT every change matches exactly its listed row count on its old value (nothing changed since
             the list; no extra row carries the value);
      UPDATE only the rows still holding the old value (set-based: one statement per column, not per row);
      ASSERT the UPDATE touched exactly the listed total.
    Any failed ASSERT aborts the whole transaction: nothing is half-done.
    """
    by_col: dict[str, list[dict]] = {}
    for c in changes:
        if c["column"] not in ("checkinDateTime", *REPLY_COLS):
            raise SystemExit(f"unexpected column {c['column']}")
        by_col.setdefault(c["column"], []).append(c)
    script, params = ["BEGIN TRANSACTION"], []
    for col, cs in by_col.items():
        m, sids = f"m_{col}", f"sids_{col}"
        total = sum(int(c["rows"]) for c in cs)
        params += [_map_param(m, cs, forward),
                   bigquery.ArrayQueryParameter(sids, "STRING", sorted({c["SessionID"] for c in cs}))]
        script.append(
            f"ASSERT (SELECT COUNT(*) FROM UNNEST(@{m}) LEFT JOIN "
            f"(SELECT SessionID, `{col}` AS v, COUNT(*) AS n FROM `{table}` WHERE SessionID IN UNNEST(@{sids}) GROUP BY 1, 2) t "
            f"ON t.SessionID = fix_sid AND t.v = fix_old WHERE IFNULL(t.n, 0) != fix_rows) = 0 "
            f"AS '{col}: a listed row no longer holds its listed value, or the row count differs from the list'")
        script.append(
            f"UPDATE `{table}` AS tgt SET `{col}` = fix_new FROM UNNEST(@{m}) "
            f"WHERE tgt.SessionID = fix_sid AND tgt.`{col}` = fix_old AND tgt.SessionID IN UNNEST(@{sids})")
        script.append(f"ASSERT @@row_count = {total} AS '{col}: the update did not touch exactly {total} row(s)'")
    script.append("COMMIT TRANSACTION")
    return ";\n".join(script) + ";", params


def run_changes(client, changes, forward: bool, table: str = TABLE):
    """Runs the transaction; returns the finished job (for duration and bytes billed)."""
    sql, params = build_script(table, changes, forward)
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()
    return job


def refuse_unless_allowed(table: str, production: bool) -> None:
    if not table.startswith(DEV_PREFIX) and not production:
        raise SystemExit(f"refusing to write {table}: it is not a DEV table. The cutover's correction step on "
                         f"production passes --production; anything else runs on a DEV clone (rehearse).")


def job_cost(client, job) -> dict:
    """Duration and bytes billed of a finished job; for a script, the sum over its child statements too."""
    children = []
    try:
        children = list(client.list_jobs(parent_job=job.job_id))
    except Exception:
        pass
    secs = (job.ended - job.started).total_seconds() if getattr(job, "ended", None) and getattr(job, "started", None) else None
    return {"seconds": secs, "bytes_billed": job.total_bytes_billed,
            "child_statements": len(children),
            "child_bytes_billed": sum((c.total_bytes_billed or 0) for c in children) if children else None}


def remaining(client, table: str) -> int:
    return len(list(client.query(candidates_sql(table)).result()))


def cmd_apply(client, plan: dict, production: bool, forward: bool = True) -> dict:
    table = plan["table"]
    refuse_unless_allowed(table, production)
    changes = plan["changes"]
    sids = sorted({c["SessionID"] for c in changes})
    backup = None
    if forward:
        backup = f"{DEV_PREFIX}adb_eastern_fix_backup_{stamp()}"
        client.query(f"CREATE TABLE `{backup}` AS SELECT * FROM `{table}` WHERE SessionID IN UNNEST(@s)",
                     job_config=bigquery.QueryJobConfig(query_parameters=[
                         bigquery.ArrayQueryParameter("s", "STRING", sids)])).result()
        n = list(client.query(f"SELECT COUNT(*) n FROM `{backup}`").result())[0]["n"]
        print(f"backup: {backup} ({n} rows)")
    job = run_changes(client, changes, forward=forward, table=table)
    cost = job_cost(client, job)
    left = remaining(client, table)
    what = "applied" if forward else "rolled back"
    secs = f"{cost['seconds']:.1f} s, " if cost["seconds"] is not None else ""
    print(f"{what} {len(changes)} changes on {table} in one transaction (committed): {secs}"
          f"{cost['bytes_billed'] or 0:,} bytes billed, {cost['child_statements']} statements")
    print(f"rows still stored in local time: {left}")
    return {"cost": cost, "left": left, "backup": backup}


def cmd_rehearse(client, keep: bool = False) -> bool:
    run = stamp()
    clone = f"{DEV_PREFIX}adb_eastern_rehearsal_{run}"
    client.query(f"CREATE TABLE `{clone}` CLONE `{TABLE}`").result()
    print(f"clone: {clone} (of {TABLE} as of now)")
    made = [clone]
    try:
        before = remaining(client, clone)
        plan_path = cmd_list(client, table=clone)
        with open(plan_path) as f:
            plan = json.load(f)
        rows_listed = sum(int(c["rows"]) for c in plan["changes"] if c["column"] == "checkinDateTime")
        print(f"\nrehearsal: {before} rows in local time on the clone; {len(plan['changes'])} changes planned")
        fwd = cmd_apply(client, plan, production=False, forward=True)
        made.append(fwd["backup"])
        back = cmd_apply(client, plan, production=False, forward=False)
        ok = fwd["left"] == 0 and back["left"] == before and rows_listed == before
        print(f"\nREHEARSAL {'PASS' if ok else 'FAIL'}: apply committed in {fwd['cost']['seconds']:.1f} s, "
              f"{fwd['cost']['bytes_billed'] or 0:,} bytes billed, left {fwd['left']} rows in local time (want 0); "
              f"rollback committed in {back['cost']['seconds']:.1f} s, restored {back['left']} of {before}")
        return ok
    finally:
        if keep:
            print(f"kept {', '.join(made)}")
        else:
            for t in made:
                client.delete_table(t, not_found_ok=True)
            print(f"dropped {', '.join(made)}")


def main_(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("list", "rehearse", "apply", "rollback"))
    ap.add_argument("plan", nargs="?")
    ap.add_argument("--table", default=TABLE, help="list only: the table to read (default RESPONSES.response_data)")
    ap.add_argument("--production", action="store_true", help="apply/rollback: allow a plan whose table is not in DEV")
    ap.add_argument("--keep", action="store_true", help="rehearse: keep the clone and its backup")
    args = ap.parse_args(argv)
    if args.action in ("apply", "rollback"):
        if not args.plan:
            raise SystemExit(f"{args.action} needs the plan file")
        with open(args.plan) as f:
            plan = json.load(f)
        refuse_unless_allowed(plan["table"], args.production)      # before any sign-in or query
    client = make_client()
    if args.action == "list":
        cmd_list(client, table=args.table)
    elif args.action == "rehearse":
        if not cmd_rehearse(client, keep=args.keep):
            raise SystemExit(1)
    else:
        cmd_apply(client, plan, production=args.production, forward=args.action == "apply")


if __name__ == "__main__":
    main_()

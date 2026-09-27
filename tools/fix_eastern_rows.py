"""
Correct response_data rows that add-to-db's 2026 load tests stored in the
payload's LOCAL wall clock (Eastern) instead of UTC, before cutover.

A row qualifies when its checkinDateTime equals the local wall-clock time in
its own SessionID (within 5 s) and not that time in UTC. Its checkinDateTime
moves to UTC by the SessionID's own offset. checkinReplyDateTime and
resourceOfferReplyDatetime move only when the stored value equals the LOCAL
form of a value some logged call carried for that session and not the UTC
form (otherwise they are left as they are, and the list says why).

Three steps, each run on purpose:

    python tools/fix_eastern_rows.py list
        prints every row and every proposed change; writes eastern_fix_plan_<stamp>.json.
        Writes nothing to BigQuery.
    python tools/fix_eastern_rows.py apply eastern_fix_plan_<stamp>.json
        copies the affected rows to DEV.adb_eastern_fix_backup_<stamp>, then makes every
        change in ONE transaction; each UPDATE only touches a row still holding the old
        value, and the transaction aborts unless every change hits exactly the rows listed.
    python tools/fix_eastern_rows.py rollback eastern_fix_plan_<stamp>.json
        the reverse, in one transaction, for rows still holding the new value.
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from _harness import PROJECT, make_client, stamp

from google.cloud import bigquery

TABLE = f"{PROJECT}.RESPONSES.response_data"
LOG = f"{PROJECT}.OPS.webhook_log_detail"
REPLY_COLS = ["checkinReplyDateTime", "resourceOfferReplyDatetime"]

CANDIDATES = f"""
WITH r AS (
  SELECT SessionID, uuid, contactType, checkinDateTime, checkinReplyDateTime, resourceOfferReplyDatetime,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SUBSTR(SessionID, 37)) sid_ts,
    SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*S', REGEXP_EXTRACT(SUBSTR(SessionID, 37), r'^([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9:.]+)')) sid_local
  FROM `{TABLE}` WHERE SessionID IS NOT NULL AND checkinDateTime IS NOT NULL
)
SELECT SessionID, uuid, contactType, checkinDateTime, checkinReplyDateTime, resourceOfferReplyDatetime,
  DATETIME_DIFF(sid_local, DATETIME(sid_ts, 'UTC'), MINUTE) offset_min
FROM r
WHERE sid_ts IS NOT NULL AND sid_local IS NOT NULL
  AND ABS(DATETIME_DIFF(checkinDateTime, sid_local, SECOND)) <= 5
  AND ABS(DATETIME_DIFF(checkinDateTime, DATETIME(sid_ts, 'UTC'), SECOND)) > 60
ORDER BY checkinDateTime, SessionID
"""


def parse_aware(text):
    try:
        v = datetime.fromisoformat(unquote(str(text)).replace("Z", "+00:00"))
    except ValueError:
        return None
    return v if v.tzinfo else None


def logged_values(client, sessions):
    """{(SessionID, column): {(local naive, utc naive)}} from every logged call body since 2026-08-24."""
    uuids = sorted({s[:36] for s in sessions})
    sql = (f"SELECT request_body FROM `{LOG}` WHERE fired_at >= TIMESTAMP('2026-08-24') "
           f"AND REGEXP_CONTAINS(request_body, @pat)")
    pat = "|".join(re.escape(u) for u in uuids)
    out = defaultdict(set)
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("pat", "STRING", pat)])
    for r in client.query(sql, job_config=cfg).result():
        try:
            body = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        datas = []
        if isinstance(body.get("Responses"), dict):
            datas.append(body["Responses"])
        for t in body.get("tables") or []:
            if isinstance(t, dict) and t.get("table") == "responses" and isinstance(t.get("data"), dict):
                datas.append(t["data"])
        if body.get("table") == "responses" and isinstance(body.get("data"), dict):
            datas.append(body["data"])
        for d in datas:
            sid = unquote(str(d.get("sessionID") or d.get("SessionID") or ""))
            if sid not in sessions:
                continue
            for col in REPLY_COLS:
                v = parse_aware(d.get(col)) if d.get(col) else None
                if v:
                    out[(sid, col)].add((v.replace(tzinfo=None), v.astimezone(timezone.utc).replace(tzinfo=None)))
    return out


def cmd_list(client):
    rows = [dict(r) for r in client.query(CANDIDATES).result()]
    sessions = {r["SessionID"] for r in rows}
    logged = logged_values(client, sessions) if rows else {}
    changes = []
    print(f"{len(rows)} rows stored in local time ({len(sessions)} sessions)\n")
    print(f"{'#':>3}  {'SessionID':<70} {'contactType':<11} {'column':<27} {'stored':<27} proposed")
    for i, r in enumerate(rows, 1):
        off = timedelta(minutes=r["offset_min"])
        new = r["checkinDateTime"] - off
        changes.append({"SessionID": r["SessionID"], "column": "checkinDateTime",
                        "old": r["checkinDateTime"].isoformat(), "new": new.isoformat()})
        print(f"{i:>3}  {r['SessionID']:<70} {str(r['contactType']):<11} {'checkinDateTime':<27} "
              f"{r['checkinDateTime'].isoformat():<27} {new.isoformat()}")
        for col in REPLY_COLS:
            v = r[col]
            if v is None:
                continue
            pairs = logged.get((r["SessionID"], col), set())
            locals_, utcs = {p[0] for p in pairs}, {p[1] for p in pairs}
            if v in locals_ and v not in utcs:
                target = next(p[1] for p in pairs if p[0] == v)
                changes.append({"SessionID": r["SessionID"], "column": col, "old": v.isoformat(), "new": target.isoformat()})
                print(f"{'':>3}  {'':<70} {'':<11} {col:<27} {v.isoformat():<27} {target.isoformat()}")
            else:
                why = "matches a logged UTC value" if v in utcs else "no logged call carries it"
                print(f"{'':>3}  {'':<70} {'':<11} {col:<27} {v.isoformat():<27} unchanged ({why})")
    # a change applies to every row holding (SessionID, column, old): count them
    counts = defaultdict(int)
    for c in changes:
        counts[(c["SessionID"], c["column"], c["old"])] += 1
    plan = [dict(c, rows=n) for c, n in ((c, counts[(c["SessionID"], c["column"], c["old"])]) for c in changes)]
    uniq = {(c["SessionID"], c["column"], c["old"]): c for c in plan}
    path = f"eastern_fix_plan_{stamp()}.json"
    with open(path, "w") as f:
        json.dump({"table": TABLE, "changes": list(uniq.values())}, f, indent=2)
    print(f"\n{len(uniq)} changes on {len(rows)} rows; plan written to {path}. Nothing was written to BigQuery.")


def run_changes(client, changes, forward: bool):
    script, params = ["BEGIN TRANSACTION"], []
    for i, c in enumerate(changes):
        col = c["column"]
        if col not in ("checkinDateTime", *REPLY_COLS):
            raise SystemExit(f"unexpected column {col}")
        old, new = (c["old"], c["new"]) if forward else (c["new"], c["old"])
        script.append(f"UPDATE `{TABLE}` SET `{col}` = @n{i} WHERE SessionID = @s{i} AND `{col}` = @o{i}")
        script.append(f"ASSERT @@row_count = {int(c['rows'])} AS 'change {i + 1} ({c['SessionID']} {col}) did not match {c['rows']} row(s)'")
        params += [bigquery.ScalarQueryParameter(f"s{i}", "STRING", c["SessionID"]),
                   bigquery.ScalarQueryParameter(f"o{i}", "DATETIME", datetime.fromisoformat(old)),
                   bigquery.ScalarQueryParameter(f"n{i}", "DATETIME", datetime.fromisoformat(new))]
    script.append("COMMIT TRANSACTION")
    client.query(";\n".join(script) + ";", job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def main_():
    if len(sys.argv) < 2 or sys.argv[1] not in ("list", "apply", "rollback"):
        raise SystemExit(__doc__)
    client = make_client()
    if sys.argv[1] == "list":
        return cmd_list(client)
    with open(sys.argv[2]) as f:
        plan = json.load(f)
    changes = plan["changes"]
    sids = sorted({c["SessionID"] for c in changes})
    if sys.argv[1] == "apply":
        backup = f"{PROJECT}.DEV.adb_eastern_fix_backup_{stamp()}"
        client.query(f"CREATE TABLE `{backup}` AS SELECT * FROM `{TABLE}` WHERE SessionID IN UNNEST(@s)",
                     job_config=bigquery.QueryJobConfig(query_parameters=[
                         bigquery.ArrayQueryParameter("s", "STRING", sids)])).result()
        n = list(client.query(f"SELECT COUNT(*) n FROM `{backup}`").result())[0]["n"]
        print(f"backup: {backup} ({n} rows)")
        run_changes(client, changes, forward=True)
        print(f"applied {len(changes)} changes in one transaction")
    else:
        run_changes(client, changes, forward=False)
        print(f"rolled back {len(changes)} changes in one transaction")
    left = [dict(r) for r in client.query(CANDIDATES).result()]
    print(f"rows still stored in local time: {len(left)}")


if __name__ == "__main__":
    main_()

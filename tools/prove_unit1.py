"""
Prove the responses write path on real BigQuery, against a DEV copy.

Creates DEV.adb_unit1_<stamp> as `CREATE TABLE ... LIKE RESPONSES.response_data`
(same partitioning and clustering), runs each scenario through the service's
own worker code, checks the rows, prints the bytes of every statement, then
dry-runs (plans only, writes nothing) the MERGE against RESPONSES.response_data
with and without the NULL branch. Drops the DEV table unless --keep.

    python tools/prove_unit1.py
Exit code 0 = every scenario passed.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from _harness import PROJECT, JobLog, load_service, make_client, stamp

from google.cloud import bigquery

UUID = "00000000-0000-4000-8000-000000000001"   # not a real contact
CHECKIN = "2026-09-18T16%3A01%3A25.006790-04%3A00"   # as TextIt sends it
CHECKIN_UTC = datetime(2026, 9, 18, 20, 1, 25, 6790)


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the DEV table in place")
    args = ap.parse_args()

    client = make_client()
    run = stamp()
    table = f"{PROJECT}.DEV.adb_unit1_{run}"
    client.query(f"CREATE TABLE `{table}` LIKE `{PROJECT}.RESPONSES.response_data`").result()
    print(f"DEV table: {table}")

    jobs = JobLog(client)
    svc = load_service(client, {"responses": table})

    def sid(n):
        return f"unit1-proof-{run}-{n}"

    def body(n, checkin, reply, **extra):
        d = {"uuid": UUID, "orgID": "0", "orgCode": "unit1-proof", "sessionID": sid(n),
             "contactType": "CheckIn", "checkinDateTime": checkin, "wellnessDomain": "Relational",
             "checkinReply": reply, "userWeek": "1"}
        d.update(extra)
        return d

    def write(data):
        before = len(jobs.jobs)
        out, status = svc.perform_upsert("responses", data)
        if status != 200 or out.get("status") != "ok":
            raise RuntimeError(f"write failed: {status} {out}")
        return jobs.jobs[before:]

    def rows_for(sid_value=None, keyless_marker=None):
        if keyless_marker:
            sql = (f"SELECT SessionID, checkinDateTime, checkinReply, contactType, zipcode "
                   f"FROM `{table}` WHERE SessionID IS NULL AND orgCode = @m")
            p = [bigquery.ScalarQueryParameter("m", "STRING", keyless_marker)]
        else:
            sql = (f"SELECT SessionID, checkinDateTime, checkinReply, contactType, "
                   f"referralFollowUpAttempts_str FROM `{table}` WHERE SessionID = @s")
            p = [bigquery.ScalarQueryParameter("s", "STRING", sid_value)]
        return [dict(r) for r in client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=p)).result()]

    results = []

    def check(name, cond, detail, stmt_jobs):
        results.append((name, bool(cond), detail, stmt_jobs))

    # 1. value -> value
    j = write(body(1, CHECKIN, "No")) + write(body(1, CHECKIN, "Yes"))
    r = rows_for(sid(1))
    check("value then value", len(r) == 1 and r[0]["checkinDateTime"] == CHECKIN_UTC and r[0]["checkinReply"] == "Yes", r, j)

    # 2. blank -> value (the stored NULL must be matched)
    j = write(body(2, "", "No")) + write(body(2, CHECKIN, "Yes"))
    r = rows_for(sid(2))
    check("blank then value", len(r) == 1 and r[0]["checkinDateTime"] == CHECKIN_UTC and r[0]["checkinReply"] == "Yes", r, j)

    # 3. value -> blank (the stored time is kept)
    j = write(body(3, CHECKIN, "No")) + write(body(3, "", "Yes"))
    r = rows_for(sid(3))
    check("value then blank", len(r) == 1 and r[0]["checkinDateTime"] == CHECKIN_UTC and r[0]["checkinReply"] == "Yes", r, j)

    # 4. blank -> blank
    j = write(body(4, "", "No")) + write(body(4, "", "Yes"))
    r = rows_for(sid(4))
    check("blank then blank", len(r) == 1 and r[0]["checkinDateTime"] is None and r[0]["checkinReply"] == "Yes", r, j)

    # 5. partial body with no check-in-time key (follow-up flow shape)
    j = write(body(5, CHECKIN, "Yes")) + write({"uuid": UUID, "orgID": "0", "orgCode": "unit1-proof",
                                               "sessionID": sid(5), "referralFollowUpAttempts_str": "%5B%7B%7D%5D"})
    r = rows_for(sid(5))
    check("partial body keeps check-in", len(r) == 1 and r[0]["checkinDateTime"] == CHECKIN_UTC
          and r[0]["checkinReply"] == "Yes" and r[0]["referralFollowUpAttempts_str"] == "[{}]", r, j)

    # 6. a row stored by the old writer (UTC) is matched by a call carrying the local time
    client.query(f"INSERT INTO `{table}` (SessionID, checkinDateTime, checkinReply, uuid) VALUES (@s, @d, 'No', @u)",
                 job_config=bigquery.QueryJobConfig(query_parameters=[
                     bigquery.ScalarQueryParameter("s", "STRING", sid(6)),
                     bigquery.ScalarQueryParameter("d", "DATETIME", CHECKIN_UTC),
                     bigquery.ScalarQueryParameter("u", "STRING", UUID)])).result()
    j = write(body(6, CHECKIN, "Yes"))
    r = rows_for(sid(6))
    check("old-writer UTC row matched", len(r) == 1 and r[0]["checkinReply"] == "Yes", r, j)

    # 7. two sign-up calls with no session -> two keyless rows
    marker = f"unit1-proof-{run}-keyless"
    sub = {"uuid": UUID, "orgID": "0", "orgCode": marker, "sessionID": "", "contactType": "Subscribe",
           "checkinDateTime": CHECKIN, "subscribed": "Yes"}
    j = write(dict(sub)) + write(dict(sub))
    r = rows_for(keyless_marker=marker)
    check("sign-up without session inserted", len(r) == 2 and all(x["contactType"] == "Subscribe" and x["checkinDateTime"] == CHECKIN_UTC for x in r), r, j)

    # 8. an all-blank call with no session -> one keyless row
    marker2 = f"unit1-proof-{run}-blank"
    j = write({"uuid": UUID, "orgID": "0", "orgCode": marker2, "zipcode": "81521", "sessionID": "",
               "contactType": "", "checkinDateTime": "", "checkinReply": ""})
    r = rows_for(keyless_marker=marker2)
    check("all-blank without session inserted", len(r) == 1 and r[0]["contactType"] is None and r[0]["zipcode"] == "81521", r, j)

    print()
    failed = 0
    for name, ok, detail, stmt_jobs in results:
        failed += not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
        if not ok:
            print(f"      rows: {detail}")
        for s in jobs.stats(stmt_jobs):
            print(f"      {s['statement']:<7} processed {s['bytes_processed'] or 0:>12,}  billed {s['bytes_billed'] or 0:>12,}  {s['ms']} ms"
                  + (f"  ERROR {s['error']}" if s['error'] else ""))

    # Dry run of the real MERGE against the live table (plans only; nothing is written).
    print()
    live = f"{PROJECT}.RESPONSES.response_data"
    sample = list(client.query(
        f"SELECT SessionID, checkinDateTime FROM `{live}` "
        f"WHERE checkinDateTime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 DAY) AND SessionID IS NOT NULL "
        f"ORDER BY checkinDateTime DESC LIMIT 1").result())[0]
    schema = client.get_table(live).schema
    row = {"SessionID": sample["SessionID"], "checkinDateTime": sample["checkinDateTime"], "checkinReply": "Yes"}
    from bq_writer import build_struct_param, build_upsert_query
    new_sql = build_upsert_query(live, row, ["SessionID"], "checkinDateTime", ["checkinDateTime"])
    old_sql = new_sql.replace(" OR T.`checkinDateTime` IS NULL)", ")").replace(
        "COALESCE(S.`checkinDateTime`, T.`checkinDateTime`)", "S.`checkinDateTime`")
    key_sql = new_sql.replace("AND (T.`checkinDateTime` BETWEEN @min_dt AND @max_dt OR T.`checkinDateTime` IS NULL)", "")
    params = [bigquery.ArrayQueryParameter("rows", "RECORD", [build_struct_param(row, schema, "placeholder")]),
              bigquery.ScalarQueryParameter("min_dt", "DATETIME", row["checkinDateTime"]),
              bigquery.ScalarQueryParameter("max_dt", "DATETIME", row["checkinDateTime"])]
    for label, sql in (("range OR NULL (this build)", new_sql), ("range only (live revision)", old_sql),
                       ("key only (no check-in time)", key_sql)):
        cfg = bigquery.QueryJobConfig(query_parameters=params if "@min_dt" in sql else params[:1],
                                      dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=cfg)
        print(f"dry run  {label:<30} would process {job.total_bytes_processed:>13,} bytes")

    if args.keep:
        print(f"\nkept {table}")
    else:
        client.delete_table(table)
        print(f"\ndropped {table}")
    print(f"\n{len(results) - failed} of {len(results)} scenarios passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main_()

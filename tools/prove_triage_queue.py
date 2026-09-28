"""
Prove on real BigQuery, against a DEV copy of triage-message-data, that a triage message's three writes land
on ONE row when they run one at a time -- and that the harness can see the split when they do not.

Creates DEV.adb_triage_proof_<stamp> as `CREATE TABLE ... LIKE RESPONSES.triage-message-data`, runs the
service's own worker code (perform_upsert) against it, checks the rows, drops the table unless --keep.

  A  one after another, all six arrival orders of message / triage request / determination -> 1 row each
  B  CONTROL: message and triage request released at the same instant on two threads, no serialization
     -> the split (2 rows) is expected on at least one of the ids; shows the check can see the failure
  C  message and triage request released at the same instant into ONE worker (what a queue at max
     concurrency 1 guarantees), then the determination -> 1 row each

C models the queue's guarantee; the queue itself is proved by running (a day of new triage messages with
zero splits after the switch).

    python tools/prove_triage_queue.py
Exit code 0 = A and C passed and B reproduced the split.
"""
from __future__ import annotations

import argparse
import itertools
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from _harness import PROJECT, load_service, make_client, stamp

UUID = "00000000-0000-4000-8000-00000000a397"   # not a real contact


def parts(mid: str) -> dict:
    return {
        "message": {"message_id": mid, "uuid": UUID, "sessionid": f"{UUID}2026-09-28T12:00:00.000000-04:00",
                    "message": "triage-queue proof", "classification": "Other",
                    "message_time": "2026-09-28T12:00:01.000000-04:00", "message_day": "2026-09-28",
                    "referring_flow_id": "proof", "referring_flow_name": "proof"},
        "triage": {"message_id": mid, "triage_request_id": f"2026-09-28_12:00:01_{UUID}",
                   "triage_request_time": "2026-09-28T12:00:01.100000-04:00"},
        "determination": {"message_id": mid, "determination": "LowConcern",
                          "determination_time": "2026-09-28T12:00:30.000000-04:00"},
    }


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the DEV table in place")
    ap.add_argument("--pairs", type=int, default=5, help="ids per concurrent scenario (B and C)")
    args = ap.parse_args()

    client = make_client()
    run = stamp()
    table = f"{PROJECT}.DEV.adb_triage_proof_{run}"
    client.query(f"CREATE TABLE `{table}` LIKE `{PROJECT}.RESPONSES.triage-message-data`").result()
    print(f"DEV table: {table}")
    svc = load_service(client, {"triage_data": table})

    def write(data):
        body, status = svc.perform_upsert("triage_data", dict(data))
        if status != 200 or body.get("status") != "ok":
            raise RuntimeError(f"write failed: {status} {body}")

    def rows(mid):
        sql = (f"SELECT uuid, message, triage_request_id, determination FROM `{table}` WHERE message_id = @m")
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("m", "STRING", mid)])
        return [dict(r) for r in client.query(sql, job_config=cfg).result()]

    def whole(r):
        return (r["uuid"] == UUID and r["message"] == "triage-queue proof"
                and r["triage_request_id"] and r["determination"] == "LowConcern")

    results = {}
    try:
        # A
        a_ok = True
        for n, order in enumerate(itertools.permutations(("message", "triage", "determination"))):
            mid = f"proof-{run}-A{n}"
            p = parts(mid)
            for name in order:
                write(p[name])
            got = rows(mid)
            good = len(got) == 1 and whole(got[0])
            a_ok &= good
            print(f"A {' -> '.join(order):40s} rows {len(got)} {'ok' if good else 'FAIL'}")
        results["A"] = a_ok

        # B (control)
        split = 0
        for n in range(args.pairs):
            mid = f"proof-{run}-B{n}"
            p = parts(mid)
            gate = threading.Barrier(2)
            errs = []

            def go(d):
                try:
                    gate.wait()
                    write(d)
                except Exception as exc:   # a serialization conflict is also an outcome worth seeing
                    errs.append(str(exc)[:200])
            ts = [threading.Thread(target=go, args=(p[k],)) for k in ("triage", "message")]
            [t.start() for t in ts]
            [t.join() for t in ts]
            got = rows(mid)
            split += len(got) == 2
            print(f"B concurrent, no queue      {mid}: rows {len(got)}{' errors ' + str(errs) if errs else ''}")
        results["B"] = split > 0
        print(f"B control: {split} of {args.pairs} ids split")

        # C
        c_ok = True
        for n in range(args.pairs):
            mid = f"proof-{run}-C{n}"
            p = parts(mid)
            with ThreadPoolExecutor(max_workers=1) as one:   # the queue's guarantee: one write at a time
                futs = [one.submit(write, p["triage"]), one.submit(write, p["message"])]
                [f.result() for f in futs]
            write(p["determination"])
            got = rows(mid)
            good = len(got) == 1 and whole(got[0])
            c_ok &= good
            print(f"C same instant, one worker  {mid}: rows {len(got)} {'ok' if good else 'FAIL'}")
        results["C"] = c_ok
    finally:
        if not args.keep:
            client.query(f"DROP TABLE `{table}`").result()
            print(f"dropped {table}")

    print("A (six orders, one at a time):", "PASS" if results.get("A") else "FAIL")
    print("B (control, concurrent):", "split reproduced" if results.get("B") else "NOT REPRODUCED - C is not evidence")
    print("C (same instant, one worker):", "PASS" if results.get("C") else "FAIL")
    ok = results.get("A") and results.get("B") and results.get("C")
    print("RESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main_()

"""
Proof on BigQuery (DEV only): a flush transaction that runs past its limit is stopped, its changes
are rolled back, and the table it held is free for the next flush.

    python tools/prove_flush_timeout.py            (creates, uses and drops one DEV table)

What it does, on `early-alert-responses.DEV.flushfix_timeout_<yyyymmdd>` (one row, n = 0):

  A   starts a transaction the way the flusher does (src/main.py _run_flush_script): UPDATE the
      table (n + 1), then a statement that computes for minutes, with a job timeout of 20 s; the
      flusher's wait ends 10 s after that and asks BigQuery to cancel the job.
  B0  the control, 8 s in, while A is still running: a second transaction updating the same table.
      It must be REFUSED (BigQuery lets one transaction at a time change a table). If it is not,
      A never held the table and the run proves nothing: CONTROL FAILED.
  B   after A has ended: the same second transaction (n + 100), tried every 3 s. PASS needs it to
      commit within 30 s of A ending.
  n   read at the end: 100 means A's change was rolled back and B's landed; 101 means A committed.

It reports how A ended (BigQuery's own timeout, or our cancel request) and how long each step took.
This proves the limit for a statement that is busy. It cannot reproduce a statement stuck inside
BigQuery itself (what happened on 2026-09-30 ended in a BigQuery internal error); for that case the
limit is the same request to BigQuery, and the first real slow moment is the proof.

The table is dropped at the end, whatever happened. Nothing outside DEV is read or written.
Exit 0 = PASS, 1 = FAIL, 2 = CONTROL FAILED (inconclusive).
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT = "early-alert-responses"
LIMIT_S, GRACE_S = 20.0, 10.0          # the service uses 120 / 75 s and 10 s; short here so the proof takes a minute
CONTROL_AT_S = 8.0
FREE_WITHIN_S = 30.0
# Busy for a long time and reads no table (0 bytes billed). Measured 2026-10-03: the same statement over
# 30,000 x 30,000 ran 64.8 s, so 200,000 x 200,000 runs far past the limit.
LONG = ("SELECT COUNT(*) FROM UNNEST(GENERATE_ARRAY(1, 200000)) a "
        "CROSS JOIN UNNEST(GENERATE_ARRAY(1, 200000)) b WHERE MOD(a * b, 7) = 3")


def table_id(today=None) -> str:
    day = (today or datetime.now(timezone.utc)).strftime("%Y%m%d")
    return f"{PROJECT}.DEV.flushfix_timeout_{day}"


def held_script(table: str) -> str:
    return f"BEGIN TRANSACTION;\nUPDATE `{table}` SET n = n + 1 WHERE TRUE;\n{LONG};\nCOMMIT TRANSACTION;"


def next_script(table: str) -> str:
    return f"BEGIN TRANSACTION;\nUPDATE `{table}` SET n = n + 100 WHERE TRUE;\nCOMMIT TRANSACTION;"


def _short(exc) -> str:
    return " ".join(str(exc).split())[:300]


def run_held(client, table: str, say, sleep, clock, control) -> dict:
    """A: the transaction that runs past its limit, exactly as the flusher runs a flush script. Calls control() at 8 s."""
    started = clock()
    job = client.query(held_script(table), job_config=bigquery.QueryJobConfig(job_timeout_ms=int(LIMIT_S * 1000)))
    say(f"A  started (job {job.job_id}), job timeout {LIMIT_S:.0f} s")
    sleep(CONTROL_AT_S)
    control_result = control()
    out = {"job_id": job.job_id, "control": control_result}
    try:
        job.result(timeout=max(1.0, LIMIT_S + GRACE_S - (clock() - started)))
        out.update(how="A FINISHED (the long statement did not run long enough)", ended=False)
        return out
    except TimeoutError:
        out["how"] = f"our wait ended at {clock() - started:.0f} s and BigQuery was asked to cancel the job"
        job.cancel()
    except Exception as exc:
        out["how"] = f"BigQuery ended the job at {clock() - started:.0f} s: {_short(exc)}"
        out["early"] = clock() - started < LIMIT_S - 2      # ended before its limit: not the timeout at work
    deadline = clock() + 60                     # wait for the job to be DONE, so "A ended" is a fact
    while clock() < deadline:
        job.reload()
        if job.state == "DONE":
            break
        sleep(2)
    out.update(ended=job.state == "DONE", seconds=clock() - started,
               error=(job.error_result or {}).get("message") if job.error_result else None)
    return out


def try_next(client, table: str) -> str | None:
    """One attempt at the next transaction. None = committed; otherwise the reason it was refused."""
    try:
        client.query(next_script(table)).result(timeout=60)
        return None
    except Exception as exc:
        return _short(exc)


def run_proof(client, table: str | None = None, say=print, sleep=time.sleep, clock=time.monotonic) -> int:
    table = table or table_id()
    if not table.startswith(f"{PROJECT}.DEV."):
        raise SystemExit("refusing: the proof table must be in DEV")
    client.query(f"CREATE TABLE `{table}` (id STRING, n INT64) "
                 f"OPTIONS (description = 'flush timeout proof; dropped by the run that made it')").result()
    say(f"created {table}")
    try:
        client.query(f"INSERT INTO `{table}` (id, n) VALUES ('row', 0)").result()

        def control():
            refused = try_next(client, table)
            say(f"B0 (control, while A runs): " + (f"REFUSED -- {refused}" if refused else "COMMITTED"))
            return refused

        a = run_held(client, table, say, sleep, clock, control)
        say(f"A  {a['how']}")
        if a.get("error"):
            say(f"A  job error: {' '.join(a['error'].split())[:300]}")
        if a.get("early"):
            say("CONTROL FAILED: A ended before its limit, for another reason; this run proves nothing")
            return 2
        if a["control"] is None:
            say("CONTROL FAILED: a second transaction committed while A was running, so A did not hold the table; "
                "this run proves nothing")
            return 2
        if not a.get("ended"):
            say("FAIL: A had not ended 60 s after it was asked to stop" if "how" in a and "FINISHED" not in a["how"]
                else "CONTROL FAILED: A finished by itself; this run proves nothing")
            return 1 if "FINISHED" not in a["how"] else 2
        say(f"A  ended {a['seconds']:.0f} s after it started")
        freed_from, waited, refused = clock(), None, None
        while clock() - freed_from <= FREE_WITHIN_S:
            refused = try_next(client, table)
            if refused is None:
                waited = clock() - freed_from
                break
            sleep(3)
        if waited is None:
            say(f"B  still refused {FREE_WITHIN_S:.0f} s after A ended: {refused}")
        else:
            say(f"B  committed {waited:.0f} s after A ended")
        n = [r["n"] for r in client.query(f"SELECT n FROM `{table}`").result()]
        say(f"n  = {n} (100 = A rolled back and B landed; 101 = A committed; 0 = B never landed)")
        ok = waited is not None and n == [100]
        say("PROOF PASS: the timed-out transaction was stopped and rolled back, and the table was free "
            f"{waited:.0f} s later" if ok else "PROOF FAIL")
        return 0 if ok else 1
    finally:
        client.delete_table(table, not_found_ok=True)
        say(f"dropped {table}")


if __name__ == "__main__":
    from _harness import make_client
    sys.exit(run_proof(make_client()))

"""
Maintenance pause for the single-writer flush (the cutover's switch window).

    python tools/flush_pause.py on     [--dataset OPS|DEV]
    python tools/flush_pause.py off    [--dataset OPS|DEV]
    python tools/flush_pause.py status [--dataset OPS|DEV]

OPS (the default) is production; DEV is staging and proofs. The single-writer tables are never in RESPONSES.

on:  sets paused_since on both flush-state rows (each target's row is in its own table:
     adb_flush_state for responses, adb_flush_state_users for users). While it is set the flush
     writes nothing (a flush already under way cannot commit either: its watermark update, in the
     same transaction as its writes, requires paused_since IS NULL),
     /health/flush reports "paused", and the 5-minute sweep does not raise the backlog alert. The
     sweep raises PAUSE instead if the pause lasts longer than 4 hours (a pause left on by mistake).
off: clears it; the next flush drains the backlog in receive order.
Each prints the state afterwards and exits 1 unless both rows are in the state asked for.
Runs as the active gcloud account. Retries a write that collides with a running flush.
"""
from __future__ import annotations

import argparse
import sys
import time

from _harness import PROJECT, make_client

from google.cloud import bigquery

IDS = ("flush:responses", "flush:users")


def state_tables(dataset: str) -> dict[str, str]:
    """State row id -> the table holding it. One table per target, as the service reads them (src/config.py)."""
    base = f"{PROJECT}.{dataset}.adb_flush_state"
    return {"flush:responses": base, "flush:users": f"{base}_users"}


def _id_param(row_id: str):
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("id", "STRING", row_id)])


def state(client, dataset) -> list[dict]:
    rows = []
    for row_id, table in state_tables(dataset).items():
        rows += [dict(r) for r in client.query(
            f"SELECT id, watermark, version, paused_since FROM `{table}` WHERE id = @id",
            job_config=_id_param(row_id)).result()]
    return sorted(rows, key=lambda r: r["id"])


def backlog(client, dataset) -> dict:
    out = {}
    for row_id, table in state_tables(dataset).items():
        for r in client.query(
                f"SELECT st.id, COUNT(s.request_id) n, MIN(s.received_at) oldest "
                f"FROM `{table}` st LEFT JOIN `{PROJECT}.{dataset}.adb_staging` s "
                f"ON st.id = CONCAT('flush:', s.target) AND s.received_at > st.watermark "
                f"WHERE st.id = @id GROUP BY 1", job_config=_id_param(row_id)).result():
            out[r["id"]] = (r["n"], r["oldest"])
    return out


def set_pause(client, dataset, on: bool, attempts: int = 6) -> None:
    for row_id, table in state_tables(dataset).items():
        sql = (f"UPDATE `{table}` SET paused_since = " + ("CURRENT_TIMESTAMP()" if on else "NULL") +
               " WHERE id = @id" + (" AND paused_since IS NULL" if on else ""))
        for i in range(attempts):
            try:
                client.query(sql, job_config=_id_param(row_id)).result()
                break
            except Exception as exc:          # that target's flush committing at the same moment: wait and go again
                if "concurrent" not in str(exc).lower() or i == attempts - 1:
                    raise
                time.sleep(2 + 2 * i)


def main_(argv=None, client=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=("on", "off", "status"))
    ap.add_argument("--dataset", default="OPS", choices=("OPS", "DEV"))
    args = ap.parse_args(argv)
    client = client or make_client()
    if args.action != "status":
        set_pause(client, args.dataset, args.action == "on")
    rows = state(client, args.dataset)
    waiting = backlog(client, args.dataset)
    for r in rows:
        n, oldest = waiting.get(r["id"], (0, None))
        print(f"{r['id']:<16} paused_since {str(r['paused_since']):<34} watermark {r['watermark']}  "
              f"version {r['version']}  unflushed calls {n}" + (f" (oldest {oldest})" if oldest else ""))
    if args.action == "status":
        return 0
    want_paused = args.action == "on"
    ok = [r["id"] for r in rows] == list(IDS) and all((r["paused_since"] is not None) == want_paused for r in rows)
    print(("OK: the flush is PAUSED for maintenance" if want_paused else "OK: the flush is NOT paused")
          if ok else "FAIL: the flush-state rows are not in the state asked for")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main_())

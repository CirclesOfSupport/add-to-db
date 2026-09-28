"""
DDL for the single-writer tables, and the step that creates and checks them.

    python tools/staged_ddl.py OPS                    print the statements (to read them)
    python tools/staged_ddl.py --dry-run OPS          ask BigQuery to validate every CREATE (writes nothing)
    python tools/staged_ddl.py --apply OPS            validate, create the four tables and the two state
                                                      rows, then verify them; exit 0 only if all is right
    python tools/staged_ddl.py --verify OPS           verify only (read-only): the four tables, their
                                                      columns, and exactly the two state rows

OPS (the default) is production; DEV is staging and proofs. Any other dataset, RESPONSES included, is
refused: the single-writer tables are operational objects and never live beside the data they write.

--apply runs each statement itself through the BigQuery client (never through a shell argument, so
there is no line-joining step to get wrong). A table that already exists stops it before anything
else is created; nothing is replaced. --verify is the read-only check.
"""
from __future__ import annotations

import sys

PROJECT = "early-alert-responses"
TABLES = ("staging", "set_aside", "flush_log", "flush_state")
STATE_IDS = ("flush:responses", "flush:users")
DATASETS = ("OPS", "DEV")   # OPS = production, DEV = staging and proofs; RESPONSES is refused

# column -> (type, mode) the flush code relies on; --verify checks each one
REQUIRED_COLUMNS = {
    "staging": {"request_id": ("STRING", "REQUIRED"), "item_index": ("INTEGER", "REQUIRED"),
                "target": ("STRING", "REQUIRED"), "received_at": ("TIMESTAMP", "REQUIRED"),
                "payload": ("STRING", "REQUIRED")},
    "set_aside": {"recorded_at": ("TIMESTAMP", "NULLABLE"), "received_at": ("TIMESTAMP", "NULLABLE"),
                    "request_id": ("STRING", "NULLABLE"), "item_index": ("INTEGER", "NULLABLE"),
                    "target": ("STRING", "NULLABLE"), "stage": ("STRING", "NULLABLE"),
                    "errors": ("STRING", "NULLABLE"), "payload": ("STRING", "NULLABLE")},
    "flush_log": {"flush_id": ("STRING", "NULLABLE"), "target": ("STRING", "NULLABLE"),
                  "started_at": ("TIMESTAMP", "NULLABLE"), "finished_at": ("TIMESTAMP", "NULLABLE"),
                  "from_wm": ("TIMESTAMP", "NULLABLE"), "to_wm": ("TIMESTAMP", "NULLABLE"),
                  "items": ("INTEGER", "NULLABLE"), "statements": ("INTEGER", "NULLABLE"),
                  "dead_letters": ("INTEGER", "NULLABLE"), "attempts": ("INTEGER", "NULLABLE"),
                  "status": ("STRING", "NULLABLE"), "error": ("STRING", "NULLABLE"),
                  "refs": ("STRING", "REPEATED")},
    "flush_state": {"id": ("STRING", "REQUIRED"), "watermark": ("TIMESTAMP", "REQUIRED"),
                    "version": ("INTEGER", "REQUIRED"), "updated_at": ("TIMESTAMP", "NULLABLE"),
                    "paused_since": ("TIMESTAMP", "NULLABLE")},
}


def _dataset(dataset: str) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"dataset {dataset!r} refused: the single-writer tables go in OPS (production) or DEV")
    return dataset


def ddl(dataset: str, prefix: str = "adb_") -> list[str]:
    _dataset(dataset)
    t = lambda name: f"`{PROJECT}.{dataset}.{prefix}{name}`"
    return [
        f"""CREATE TABLE {t('staging')} (
  request_id STRING NOT NULL, item_index INT64 NOT NULL, target STRING NOT NULL,
  received_at TIMESTAMP NOT NULL, payload STRING NOT NULL)
PARTITION BY DATE(received_at)
OPTIONS (partition_expiration_days = 30,
  description = 'add-to-db: calls held for the single writer, in receive order. Append-only.')""",
        f"""CREATE TABLE {t('set_aside')} (
  recorded_at TIMESTAMP, received_at TIMESTAMP, request_id STRING, item_index INT64,
  target STRING, stage STRING, errors STRING, payload STRING)
PARTITION BY DATE(recorded_at)
OPTIONS (description = 'add-to-db: calls not written -- failed validation (upsert), pre-flight at flush (flush), or became visible after their flush (late).')""",
        f"""CREATE TABLE {t('flush_log')} (
  flush_id STRING, target STRING, started_at TIMESTAMP, finished_at TIMESTAMP, from_wm TIMESTAMP, to_wm TIMESTAMP,
  items INT64, statements INT64, dead_letters INT64, attempts INT64, status STRING, error STRING,
  refs ARRAY<STRING>)
PARTITION BY DATE(started_at)
OPTIONS (description = 'add-to-db: one row per target per flush (ok rows are written in the flush transaction); refs = request_id:item_index of every call the flush took.')""",
        f"""CREATE TABLE {t('flush_state')} (
  id STRING NOT NULL, watermark TIMESTAMP NOT NULL, version INT64 NOT NULL, updated_at TIMESTAMP,
  paused_since TIMESTAMP)
OPTIONS (description = 'add-to-db: one watermark per target for the single writer (compare-and-set on version); paused_since set = maintenance pause.')""",
        f"""INSERT INTO {t('flush_state')} (id, watermark, version, updated_at, paused_since)
VALUES ('flush:responses', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP(), NULL),
       ('flush:users', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP(), NULL)""",
    ]


def dry_run(client, statements) -> list[str]:
    """BigQuery-validate each statement without running it; returns the failures."""
    from google.cloud import bigquery
    failures = []
    for stmt in statements:
        try:
            client.query(stmt, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        except Exception as exc:
            failures.append(f"{stmt.splitlines()[0]} -> {str(exc).splitlines()[0]}")
    return failures


def verify(client, dataset: str, prefix: str = "adb_") -> list[str]:
    """Read-only: every table and required column present, and exactly the two state rows, unpaused. Returns problems."""
    _dataset(dataset)
    problems = []
    for name in TABLES:
        table_id = f"{PROJECT}.{dataset}.{prefix}{name}"
        try:
            schema = {f.name: (f.field_type, f.mode) for f in client.get_table(table_id).schema}
        except Exception as exc:
            problems.append(f"{table_id}: not readable ({str(exc).splitlines()[0]})")
            continue
        for col, want in REQUIRED_COLUMNS[name].items():
            got = schema.get(col)
            if got is None:
                problems.append(f"{table_id}: column {col} missing")
            elif (got[0].replace("INT64", "INTEGER"), got[1] or "NULLABLE") != want:
                problems.append(f"{table_id}: column {col} is {got[0]} {got[1]}, expected {want[0]} {want[1]}")
    if not problems:
        rows = [dict(r) for r in client.query(
            f"SELECT id, version, paused_since FROM `{PROJECT}.{dataset}.{prefix}flush_state` ORDER BY id").result()]
        ids = [r["id"] for r in rows]
        if ids != sorted(STATE_IDS):
            problems.append(f"flush_state rows are {ids}, expected exactly {sorted(STATE_IDS)}")
        problems += [f"flush_state {r['id']} is paused since {r['paused_since']}" for r in rows if r["paused_since"] is not None]
    return problems


def apply(client, dataset: str, prefix: str = "adb_", out=print) -> list[str]:
    """Validate, create and verify. Stops at the first failure; never replaces an existing table."""
    _dataset(dataset)
    statements = ddl(dataset, prefix)
    creates = [s for s in statements if s.startswith("CREATE")]
    failed = dry_run(client, creates)
    if failed:
        return [f"DDL does not validate, nothing created: {f}" for f in failed]
    existing = {t.table_id for t in client.list_tables(f"{PROJECT}.{dataset}")}
    already = [f"{prefix}{n}" for n in TABLES if f"{prefix}{n}" in existing]
    if already:
        return [f"already exists, nothing created: {PROJECT}.{dataset}.{n}" for n in already]
    for stmt in statements:
        client.query(stmt).result()
        out(f"ran: {stmt.splitlines()[0]}")
    return verify(client, dataset, prefix)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dataset = args[0] if args else "OPS"
    if dataset not in DATASETS:
        print(f"FAIL dataset {dataset!r} refused: the single-writer tables go in OPS (production) or DEV")
        sys.exit(2)
    statements = ddl(dataset)
    if "--dry-run" in sys.argv or "--apply" in sys.argv or "--verify" in sys.argv:
        from _harness import make_client
        client = make_client()
        if "--dry-run" in sys.argv:
            creates = [s for s in statements if s.startswith("CREATE")]
            failed = dry_run(client, creates)
            for f in failed:
                print("FAIL", f)
            print(f"{len(creates) - len(failed)} of {len(creates)} CREATE statements valid for {dataset}")
            sys.exit(1 if failed else 0)
        problems = apply(client, dataset) if "--apply" in sys.argv else verify(client, dataset)
        for p in problems:
            print("FAIL", p)
        if problems:
            sys.exit(1)
        print(f"OK: {PROJECT}.{dataset} has adb_staging, adb_set_aside, adb_flush_log, adb_flush_state with every "
              f"required column, and exactly the state rows {', '.join(STATE_IDS)}, neither paused")
        sys.exit(0)
    print(";\n\n".join(statements) + ";")

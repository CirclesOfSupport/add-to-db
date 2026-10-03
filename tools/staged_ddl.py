"""
DDL for the single-writer tables, and the step that creates and checks them.

    python tools/staged_ddl.py OPS                    print the statements (to read them)
    python tools/staged_ddl.py --dry-run OPS          ask BigQuery to validate every CREATE (writes nothing)
    python tools/staged_ddl.py --apply OPS            validate, create the five tables and the two state
                                                      rows, then verify them; exit 0 only if all is right
    python tools/staged_ddl.py --verify OPS           verify only (read-only): the five tables, their
                                                      columns, and each target's state row in its own table
    python tools/staged_ddl.py --split-users-state OPS
                                                      for a dataset created when both state rows shared
                                                      adb_flush_state: create adb_flush_state_users and
                                                      copy the users row into it, once; then verify

Each target has its own state table (adb_flush_state for responses, adb_flush_state_users for users):
BigQuery lets one transaction at a time change rows in a table, so a shared state table made each
target's flush wait on the other's. --split-users-state replaces nothing: an existing table is kept,
an existing users row is kept as it is, and the row left behind in adb_flush_state is not touched.

OPS (the default) is production; DEV is staging and proofs. Any other dataset, RESPONSES included, is
refused: the single-writer tables are operational objects and never live beside the data they write.

--apply runs each statement itself through the BigQuery client (never through a shell argument, so
there is no line-joining step to get wrong). A table that already exists stops it before anything
else is created; nothing is replaced. --verify is the read-only check.
"""
from __future__ import annotations

import sys

PROJECT = "early-alert-responses"
TABLES = ("staging", "set_aside", "flush_log", "flush_state", "flush_state_users")
STATE_IDS = ("flush:responses", "flush:users")
STATE_TABLES = {"flush:responses": "flush_state", "flush:users": "flush_state_users"}   # row id -> its table
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
REQUIRED_COLUMNS["flush_state_users"] = REQUIRED_COLUMNS["flush_state"]


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
OPTIONS (description = 'add-to-db: the single writer watermark for responses (compare-and-set on version); paused_since set = maintenance pause.')""",
        _users_state_create(dataset, prefix),
        f"""INSERT INTO {t('flush_state')} (id, watermark, version, updated_at, paused_since)
VALUES ('flush:responses', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP(), NULL)""",
        f"""INSERT INTO {t('flush_state_users')} (id, watermark, version, updated_at, paused_since)
VALUES ('flush:users', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP(), NULL)""",
    ]


def _users_state_create(dataset: str, prefix: str = "adb_") -> str:
    return f"""CREATE TABLE `{PROJECT}.{dataset}.{prefix}flush_state_users` (
  id STRING NOT NULL, watermark TIMESTAMP NOT NULL, version INT64 NOT NULL, updated_at TIMESTAMP,
  paused_since TIMESTAMP)
OPTIONS (description = 'add-to-db: the single writer watermark for users, in its own table so a users flush and a responses flush never wait on each other; paused_since set = maintenance pause.')"""


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
    """
    Read-only: every table and required column present, and each target's state row in its own
    table, unpaused. adb_flush_state may still carry a flush:users row from before the users row had
    its own table; that row is unused and is not a problem. Returns problems.
    """
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
        for row_id, name in STATE_TABLES.items():
            rows = [dict(r) for r in client.query(
                f"SELECT id, version, paused_since FROM `{PROJECT}.{dataset}.{prefix}{name}` ORDER BY id").result()]
            ids = [r["id"] for r in rows]
            unused = ["flush:users"] if name == "flush_state" else []       # left from before the split
            if ids.count(row_id) != 1 or any(i != row_id and i not in unused for i in ids):
                problems.append(f"{name} rows are {ids}, expected exactly one '{row_id}'"
                                + (" (a 'flush:users' row from before the split is allowed)" if unused else ""))
            problems += [f"{name} {r['id']} is paused since {r['paused_since']}"
                         for r in rows if r["id"] == row_id and r["paused_since"] is not None]
    return problems


def split_users_state(client, dataset: str, prefix: str = "adb_", out=print) -> list[str]:
    """
    For a dataset whose tables were created when both state rows shared <prefix>flush_state: create
    <prefix>flush_state_users and copy the users row into it (watermark, version and pause as they
    are at this moment), once. Nothing is replaced: an existing table is kept and an existing users
    row is kept as it is, so running this again changes nothing. The row left in <prefix>flush_state
    is not touched. Ends with verify. Run it just before deploying the code that reads the new table:
    users calls flushed between this copy and that deploy are flushed once more by the new code,
    which rewrites the same rows (a users row is keyed, and each column takes the last call's value).
    """
    _dataset(dataset)
    old, new = f"{PROJECT}.{dataset}.{prefix}flush_state", f"{PROJECT}.{dataset}.{prefix}flush_state_users"
    existing = {t.table_id for t in client.list_tables(f"{PROJECT}.{dataset}")}
    if f"{prefix}flush_state" not in existing:
        return [f"{old} does not exist, nothing created (a new dataset is set up with --apply)"]
    if f"{prefix}flush_state_users" not in existing:
        create = _users_state_create(dataset, prefix)
        failed = dry_run(client, [create])
        if failed:
            return [f"DDL does not validate, nothing created: {f}" for f in failed]
        client.query(create).result()
        out(f"ran: {create.splitlines()[0]}")
    else:
        out(f"kept: {new} already exists")
    read = f"SELECT id, watermark, version, paused_since FROM `{new}` ORDER BY id"
    rows = [dict(r) for r in client.query(read).result()]
    if not rows:
        copy = (f"INSERT INTO `{new}` (id, watermark, version, updated_at, paused_since)\n"
                f"SELECT id, watermark, version, CURRENT_TIMESTAMP(), paused_since FROM `{old}` WHERE id = 'flush:users'")
        client.query(copy).result()
        out(f"ran: {copy.splitlines()[0]}")
        rows = [dict(r) for r in client.query(read).result()]
    else:
        out(f"kept: {new} already holds {[r['id'] for r in rows]}")
    for r in rows:
        out(f"{prefix}flush_state_users: {r['id']} watermark {r['watermark']} version {r['version']}")
    return verify(client, dataset, prefix)


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
    if any(f in sys.argv for f in ("--dry-run", "--apply", "--verify", "--split-users-state")):
        from _harness import make_client
        client = make_client()
        if "--dry-run" in sys.argv:
            creates = [s for s in statements if s.startswith("CREATE")]
            failed = dry_run(client, creates)
            for f in failed:
                print("FAIL", f)
            print(f"{len(creates) - len(failed)} of {len(creates)} CREATE statements valid for {dataset}")
            sys.exit(1 if failed else 0)
        if "--apply" in sys.argv:
            problems = apply(client, dataset)
        elif "--split-users-state" in sys.argv:
            problems = split_users_state(client, dataset)
        else:
            problems = verify(client, dataset)
        for p in problems:
            print("FAIL", p)
        if problems:
            sys.exit(1)
        print(f"OK: {PROJECT}.{dataset} has adb_staging, adb_set_aside, adb_flush_log, adb_flush_state and "
              f"adb_flush_state_users with every required column, and each state row in its own table "
              f"({', '.join(f'{i} in adb_{n}' for i, n in STATE_TABLES.items())}), neither paused")
        sys.exit(0)
    print(";\n\n".join(statements) + ";")

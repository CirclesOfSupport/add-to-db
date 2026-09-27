"""
DDL for the single-writer tables.

    python tools/staged_ddl.py RESPONSES              print the statements (review, then run them)
    python tools/staged_ddl.py --dry-run RESPONSES    ask BigQuery to validate every CREATE (writes nothing)

The proof tool dry-runs, creates and uses the same statements in DEV; the
closing INSERT can only be validated once its table exists, which the proof does.
"""
from __future__ import annotations

import sys

PROJECT = "early-alert-responses"


def ddl(dataset: str, prefix: str = "adb_") -> list[str]:
    t = lambda name: f"`{PROJECT}.{dataset}.{prefix}{name}`"
    return [
        f"""CREATE TABLE {t('staging')} (
  request_id STRING NOT NULL, item_index INT64 NOT NULL, target STRING NOT NULL,
  received_at TIMESTAMP NOT NULL, payload STRING NOT NULL)
PARTITION BY DATE(received_at)
OPTIONS (partition_expiration_days = 30,
  description = 'add-to-db: calls held for the single writer, in receive order. Append-only.')""",
        f"""CREATE TABLE {t('dead_letter')} (
  recorded_at TIMESTAMP, received_at TIMESTAMP, request_id STRING, item_index INT64,
  target STRING, stage STRING, errors STRING, payload STRING)
PARTITION BY DATE(recorded_at)
OPTIONS (description = 'add-to-db: calls that failed validation (stage upsert) or pre-flight at flush (stage flush).')""",
        f"""CREATE TABLE {t('flush_log')} (
  flush_id STRING, started_at TIMESTAMP, finished_at TIMESTAMP, from_wm TIMESTAMP, to_wm TIMESTAMP,
  items INT64, statements INT64, dead_letters INT64, status STRING, error STRING)
PARTITION BY DATE(started_at)
OPTIONS (description = 'add-to-db: one row per flush attempt (ok rows are written in the flush transaction).')""",
        f"""CREATE TABLE {t('flush_state')} (
  id STRING NOT NULL, watermark TIMESTAMP NOT NULL, version INT64 NOT NULL, updated_at TIMESTAMP)
OPTIONS (description = 'add-to-db: watermark of the single writer (compare-and-set on version).')""",
        f"""INSERT INTO {t('flush_state')} (id, watermark, version, updated_at)
VALUES ('flush', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP())""",
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


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dataset = args[0] if args else "RESPONSES"
    statements = ddl(dataset)
    if "--dry-run" in sys.argv:
        from _harness import make_client
        creates = [s for s in statements if s.startswith("CREATE")]
        failed = dry_run(make_client(), creates)
        for f in failed:
            print("FAIL", f)
        print(f"{len(creates) - len(failed)} of {len(creates)} CREATE statements valid for {dataset}")
        sys.exit(1 if failed else 0)
    print(";\n\n".join(statements) + ";")

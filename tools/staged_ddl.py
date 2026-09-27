"""
DDL for the single-writer tables. `python tools/staged_ddl.py RESPONSES` prints
the statements for a dataset (review, then run them in the console or bq).
The proof tool creates the same tables in DEV.
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
OPTIONS (description = 'add-to-db: the single writer''s watermark (compare-and-set on version).')""",
        f"""INSERT INTO {t('flush_state')} (id, watermark, version, updated_at)
VALUES ('flush', CURRENT_TIMESTAMP(), 0, CURRENT_TIMESTAMP())""",
    ]


if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "RESPONSES"
    print(";\n\n".join(ddl(dataset)) + ";")

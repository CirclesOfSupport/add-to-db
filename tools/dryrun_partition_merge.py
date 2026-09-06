"""
Dry-run the service's real MERGE against a partitioned test table and report
bytes that WOULD be scanned, with and without the partition-range predicate.

Nothing is written. A dry run only plans the query.

Run from the repo root with Application Default Credentials that can read the
dataset:

    python tools/dryrun_partition_merge.py
    python tools/dryrun_partition_merge.py --table early-alert-responses.RESPONSES.rd_liketest

Pass criterion: `with_predicate` well under 1 MB while `without_predicate`
is tens of MB. If both are the same, pruning is not happening.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google.cloud import bigquery  # noqa: E402

from bq_writer import (  # noqa: E402
    build_struct_param,
    build_upsert_query,
    coerce_payload_to_schema,
    resolve_partition_column,
)

DEFAULT_TABLE = "early-alert-responses.RESPONSES.rd_liketest"
PARTITION_COLUMN = "checkinDateTime"
KEY_COLUMNS = ["SessionID"]
# A representative response payload: key, partition column, and a few
# ordinary fields. The dry-run scan estimate depends on which columns are
# referenced, not on their values.
SAMPLE_COLUMNS = ["SessionID", "checkinDateTime", "uuid", "wellnessDomain", "response"]


def pick_sample_row(client: bigquery.Client, table_id: str) -> dict:
    cols = ", ".join(f"`{c}`" for c in SAMPLE_COLUMNS)
    sql = f"""
    SELECT {cols}
    FROM `{table_id}`
    WHERE SessionID IS NOT NULL AND checkinDateTime IS NOT NULL
    ORDER BY checkinDateTime DESC
    LIMIT 1
    """
    rows = list(client.query(sql).result())
    if not rows:
        raise SystemExit("no usable row found in " + table_id)
    return dict(rows[0].items())


def dry_run(client, table_id, schema, row, partition_column):
    partition_col, partition_value = resolve_partition_column(partition_column, schema, row)
    query = build_upsert_query(table_id, row, KEY_COLUMNS, partition_col)
    params = [bigquery.ArrayQueryParameter("rows", "RECORD", [build_struct_param(row, schema, "placeholder")])]
    if partition_col:
        params.append(bigquery.ScalarQueryParameter("min_dt", "DATETIME", partition_value))
        params.append(bigquery.ScalarQueryParameter("max_dt", "DATETIME", partition_value))
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params, dry_run=True, use_query_cache=False))
    return job.total_bytes_processed, query, partition_col, partition_value


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default=DEFAULT_TABLE)
    args = ap.parse_args()

    client = bigquery.Client(project=args.table.split(".")[0])
    schema = client.get_table(args.table).schema
    raw = pick_sample_row(client, args.table)
    row, errors = coerce_payload_to_schema(raw, schema)
    if errors:
        raise SystemExit("coercion errors: " + "; ".join(errors))

    print(f"table: {args.table}")
    print(f"sample SessionID={row['SessionID']} checkinDateTime={row['checkinDateTime']}")

    b_with, q_with, col, val = dry_run(client, args.table, schema, row, PARTITION_COLUMN)
    b_without, q_without, _, _ = dry_run(client, args.table, schema, row, None)

    print()
    print(f"with_predicate    ({col} BETWEEN {val} AND {val}): {b_with:,} bytes")
    print(f"without_predicate (key only):                              {b_without:,} bytes")
    if b_without:
        print(f"reduction: {b_without / max(b_with, 1):,.0f}x")
    print()
    print("MERGE as planned (with predicate):")
    print(q_with)


if __name__ == "__main__":
    main()

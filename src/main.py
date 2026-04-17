from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request
from google.cloud import bigquery

from auth import is_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS, UPSERT_KEYS

app = Flask(__name__)
client = bigquery.Client()


def get_table_schema(table_id: str) -> list[bigquery.SchemaField]:
    table = client.get_table(table_id)
    return list(table.schema)


def validate_payload(
    payload: dict,
    schema: list[bigquery.SchemaField],
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    schema_fields = {field.name: field for field in schema}

    for field_name, field in schema_fields.items():
        if field.mode == "REQUIRED" and field_name not in payload:
            errors.append(f"Missing required field: {field_name}")

    for key, value in payload.items():
        if key not in schema_fields:
            warnings.append(f"Unknown field ignored: {key}")
            continue

        field = schema_fields[key]

        if value is None:
            if field.mode == "REQUIRED":
                errors.append(f"Field '{key}' cannot be null")
            continue

        checker = TYPE_CHECKERS.get(field.field_type)
        if checker and not checker(value):
            errors.append(
                f"Field '{key}' expected type {field.field_type}, got {type(value).__name__}"
            )

    return errors, warnings


def filter_to_schema(payload: dict, schema: list[bigquery.SchemaField]) -> dict:
    allowed_names = {field.name for field in schema}
    return {k: v for k, v in payload.items() if k in allowed_names}


def quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"


def validate_upsert_keys(
    key_columns: list[str],
    schema: list[bigquery.SchemaField],
    row: dict,
) -> list[str]:
    errors: list[str] = []
    schema_names = {field.name for field in schema}

    for key in key_columns:
        if key not in schema_names:
            errors.append(f"Configured upsert key '{key}' does not exist in the table schema")
        elif key not in row:
            errors.append(f"Missing upsert key field: {key}")
        elif row[key] is None:
            errors.append(f"Upsert key field '{key}' cannot be null")

    return errors


def create_staging_table_from_target(
    target_table: bigquery.Table,
    staging_table_id: str,
) -> bigquery.Table:
    staging_table = bigquery.Table(staging_table_id, schema=target_table.schema)
    staging_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    return client.create_table(staging_table)


def build_merge_sql(
    target_table_id: str,
    staging_table_id: str,
    schema: list[bigquery.SchemaField],
    key_columns: list[str],
) -> str:
    column_names = [field.name for field in schema]

    on_clause = " AND ".join(
        [f"T.{quote_identifier(col)} = S.{quote_identifier(col)}" for col in key_columns]
    )

    non_key_columns = [col for col in column_names if col not in key_columns]

    if non_key_columns:
        update_clause = ",\n        ".join(
            [f"{quote_identifier(col)} = S.{quote_identifier(col)}" for col in non_key_columns]
        )
        when_matched_clause = f"""WHEN MATCHED THEN
      UPDATE SET
        {update_clause}"""
    else:
        # Rare case: all columns are keys. Keep the MERGE valid.
        noop_col = key_columns[0]
        when_matched_clause = f"""WHEN MATCHED THEN
      UPDATE SET
        {quote_identifier(noop_col)} = T.{quote_identifier(noop_col)}"""

    insert_columns_sql = ", ".join([quote_identifier(col) for col in column_names])
    insert_values_sql = ", ".join([f"S.{quote_identifier(col)}" for col in column_names])

    sql = f"""
MERGE {quote_identifier(target_table_id)} T
USING {quote_identifier(staging_table_id)} S
ON {on_clause}
{when_matched_clause}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns_sql})
  VALUES ({insert_values_sql})
"""
    return sql


def upsert_row(
    table: bigquery.Table,
    row: dict,
    key_columns: list[str],
) -> None:
    staging_table_id = (
        f"{table.project}.{table.dataset_id}._upsert_staging_{uuid.uuid4().hex}"
    )

    create_staging_table_from_target(table, staging_table_id)

    try:
        insert_errors = client.insert_rows_json(staging_table_id, [row])
        if insert_errors:
            raise RuntimeError(f"Failed to insert row into staging table: {insert_errors}")

        target_table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
        merge_sql = build_merge_sql(
            target_table_id=target_table_id,
            staging_table_id=staging_table_id,
            schema=list(table.schema),
            key_columns=key_columns,
        )

        query_job = client.query(merge_sql)
        query_job.result()

    finally:
        client.delete_table(staging_table_id, not_found_ok=True)


@app.get("/")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/ingest")
def ingest():
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    target = request.args.get("target") or body.get("target")
    data = body.get("data")

    if not target:
        return jsonify({"error": "Missing target"}), 400

    if not isinstance(data, dict):
        return jsonify({"error": "Field 'data' must be a JSON object"}), 400

    table_id = ALLOWED_TARGETS.get(target)
    if not table_id:
        return jsonify({
            "error": "Invalid target",
            "allowed_targets": sorted(ALLOWED_TARGETS.keys())
        }), 400

    try:
        table = client.get_table(table_id)
        schema = list(table.schema)
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": f"Unable to load schema for target '{target}'",
            "details": str(exc),
        }), 500

    errors, warnings = validate_payload(data, schema)
    if errors:
        return jsonify({
            "status": "error",
            "errors": errors,
            "warnings": warnings,
        }), 400

    row = filter_to_schema(data, schema)

    try:
        insert_errors = client.insert_rows_json(table.reference, [row])
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "BigQuery insert failed",
            "details": str(exc),
        }), 500

    if insert_errors:
        return jsonify({
            "status": "error",
            "details": insert_errors,
        }), 500

    return jsonify({
        "status": "ok",
        "operation": "insert",
        "target": target,
        "table_id": table_id,
        "warnings": warnings,
    }), 200


@app.post("/upsert")
def upsert():
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    target = request.args.get("target") or body.get("target")
    data = body.get("data")

    if not target:
        return jsonify({"error": "Missing target"}), 400

    if not isinstance(data, dict):
        return jsonify({"error": "Field 'data' must be a JSON object"}), 400

    table_id = ALLOWED_TARGETS.get(target)
    if not table_id:
        return jsonify({
            "error": "Invalid target",
            "allowed_targets": sorted(ALLOWED_TARGETS.keys())
        }), 400

    key_columns = UPSERT_KEYS.get(target)
    if not key_columns:
        return jsonify({
            "status": "error",
            "error": f"Target '{target}' is not configured for upsert",
            "configured_upsert_targets": sorted(UPSERT_KEYS.keys()),
        }), 400

    try:
        table = client.get_table(table_id)
        schema = list(table.schema)
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": f"Unable to load schema for target '{target}'",
            "details": str(exc),
        }), 500

    errors, warnings = validate_payload(data, schema)
    row = filter_to_schema(data, schema)
    errors.extend(validate_upsert_keys(key_columns, schema, row))

    if errors:
        return jsonify({
            "status": "error",
            "errors": errors,
            "warnings": warnings,
        }), 400

    try:
        upsert_row(table=table, row=row, key_columns=key_columns)
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "BigQuery upsert failed",
            "details": str(exc),
        }), 500

    return jsonify({
        "status": "ok",
        "operation": "upsert",
        "target": target,
        "table_id": table_id,
        "keys": key_columns,
        "warnings": warnings,
    }), 200
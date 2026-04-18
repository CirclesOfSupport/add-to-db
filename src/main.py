from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request
from google.cloud import bigquery

from auth import is_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS, UPSERT_KEYS
from bq_writer import BQ_TYPE_MAP

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

def build_struct_param(row: dict, schema: list[bigquery.SchemaField], name: str) -> bigquery.StructQueryParameter:
    schema_fields = {field.name: field for field in schema}
    scalar_params = []
    for key, value in row.items():
        field = schema_fields.get(key)
        if field is None:
            continue
        bq_type = BQ_TYPE_MAP.get(field.field_type, "STRING")
        scalar_params.append(bigquery.ScalarQueryParameter(key, bq_type, value))
    return bigquery.StructQueryParameter(name, *scalar_params)


def build_upsert_query(target_table_id: str, row: dict, key_columns: list[str]):
    """
    Generates a parameterized MERGE statement using only columns present in the row.
    """
    column_names = list(row.keys())  # Only columns we actually have

    on_clause = " AND ".join([
        f"T.{quote_identifier(col)} = S.{quote_identifier(col)}" 
        for col in key_columns
    ])

    non_key_columns = [col for col in column_names if col not in key_columns]

    if non_key_columns:
        update_clause = ",\n        ".join([
            f"{quote_identifier(col)} = S.{quote_identifier(col)}" 
            for col in non_key_columns
        ])
        matched_action = f"WHEN MATCHED THEN UPDATE SET {update_clause}"
    else:
        matched_action = f"WHEN MATCHED THEN UPDATE SET {quote_identifier(key_columns[0])} = S.{quote_identifier(key_columns[0])}"

    insert_cols = ", ".join([quote_identifier(col) for col in column_names])
    insert_vals = ", ".join([f"S.{quote_identifier(col)}" for col in column_names])

    return f"""
    MERGE {quote_identifier(target_table_id)} T
    USING UNNEST(@rows) S
    ON {on_clause}
    {matched_action}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_vals})
    """

def run_upsert(table_id: str, schema: list[bigquery.SchemaField], row: dict, key_columns: list[str]):
    query = build_upsert_query(table_id, row, key_columns)  # pass row, not schema
    struct_param = build_struct_param(row, schema, "placeholder")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("rows", "RECORD", [struct_param])
        ]
    )

    query_job = client.query(query, job_config=job_config)
    return query_job.result()


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
        insert_errors = client.insert_rows(table=table, rows=[row])
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
        return jsonify({"status": "error", "errors": errors}), 400

    # 2. Execute the optimized Upsert
    try:
        run_upsert(
            table_id=table_id,
            schema=schema,
            row=row,
            key_columns=key_columns
        )
    except Exception as exc:
        return jsonify({
            "status": "error", 
            "error": "BigQuery MERGE failed", 
            "details": str(exc)
        }), 500

    return jsonify({
        "status": "ok",
        "operation": "upsert",
        "target": target,
        "warnings": warnings,
    }), 200
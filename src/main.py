from __future__ import annotations

from flask import Flask, jsonify, request
from google.cloud import bigquery

from auth import is_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS, UPSERT_KEYS
from bq_writer import build_upsert_query, build_struct_param, validate_upsert_keys, add_missing_fields_to_table

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

        field = schema_fields.get(key)
        if field is None:
            warnings.append(f"Field not found in BigQuery schema after schema update: {key}")
            continue

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
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

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

    try:
        table, added_fields = add_missing_fields_to_table(client, table, data)
        schema = list(table.schema)
    except ValueError as exc:
        return jsonify({
            "status": "error",
            "error": "Invalid new field name",
            "details": str(exc),
        }), 400
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "Unable to update BigQuery schema",
            "details": str(exc),
        }), 500

    errors, warnings = validate_payload(data, schema)

    if added_fields:
        warnings.extend([
            f"Added new BigQuery field: {field_name}"
            for field_name in added_fields
        ])

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
        "added_fields": added_fields,
        "warnings": warnings,
    }), 200


@app.post("/upsert")
def upsert():
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401

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

    try:
        table, added_fields = add_missing_fields_to_table(client, table, data)
        schema = list(table.schema)
    except ValueError as exc:
        return jsonify({
            "status": "error",
            "error": "Invalid new field name",
            "details": str(exc),
        }), 400
    except Exception as exc:
        return jsonify({
            "status": "error",
            "error": "Unable to update BigQuery schema",
            "details": str(exc),
        }), 500

    errors, warnings = validate_payload(data, schema)

    if added_fields:
        warnings.extend([
            f"Added new BigQuery field: {field_name}"
            for field_name in added_fields
        ])

    row = filter_to_schema(data, schema)
    errors.extend(validate_upsert_keys(key_columns, schema, row))

    if errors:
        return jsonify({
            "status": "error",
            "errors": errors,
            "warnings": warnings,
        }), 400

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
        "added_fields": added_fields,
        "warnings": warnings,
    }), 200
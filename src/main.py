from __future__ import annotations
from flask import Flask, jsonify, request
from google.cloud import bigquery

from auth import is_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS

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

@app.get("/")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/ingest")
def ingest():
    if not is_authorized(request):
        return jsonify({"error": "unauthorized"}), 401

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
            "allowed_targets": sorted(ALLOWED_TARGETS.keys()),
        }), 400

    try:
        schema = get_table_schema(table_id)
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
        insert_errors = client.insert_rows_json(table_id, [row])
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
        "target": target,
        "table_id": table_id,
        "warnings": warnings,
    }), 200
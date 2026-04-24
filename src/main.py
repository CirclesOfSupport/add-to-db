from __future__ import annotations

from flask import Flask, jsonify, request, Response
from google.cloud import bigquery

from auth import is_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS, UPSERT_KEYS, PROJECT_ID
from bq_writer import (
    build_upsert_query,
    build_struct_param,
    validate_upsert_keys,
    add_missing_fields_to_table,
    get_users_and_responses_view_query
)

app = Flask(__name__)
client = bigquery.Client()

USERS_RESPONSES_TARGETS = {"users", "responses", "users_copy", "responses_copy"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def err(message: str | dict, status: int, **extra) -> tuple[Response, int]:
    """Return a JSON error response, merging any extra fields into the body."""
    body = {"status": "error", **({"error": message} if isinstance(message, str) else message), **extra}
    return jsonify(body), status


def get_table_schema(table_id: str) -> list[bigquery.SchemaField]:
    table = client.get_table(table_id)
    return list(table.schema)


def normalize_target_requests(body: dict, query_target: str | None = None) -> tuple[list[dict], str | None]:
    """
    Supports both existing single-target requests:

    {
        "target": "users_copy",
        "data": {...}
    }

    and new multi-target requests:

    {
        "targets": [
            {"target": "users_copy", "data": {...}},
            {"target": "responses_copy", "data": {...}}
        ]
    }
    """

    if "targets" in body:
        if query_target:
            return [], "Do not use query parameter 'target' with multi-target requests"

        targets = body.get("targets")

        if not isinstance(targets, list):
            return [], "Field 'targets' must be a list"

        if not targets:
            return [], "Field 'targets' must not be empty"

        normalized = []

        for index, item in enumerate(targets):
            if not isinstance(item, dict):
                return [], f"Each item in 'targets' must be a JSON object. Invalid item at index {index}"

            target = item.get("target")
            data = item.get("data")

            if not target:
                return [], f"Missing target at targets[{index}]"

            if not isinstance(data, dict):
                return [], f"Field 'data' at targets[{index}] must be a JSON object"

            normalized.append({"target": target, "data": data})

        return normalized, None

    target = query_target or body.get("target")
    data = body.get("data")

    if not target:
        return [], "Missing target"

    if not isinstance(data, dict):
        return [], "Field 'data' must be a JSON object"

    return [{"target": target, "data": data}], None


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
    query = build_upsert_query(table_id, row, key_columns)
    struct_param = build_struct_param(row, schema, "placeholder")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("rows", "RECORD", [struct_param])
        ]
    )

    query_job = client.query(query, job_config=job_config)
    return query_job.result()


def update_users_and_responses_view(client: bigquery.Client, project_id: str, target: str):
    query = get_users_and_responses_view_query(project_id, target)
    query_job = client.query(query)
    return query_job.result()


def prepare_item(target: str, data: dict, results: list) -> tuple[bigquery.Table, list[bigquery.SchemaField], list[str], list[str]] | tuple[Response, int]:
    """
    Shared pre-flight for both /ingest and /upsert: validates the target,
    loads + migrates the schema, and runs payload validation.

    Returns (table, schema, added_fields, warnings) on success, or a Flask
    error response tuple on failure.
    """
    table_id = ALLOWED_TARGETS.get(target)
    if not table_id:
        return err(
            "Invalid target",
            400,
            target=target,
            allowed_targets=sorted(ALLOWED_TARGETS.keys()),
        )

    try:
        table = client.get_table(table_id)
        schema = list(table.schema)
    except Exception as exc:
        return err(f"Unable to load schema for target '{target}'", 500, details=str(exc))

    try:
        table, added_fields = add_missing_fields_to_table(client, table, data)
        schema = list(table.schema)
    except ValueError as exc:
        return err("Invalid new field name", 400, target=target, details=str(exc))
    except Exception as exc:
        return err("Unable to update BigQuery schema", 500, target=target, details=str(exc))

    errors, warnings = validate_payload(data, schema)

    if added_fields:
        warnings.extend(f"Added new BigQuery field: {f}" for f in added_fields)
        if target in USERS_RESPONSES_TARGETS:
            update_users_and_responses_view(client, PROJECT_ID, target)

    return table, schema, added_fields, errors, warnings


def parse_request() -> tuple[list[dict], None] | tuple[None, tuple[Response, int]]:
    """Authorize, parse JSON, and normalize target requests from the current Flask request."""
    if not is_authorized(request):
        return None, (jsonify({"error": "Unauthorized"}), 401)

    body = request.get_json(silent=True)
    if body is None:
        return None, (jsonify({"error": "Invalid or missing JSON body"}), 400)

    target_requests, normalize_error = normalize_target_requests(
        body=body,
        query_target=request.args.get("target"),
    )

    if normalize_error:
        return None, (jsonify({"error": normalize_error}), 400)

    return target_requests, None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/ingest")
def ingest():
    target_requests, error_response = parse_request()
    if error_response:
        return error_response

    results = []

    for item in target_requests:
        target, data = item["target"], item["data"]

        prepared = prepare_item(target, data, results)
        if isinstance(prepared[0], Response):
            return prepared

        table, schema, added_fields, errors, warnings = prepared

        if errors:
            return jsonify({
                "status": "error",
                "target": target,
                "errors": errors,
                "warnings": warnings,
                "completed_results": results,
            }), 400

        row = filter_to_schema(data, schema)

        try:
            insert_errors = client.insert_rows(table=table, rows=[row])
        except Exception as exc:
            return err("BigQuery insert failed", 500, target=target, details=str(exc), completed_results=results)

        if insert_errors:
            return jsonify({
                "status": "error",
                "target": target,
                "details": insert_errors,
                "completed_results": results,
            }), 500

        results.append({
            "status": "ok",
            "operation": "insert",
            "target": target,
            "table_id": ALLOWED_TARGETS[target],
            "added_fields": added_fields,
            "warnings": warnings,
        })

    if len(results) == 1:
        return jsonify(results[0]), 200

    return jsonify({"status": "ok", "operation": "insert", "results": results}), 200


@app.post("/upsert")
def upsert():
    target_requests, error_response = parse_request()
    if error_response:
        return error_response

    results = []

    for item in target_requests:
        target, data = item["target"], item["data"]

        key_columns = UPSERT_KEYS.get(target)
        if not key_columns:
            return jsonify({
                "status": "error",
                "error": f"Target '{target}' is not configured for upsert",
                "target": target,
                "configured_upsert_targets": sorted(UPSERT_KEYS.keys()),
                "completed_results": results,
            }), 400

        prepared = prepare_item(target, data, results)
        if isinstance(prepared[0], Response):
            return prepared

        table, schema, added_fields, errors, warnings = prepared

        row = filter_to_schema(data, schema)
        errors.extend(validate_upsert_keys(key_columns, schema, row))

        if errors:
            return jsonify({
                "status": "error",
                "target": target,
                "errors": errors,
                "warnings": warnings,
                "completed_results": results,
            }), 400

        try:
            run_upsert(table_id=ALLOWED_TARGETS[target], schema=schema, row=row, key_columns=key_columns)
        except Exception as exc:
            return err("BigQuery MERGE failed", 500, target=target, details=str(exc), completed_results=results)

        results.append({
            "status": "ok",
            "operation": "upsert",
            "target": target,
            "table_id": ALLOWED_TARGETS[target],
            "added_fields": added_fields,
            "warnings": warnings,
        })

    if len(results) == 1:
        return jsonify(results[0]), 200

    return jsonify({"status": "ok", "operation": "upsert", "results": results}), 200
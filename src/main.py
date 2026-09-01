from __future__ import annotations
import time as time_module
import random
from flask import Flask, jsonify, request, Response
from google.cloud import bigquery
from auth import is_authorized, is_task_request_authorized
from config import ALLOWED_TARGETS, TYPE_CHECKERS, UPSERT_KEYS, PROJECT_ID
from bq_writer import (
    build_upsert_query,
    build_struct_param,
    validate_upsert_keys,
    add_missing_fields_to_table,
    get_users_and_responses_view_query,
    normalize_payload_to_schema,
    resolve_key_columns,
    coerce_payload_to_schema
)
from tasks import enqueue_write

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


# Module-level schema cache: table_id -> (schema, fetched_at_epoch_seconds).
# Cloud Run reuses a warm container across requests, so this persists between
# calls; after the first request per table the schema is served from memory and
# /upsert returns 202 with no BigQuery round-trip on the hot path. TTL is short
# because schemas only change when the /tasks/* worker runs a column migration --
# an infrequent, write-side event -- and the worker re-reads/re-validates against
# the freshly migrated schema before writing regardless, so a briefly-stale
# pre-flight schema here affects warnings only, never write correctness.
_SCHEMA_CACHE: dict[str, tuple[list[bigquery.SchemaField], float]] = {}
_SCHEMA_CACHE_TTL_S = 300


def get_table_schema(table_id: str, *, force_refresh: bool = False) -> list[bigquery.SchemaField]:
    now = time_module.time()
    if not force_refresh:
        cached = _SCHEMA_CACHE.get(table_id)
        if cached is not None and (now - cached[1]) < _SCHEMA_CACHE_TTL_S:
            return cached[0]
    table = client.get_table(table_id)
    schema = list(table.schema)
    _SCHEMA_CACHE[table_id] = (schema, now)
    return schema


def normalize_target_requests(body: dict, query_table: str | None = None) -> tuple[list[dict], str | None]:
    """
    Supports both existing single-table requests:

    {
        "table": "users_copy",
        "data": {...}
    }

    and new multi-table requests:

    {
        "tables": [
            {"table": "users_copy", "data": {...}},
            {"table": "responses_copy", "data": {...}}
        ]
    }
    """

    if "tables" in body:
        if query_table:
            return [], "Do not use query parameter 'table' with multi-table requests"

        tables = body.get("tables")

        if not isinstance(tables, list):
            return [], "Field 'tables' must be a list"

        if not tables:
            return [], "Field 'tables' must not be empty"

        normalized = []

        for index, item in enumerate(tables):
            if not isinstance(item, dict):
                return [], f"Each item in 'tables' must be a JSON object. Invalid item at index {index}"

            table = item.get("table")
            data = item.get("data")

            if not table:
                return [], f"Missing table at tables[{index}]"

            if not isinstance(data, dict):
                return [], f"Field 'data' at tables[{index}] must be a JSON object"

            normalized.append({"target": table, "data": data})

        return normalized, None

    table = query_table or body.get("table")
    data = body.get("data")

    if not table:
        return [], "Missing table"

    if not isinstance(data, dict):
        return [], "Field 'data' must be a JSON object"

    return [{"target": table, "data": data}], None


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

def run_upsert_with_retry(
    table_id: str,
    schema: list[bigquery.SchemaField],
    row: dict,
    key_columns: list[str],
    max_attempts: int = 4,
    base_delay: float = 0.25,
):
    """
    Retries the MERGE on concurrent update errors with exponential backoff + jitter.
    4 attempts with 0.25s base delay = ~0.25, ~0.5, ~1.0s waits = ~2s total worst case.
    """
    for attempt in range(max_attempts):
        try:
            return run_upsert(table_id, schema, row, key_columns)
        except Exception as exc:
            is_concurrent_error = "Could not serialize access" in str(exc)
            is_last_attempt = attempt == max_attempts - 1

            if not is_concurrent_error or is_last_attempt:
                raise

            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
            time_module.sleep(delay)

def update_users_and_responses_view(client: bigquery.Client, project_id: str, target: str):
    query = get_users_and_responses_view_query(project_id, target)
    query_job = client.query(query)
    return query_job.result()


def prepare_item(
    target: str,
    data: dict,
    results: list,
) -> tuple[bigquery.Table, list[bigquery.SchemaField], list[str], list[str], list[str], dict] | tuple[Response, int]:
    """
    Shared pre-flight for both /ingest and /upsert: validates the target,
    loads + migrates the schema, normalizes payload keys to BigQuery schema
    casing, and runs payload validation.

    Returns:
      table, schema, added_fields, errors, warnings, normalized_data

    Or:
      Flask error response tuple
    """
    table_id = ALLOWED_TARGETS.get(target)
    if not table_id:
        return err(
            "Invalid table",
            400,
            table=target,
            allowed_tables=sorted(ALLOWED_TARGETS.keys()),
        )

    try:
        table = client.get_table(table_id)
        schema = list(table.schema)
    except Exception as exc:
        return err(f"Unable to load schema for table '{target}'", 500, details=str(exc))

    try:
        table, added_fields = add_missing_fields_to_table(client, table, data)
        schema = list(table.schema)
    except ValueError as exc:
        return err("Invalid new field name", 400, table=target, details=str(exc))
    except Exception as exc:
        return err("Unable to update BigQuery schema", 500, table=target, details=str(exc))

    normalized_data, normalize_errors = normalize_payload_to_schema(data, schema)
    coerced_data, coerce_errors = coerce_payload_to_schema(normalized_data, schema)

    errors, warnings = validate_payload(coerced_data, schema)
    errors.extend(normalize_errors)
    errors.extend(coerce_errors)

    if added_fields:
        warnings.extend(f"Added new BigQuery field: {f}" for f in added_fields)
        # A migration just changed this table's schema. Refresh the pre-flight
        # cache now so the next /upsert /ingest request validates against the new
        # column set immediately instead of serving a stale schema for up to TTL.
        _SCHEMA_CACHE[table_id] = (schema, time_module.time())
        if target in USERS_RESPONSES_TARGETS:
            update_users_and_responses_view(client, PROJECT_ID, target)

    return table, schema, added_fields, errors, warnings, coerced_data


def precheck_payload(
    target: str,
    data: dict,
) -> tuple[list[bigquery.SchemaField], dict, list[str], list[str]] | tuple[Response, int]:
    """
    Fast synchronous pre-flight for the public /ingest and /upsert endpoints:
    validates the target and payload against the *current* table schema so
    callers still get an immediate 400 on bad data, without doing the schema
    migration or BigQuery write. Those happen in the queued /tasks/* worker,
    which re-validates against the freshly migrated schema before writing --
    so unrecognized fields here just produce warnings, not errors.

    Returns:
      schema, coerced_data, errors, warnings

    Or:
      Flask error response tuple
    """
    table_id = ALLOWED_TARGETS.get(target)
    if not table_id:
        return err(
            "Invalid table",
            400,
            table=target,
            allowed_tables=sorted(ALLOWED_TARGETS.keys()),
        )

    try:
        schema = get_table_schema(table_id)
    except Exception as exc:
        return err(f"Unable to load schema for table '{target}'", 500, details=str(exc))

    normalized_data, normalize_errors = normalize_payload_to_schema(data, schema)
    coerced_data, coerce_errors = coerce_payload_to_schema(normalized_data, schema)

    errors, warnings = validate_payload(coerced_data, schema)
    errors.extend(normalize_errors)
    errors.extend(coerce_errors)

    return schema, coerced_data, errors, warnings


def parse_request() -> tuple[list[dict], None] | tuple[None, tuple[Response, int]]:
    """Authorize, parse JSON, and normalize target requests from the current Flask request."""
    if not is_authorized(request):
        return None, (jsonify({"error": "Unauthorized"}), 401)

    body = request.get_json(silent=True)
    if body is None:
        return None, (jsonify({"error": "Invalid or missing JSON body"}), 400)

    target_requests, normalize_error = normalize_target_requests(
        body=body,
        query_table=request.args.get("table"),
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

    queued = []

    for item in target_requests:
        target, data = item["target"], item["data"]

        precheck = precheck_payload(target, data)
        if isinstance(precheck[0], Response):
            return precheck

        schema, coerced_data, errors, warnings = precheck

        if errors:
            return jsonify({
                "status": "error",
                "table": target,
                "errors": errors,
                "warnings": warnings,
                "queued_results": queued,
            }), 400

        try:
            task_name = enqueue_write("/tasks/ingest", target, data)
        except Exception as exc:
            return err("Failed to queue insert", 500, table=target, details=str(exc), queued_results=queued)

        queued.append({
            "status": "queued",
            "operation": "insert",
            "table": target,
            "table_id": ALLOWED_TARGETS[target],
            "warnings": warnings,
            "task_name": task_name,
        })

    if len(queued) == 1:
        return jsonify(queued[0]), 202

    return jsonify({"status": "queued", "operation": "insert", "results": queued}), 202


@app.post("/upsert")
def upsert():
    target_requests, error_response = parse_request()
    if error_response:
        return error_response

    queued = []

    for item in target_requests:
        target, data = item["target"], item["data"]

        key_columns = UPSERT_KEYS.get(target)
        if not key_columns:
            return jsonify({
                "status": "error",
                "error": f"Table '{target}' is not configured for upsert",
                "table": target,
                "configured_upsert_tables": sorted(UPSERT_KEYS.keys()),
                "queued_results": queued,
            }), 400

        precheck = precheck_payload(target, data)
        if isinstance(precheck[0], Response):
            return precheck

        schema, coerced_data, errors, warnings = precheck

        resolved_key_columns, key_errors = resolve_key_columns(key_columns, schema)
        errors.extend(key_errors)

        row = filter_to_schema(coerced_data, schema)
        errors.extend(validate_upsert_keys(resolved_key_columns, schema, row))

        if errors:
            return jsonify({
                "status": "error",
                "table": target,
                "errors": errors,
                "warnings": warnings,
                "queued_results": queued,
            }), 400

        try:
            task_name = enqueue_write("/tasks/upsert", target, data)
        except Exception as exc:
            return err("Failed to queue upsert", 500, table=target, details=str(exc), queued_results=queued)

        queued.append({
            "status": "queued",
            "operation": "upsert",
            "table": target,
            "table_id": ALLOWED_TARGETS[target],
            "warnings": warnings,
            "task_name": task_name,
        })

    if len(queued) == 1:
        return jsonify(queued[0]), 202

    return jsonify({"status": "queued", "operation": "upsert", "results": queued}), 202


# ---------------------------------------------------------------------------
# Cloud Tasks worker endpoints
#
# These perform the actual schema migration + BigQuery write that /ingest and
# /upsert used to do inline. They're only reachable with a valid OIDC token
# from our own Cloud Tasks queue (see auth.is_task_request_authorized) --
# never called directly by webhook clients. A non-2xx response tells Cloud
# Tasks to retry with backoff; a 2xx acknowledges the task so it is not
# retried, even when the write failed for reasons a retry can't fix.
# ---------------------------------------------------------------------------

def _parse_task_request() -> tuple[str, dict, None] | tuple[None, None, tuple[Response, int]]:
    if not is_task_request_authorized(request):
        return None, None, (jsonify({"error": "Unauthorized"}), 401)

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return None, None, (jsonify({"error": "Invalid or missing JSON body"}), 400)

    target = body.get("table")
    data = body.get("data")

    if not target or not isinstance(data, dict):
        return None, None, (jsonify({"error": "Malformed task payload"}), 400)

    return target, data, None


@app.post("/tasks/ingest")
def tasks_ingest():
    target, data, error_response = _parse_task_request()
    if error_response:
        return error_response

    prepared = prepare_item(target, data, [])
    if isinstance(prepared[0], Response):
        app.logger.error("tasks/ingest: pre-flight failed for table '%s'", target)
        return jsonify({"status": "error", "table": target}), 200

    table, schema, added_fields, errors, warnings, data = prepared

    if errors:
        app.logger.error("tasks/ingest: validation errors for table '%s': %s", target, errors)
        return jsonify({"status": "error", "table": target, "errors": errors}), 200

    row = filter_to_schema(data, schema)

    try:
        insert_errors = client.insert_rows(table=table, rows=[row])
    except Exception as exc:
        app.logger.error("tasks/ingest: BigQuery insert failed for table '%s': %s", target, exc)
        return err("BigQuery insert failed", 500, table=target, details=str(exc))

    if insert_errors:
        app.logger.error("tasks/ingest: insert row errors for table '%s': %s", target, insert_errors)
        return jsonify({"status": "error", "table": target, "details": insert_errors}), 500

    return jsonify({
        "status": "ok",
        "operation": "insert",
        "table": target,
        "table_id": ALLOWED_TARGETS[target],
        "added_fields": added_fields,
        "warnings": warnings,
    }), 200


@app.post("/tasks/upsert")
def tasks_upsert():
    target, data, error_response = _parse_task_request()
    if error_response:
        return error_response

    key_columns = UPSERT_KEYS.get(target)
    if not key_columns:
        app.logger.error("tasks/upsert: table '%s' is not configured for upsert", target)
        return jsonify({"status": "error", "table": target}), 200

    prepared = prepare_item(target, data, [])
    if isinstance(prepared[0], Response):
        app.logger.error("tasks/upsert: pre-flight failed for table '%s'", target)
        return jsonify({"status": "error", "table": target}), 200

    table, schema, added_fields, errors, warnings, data = prepared

    resolved_key_columns, key_errors = resolve_key_columns(key_columns, schema)
    errors.extend(key_errors)

    row = filter_to_schema(data, schema)
    errors.extend(validate_upsert_keys(resolved_key_columns, schema, row))

    if errors:
        app.logger.error("tasks/upsert: validation errors for table '%s': %s", target, errors)
        return jsonify({"status": "error", "table": target, "errors": errors}), 200

    try:
        run_upsert_with_retry(
            table_id=ALLOWED_TARGETS[target],
            schema=schema,
            row=row,
            key_columns=resolved_key_columns,
        )
    except Exception as exc:
        app.logger.error("tasks/upsert: BigQuery MERGE failed for table '%s': %s", target, exc)
        return err("BigQuery MERGE failed", 500, table=target, details=str(exc))

    return jsonify({
        "status": "ok",
        "operation": "upsert",
        "table": target,
        "table_id": ALLOWED_TARGETS[target],
        "added_fields": added_fields,
        "warnings": warnings,
    }), 200
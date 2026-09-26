from __future__ import annotations
import time as time_module
import random
from flask import Flask, jsonify, request, Response
from google.cloud import bigquery
from auth import is_authorized, is_task_request_authorized
from config import (
    ALLOWED_TARGETS,
    TYPE_CHECKERS,
    UPSERT_KEYS,
    PROJECT_ID,
    PARTITION_COLUMNS,
    DATETIME_CONVENTIONS,
    PRESERVE_ON_BLANK,
    KEYLESS_INSERT_TARGETS,
)
from bq_writer import (
    build_upsert_query,
    build_batch_merge_query,
    build_batch_struct_params,
    build_batch_insert_query,
    fold_rows,
    build_insert_query,
    is_keyless_row,
    build_struct_param,
    validate_upsert_keys,
    add_missing_fields_to_table,
    get_users_and_responses_view_query,
    normalize_payload_to_schema,
    resolve_key_columns,
    resolve_partition_column,
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


def run_upsert(
    table_id: str,
    schema: list[bigquery.SchemaField],
    row: dict,
    key_columns: list[str],
    partition_column: str | None = None,
    preserve_columns: list[str] | None = None,
):
    partition_col, partition_value = resolve_partition_column(partition_column, schema, row)

    query = build_upsert_query(table_id, row, key_columns, partition_col, preserve_columns)
    struct_param = build_struct_param(row, schema, "placeholder")

    query_parameters = [
        bigquery.ArrayQueryParameter("rows", "RECORD", [struct_param])
    ]
    if partition_col:
        # One row per MERGE, so min == max == the row's own partition value.
        query_parameters.append(
            bigquery.ScalarQueryParameter("min_dt", "DATETIME", partition_value)
        )
        query_parameters.append(
            bigquery.ScalarQueryParameter("max_dt", "DATETIME", partition_value)
        )

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

    query_job = client.query(query, job_config=job_config)
    return query_job.result()

def run_insert(
    table_id: str,
    schema: list[bigquery.SchemaField],
    row: dict,
):
    """DML INSERT of one keyless row (see config.KEYLESS_INSERT_TARGETS)."""
    query = build_insert_query(table_id, row)
    struct_param = build_struct_param(row, schema, "placeholder")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "RECORD", [struct_param])]
    )
    return client.query(query, job_config=job_config).result()


def run_upsert_with_retry(
    table_id: str,
    schema: list[bigquery.SchemaField],
    row: dict,
    key_columns: list[str],
    partition_column: str | None = None,
    max_attempts: int = 4,
    base_delay: float = 0.25,
    preserve_columns: list[str] | None = None,
):
    """
    Retries the MERGE on concurrent update errors with exponential backoff + jitter.
    4 attempts with 0.25s base delay = ~0.25, ~0.5, ~1.0s waits = ~2s total worst case.
    """
    for attempt in range(max_attempts):
        try:
            return run_upsert(table_id, schema, row, key_columns, partition_column, preserve_columns)
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
    coerced_data, coerce_errors = coerce_payload_to_schema(
        normalized_data, schema, DATETIME_CONVENTIONS.get(target)
    )

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
    coerced_data, coerce_errors = coerce_payload_to_schema(
        normalized_data, schema, DATETIME_CONVENTIONS.get(target)
    )

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
        if not (target in KEYLESS_INSERT_TARGETS and is_keyless_row(resolved_key_columns, row)):
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


def perform_upsert(target: str, data: dict) -> tuple[dict, int]:
    """
    The /tasks/upsert worker: schema migration, coercion, validation and the
    BigQuery write for one queued item. Returns (body, status). A non-2xx status
    tells Cloud Tasks to retry; a 2xx acknowledges the task even when the write
    failed for a reason a retry cannot fix.
    """
    key_columns = UPSERT_KEYS.get(target)
    if not key_columns:
        app.logger.error("tasks/upsert: table '%s' is not configured for upsert", target)
        return {"status": "error", "table": target}, 200

    prepared = prepare_item(target, data, [])
    if isinstance(prepared[0], Response):
        app.logger.error("tasks/upsert: pre-flight failed for table '%s'", target)
        return {"status": "error", "table": target}, 200

    table, schema, added_fields, errors, warnings, data = prepared

    resolved_key_columns, key_errors = resolve_key_columns(key_columns, schema)
    errors.extend(key_errors)

    row = filter_to_schema(data, schema)
    keyless = target in KEYLESS_INSERT_TARGETS and is_keyless_row(resolved_key_columns, row)
    if not keyless:
        errors.extend(validate_upsert_keys(resolved_key_columns, schema, row))

    if errors:
        app.logger.error("tasks/upsert: validation errors for table '%s': %s", target, errors)
        return {"status": "error", "table": target, "errors": errors}, 200

    try:
        if keyless:
            run_insert(ALLOWED_TARGETS[target], schema, row)
        else:
            run_upsert_with_retry(
                table_id=ALLOWED_TARGETS[target],
                schema=schema,
                row=row,
                key_columns=resolved_key_columns,
                partition_column=PARTITION_COLUMNS.get(target),
                preserve_columns=PRESERVE_ON_BLANK.get(target),
            )
    except Exception as exc:
        app.logger.error("tasks/upsert: BigQuery %s failed for table '%s': %s",
                         "INSERT" if keyless else "MERGE", target, exc)
        return {
            "status": "error",
            "error": "BigQuery INSERT failed" if keyless else "BigQuery MERGE failed",
            "table": target,
            "details": str(exc),
        }, 500

    return {
        "status": "ok",
        "operation": "insert" if keyless else "upsert",
        "table": target,
        "table_id": ALLOWED_TARGETS[target],
        "added_fields": added_fields,
        "warnings": warnings,
    }, 200


@app.post("/tasks/upsert")
def tasks_upsert():
    target, data, error_response = _parse_task_request()
    if error_response:
        return error_response

    body, status = perform_upsert(target, data)
    return jsonify(body), status


def perform_flush(target: str, items: list[dict]) -> dict:
    """
    The batched single-writer path: fold every queued call for `target` (in
    the order received) into one row per key and write them with one MERGE
    (plus one INSERT for rows without a key). Rows whose partition value is
    unknown go in a second MERGE on the key alone. Only ONE flush may run per
    target at a time; that single writer is what makes the last call win and
    keeps two calls from inserting the same key twice.

    items: payload dicts, oldest first. Raises on a BigQuery failure so the
    caller can keep the items and retry them in the next flush.
    """
    key_columns = UPSERT_KEYS[target]
    table_id = ALLOWED_TARGETS[target]
    rows = []
    schema = None
    skipped = []
    for data in items:
        prepared = prepare_item(target, data, [])
        if isinstance(prepared[0], Response):
            skipped.append("pre-flight failed")
            continue
        _table, schema, _added, errors, _warnings, coerced = prepared
        resolved, key_errors = resolve_key_columns(key_columns, schema)
        row = filter_to_schema(coerced, schema)
        keyless = target in KEYLESS_INSERT_TARGETS and is_keyless_row(resolved, row)
        if not keyless:
            errors = errors + key_errors + validate_upsert_keys(resolved, schema, row)
        if errors:
            skipped.append("; ".join(errors))
            continue
        rows.append(row)
    if not rows:
        return {"rows": 0, "statements": 0, "skipped": skipped}

    resolved, _ = resolve_key_columns(key_columns, schema)   # schema of the last prepared item
    preserve = PRESERVE_ON_BLANK.get(target)
    folded, keyless_rows = fold_rows(rows, resolved, preserve)
    partition_column = PARTITION_COLUMNS.get(target)
    pcol = None
    if partition_column:
        pcol = next((f.name for f in schema if f.name.lower() == partition_column.lower()), None)

    statements = 0
    groups = [list(folded.values())]
    if pcol:
        groups = [[r for r in folded.values() if r.get(pcol) is not None],
                  [r for r in folded.values() if r.get(pcol) is None]]
    for i, group in enumerate(groups):
        if not group:
            continue
        columns = sorted({c for r in group for c in r}, key=lambda c: [f.name for f in schema].index(c))
        use_range = pcol is not None and i == 0
        if use_range and pcol not in columns:
            columns.append(pcol)
        query = build_batch_merge_query(table_id, columns, resolved,
                                        pcol if use_range else None, preserve)
        params = [bigquery.ArrayQueryParameter("rows", "RECORD", build_batch_struct_params(group, columns, schema))]
        if use_range:
            values = [r[pcol] for r in group]
            params += [bigquery.ScalarQueryParameter("min_dt", "DATETIME", min(values)),
                       bigquery.ScalarQueryParameter("max_dt", "DATETIME", max(values))]
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        statements += 1

    if keyless_rows:
        columns = sorted({c for r in keyless_rows for c in r}, key=lambda c: [f.name for f in schema].index(c))
        structs = [build_struct_param({c: r.get(c) for c in columns}, schema, "placeholder") for r in keyless_rows]
        client.query(build_batch_insert_query(table_id, columns),
                     job_config=bigquery.QueryJobConfig(query_parameters=[
                         bigquery.ArrayQueryParameter("rows", "RECORD", structs)])).result()
        statements += 1

    return {"rows": len(rows), "keys": len(folded), "keyless": len(keyless_rows),
            "statements": statements, "skipped": skipped}

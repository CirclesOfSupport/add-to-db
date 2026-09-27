from __future__ import annotations
import time as time_module
import random
import json
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
import config
from bq_writer import apply_stale_reply_guard
from bq_writer import (
    quote_identifier,
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
from tasks import enqueue_write, enqueue_flush

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
    received_at = _dt.now(_tz.utc)      # stamped on arrival: the order the flusher writes in
    staged: list[tuple[str, dict]] = []

    def stage_pending() -> str | None:
        """Make the validated staged calls durable (before any response goes back)."""
        if not staged:
            return None
        request_id = stage_calls(staged, received_at)
        for t, _ in staged:
            queued.append({"status": "queued", "operation": "upsert", "table": t,
                           "table_id": ALLOWED_TARGETS[t], "staged_request_id": request_id})
        staged.clear()
        kick_flush(received_at)
        return request_id

    def dead_letter_rejected(target, data, errors):
        if target in config.STAGED_TARGETS:
            try:
                record_dead_letters([{"received_at": received_at, "request_id": None, "item_index": None,
                                      "target": target, "stage": "upsert",
                                      "errors": json.dumps(errors, default=str)[:4000],
                                      "payload": json.dumps(data, default=str)}])
            except Exception as exc:
                app.logger.error("upsert: could not record dead letter for '%s': %s", target, exc)

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
            dead_letter_rejected(target, data, precheck[0].get_json(silent=True))
            try:
                stage_pending()
            except Exception as exc:
                return err("Failed to stage upsert", 500, details=str(exc), queued_results=queued)
            return precheck

        schema, coerced_data, errors, warnings = precheck

        resolved_key_columns, key_errors = resolve_key_columns(key_columns, schema)
        errors.extend(key_errors)

        row = filter_to_schema(coerced_data, schema)
        if not (target in KEYLESS_INSERT_TARGETS and is_keyless_row(resolved_key_columns, row)):
            errors.extend(validate_upsert_keys(resolved_key_columns, schema, row))

        if errors:
            dead_letter_rejected(target, data, errors)
            try:
                stage_pending()
            except Exception as exc:
                return err("Failed to stage upsert", 500, details=str(exc), queued_results=queued)
            return jsonify({
                "status": "error",
                "table": target,
                "errors": errors,
                "warnings": warnings,
                "queued_results": queued,
            }), 400

        if target in config.STAGED_TARGETS:
            staged.append((target, data))
            continue

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

    try:
        stage_pending()
    except Exception as exc:
        return err("Failed to stage upsert", 500, details=str(exc), queued_results=queued)

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

    row = apply_stale_reply_guard(filter_to_schema(data, schema), config.STALE_REPLY_FIELDS.get(target))
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


def plan_target_writes(target: str, items: list[dict], prefix: str = "") -> dict:
    """
    Fold the queued calls for `target` (oldest first) into the statements that
    write them: one MERGE for rows with a check-in time (partition range), one
    key-only MERGE for rows without one, one INSERT for rows without a key.
    items: [{"data": payload, "ref": caller's reference}]. A call that fails
    pre-flight is returned in "dead" with its errors instead of being written.
    """
    key_columns = UPSERT_KEYS[target]
    table_id = ALLOWED_TARGETS[target]
    rows, dead, schema = [], [], None
    reply_fields = config.STALE_REPLY_FIELDS.get(target)
    for item in items:
        prepared = prepare_item(target, dict(item["data"]), [])
        if isinstance(prepared[0], Response):
            body = prepared[0].get_json(silent=True) or {}
            dead.append({"ref": item.get("ref"), "errors": json.dumps(body, default=str)[:4000]})
            continue
        _table, schema, _added, errors, _warnings, coerced = prepared
        resolved, key_errors = resolve_key_columns(key_columns, schema)
        row = filter_to_schema(coerced, schema)
        keyless = target in KEYLESS_INSERT_TARGETS and is_keyless_row(resolved, row)
        if not keyless:
            errors = errors + key_errors + validate_upsert_keys(resolved, schema, row)
        if errors:
            dead.append({"ref": item.get("ref"), "errors": "; ".join(errors)[:4000]})
            continue
        rows.append(apply_stale_reply_guard(row, reply_fields))

    statements = []
    if not rows:
        return {"statements": statements, "dead": dead, "rows": 0, "keys": 0, "keyless": 0}

    names = [f.name for f in schema]
    resolved, _ = resolve_key_columns(key_columns, schema)
    preserve = PRESERVE_ON_BLANK.get(target)
    folded, keyless_rows = fold_rows(rows, resolved, preserve)
    partition_column = PARTITION_COLUMNS.get(target)
    pcol = next((n for n in names if partition_column and n.lower() == partition_column.lower()), None)

    groups = [("all", list(folded.values()), False)]
    if pcol:
        groups = [("ranged", [r for r in folded.values() if r.get(pcol) is not None], True),
                  ("keyonly", [r for r in folded.values() if r.get(pcol) is None], False)]
    for label, group, use_range in groups:
        if not group:
            continue
        columns = sorted({c for r in group for c in r}, key=names.index)
        rows_param = f"{prefix}{target}_{label}_rows"
        min_param, max_param = f"{prefix}{target}_min_dt", f"{prefix}{target}_max_dt"
        sql = build_batch_merge_query(table_id, columns, resolved, pcol if use_range else None, preserve,
                                      rows_param=rows_param, min_param=min_param, max_param=max_param)
        params = [bigquery.ArrayQueryParameter(rows_param, "RECORD", build_batch_struct_params(group, columns, schema))]
        if use_range:
            values = [r[pcol] for r in group]
            params += [bigquery.ScalarQueryParameter(min_param, "DATETIME", min(values)),
                       bigquery.ScalarQueryParameter(max_param, "DATETIME", max(values))]
        statements.append((sql, params))

    if keyless_rows:
        columns = sorted({c for r in keyless_rows for c in r}, key=names.index)
        rows_param = f"{prefix}{target}_keyless_rows"
        structs = [build_struct_param({c: r.get(c) for c in columns}, schema, "placeholder") for r in keyless_rows]
        statements.append((build_batch_insert_query(table_id, columns, rows_param),
                           [bigquery.ArrayQueryParameter(rows_param, "RECORD", structs)]))

    return {"statements": statements, "dead": dead, "rows": len(rows),
            "keys": len(folded), "keyless": len(keyless_rows)}


def perform_flush(target: str, items: list[dict]) -> dict:
    """
    Fold and write one target's calls now, statement by statement (no staging,
    no watermark). Used by the replay tool and tests; the live path is
    run_flush_cycle, which does the same inside one transaction.
    """
    plan = plan_target_writes(target, [{"data": d, "ref": i} for i, d in enumerate(items)])
    for sql, params in plan["statements"]:
        client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return {"rows": plan["rows"], "keys": plan["keys"], "keyless": plan["keyless"],
            "statements": len(plan["statements"]), "skipped": [d["errors"] for d in plan["dead"]]}


# ---------------------------------------------------------------------------
# Live single-writer hookup (config.STAGED_TARGETS)
# ---------------------------------------------------------------------------

import random as random_module
import uuid as uuid_module
from datetime import datetime as _dt, timedelta as _td, timezone as _tz

_last_kicked_bucket: int | None = None


def stage_calls(items: list[tuple[str, dict]], received_at) -> str:
    """Append validated calls to the staging table; durable once this returns."""
    request_id = uuid_module.uuid4().hex
    rows = [{"request_id": request_id, "item_index": i, "target": t,
             "received_at": received_at.isoformat(), "payload": json.dumps(d, default=str)}
            for i, (t, d) in enumerate(items)]
    errors = client.insert_rows_json(config.STAGING_TABLE, rows,
                                     row_ids=[f"{request_id}-{i}" for i in range(len(rows))])
    if errors:
        raise RuntimeError(f"staging append failed: {errors}")
    return request_id


def record_dead_letters(entries: list[dict]) -> None:
    """DML INSERT (not streaming) so the table never holds a streaming buffer."""
    if not entries:
        return
    client.query(
        f"INSERT INTO {quote_identifier(config.DEAD_LETTER_TABLE)} "
        f"(recorded_at, received_at, request_id, item_index, target, stage, errors, payload) "
        f"SELECT CURRENT_TIMESTAMP(), received_at, request_id, item_index, target, stage, errors, payload "
        f"FROM UNNEST(@dl_rows)",
        job_config=bigquery.QueryJobConfig(query_parameters=[_dead_letter_param(entries)])).result()


def _dead_letter_param(entries: list[dict]) -> bigquery.ArrayQueryParameter:
    return bigquery.ArrayQueryParameter("dl_rows", "RECORD", [
        bigquery.StructQueryParameter(
            "placeholder",
            bigquery.ScalarQueryParameter("received_at", "TIMESTAMP", e.get("received_at")),
            bigquery.ScalarQueryParameter("request_id", "STRING", e.get("request_id")),
            bigquery.ScalarQueryParameter("item_index", "INT64", e.get("item_index")),
            bigquery.ScalarQueryParameter("target", "STRING", e.get("target")),
            bigquery.ScalarQueryParameter("stage", "STRING", e.get("stage")),
            bigquery.ScalarQueryParameter("errors", "STRING", e.get("errors")),
            bigquery.ScalarQueryParameter("payload", "STRING", e.get("payload")),
        ) for e in entries])


def kick_flush(received_at) -> None:
    """Ask for a flush of this receive bucket (named task: one per bucket, deduplicated)."""
    global _last_kicked_bucket
    bucket = int(received_at.timestamp()) // config.FLUSH_BUCKET_S
    if bucket == _last_kicked_bucket:
        return
    try:
        enqueue_flush(bucket, (bucket + 1) * config.FLUSH_BUCKET_S + config.FLUSH_SAFETY_S)
        _last_kicked_bucket = bucket
    except Exception as exc:   # the calls are durable; the next call or a retry flushes them
        app.logger.error("kick_flush: could not enqueue flush for bucket %s: %s", bucket, exc)


class FlushFailed(RuntimeError):
    """One or more targets could not be flushed in this cycle (the others were)."""

    def __init__(self, results: dict, errors: dict):
        self.results, self.errors = results, errors
        super().__init__("; ".join(f"{t}: {e}" for t, e in errors.items()))


_RETRYABLE = ("concurrent update", "could not serialize", "transaction is aborted", "watermark was moved",
              "aborted due to concurrent", "resources exceeded", "rate limit", "backend error",
              "internal error", "timed out", "deadline exceeded", "503", "500")


def _reason(exc: Exception) -> str:
    """The whole BigQuery reason on one line (job id included when the client gives one)."""
    text = " ".join(str(exc).split())
    job_id = getattr(getattr(exc, "response", None), "job_id", None) or getattr(exc, "job_id", None)
    return f"{text} [job {job_id}]" if job_id else text


def _is_retryable(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(k in text for k in _RETRYABLE)


def _state_id(target: str) -> str:
    return f"flush:{target}"


def _read_flush_state(target: str) -> dict:
    rows = list(client.query(
        f"SELECT watermark, version FROM {quote_identifier(config.FLUSH_STATE_TABLE)} WHERE id = @id",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", _state_id(target))])).result())
    if len(rows) != 1:
        raise RuntimeError(f"flush state must hold exactly one row with id '{_state_id(target)}', found {len(rows)}")
    return {"watermark": rows[0]["watermark"], "version": rows[0]["version"]}


def _read_staged(target: str, watermark, cutoff) -> list[dict]:
    sql = (f"SELECT request_id, item_index, target, received_at, payload "
           f"FROM {quote_identifier(config.STAGING_TABLE)} "
           f"WHERE target = @t AND received_at > @wm AND received_at <= @cutoff "
           f"ORDER BY received_at, request_id, item_index LIMIT {config.FLUSH_MAX_ITEMS + 1}")
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("t", "STRING", target),
        bigquery.ScalarQueryParameter("wm", "TIMESTAMP", watermark),
        bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff)])
    return [dict(r) for r in client.query(sql, job_config=cfg).result()]


def _record_flush_failure(target, flush_id, started, watermark, cutoff, items, attempts, error) -> None:
    client.query(
        f"INSERT INTO {quote_identifier(config.FLUSH_LOG_TABLE)} "
        f"(flush_id, target, started_at, finished_at, from_wm, to_wm, items, statements, dead_letters, attempts, status, error) "
        f"VALUES (@f, @t, @s, CURRENT_TIMESTAMP(), @a, @b, @n, 0, 0, @k, 'failed', @e)",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("f", "STRING", flush_id),
            bigquery.ScalarQueryParameter("t", "STRING", target),
            bigquery.ScalarQueryParameter("s", "TIMESTAMP", started),
            bigquery.ScalarQueryParameter("a", "TIMESTAMP", watermark),
            bigquery.ScalarQueryParameter("b", "TIMESTAMP", cutoff),
            bigquery.ScalarQueryParameter("n", "INT64", items),
            bigquery.ScalarQueryParameter("k", "INT64", attempts),
            bigquery.ScalarQueryParameter("e", "STRING", _reason(error)[:4000])])).result()


def consecutive_flush_failures(target: str) -> int:
    t = bigquery.ScalarQueryParameter("t", "STRING", target)
    rows = list(client.query(
        f"SELECT COUNT(*) n FROM {quote_identifier(config.FLUSH_LOG_TABLE)} WHERE target = @t AND status = 'failed' "
        f"AND started_at > (SELECT IFNULL(MAX(started_at), TIMESTAMP '1970-01-01') "
        f"FROM {quote_identifier(config.FLUSH_LOG_TABLE)} WHERE target = @t AND status = 'ok')",
        job_config=bigquery.QueryJobConfig(query_parameters=[t])).result())
    return rows[0]["n"]


def _flush_target_once(target: str, now, flush_id: str, started, attempt: int) -> dict:
    """One attempt: read, fold and write one target's calls in ONE transaction with its own watermark."""
    cutoff = (now or _dt.now(_tz.utc)) - _td(seconds=config.FLUSH_SAFETY_S)
    state = _read_flush_state(target)
    watermark = state["watermark"]
    if cutoff <= watermark:
        return {"status": "noop", "items": 0, "watermark": watermark, "cutoff": cutoff}
    staged = _read_staged(target, watermark, cutoff)
    if len(staged) > config.FLUSH_MAX_ITEMS:
        cutoff = staged[config.FLUSH_MAX_ITEMS - 1]["received_at"]
        staged = [r for r in staged if r["received_at"] <= cutoff]

    items, dead = [], []
    for r in staged:
        try:
            items.append({"data": json.loads(r["payload"]), "ref": r})
        except ValueError as exc:
            dead.append({**r, "stage": "flush", "errors": f"unreadable payload: {exc}"})
    plan = plan_target_writes(target, items, prefix="f_") if items else {"statements": [], "dead": [], "rows": 0, "keys": 0, "keyless": 0}
    dead += [{**d["ref"], "stage": "flush", "errors": d["errors"]} for d in plan["dead"]]
    statements = [sql.strip() for sql, _ in plan["statements"]]
    params = [p for _, ps in plan["statements"] for p in ps]

    script = ["BEGIN TRANSACTION"] + statements
    if dead:
        script.append(
            f"INSERT INTO {quote_identifier(config.DEAD_LETTER_TABLE)} "
            f"(recorded_at, received_at, request_id, item_index, target, stage, errors, payload) "
            f"SELECT CURRENT_TIMESTAMP(), received_at, request_id, item_index, target, stage, errors, payload "
            f"FROM UNNEST(@dl_rows)")
        params.append(_dead_letter_param(dead))
    script.append(
        f"INSERT INTO {quote_identifier(config.FLUSH_LOG_TABLE)} "
        f"(flush_id, target, started_at, finished_at, from_wm, to_wm, items, statements, dead_letters, attempts, status, error) "
        f"VALUES (@flush_id, @target, @started, CURRENT_TIMESTAMP(), @wm, @cutoff, @n_items, @n_statements, @n_dead, @attempt, 'ok', NULL)")
    script.append(
        f"UPDATE {quote_identifier(config.FLUSH_STATE_TABLE)} "
        f"SET watermark = @cutoff, version = version + 1, updated_at = CURRENT_TIMESTAMP() "
        f"WHERE id = @state_id AND version = @version")
    script.append("ASSERT @@row_count = 1 AS 'flush watermark was moved by another writer'")
    script.append("COMMIT TRANSACTION")
    params += [
        bigquery.ScalarQueryParameter("flush_id", "STRING", flush_id),
        bigquery.ScalarQueryParameter("target", "STRING", target),
        bigquery.ScalarQueryParameter("started", "TIMESTAMP", started),
        bigquery.ScalarQueryParameter("wm", "TIMESTAMP", watermark),
        bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        bigquery.ScalarQueryParameter("n_items", "INT64", len(staged)),
        bigquery.ScalarQueryParameter("n_statements", "INT64", len(statements)),
        bigquery.ScalarQueryParameter("n_dead", "INT64", len(dead)),
        bigquery.ScalarQueryParameter("attempt", "INT64", attempt),
        bigquery.ScalarQueryParameter("state_id", "STRING", _state_id(target)),
        bigquery.ScalarQueryParameter("version", "INT64", state["version"]),
    ]
    client.query(";\n".join(script) + ";", job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return {"status": "ok", "items": len(staged), "statements": len(statements), "dead_letters": len(dead),
            "rows": plan["rows"], "keys": plan["keys"], "keyless": plan["keyless"],
            "watermark": watermark, "cutoff": cutoff}


def flush_target(target: str, now=None, budget_s: float | None = None) -> dict:
    """
    Flush one target, retrying a contended transaction with full-jitter
    exponential backoff until that target's FLUSH_RETRY_BUDGET_S is spent. A failure here
    never touches another target's calls, watermark or tables.
    """
    budget = config.FLUSH_RETRY_BUDGET_S.get(target, 30.0) if budget_s is None else budget_s
    flush_id = uuid_module.uuid4().hex
    started = _dt.now(_tz.utc)
    deadline = time_module.monotonic() + budget
    attempt = 0
    while True:
        attempt += 1
        try:
            out = _flush_target_once(target, now, flush_id, started, attempt)
            out["attempts"] = attempt
            return out
        except Exception as exc:
            delay = random_module.uniform(0, min(config.FLUSH_BACKOFF_CAP_S, config.FLUSH_BACKOFF_BASE_S * 2 ** (attempt - 1)))
            if _is_retryable(exc) and time_module.monotonic() + delay < deadline:
                app.logger.warning("flush %s: attempt %s contended; retrying in %.1f s. Reason: %s",
                                   target, attempt, delay, _reason(exc))
                time_module.sleep(delay)
                continue
            last = exc
            break
    try:
        state = _read_flush_state(target)
        cutoff = (now or _dt.now(_tz.utc)) - _td(seconds=config.FLUSH_SAFETY_S)
        _record_flush_failure(target, flush_id, started, state["watermark"], cutoff, None, attempt, last)
        failures = consecutive_flush_failures(target)
    except Exception as log_exc:
        failures = None
        app.logger.error("flush %s: could not record the failure: %s", target, log_exc)
    if failures is None or failures >= config.FLUSH_ALERT_AFTER:
        app.logger.error("FLUSH_ALERT: %s: %s consecutive failed flushes; last reason: %s", target, failures, _reason(last))
    else:
        app.logger.warning("flush %s: failed after %s attempts (the queue retries). Reason: %s", target, attempt, _reason(last))
    raise last


def run_flush_cycle(now=None) -> dict:
    """
    Flush every staged target, each in its own transaction with its own
    watermark (check-in responses first). Contention on one target -- the
    nightly contacts sync on users, say -- delays only that target. Raises
    FlushFailed if any target failed, after flushing the others.
    """
    results, errors = {}, {}
    for target in sorted(config.STAGED_TARGETS, key=lambda t: (config.FLUSH_TARGET_ORDER + [t]).index(t)):
        try:
            out = flush_target(target, now)
            results[target] = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in out.items()}
        except Exception as exc:
            errors[target] = _reason(exc)
    if errors:
        raise FlushFailed(results, errors)
    return {"status": "ok", "targets": results,
            "items": sum(r.get("items", 0) for r in results.values())}


def flush_health() -> dict:
    """
    Per staged target: backlog, oldest backlog age, last successful flush,
    consecutive failed flushes and flushes with late calls; plus dead letters.
    "alert" while any target has failed FLUSH_ALERT_AFTER times in a row, a
    backlog older than ten flush buckets, or a call written after its flush;
    "ok" otherwise.
    """
    q = lambda sql, p=(): list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=list(p))).result())
    now = _dt.now(_tz.utc)
    targets, alert = {}, False
    for target in sorted(config.STAGED_TARGETS):
        state = _read_flush_state(target)
        t = bigquery.ScalarQueryParameter("t", "STRING", target)
        wm = bigquery.ScalarQueryParameter("wm", "TIMESTAMP", state["watermark"])
        backlog = q(f"SELECT COUNT(*) n, MIN(received_at) oldest FROM {quote_identifier(config.STAGING_TABLE)} "
                    f"WHERE target = @t AND received_at > @wm", [t, wm])[0]
        last_ok = q(f"SELECT MAX(finished_at) x FROM {quote_identifier(config.FLUSH_LOG_TABLE)} "
                    f"WHERE target = @t AND status = 'ok'", [t])[0]["x"]
        late = q(f"SELECT COUNT(*) n FROM (SELECT l.flush_id, l.items, COUNT(s.request_id) staged_now "
                 f"FROM {quote_identifier(config.FLUSH_LOG_TABLE)} l LEFT JOIN {quote_identifier(config.STAGING_TABLE)} s "
                 f"ON s.target = l.target AND s.received_at > l.from_wm AND s.received_at <= l.to_wm "
                 f"WHERE l.target = @t AND l.status = 'ok' "
                 f"AND l.finished_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) "
                 f"GROUP BY 1, 2) WHERE staged_now > items", [t])[0]["n"]
        failures = consecutive_flush_failures(target)
        age = (now - backlog["oldest"]).total_seconds() if backlog["oldest"] else 0.0
        target_alert = failures >= config.FLUSH_ALERT_AFTER or late > 0 or age > 10 * config.FLUSH_BUCKET_S
        alert |= target_alert
        targets[target] = {"status": "alert" if target_alert else "ok",
                           "watermark": state["watermark"].isoformat(), "backlog_calls": backlog["n"],
                           "oldest_backlog_age_s": round(age, 1),
                           "last_ok_flush": last_ok.isoformat() if last_ok else None,
                           "consecutive_failed_flushes": failures, "flushes_with_late_calls_24h": late}
    dead_24h = q(f"SELECT COUNT(*) n FROM {quote_identifier(config.DEAD_LETTER_TABLE)} "
                 f"WHERE recorded_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)")[0]["n"]
    return {"status": "alert" if alert else "ok", "targets": targets, "dead_letters_24h": dead_24h}


@app.post("/tasks/flush")
def tasks_flush():
    if not is_task_request_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        return jsonify(run_flush_cycle()), 200
    except FlushFailed as exc:   # 500: the flush queue retries; the targets that did flush stay flushed
        return jsonify({"status": "error", "flushed": exc.results, "failed": exc.errors}), 500
    except Exception as exc:
        return jsonify({"status": "error", "details": str(exc)[:2000]}), 500


@app.post("/tasks/flush-kick")
def tasks_flush_kick():
    """
    Safety sweep for a scheduler: request the flush of the current bucket on the flush
    queue (the same named task a call would create), so a backlog left by an exhausted
    flush task is picked up even when no new calls arrive. Never flushes inline.
    """
    if not is_task_request_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401
    now = _dt.now(_tz.utc)
    bucket = int(now.timestamp()) // config.FLUSH_BUCKET_S
    try:
        name = enqueue_flush(bucket, int(now.timestamp()))
    except Exception as exc:
        return jsonify({"status": "error", "details": _reason(exc)}), 500
    return jsonify({"status": "ok", "task": name, "bucket": bucket}), 200


@app.get("/health/flush")
def health_flush():
    if not is_authorized(request):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        body = flush_health()
    except Exception as exc:
        return jsonify({"status": "error", "details": str(exc)[:2000]}), 500
    return jsonify(body), (200 if body["status"] == "ok" else 503)

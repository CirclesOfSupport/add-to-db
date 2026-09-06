from __future__ import annotations

import re
import json
from decimal import Decimal
from urllib.parse import unquote
from datetime import date, datetime, time, timezone
from google.cloud import bigquery


BQ_TYPE_MAP = {
    "STRING": "STRING",
    "BYTES": "BYTES",
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "TIME": "TIME",
    "DATETIME": "DATETIME",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "JSON": "JSON",
}


VALID_BQ_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")


def quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"

def decode_webhook_string(value: str) -> str:
    """
    Decode URL-encoded webhook string values.

    Example:
      2026-05-26T11%3A36%3A27.979285-04%3A00
      becomes
      2026-05-26T11:36:27.979285-04:00
    """
    return unquote(value)


def parse_datetime_like(value: str) -> datetime:
    """
    Parse common ISO datetime strings from the webhook.

    Supports:
      2026-05-26T11:36:27.979285-04:00
      2026-05-26T11:36:27
      2026-05-26 11:36:27
      2026-05-26T11:36:27Z
    """
    cleaned = decode_webhook_string(value).strip()

    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    return datetime.fromisoformat(cleaned)


def coerce_value_to_bq_type(value, field: bigquery.SchemaField):
    """
    Convert webhook values into Python values compatible with BigQuery
    query parameters based on the actual BigQuery schema field type.
    """
    field_type = field.field_type.upper()

    if value is None:
        return None

    if isinstance(value, str):
        decoded = decode_webhook_string(value)
        stripped = decoded.strip()

        # Blank strings are never a meaningful value at this boundary: TextIt has
        # no way to send NULL for a string field, so '' always means "absent", not
        # "answered empty". Verified empirically across users + response_data --
        # every STRING column shows a small '' contamination against an
        # overwhelming NULL majority, i.e. artifacts of the write path rather than
        # a distinct state. Normalize to NULL for ALL field types.
        if stripped == "":
            return None

        if field_type in {"STRING", "BYTES"}:
            return decoded

        if field_type in {"INTEGER", "INT64"}:
            return int(stripped)

        if field_type in {"FLOAT", "FLOAT64"}:
            return float(stripped)

        if field_type in {"NUMERIC", "BIGNUMERIC"}:
            return Decimal(stripped)

        if field_type in {"BOOLEAN", "BOOL"}:
            lowered = stripped.lower()
            if lowered in {"true", "t", "yes", "y", "1"}:
                return True
            if lowered in {"false", "f", "no", "n", "0"}:
                return False
            raise ValueError(f"Cannot coerce value '{value}' to BOOLEAN")

        if field_type == "DATETIME":
            parsed = parse_datetime_like(stripped)

            # BigQuery DATETIME has no timezone. Preserve the local wall-clock
            # time from the webhook and remove tzinfo.
            return parsed.replace(tzinfo=None)

        if field_type == "TIMESTAMP":
            parsed = parse_datetime_like(stripped)

            # BigQuery TIMESTAMP represents an absolute instant. If timezone is
            # missing, assume UTC. If present, keep/normalize it.
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)

            return parsed.astimezone(timezone.utc)

        if field_type == "DATE":
            # Handles either a date-only string or a full datetime string.
            if "T" in stripped or " " in stripped:
                return parse_datetime_like(stripped).date()

            return date.fromisoformat(stripped)

        if field_type == "TIME":
            # Handles either a time-only string or a full datetime string.
            if "T" in stripped or " " in stripped:
                return parse_datetime_like(stripped).time().replace(tzinfo=None)

            return time.fromisoformat(stripped)

        if field_type == "JSON":
            # BigQuery JSON parameters can accept JSON strings. Decode URL
            # encoding but do not force json.loads here.
            return decoded

    # Already correctly typed Python values.
    # TextIt sends some fields as JSON numbers (e.g. k12 "grade": 8) where the
    # BigQuery column is STRING. BigQuery itself accepts this on insert, so
    # rejecting it here invents a failure the target column would not have:
    # stringify instead. Bool is excluded so True does not become "True".
    if field_type in {"STRING", "BYTES"} and isinstance(value, (int, float, Decimal)) \
            and not isinstance(value, bool):
        return str(value)
    if field_type == "DATETIME" and isinstance(value, datetime):
        return value.replace(tzinfo=None)

    if field_type == "TIMESTAMP" and isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if field_type == "DATE" and isinstance(value, datetime):
        return value.date()

    if field_type == "TIME" and isinstance(value, datetime):
        return value.time().replace(tzinfo=None)

    return value


def coerce_payload_to_schema(
    payload: dict,
    schema: list[bigquery.SchemaField],
) -> tuple[dict, list[str]]:
    """
    Convert payload values to match the BigQuery schema types.
    """
    errors: list[str] = []
    schema_fields = {field.name: field for field in schema}

    coerced: dict = {}

    for key, value in payload.items():
        field = schema_fields.get(key)

        if field is None:
            coerced[key] = value
            continue

        try:
            coerced[key] = coerce_value_to_bq_type(value, field)
        except Exception as exc:
            errors.append(
                f"Field '{key}' could not be converted to {field.field_type}: {exc}"
            )

    return coerced, errors


def normalize_payload_to_schema(
    payload: dict,
    schema: list[bigquery.SchemaField],
) -> tuple[dict, list[str]]:
    """
    Convert incoming payload keys to the exact BigQuery schema field names.

    Example:
      payload key: sessionID
      schema field: SessionID
      normalized key: SessionID

    Returns:
      normalized_payload, errors
    """
    errors: list[str] = []
    schema_name_by_lower = {field.name.lower(): field.name for field in schema}

    normalized: dict = {}
    seen_lower_payload_keys: dict[str, str] = {}

    for key, value in payload.items():
        key_lower = key.lower()

        if key_lower in seen_lower_payload_keys:
            errors.append(
                f"Ambiguous duplicate payload fields: "
                f"'{seen_lower_payload_keys[key_lower]}' and '{key}' differ only by case"
            )
            continue

        seen_lower_payload_keys[key_lower] = key

        canonical_name = schema_name_by_lower.get(key_lower, key)

        if canonical_name in normalized:
            errors.append(
                f"Ambiguous payload field '{key}' maps to already-normalized field '{canonical_name}'"
            )
            continue

        normalized[canonical_name] = value

    return normalized, errors


def resolve_key_columns(
    key_columns: list[str],
    schema: list[bigquery.SchemaField],
) -> tuple[list[str], list[str]]:
    """
    Resolve configured upsert keys to exact BigQuery schema field names.

    Example:
      configured key: sessionID
      schema field: SessionID
      resolved key: SessionID
    """
    errors: list[str] = []
    schema_name_by_lower = {field.name.lower(): field.name for field in schema}

    resolved: list[str] = []

    for key in key_columns:
        canonical_key = schema_name_by_lower.get(key.lower())

        if not canonical_key:
            errors.append(
                f"Configured upsert key '{key}' does not exist in the table schema"
            )
        else:
            resolved.append(canonical_key)

    return resolved, errors


def infer_bq_field_type(value) -> str:
    """
    Infer a safe BigQuery type for a previously unknown field.

    Dicts/lists are stored as JSON.
    Unknown or null-only values default to STRING because BigQuery needs
    an actual column type.
    """
    if isinstance(value, bool):
        return "BOOLEAN"

    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"

    if isinstance(value, float):
        return "FLOAT"

    if isinstance(value, (dict, list)):
        return "JSON"

    if isinstance(value, datetime):
        return "TIMESTAMP"

    if isinstance(value, date):
        return "DATE"

    if isinstance(value, time):
        return "TIME"

    return "STRING"


def validate_new_column_name(name: str) -> None:
    """
    Prevent schema update failures and avoid unsafe/generated SQL problems.
    """
    if not VALID_BQ_COLUMN_RE.match(name):
        raise ValueError(
            f"Invalid BigQuery column name '{name}'. "
            "Column names must start with a letter or underscore and contain "
            "only letters, numbers, and underscores."
        )


def add_missing_fields_to_table(
    client: bigquery.Client,
    table: bigquery.Table,
    payload: dict,
) -> tuple[bigquery.Table, list[str]]:
    """
    Adds payload keys that do not already exist in the BigQuery table schema.

    Field matching is case-insensitive to avoid trying to add duplicate
    fields such as enrolledBy when BigQuery already has EnrolledBy/enrolledby.
    """
    existing_field_names = {field.name for field in table.schema}
    existing_field_names_lower = {field.name.lower() for field in table.schema}

    new_fields: list[bigquery.SchemaField] = []

    for key, value in payload.items():
        if key in existing_field_names:
            continue

        if key.lower() in existing_field_names_lower:
            # Exists already with different casing. Do not attempt to add it.
            continue

        validate_new_column_name(key)

        inferred_type = infer_bq_field_type(value)

        new_fields.append(
            bigquery.SchemaField(
                name=key,
                field_type=inferred_type,
                mode="NULLABLE",
            )
        )

    if not new_fields:
        return table, []

    updated_schema = list(table.schema) + new_fields
    table.schema = updated_schema

    try:
        updated_table = client.update_table(table, ["schema"])
    except Exception as exc:
        # Another request may have added one of the same columns between
        # get_table() and update_table(). Refresh once and retry against
        # the latest schema so we do not silently drop other new fields.
        if "already exists in schema" in str(exc):
            refreshed_table = client.get_table(table.reference)

            existing_field_names = {field.name for field in refreshed_table.schema}
            existing_field_names_lower = {field.name.lower() for field in refreshed_table.schema}

            retry_new_fields: list[bigquery.SchemaField] = []

            for key, value in payload.items():
                if key in existing_field_names:
                    continue

                if key.lower() in existing_field_names_lower:
                    continue

                validate_new_column_name(key)

                retry_new_fields.append(
                    bigquery.SchemaField(
                        name=key,
                        field_type=infer_bq_field_type(value),
                        mode="NULLABLE",
                    )
                )

            if not retry_new_fields:
                return refreshed_table, []

            refreshed_table.schema = list(refreshed_table.schema) + retry_new_fields
            updated_table = client.update_table(refreshed_table, ["schema"])

            return updated_table, [field.name for field in retry_new_fields]

        raise

    return updated_table, [field.name for field in new_fields]


def resolve_partition_column(
    partition_column: str | None,
    schema: list[bigquery.SchemaField],
    row: dict,
) -> tuple[str | None, datetime | None]:
    """
    Decide whether this MERGE can carry a partition-range predicate.

    Returns (column_name, value). Both are None -- meaning OMIT the predicate --
    unless every condition holds:
      * a partition column is configured for the target,
      * it exists in the table schema (case-insensitive match, exact name returned),
      * its BigQuery type is DATETIME (a TIMESTAMP/DATE parameter, or a cast on
        the column, disables pruning silently),
      * the row carries a non-null value of that column, already coerced to a
        naive datetime.

    A NULL value MUST omit the predicate: `BETWEEN NULL AND NULL` matches nothing,
    so the MERGE would INSERT a duplicate instead of UPDATE.
    """
    if not partition_column:
        return None, None

    field = next(
        (f for f in schema if f.name.lower() == partition_column.lower()),
        None,
    )
    if field is None or field.field_type.upper() != "DATETIME":
        return None, None

    value = row.get(field.name)
    if not isinstance(value, datetime) or value.tzinfo is not None:
        return None, None

    return field.name, value


def build_upsert_query(
    target_table_id: str,
    row: dict,
    key_columns: list[str],
    partition_column: str | None = None,
):
    """
    Generates a parameterized MERGE statement using only columns present in the row.

    When partition_column is given, the ON clause also carries
    `T.<partition_column> BETWEEN @min_dt AND @max_dt`, bound to DATETIME query
    parameters by the caller. The parameters are plan-time constants, which is
    what lets BigQuery prune partitions; the same range expressed through the
    UNNEST join does not prune.
    """
    column_names = list(row.keys())

    on_terms = [
        f"T.{quote_identifier(col)} = S.{quote_identifier(col)}"
        for col in key_columns
    ]
    if partition_column:
        on_terms.append(
            f"T.{quote_identifier(partition_column)} BETWEEN @min_dt AND @max_dt"
        )
    on_clause = " AND ".join(on_terms)

    non_key_columns = [col for col in column_names if col not in key_columns]

    if non_key_columns:
        update_clause = ",\n        ".join([
            f"{quote_identifier(col)} = S.{quote_identifier(col)}"
            for col in non_key_columns
        ])
        matched_action = f"WHEN MATCHED THEN UPDATE SET {update_clause}"
    else:
        matched_action = (
            f"WHEN MATCHED THEN UPDATE SET "
            f"{quote_identifier(key_columns[0])} = S.{quote_identifier(key_columns[0])}"
        )

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


def normalize_query_param_value(value, field: bigquery.SchemaField):
    """
    Final normalization before passing values as BigQuery query parameters.
    Most type coercion should already happen in coerce_payload_to_schema.
    """
    if value is None:
        return None

    field_type = field.field_type.upper()

    if field_type == "JSON":
        if isinstance(value, str):
            return value
        return json.dumps(value)

    if field_type in {"NUMERIC", "BIGNUMERIC"}:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    return value


def build_struct_param(
    row: dict,
    schema: list[bigquery.SchemaField],
    name: str,
) -> bigquery.StructQueryParameter:
    schema_fields = {field.name: field for field in schema}
    scalar_params = []

    for key, value in row.items():
        field = schema_fields.get(key)
        if field is None:
            continue

        bq_type = BQ_TYPE_MAP.get(field.field_type.upper(), "STRING")
        normalized_value = normalize_query_param_value(value, field)

        scalar_params.append(
            bigquery.ScalarQueryParameter(key, bq_type, normalized_value)
        )

    return bigquery.StructQueryParameter(name, *scalar_params)


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


def get_users_and_responses_view_query(project_id: str, target: str) -> str:
    table_type = "COPY" if ("copy" in target.lower()) else "RESPONSES"
    return f"""
    CREATE OR REPLACE VIEW `{project_id}.{table_type}.users_and_responses` AS
    SELECT
      response_data AS resp,
      users AS user
    FROM `{project_id}.{table_type}.response_data` AS response_data
    LEFT JOIN `{project_id}.{table_type}.users` AS users
      ON users.uuid = response_data.uuid
    """
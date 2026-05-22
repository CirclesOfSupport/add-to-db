from __future__ import annotations

import re
import json
from datetime import date, datetime, time
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


def build_upsert_query(target_table_id: str, row: dict, key_columns: list[str]):
    """
    Generates a parameterized MERGE statement using only columns present in the row.
    """
    column_names = list(row.keys())

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
    Normalize values before passing them as BigQuery query parameters.
    """
    if value is None:
        return None

    if field.field_type == "JSON":
        if isinstance(value, str):
            return value
        return json.dumps(value)

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

        bq_type = BQ_TYPE_MAP.get(field.field_type, "STRING")
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
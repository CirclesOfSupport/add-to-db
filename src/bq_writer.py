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

    Returns:
        updated_table, added_field_names
    """
    existing_field_names = {field.name for field in table.schema}
    new_fields: list[bigquery.SchemaField] = []

    for key, value in payload.items():
        if key in existing_field_names:
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

    updated_table = client.update_table(table, ["schema"])

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
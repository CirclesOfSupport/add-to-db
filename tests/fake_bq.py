"""
A stand-in for google.cloud.bigquery.Client that runs the service's generated
SQL on DuckDB, so a test can execute the real MERGE / INSERT text end to end.

Only the statement shapes the service emits are translated:
  MERGE `p.d.t` T USING UNNEST(@rows) S ON ...
  INSERT INTO `p.d.t` (cols) SELECT cols FROM UNNEST(@rows)
Query parameters are rendered as typed literals. Every statement received is
kept (sql + parameters) so tests can also assert on what would reach BigQuery.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal

import duckdb
from google.cloud import bigquery

DUCK_TYPES = {
    "STRING": "VARCHAR", "BYTES": "BLOB", "INTEGER": "BIGINT", "INT64": "BIGINT",
    "FLOAT": "DOUBLE", "FLOAT64": "DOUBLE", "NUMERIC": "DECIMAL(38,9)",
    "BIGNUMERIC": "DECIMAL(38,9)", "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMPTZ", "DATETIME": "TIMESTAMP", "DATE": "DATE",
    "TIME": "TIME", "JSON": "VARCHAR", "RECORD": "VARCHAR",
}


def _literal(value, bq_type: str) -> str:
    duck = DUCK_TYPES.get(bq_type.upper(), "VARCHAR")
    if value is None:
        return f"CAST(NULL AS {duck})"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return f"CAST({value!r} AS {duck})" if not isinstance(value, Decimal) else f"CAST('{value}' AS {duck})"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return f"CAST('{value.isoformat()}' AS TIMESTAMPTZ)"
        return f"CAST('{value.isoformat(sep=' ')}' AS TIMESTAMP)"
    if isinstance(value, date):
        return f"CAST('{value.isoformat()}' AS DATE)"
    if isinstance(value, time):
        return f"CAST('{value.isoformat()}' AS TIME)"
    text = str(value).replace("'", "''")
    return f"CAST('{text}' AS {duck})"


@dataclass
class FakeTable:
    table_id: str
    schema: list

    @property
    def reference(self):
        return self.table_id


@dataclass
class FakeJob:
    sql: str
    params: dict

    def result(self, *args, **kwargs):
        return []


@dataclass
class FakeClient:
    tables: dict = field(default_factory=dict)       # table_id -> schema
    statements: list = field(default_factory=list)   # FakeJob, in order
    duck: duckdb.DuckDBPyConnection = field(default_factory=duckdb.connect)
    fail_next: list = field(default_factory=list)    # exceptions to raise on the next query calls

    # --- table management -------------------------------------------------
    def _name(self, table_id: str) -> str:
        return '"' + table_id.replace("`", "").replace(".", "__").replace("-", "_") + '"'

    def create(self, table_id: str, schema: list) -> None:
        self.tables[table_id] = list(schema)
        cols = ", ".join(f'"{f.name}" {DUCK_TYPES[f.field_type.upper()]}' for f in schema)
        self.duck.execute(f"CREATE TABLE {self._name(table_id)} ({cols})")

    def rows(self, table_id: str) -> list[dict]:
        cur = self.duck.execute(f"SELECT * FROM {self._name(table_id)}")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]

    def insert_raw(self, table_id: str, row: dict) -> None:
        schema = {f.name: f for f in self.tables[table_id]}
        cols = ", ".join(f'"{k}"' for k in row)
        vals = ", ".join(_literal(v, schema[k].field_type) for k, v in row.items())
        self.duck.execute(f"INSERT INTO {self._name(table_id)} ({cols}) VALUES ({vals})")

    # --- the Client surface the service uses ------------------------------
    def get_table(self, table_id):
        return FakeTable(table_id, list(self.tables[table_id]))

    def update_table(self, table, fields):
        raise AssertionError("tests do not expect a schema migration")

    def query(self, sql: str, job_config=None):
        params = {}
        for p in (job_config.query_parameters if job_config else []):
            params[p.name] = p
        job = FakeJob(sql, params)
        self.statements.append(job)
        if self.fail_next:
            raise self.fail_next.pop(0)
        self.duck.execute(self._translate(sql, params))
        return job

    # --- translation ------------------------------------------------------
    def _rows_source(self, params: dict) -> str:
        arr = params["rows"]
        selects = []
        for struct in arr.values:
            parts = []
            for name, sub_type in struct.struct_types.items():
                parts.append(f"{_literal(struct.struct_values[name], sub_type)} AS \"{name}\"")
            selects.append("SELECT " + ", ".join(parts))
        return "(" + " UNION ALL ".join(selects) + ")"

    def _translate(self, sql: str, params: dict) -> str:
        out = sql
        m = re.search(r"MERGE\s+`([^`]+)`\s+T\s+USING\s+UNNEST\(@rows\)\s+S", out)
        if m:
            out = out.replace(m.group(0), f"MERGE INTO {self._name(m.group(1))} AS T USING {self._rows_source(params)} AS S")
        m = re.search(r"INSERT INTO\s+`([^`]+)`", out)
        if m:
            out = out.replace(m.group(0), f"INSERT INTO {self._name(m.group(1))}")
            out = out.replace("FROM UNNEST(@rows)", f"FROM {self._rows_source(params)} AS S")
        for name in ("min_dt", "max_dt"):
            if name in params:
                p = params[name]
                out = out.replace(f"@{name}", _literal(p.value, p.type_))
        return out.replace("`", '"')

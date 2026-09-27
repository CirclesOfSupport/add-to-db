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


def _utc_duck():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


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
    rows: list = field(default_factory=list)

    def result(self, *args, **kwargs):
        return self.rows


@dataclass
class FakeClient:
    tables: dict = field(default_factory=dict)       # table_id -> schema
    statements: list = field(default_factory=list)   # FakeJob, in order
    duck: duckdb.DuckDBPyConnection = field(default_factory=lambda: _utc_duck())
    fail_next: list = field(default_factory=list)    # exceptions to raise on the next query calls
    fail_in_script: list = field(default_factory=list)  # substrings: the script statement containing it fails
    streamed: list = field(default_factory=list)      # rows appended with insert_rows_json

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
        if sql.lstrip().startswith("BEGIN TRANSACTION"):
            self._run_script(sql, params)
            return job
        if sql.lstrip().upper().startswith("SELECT"):
            job.rows = self.select(sql, params)
            return job
        self.duck.execute(self._translate(sql, params))
        return job

    # --- translation ------------------------------------------------------
    def _rows_source(self, params: dict, name: str = "rows") -> str:
        arr = params[name]
        selects = []
        for struct in arr.values:
            parts = []
            for name, sub_type in struct.struct_types.items():
                parts.append(f"{_literal(struct.struct_values[name], sub_type)} AS \"{name}\"")
            selects.append("SELECT " + ", ".join(parts))
        return "(" + " UNION ALL ".join(selects) + ")"

    def _translate(self, sql: str, params: dict) -> str:
        out = sql
        for m in list(re.finditer(r"MERGE\s+`([^`]+)`\s+T\s+USING\s+UNNEST\(@(\w+)\)\s+S", out)):
            out = out.replace(m.group(0), f"MERGE INTO {self._name(m.group(1))} AS T USING {self._rows_source(params, m.group(2))} AS S")
        for m in list(re.finditer(r"INSERT INTO\s+`([^`]+)`", out)):
            out = out.replace(m.group(0), f"INSERT INTO {self._name(m.group(1))}")
        for m in list(re.finditer(r"FROM UNNEST\(@(\w+)\)", out)):
            out = out.replace(m.group(0), f"FROM {self._rows_source(params, m.group(1))} AS S")
        for m in list(re.finditer(r"(UPDATE|FROM|JOIN)\s+`([^`]+)`", out)):
            out = out.replace(m.group(0), f"{m.group(1)} {self._name(m.group(2))}")
        for name, p in sorted(params.items(), key=lambda kv: -len(kv[0])):
            if hasattr(p, "type_") and not hasattr(p, "array_type"):
                out = re.sub(rf"@{name}\b", _literal(p.value, p.type_).replace("\\", "\\\\"), out)
        out = out.replace("CURRENT_TIMESTAMP()", "CAST(now() AS TIMESTAMPTZ)")
        out = re.sub(r"TIMESTAMP_SUB\(([^,]+), (INTERVAL \d+ \w+)\)", r"(\1 - \2)", out)
        out = out.replace("IFNULL(", "COALESCE(")
        return out.replace("`", '"')

    # --- scripts (BEGIN TRANSACTION; ...; ASSERT @@row_count = 1 ...; COMMIT TRANSACTION;)
    def _run_script(self, sql: str, params: dict) -> None:
        parts = [p.strip() for p in sql.split(";\n") if p.strip().rstrip(";").strip()]
        last_count = None
        self.duck.execute("BEGIN TRANSACTION")
        try:
            for part in parts:
                part = part.rstrip(";").strip()
                if part in ("BEGIN TRANSACTION", "COMMIT TRANSACTION"):
                    continue
                if part.startswith("ASSERT @@row_count = 1"):
                    if last_count != 1:
                        raise RuntimeError("Assertion failed: " + part)
                    continue
                if self.fail_in_script and self.fail_in_script[0] in part:
                    self.fail_in_script.pop(0)
                    raise RuntimeError("Transaction is aborted due to concurrent update")
                cur = self.duck.execute(self._translate(part, params))
                row = cur.fetchone() if cur.description else None
                last_count = row[0] if row is not None else None
            self.duck.execute("COMMIT")
        except Exception:
            self.duck.execute("ROLLBACK")
            raise

    def insert_rows_json(self, table_id, rows, row_ids=None):
        if self.fail_next:
            return [{"errors": [str(self.fail_next.pop(0))]}]
        for r in rows:
            self.insert_raw(table_id, {k: (datetime.fromisoformat(v) if k == "received_at" else v) for k, v in r.items()})
        self.streamed.extend(rows)
        return []

    def select(self, sql: str, params: dict) -> list[dict]:
        cur = self.duck.execute(self._translate(sql, params))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]

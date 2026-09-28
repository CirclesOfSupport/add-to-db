"""The single-writer DDL has no construct BigQuery rejects that a local database would accept."""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
from staged_ddl import ddl  # noqa: E402


def test_no_doubled_quotes_inside_string_literals():
    # BigQuery reads 'a''b' as two adjacent literals ("concatenated string literals")
    for stmt in ddl("OPS"):
        assert "''" not in stmt, stmt.splitlines()[0]


def test_four_tables_and_the_state_row():
    stmts = ddl("DEV", "adb_x_")
    creates = [s for s in stmts if s.startswith("CREATE TABLE")]
    assert [re.search(r"adb_x_(\w+)`", s).group(1) for s in creates] == ["staging", "dead_letter", "flush_log", "flush_state"]
    assert stmts[-1].startswith("INSERT INTO `early-alert-responses.DEV.adb_x_flush_state`")


# --- --apply creates and verifies (stand-in client: records DDL, keeps tables and state rows) ---

import staged_ddl  # noqa: E402
from google.cloud import bigquery  # noqa: E402

_TYPES = {"STRING": "STRING", "INT64": "INTEGER", "TIMESTAMP": "TIMESTAMP"}


class _Job:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def result(self):
        return self.rows


class _DdlClient:
    """Parses the CREATE TABLE column lists and the state INSERT; enough to exercise apply and verify."""

    def __init__(self, existing=()):
        self.tables, self.state, self.ran = {}, [], []
        for name in existing:
            self.tables[name] = []

    def query(self, sql, job_config=None):
        if job_config is not None and getattr(job_config, "dry_run", False):
            return _Job()
        self.ran.append(sql.splitlines()[0])
        m = re.match(r"CREATE TABLE `([^`]+)` \((.*?)\)\n(PARTITION|OPTIONS)", sql, re.S)
        if m:
            fields = []
            for col in re.split(r",\s*(?![^<]*>)", " ".join(m.group(2).split())):
                name, typ, *rest = col.split(" ")
                mode = "REQUIRED" if "NOT" in rest else "NULLABLE"
                if typ.startswith("ARRAY<"):
                    typ, mode = typ[6:-1], "REPEATED"
                fields.append(bigquery.SchemaField(name, _TYPES[typ], mode=mode))
            self.tables[m.group(1).split(".")[-1]] = fields
            return _Job()
        if sql.startswith("INSERT INTO"):
            self.state += [{"id": i, "version": 0, "paused_since": None} for i in re.findall(r"\('(flush:\w+)'", sql)]
            return _Job()
        if sql.startswith("SELECT id, version, paused_since"):
            return _Job(sorted(self.state, key=lambda r: r["id"]))
        raise AssertionError(sql)

    def list_tables(self, dataset):
        return [type("T", (), {"table_id": n})() for n in self.tables]

    def get_table(self, table_id):
        return type("Tbl", (), {"schema": self.tables[table_id.split(".")[-1]]})()


def test_apply_creates_four_tables_and_two_state_rows_and_verifies(capsys):
    c = _DdlClient()
    assert staged_ddl.apply(c, "DEV") == []
    assert sorted(c.tables) == ["adb_dead_letter", "adb_flush_log", "adb_flush_state", "adb_staging"]
    assert [r["id"] for r in c.state] == ["flush:responses", "flush:users"]
    refs = next(f for f in c.tables["adb_flush_log"] if f.name == "refs")
    assert (refs.field_type, refs.mode) == ("STRING", "REPEATED")


def test_apply_refuses_when_a_table_already_exists():
    c = _DdlClient(existing=["adb_staging"])
    problems = staged_ddl.apply(c, "DEV")
    assert problems and "already exists" in problems[0] and c.ran == []


def test_verify_reports_a_missing_column_and_a_paused_row():
    c = _DdlClient()
    staged_ddl.apply(c, "DEV")
    c.tables["adb_flush_state"] = [f for f in c.tables["adb_flush_state"] if f.name != "paused_since"]
    assert any("paused_since missing" in p for p in staged_ddl.verify(c, "DEV"))
    c2 = _DdlClient()
    staged_ddl.apply(c2, "DEV")
    c2.state[0]["paused_since"] = "2026-09-28"
    assert any("paused" in p for p in staged_ddl.verify(c2, "DEV"))


def test_every_runtime_requirement_is_pinned():
    path = os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
    lines = [x.strip() for x in open(path) if x.strip() and not x.startswith("#")]
    assert lines and all(re.fullmatch(r"[A-Za-z0-9_.\-]+==[0-9][\w.]*", x) for x in lines), lines

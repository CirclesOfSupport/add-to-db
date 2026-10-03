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


def test_five_tables_and_one_state_row_in_each_state_table():
    stmts = ddl("DEV", "adb_x_")
    creates = [s for s in stmts if s.startswith("CREATE TABLE")]
    assert [re.search(r"adb_x_(\w+)`", s).group(1) for s in creates] == [
        "staging", "set_aside", "flush_log", "flush_state", "flush_state_users"]
    inserts = [s for s in stmts if s.startswith("INSERT INTO")]
    assert [(re.search(r"adb_x_(\w+)`", s).group(1), re.findall(r"\('(flush:\w+)'", s)) for s in inserts] == [
        ("flush_state", ["flush:responses"]), ("flush_state_users", ["flush:users"])]


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
        self.tables, self.state, self.ran = {}, {}, []      # state: table name -> its rows
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
        name = lambda text: re.search(r"`([^`]+)`", text).group(1).split(".")[-1]
        if sql.startswith("INSERT INTO") and "\nSELECT" in sql:              # the split's copy of the users row
            source = name(sql.split("FROM", 1)[1])
            self.state.setdefault(name(sql), []).extend(
                {**r, "updated_at": "now"} for r in self.state.get(source, []) if r["id"] == "flush:users")
            return _Job()
        if sql.startswith("INSERT INTO"):
            self.state.setdefault(name(sql), []).extend(
                {"id": i, "version": 0, "paused_since": None, "watermark": "created"}
                for i in re.findall(r"\('(flush:\w+)'", sql))
            return _Job()
        if sql.startswith("SELECT id,"):
            return _Job(sorted(self.state.get(name(sql), []), key=lambda r: r["id"]))
        raise AssertionError(sql)

    def list_tables(self, dataset):
        return [type("T", (), {"table_id": n})() for n in self.tables]

    def get_table(self, table_id):
        return type("Tbl", (), {"schema": self.tables[table_id.split(".")[-1]]})()


def test_apply_creates_five_tables_and_each_state_row_in_its_own_table_and_verifies(capsys):
    c = _DdlClient()
    assert staged_ddl.apply(c, "DEV") == []
    assert sorted(c.tables) == ["adb_flush_log", "adb_flush_state", "adb_flush_state_users", "adb_set_aside",
                                "adb_staging"]
    assert [r["id"] for r in c.state["adb_flush_state"]] == ["flush:responses"]
    assert [r["id"] for r in c.state["adb_flush_state_users"]] == ["flush:users"]
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
    c2.state["adb_flush_state_users"][0]["paused_since"] = "2026-09-28"
    assert any("flush_state_users flush:users is paused" in p for p in staged_ddl.verify(c2, "DEV"))


# --- --split-users-state: a dataset created when both rows shared adb_flush_state ---------------------

def _before_the_split():
    """The four tables and the two rows in one state table, as --apply created them before this change."""
    c = _DdlClient()
    staged_ddl.apply(c, "DEV")
    del c.tables["adb_flush_state_users"]
    c.state = {"adb_flush_state": [
        {"id": "flush:responses", "version": 41, "paused_since": None, "watermark": "r-wm"},
        {"id": "flush:users", "version": 37, "paused_since": None, "watermark": "u-wm"}]}
    c.ran.clear()
    return c


def test_verify_names_the_missing_users_state_table_before_the_split():
    assert any("adb_flush_state_users: not readable" in p for p in staged_ddl.verify(_FailingGet(_before_the_split()), "DEV"))


class _FailingGet:
    """get_table raises for a table that does not exist, as BigQuery does."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def get_table(self, table_id):
        if table_id.split(".")[-1] not in self._inner.tables:
            raise RuntimeError("404 Not found: Table " + table_id)
        return self._inner.get_table(table_id)


def test_split_creates_the_users_state_table_and_copies_the_users_row_as_it_is():
    c, said = _before_the_split(), []
    assert staged_ddl.split_users_state(c, "DEV", out=said.append) == []
    assert [x.split(" (")[0] for x in c.ran if x.startswith(("CREATE", "INSERT"))] == [
        "CREATE TABLE `early-alert-responses.DEV.adb_flush_state_users`",
        "INSERT INTO `early-alert-responses.DEV.adb_flush_state_users`"]
    moved = c.state["adb_flush_state_users"]
    assert [(r["id"], r["version"], r["watermark"]) for r in moved] == [("flush:users", 37, "u-wm")]
    # the shared table is not touched: both rows still there (the users row is now unused)
    assert [(r["id"], r["version"]) for r in c.state["adb_flush_state"]] == [("flush:responses", 41), ("flush:users", 37)]
    assert any("flush:users watermark u-wm version 37" in line for line in said)


def test_split_run_again_changes_nothing():
    c = _before_the_split()
    staged_ddl.split_users_state(c, "DEV", out=lambda line: None)
    c.state["adb_flush_state_users"][0]["version"] = 52          # the service has flushed since
    c.state["adb_flush_state"][1]["version"] = 37                # the unused row stays where it was
    c.ran.clear()
    said = []
    assert staged_ddl.split_users_state(c, "DEV", out=said.append) == []
    assert not any(x.startswith(("CREATE", "INSERT")) for x in c.ran)
    assert c.state["adb_flush_state_users"][0]["version"] == 52
    assert sum(line.startswith("kept:") for line in said) == 2


def test_split_refuses_a_dataset_with_no_state_table():
    c = _DdlClient()
    problems = staged_ddl.split_users_state(c, "DEV", out=lambda line: None)
    assert problems and "does not exist" in problems[0] and c.ran == []


def test_split_refuses_responses_as_a_dataset():
    import pytest
    with pytest.raises(ValueError, match="refused"):
        staged_ddl.split_users_state(None, "RESPONSES")


def test_every_runtime_requirement_is_pinned():
    path = os.path.join(os.path.dirname(__file__), "..", "requirements.txt")
    lines = [x.strip() for x in open(path) if x.strip() and not x.startswith("#")]
    assert lines and all(re.fullmatch(r"[A-Za-z0-9_.\-]+==[0-9][\w.]*", x) for x in lines), lines

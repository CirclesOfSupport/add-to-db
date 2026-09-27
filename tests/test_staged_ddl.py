"""The single-writer DDL has no construct BigQuery rejects that a local database would accept."""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
from staged_ddl import ddl  # noqa: E402


def test_no_doubled_quotes_inside_string_literals():
    # BigQuery reads 'a''b' as two adjacent literals ("concatenated string literals")
    for stmt in ddl("RESPONSES"):
        assert "''" not in stmt, stmt.splitlines()[0]


def test_four_tables_and_the_state_row():
    stmts = ddl("DEV", "adb_x_")
    creates = [s for s in stmts if s.startswith("CREATE TABLE")]
    assert [re.search(r"adb_x_(\w+)`", s).group(1) for s in creates] == ["staging", "dead_letter", "flush_log", "flush_state"]
    assert stmts[-1].startswith("INSERT INTO `early-alert-responses.DEV.adb_x_flush_state`")

"""The local-time correction list: what it proposes, and why, from stored rows and logged bodies."""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import fix_eastern_rows as F  # noqa: E402

A = "aaaaaaaa-0000-0000-0000-0000000000012026-08-24T13:01:49.1-04:00"
B = "bbbbbbbb-0000-0000-0000-0000000000022026-05-20T10:00:00.5-04:00"


class _Job:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class _Client:
    def query(self, sql, job_config=None):
        if "WITH r AS" in sql:
            return _Job([
                {"SessionID": A, "uuid": "a", "contactType": "CheckIn", "checkinDateTime": datetime(2026, 8, 24, 13, 1, 49, 100000),
                 "checkinReplyDateTime": datetime(2026, 8, 24, 13, 30), "resourceOfferReplyDatetime": None, "offset_min": -240},
                {"SessionID": B, "uuid": "b", "contactType": "CheckIn", "checkinDateTime": datetime(2026, 5, 20, 10, 0, 0, 500000),
                 "checkinReplyDateTime": datetime(2026, 5, 20, 11, 0), "resourceOfferReplyDatetime": datetime(2026, 5, 20, 18, 0),
                 "offset_min": -240}])
        body = {"tables": [{"table": "responses", "data": {"sessionID": A, "checkinReplyDateTime": "2026-08-24T13:30:00-04:00"}}]}
        return _Job([{"request_body": json.dumps(body)}])


def test_list_proposals(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    F.cmd_list(_Client())
    plan = json.load(open(next(tmp_path.glob("eastern_fix_plan_*.json"))))
    got = {(c["SessionID"][:8], c["column"]): (c["old"], c["new"], c["rows"]) for c in plan["changes"]}
    assert got == {
        ("aaaaaaaa", "checkinDateTime"): ("2026-08-24T13:01:49.100000", "2026-08-24T17:01:49.100000", 1),
        ("aaaaaaaa", "checkinReplyDateTime"): ("2026-08-24T13:30:00", "2026-08-24T17:30:00", 1),      # local form of a logged value
        ("bbbbbbbb", "checkinDateTime"): ("2026-05-20T10:00:00.500000", "2026-05-20T14:00:00.500000", 1),
        ("bbbbbbbb", "checkinReplyDateTime"): ("2026-05-20T11:00:00", "2026-05-20T15:00:00", 1),      # before check-in unless local
    }                                                                                                 # 18:00 offer reply: ambiguous, left
    out = capsys.readouterr().out
    assert "2026-05  not in logged add-to-db bodies" in out and "2026-08  in add-to-db bodies" in out
    assert "Nothing was written to BigQuery" in out
    assert A not in out and B not in out                              # console: summary only
    listing = next(tmp_path.glob("eastern_fix_list_*.txt")).read_text()
    assert A in listing and B in listing and "18:00:00" in listing     # full list in the file


class _StaleClient(_Client):
    """A row whose logged reply (from an earlier session) is days before its own check-in."""

    def query(self, sql, job_config=None):
        if "WITH r AS" in sql:
            return _Job([{"SessionID": A, "uuid": "a", "contactType": "CheckIn",
                          "checkinDateTime": datetime(2026, 8, 28, 17, 3, 18), "offset_min": -240,
                          "checkinReplyDateTime": datetime(2026, 8, 20, 12, 46, 35), "resourceOfferReplyDatetime": None}])
        body = {"tables": [{"table": "responses", "data": {"sessionID": A, "checkinReplyDateTime": "2026-08-20T12:46:35-04:00"}}]}
        return _Job([{"request_body": json.dumps(body)}])


def test_stale_reply_is_converted_but_flagged(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    F.cmd_list(_StaleClient())
    plan = json.load(open(next(tmp_path.glob("eastern_fix_plan_*.json"))))
    reply = next(c for c in plan["changes"] if c["column"] == "checkinReplyDateTime")
    assert reply["new"] == "2026-08-20T16:46:35" and reply["flag"] == "stale reply, value to the repair unit"
    assert "flag" not in next(c for c in plan["changes"] if c["column"] == "checkinDateTime")
    assert "1 reply-time changes on 1 rows are flagged" in capsys.readouterr().out


# --- the apply: one transaction, set-based, every guard kept (run on the DuckDB stand-in) ---------

import pytest  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from fake_bq import FakeClient  # noqa: E402

CLONE = "early-alert-responses.DEV.adb_eastern_rehearsal_x"
C = "cccccccc-0000-0000-0000-0000000000032026-08-28T13:03:18-04:00"


def _table():
    c = FakeClient()
    Fd = bigquery.SchemaField
    c.create(CLONE, [Fd("SessionID", "STRING"), Fd("checkinDateTime", "DATETIME"),
                     Fd("checkinReplyDateTime", "DATETIME"), Fd("resourceOfferReplyDatetime", "DATETIME")])
    c.insert_raw(CLONE, {"SessionID": A, "checkinDateTime": datetime(2026, 8, 24, 13, 1, 49),
                         "checkinReplyDateTime": datetime(2026, 8, 24, 13, 30)})
    for _ in range(2):   # an old-writer duplicate: two rows of one session, both listed
        c.insert_raw(CLONE, {"SessionID": C, "checkinDateTime": datetime(2026, 8, 28, 13, 3, 18)})
    c.insert_raw(CLONE, {"SessionID": B, "checkinDateTime": datetime(2026, 5, 20, 14, 0)})   # not in the plan
    return c


PLAN = {"table": CLONE, "changes": [
    {"SessionID": A, "column": "checkinDateTime", "old": "2026-08-24T13:01:49", "new": "2026-08-24T17:01:49", "rows": 1},
    {"SessionID": A, "column": "checkinReplyDateTime", "old": "2026-08-24T13:30:00", "new": "2026-08-24T17:30:00", "rows": 1},
    {"SessionID": C, "column": "checkinDateTime", "old": "2026-08-28T13:03:18", "new": "2026-08-28T17:03:18", "rows": 2}]}


def _state(c):
    return sorted((r["SessionID"][:8], r["checkinDateTime"], r["checkinReplyDateTime"]) for r in c.rows(CLONE))


def test_apply_and_rollback_are_one_set_based_transaction_each():
    c = _table()
    before = _state(c)
    F.run_changes(c, PLAN["changes"], forward=True, table=CLONE)
    after = _state(c)
    assert ("aaaaaaaa", datetime(2026, 8, 24, 17, 1, 49), datetime(2026, 8, 24, 17, 30)) in after
    assert after.count(("cccccccc", datetime(2026, 8, 28, 17, 3, 18), None)) == 2
    assert ("bbbbbbbb", datetime(2026, 5, 20, 14, 0), None) in after                  # untouched
    sql = c.statements[-1].sql
    assert sql.count("UPDATE ") == 2 and sql.startswith("BEGIN TRANSACTION")       # one UPDATE per column
    F.run_changes(c, PLAN["changes"], forward=False, table=CLONE)
    assert _state(c) == before


def test_a_row_changed_since_the_list_aborts_everything():
    c = _table()
    c.duck.execute(f"UPDATE {c._name(CLONE)} SET checkinReplyDateTime = TIMESTAMP '2026-08-24 18:00:00'")   # another writer
    before = _state(c)
    with pytest.raises(RuntimeError, match="Assertion failed"):
        F.run_changes(c, PLAN["changes"], forward=True, table=CLONE)
    assert _state(c) == before                                                       # nothing half-done


def test_an_extra_row_with_the_listed_value_aborts_everything():
    c = _table()
    c.insert_raw(CLONE, {"SessionID": C, "checkinDateTime": datetime(2026, 8, 28, 13, 3, 18)})   # a third row appeared
    before = _state(c)
    with pytest.raises(RuntimeError, match="Assertion failed"):
        F.run_changes(c, PLAN["changes"], forward=True, table=CLONE)
    assert _state(c) == before


def test_a_production_plan_is_refused_without_the_flag(tmp_path):
    plan = tmp_path / "p.json"
    plan.write_text(json.dumps({"table": "early-alert-responses.RESPONSES.response_data", "changes": []}))
    for action in ("apply", "rollback"):
        with pytest.raises(SystemExit, match="not a DEV table"):
            F.main_([action, str(plan)])
    F.refuse_unless_allowed("early-alert-responses.RESPONSES.response_data", production=True)   # the one allowed run
    F.refuse_unless_allowed(CLONE, production=False)


def test_the_list_reads_the_table_it_is_given(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    seen = []

    class _C(_Client):
        def query(self, sql, job_config=None):
            seen.append(sql)
            return super().query(sql, job_config)
    plan = json.load(open(F.cmd_list(_C(), table=CLONE)))
    assert plan["table"] == CLONE and f"`{CLONE}`" in seen[0]
    assert any(c.get("flag") is None for c in plan["changes"])

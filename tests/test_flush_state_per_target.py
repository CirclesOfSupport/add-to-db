"""
Each staged target keeps its watermark in a state table of its own.

BigQuery lets only one transaction at a time change rows in a table, whichever rows they are. When
both targets' watermark rows lived in one table, each target's flush transaction waited on the
other's, and a flush that had already merged its rows could sit with its transaction open on
response_data. These tests hold one target's tables the way an open transaction does and check the
other target is untouched; that no table is changed by both targets' transactions; that the
watermark still moves only with the commit; and that a shared table is refused at start-up.
"""
from __future__ import annotations

import importlib
import re
import sys
from datetime import datetime, timedelta, timezone

import pytest

import config
from test_responses_sequences import CHECKIN, UUID, body
from test_staged_hookup import T0, resp_rows, stage, staged, state  # noqa: F401  (fixture)

NOW = T0 + timedelta(seconds=60)
BOTH = [("users", {"uuid": UUID, "orgID": "8"}), ("responses", body(CHECKIN, "Yes"))]


def scripts(svc) -> dict[str, list[str]]:
    """target -> the tables its flush transactions changed rows in (MERGE / UPDATE / DELETE), from the SQL sent."""
    out: dict[str, list[str]] = {}
    for job in svc.fake.statements:
        if not job.sql.lstrip().startswith("BEGIN TRANSACTION"):
            continue
        target = job.params["target"].value
        out.setdefault(target, [])
        out[target] += re.findall(r"^\s*(?:MERGE|UPDATE|DELETE FROM)\s+`([^`]+)`", job.sql, re.M)
    return out


def test_state_tables_default_to_one_per_target():
    assert config.flush_state_table("responses") == config.FLUSH_STATE_TABLE
    assert config.flush_state_table("users") == config.FLUSH_STATE_TABLE + "_users"
    assert config.flush_state_table("responses") != config.flush_state_table("users")


def test_no_table_is_changed_by_both_targets_transactions(staged):
    stage(staged, BOTH, T0)
    staged.run_flush_cycle(now=NOW)
    changed = scripts(staged)
    assert set(changed["responses"]) == {staged.table("responses"), config.flush_state_table("responses")}
    assert set(changed["users"]) == {staged.table("users"), config.flush_state_table("users")}
    assert set(changed["responses"]).isdisjoint(changed["users"])


def test_an_open_users_transaction_does_not_delay_the_check_in_flush(staged):
    stage(staged, BOTH, T0)
    # a users flush from an earlier request is still open: it holds users and the users state table
    staged.fake.held_tables |= {staged.table("users"), config.flush_state_table("users")}
    with pytest.raises(staged.FlushFailed) as info:
        staged.run_flush_cycle(now=NOW)
    done = info.value.results["responses"]
    assert done["status"] == "ok" and done["attempts"] == 1 and done["items"] == 1     # first try, no waiting
    assert len(resp_rows(staged)) == 1
    assert state(staged, "responses")["version"] == 1
    assert state(staged, "responses")["watermark"] == NOW - timedelta(seconds=config.FLUSH_SAFETY_S)
    assert list(info.value.errors) == ["users"] and state(staged, "users")["version"] == 0
    staged.fake.held_tables.clear()                                                    # the open transaction ends
    out = staged.run_flush_cycle(now=NOW + timedelta(seconds=30))
    assert out["targets"]["users"]["items"] == 1 and state(staged, "users")["version"] == 1


def test_an_open_check_in_transaction_does_not_delay_the_users_flush(staged):
    stage(staged, BOTH, T0)
    staged.fake.held_tables |= {staged.table("responses"), config.flush_state_table("responses")}
    with pytest.raises(staged.FlushFailed) as info:
        staged.run_flush_cycle(now=NOW)
    done = info.value.results["users"]
    assert done["status"] == "ok" and done["attempts"] == 1 and state(staged, "users")["version"] == 1
    assert list(info.value.errors) == ["responses"] and resp_rows(staged) == []
    assert state(staged, "responses")["version"] == 0


def test_one_shared_state_table_is_what_made_one_target_wait_on_the_other(staged, monkeypatch):
    """The layout before this change, rebuilt here: with both rows in one table the same hold stops the check-in flush."""
    shared = config.flush_state_table("responses")
    staged.fake.insert_raw(shared, {"id": "flush:users", "updated_at": None, "version": 0, "paused_since": None,
                                    "watermark": datetime(1970, 1, 1, tzinfo=timezone.utc)})
    monkeypatch.setattr(config, "flush_state_table", lambda target: shared)
    stage(staged, BOTH, T0)
    staged.fake.held_tables |= {staged.table("users"), shared}        # the open users transaction now holds the shared table
    with pytest.raises(staged.FlushFailed) as info:
        staged.run_flush_cycle(now=NOW)
    assert "responses" in info.value.errors and "concurrent update" in info.value.errors["responses"]
    assert resp_rows(staged) == []                                     # the check-in rows did not land


def test_the_watermark_moves_only_with_the_commit(staged):
    stage(staged, BOTH, T0)
    staged.fake.fail_always.add("RESPONSES.response_data")             # the check-in transaction aborts at its MERGE
    with pytest.raises(staged.FlushFailed):
        staged.run_flush_cycle(now=NOW)
    assert resp_rows(staged) == [] and state(staged, "responses")["version"] == 0
    assert state(staged, "responses")["watermark"] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    ok = [x for x in staged.fake.rows(config.FLUSH_LOG_TABLE) if x["status"] == "ok"]
    assert [x["target"] for x in ok] == ["users"]                      # no ok row, no watermark, no rows: all or nothing
    staged.fake.fail_always.clear()
    staged.run_flush_cycle(now=NOW + timedelta(seconds=30))
    assert len(resp_rows(staged)) == 1 and state(staged, "responses")["version"] == 1


def test_a_missing_users_state_row_fails_users_only_and_names_the_table(staged):
    staged.fake.duck.execute(f"DELETE FROM {staged.fake._name(config.flush_state_table('users'))}")
    stage(staged, BOTH, T0)
    with pytest.raises(staged.FlushFailed) as info:
        staged.run_flush_cycle(now=NOW)
    assert list(info.value.errors) == ["users"]
    assert config.flush_state_table("users") in info.value.errors["users"]
    assert len(resp_rows(staged)) == 1 and state(staged, "responses")["version"] == 1


# --- start-up: two staged targets may never share a state table -----------------------------------

def _load(monkeypatch, env):
    """A fresh import of config under env; the module the other tests and src/main.py hold is put back."""
    original = sys.modules.get("config") or importlib.import_module("config")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    sys.modules.pop("config", None)
    try:
        return importlib.import_module("config")
    finally:
        for k in env:
            monkeypatch.delenv(k)
        sys.modules["config"] = original


def test_a_shared_state_table_is_refused_at_start_up(monkeypatch):
    with pytest.raises(RuntimeError, match="share the table"):
        _load(monkeypatch, {"STAGED_TARGETS": "users,responses",
                            "FLUSH_STATE_TABLE_USERS": "early-alert-responses.OPS.adb_flush_state"})


def test_an_override_names_the_users_state_table(monkeypatch):
    c = _load(monkeypatch, {"STAGED_TARGETS": "users,responses",
                            "FLUSH_STATE_TABLE": "early-alert-responses.DEV.adb_x_flush_state",
                            "FLUSH_STATE_TABLE_USERS": "early-alert-responses.DEV.adb_x_users_state"})
    assert c.flush_state_table("responses") == "early-alert-responses.DEV.adb_x_flush_state"
    assert c.flush_state_table("users") == "early-alert-responses.DEV.adb_x_users_state"


@pytest.mark.parametrize("value", ["early-alert-responses.RESPONSES.adb_x", "other-project.OPS.adb_x", "adb_x"])
def test_a_users_state_table_outside_ops_or_dev_is_refused(monkeypatch, value):
    with pytest.raises(RuntimeError, match="FLUSH_STATE_TABLE_USERS"):
        _load(monkeypatch, {"FLUSH_STATE_TABLE_USERS": value})

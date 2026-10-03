"""tools/flush_pause.py sets and clears the maintenance pause the flush, health and sweep read."""
import os
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import flush_pause  # noqa: E402
from fake_bq import FakeClient  # noqa: E402

F = bigquery.SchemaField
DS = "early-alert-responses.DEV"
RESPONSES_STATE, USERS_STATE = f"{DS}.adb_flush_state", f"{DS}.adb_flush_state_users"


def _used(c):
    """The two rows the service reads: each target's row in its own table."""
    return ([r for r in c.rows(RESPONSES_STATE) if r["id"] == "flush:responses"]
            + [r for r in c.rows(USERS_STATE) if r["id"] == "flush:users"])


def _client():
    c = FakeClient()
    c.create(f"{DS}.adb_staging", [F("request_id", "STRING"), F("item_index", "INT64"), F("target", "STRING"),
                                    F("received_at", "TIMESTAMP"), F("payload", "STRING")])
    for t, table in (("responses", RESPONSES_STATE), ("users", USERS_STATE)):       # one state table per target
        c.create(table, [F("id", "STRING"), F("watermark", "TIMESTAMP"), F("version", "INT64"),
                         F("updated_at", "TIMESTAMP"), F("paused_since", "TIMESTAMP")])
        c.insert_raw(table, {"id": f"flush:{t}", "version": 3, "updated_at": None, "paused_since": None,
                             "watermark": datetime(2026, 9, 28, 12, tzinfo=timezone.utc)})
    # the row the responses table still carries from before each target had its own table; never touched
    c.insert_raw(RESPONSES_STATE, {"id": "flush:users", "version": 1, "updated_at": None, "paused_since": None,
                                   "watermark": datetime(2026, 9, 28, 11, tzinfo=timezone.utc)})
    c.insert_raw(f"{DS}.adb_staging", {"request_id": "r", "item_index": 0, "target": "responses", "payload": "{}",
                                        "received_at": datetime(2026, 9, 28, 13, tzinfo=timezone.utc)})
    return c


def test_on_off_and_status(capsys):
    c = _client()
    assert flush_pause.main_(["on", "--dataset", "DEV"], client=c) == 0
    assert len(_used(c)) == 2 and all(r["paused_since"] is not None for r in _used(c))
    old_row = next(r for r in c.rows(RESPONSES_STATE) if r["id"] == "flush:users")
    assert old_row["paused_since"] is None and old_row["version"] == 1          # the unused row is left alone
    out = capsys.readouterr().out
    assert "OK: the flush is PAUSED" in out and "unflushed calls 1" in out
    assert out.count("flush:users") == 1                                         # reported once, from its own table
    assert flush_pause.main_(["off", "--dataset", "DEV"], client=c) == 0
    assert all(r["paused_since"] is None for r in _used(c))
    assert "OK: the flush is NOT paused" in capsys.readouterr().out
    assert flush_pause.main_(["status", "--dataset", "DEV"], client=c) == 0


def test_each_pause_write_changes_one_targets_table_only():
    c = _client()
    flush_pause.main_(["on", "--dataset", "DEV"], client=c)
    updates = [j.sql for j in c.statements if j.sql.startswith("UPDATE")]
    assert [u.split("`")[1] for u in updates] == [RESPONSES_STATE, USERS_STATE]

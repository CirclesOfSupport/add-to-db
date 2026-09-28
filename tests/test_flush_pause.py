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


def _client():
    c = FakeClient()
    c.create(f"{DS}.adb_flush_state", [F("id", "STRING"), F("watermark", "TIMESTAMP"), F("version", "INT64"),
                                        F("updated_at", "TIMESTAMP"), F("paused_since", "TIMESTAMP")])
    c.create(f"{DS}.adb_staging", [F("request_id", "STRING"), F("item_index", "INT64"), F("target", "STRING"),
                                    F("received_at", "TIMESTAMP"), F("payload", "STRING")])
    for t in ("responses", "users"):
        c.insert_raw(f"{DS}.adb_flush_state", {"id": f"flush:{t}", "version": 3, "updated_at": None, "paused_since": None,
                                                "watermark": datetime(2026, 9, 28, 12, tzinfo=timezone.utc)})
    c.insert_raw(f"{DS}.adb_staging", {"request_id": "r", "item_index": 0, "target": "responses", "payload": "{}",
                                        "received_at": datetime(2026, 9, 28, 13, tzinfo=timezone.utc)})
    return c


def test_on_off_and_status(capsys):
    c = _client()
    assert flush_pause.main_(["on", "--dataset", "DEV"], client=c) == 0
    assert all(r["paused_since"] is not None for r in c.rows(f"{DS}.adb_flush_state"))
    out = capsys.readouterr().out
    assert "OK: the flush is PAUSED" in out and "unflushed calls 1" in out
    assert flush_pause.main_(["off", "--dataset", "DEV"], client=c) == 0
    assert all(r["paused_since"] is None for r in c.rows(f"{DS}.adb_flush_state"))
    assert "OK: the flush is NOT paused" in capsys.readouterr().out
    assert flush_pause.main_(["status", "--dataset", "DEV"], client=c) == 0

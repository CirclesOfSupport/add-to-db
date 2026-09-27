"""
The live single-writer hookup, end to end on DuckDB: /upsert stages calls
durably and stamps their receive time; run_flush_cycle writes everything
received up to (now - safety) in one transaction with a compare-and-set
watermark; failures keep the calls and alert; nothing is dropped silently.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from google.cloud import bigquery

import config
from test_responses_sequences import CHECKIN, CHECKIN_UTC, SID, SID_DECODED, UUID, body

F = bigquery.SchemaField
T0 = datetime(2026, 9, 18, 20, 2, 0, tzinfo=timezone.utc)


@pytest.fixture
def staged(svc, monkeypatch):
    fake = svc.fake
    fake.create(config.STAGING_TABLE, [F("request_id", "STRING"), F("item_index", "INT64"), F("target", "STRING"),
                                       F("received_at", "TIMESTAMP"), F("payload", "STRING")])
    fake.create(config.DEAD_LETTER_TABLE, [F("recorded_at", "TIMESTAMP"), F("received_at", "TIMESTAMP"),
                                           F("request_id", "STRING"), F("item_index", "INT64"), F("target", "STRING"),
                                           F("stage", "STRING"), F("errors", "STRING"), F("payload", "STRING")])
    fake.create(config.FLUSH_LOG_TABLE, [F("flush_id", "STRING"), F("started_at", "TIMESTAMP"),
                                         F("finished_at", "TIMESTAMP"), F("from_wm", "TIMESTAMP"),
                                         F("to_wm", "TIMESTAMP"), F("items", "INT64"), F("statements", "INT64"),
                                         F("dead_letters", "INT64"), F("status", "STRING"), F("error", "STRING")])
    fake.create(config.FLUSH_STATE_TABLE, [F("id", "STRING"), F("watermark", "TIMESTAMP"),
                                           F("version", "INT64"), F("updated_at", "TIMESTAMP")])
    fake.insert_raw(config.FLUSH_STATE_TABLE, {"id": "flush", "watermark": datetime(1970, 1, 1, tzinfo=timezone.utc),
                                               "version": 0, "updated_at": None})
    monkeypatch.setattr(config, "STAGED_TARGETS", {"users", "responses"})
    kicks = []
    monkeypatch.setattr(svc, "enqueue_flush", lambda bucket, when: kicks.append((bucket, when)) or f"flush-{bucket}")
    monkeypatch.setattr(svc, "_last_kicked_bucket", None)
    monkeypatch.setattr(svc, "is_task_request_authorized", lambda req: True)
    svc.kicks = kicks
    return svc


def stage(svc, calls, at):
    """Stage calls exactly as /upsert does, with a chosen receive time."""
    svc.stage_calls(calls, at)


def resp_rows(svc):
    return svc.fake.rows(svc.table("responses"))


def state(svc):
    return svc.fake.rows(config.FLUSH_STATE_TABLE)[0]


def test_upsert_stages_both_halves_durably_and_kicks_one_flush(staged):
    c = staged.app.test_client()
    r = c.post("/upsert", json={"tables": [
        {"table": "users", "data": {"uuid": UUID, "checkInRepliesTotal": "33"}},
        {"table": "responses", "data": body(CHECKIN, "Yes")}]})
    assert r.status_code == 202, r.get_json()
    rows = staged.fake.rows(config.STAGING_TABLE)
    assert [x["target"] for x in sorted(rows, key=lambda x: x["item_index"])] == ["users", "responses"]
    assert len({x["request_id"] for x in rows}) == 1 and len({x["received_at"] for x in rows}) == 1
    assert staged.queued == [] and len(staged.kicks) == 1
    bucket, when = staged.kicks[0]
    assert when == (bucket + 1) * config.FLUSH_BUCKET_S + config.FLUSH_SAFETY_S
    assert resp_rows(staged) == []            # nothing written per call


def test_kick_is_once_per_bucket(staged):
    staged.kick_flush(T0)
    staged.kick_flush(T0 + timedelta(seconds=1))
    assert len(staged.kicks) == 1


def test_triage_still_uses_the_per_call_queue(staged):
    r = staged.app.test_client().post("/upsert", json={"table": "triage_data",
                                                       "data": {"message_id": "m1", "determination": "LowConcern"}})
    assert r.status_code == 202 and [q[1] for q in staged.queued] == ["triage_data"]
    assert staged.fake.rows(config.STAGING_TABLE) == []


def test_staging_failure_is_a_500_not_a_silent_202(staged):
    staged.fake.fail_next.append(RuntimeError("backend"))
    r = staged.app.test_client().post("/upsert", json={"table": "users", "data": {"uuid": UUID}})
    assert r.status_code == 500


def test_rejected_staged_call_is_dead_lettered_but_triage_is_not(staged):
    c = staged.app.test_client()
    r = c.post("/upsert", json={"table": "users", "data": {"uuid": UUID, "userWeek": "abc"}})
    assert r.status_code == 400
    r2 = c.post("/upsert", json={"table": "triage_data", "data": {"message_id": "", "determination": "x"}})
    assert r2.status_code == 400
    dl = staged.fake.rows(config.DEAD_LETTER_TABLE)
    assert len(dl) == 1 and dl[0]["target"] == "users" and dl[0]["stage"] == "upsert"


def test_flush_writes_last_call_per_session_and_advances_watermark(staged):
    stage(staged, [("users", {"uuid": UUID, "checkInRepliesTotal": "33"}), ("responses", body(CHECKIN, "No"))], T0)
    stage(staged, [("users", {"uuid": UUID, "checkInRepliesTotal": "34"}), ("responses", body(CHECKIN, "Yes"))],
          T0 + timedelta(seconds=2))
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert out["status"] == "ok" and out["items"] == 4 and out["dead_letters"] == 0
    r = resp_rows(staged)
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["checkinDateTime"] == CHECKIN_UTC
    u = staged.fake.rows(staged.table("users"))
    assert len(u) == 1 and u[0]["checkinrepliestotal"] == 34
    assert state(staged)["version"] == 1
    assert state(staged)["watermark"] == T0 + timedelta(seconds=60 - config.FLUSH_SAFETY_S)
    log = staged.fake.rows(config.FLUSH_LOG_TABLE)
    assert [(x["status"], x["items"]) for x in log] == [("ok", 4)]


def test_calls_inside_the_safety_window_wait_for_the_next_flush(staged):
    stage(staged, [("responses", body(CHECKIN, "No"))], T0)
    stage(staged, [("responses", body(CHECKIN, "Yes"))], T0 + timedelta(seconds=50))
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))       # cutoff T0+40
    assert resp_rows(staged)[0]["checkinReply"] == "No"
    staged.run_flush_cycle(now=T0 + timedelta(seconds=90))       # cutoff T0+70
    r = resp_rows(staged)
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes"


def test_stale_carry_over_as_the_last_call_is_not_replied(staged):
    stale = body(CHECKIN, "Yes", checkinReplyDateTime="2026-09-11T10%3A00%3A00-04%3A00", checkinReplyNumerical="7")
    stage(staged, [("responses", stale)], T0)
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    r = resp_rows(staged)[0]
    assert r["checkinReply"] is None and r["checkinReplyDateTime"] is None and r["checkinReplyNumerical"] is None
    assert r["wellnessDomain"] == "Relational"          # non-reply fields still written


def test_real_reply_after_checkin_is_kept(staged):
    real = body(CHECKIN, "Yes", checkinReplyDateTime="2026-09-18T16%3A30%3A00-04%3A00")
    stage(staged, [("responses", real)], T0)
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    r = resp_rows(staged)[0]
    assert r["checkinReply"] == "Yes" and r["checkinReplyDateTime"] == datetime(2026, 9, 18, 20, 30)


def test_bad_call_is_dead_lettered_inside_the_flush_and_the_rest_written(staged):
    stage(staged, [("responses", body(CHECKIN, "Yes", userWeek="not-a-number"))], T0)
    stage(staged, [("users", {"uuid": UUID, "orgID": "8"})], T0 + timedelta(seconds=1))
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert out["dead_letters"] == 1
    dl = staged.fake.rows(config.DEAD_LETTER_TABLE)
    assert len(dl) == 1 and dl[0]["stage"] == "flush" and dl[0]["target"] == "responses" and dl[0]["request_id"]
    assert resp_rows(staged) == [] and len(staged.fake.rows(staged.table("users"))) == 1


def test_failed_flush_writes_nothing_keeps_the_calls_and_retry_succeeds(staged):
    stage(staged, [("users", {"uuid": UUID, "orgID": "8"}), ("responses", body(CHECKIN, "Yes"))], T0)
    staged.fake.fail_in_script.append("RESPONSES.users")   # the users MERGE collides (e.g. nightly contacts sync)
    with pytest.raises(RuntimeError):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert resp_rows(staged) == [] and staged.fake.rows(staged.table("users")) == []   # rolled back
    assert state(staged)["version"] == 0
    assert [x["status"] for x in staged.fake.rows(config.FLUSH_LOG_TABLE)] == ["failed"]
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=90))
    assert out["items"] == 2 and len(resp_rows(staged)) == 1


def test_a_second_writer_cannot_commit(staged, monkeypatch):
    stage(staged, [("responses", body(CHECKIN, "Yes"))], T0)
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))            # version -> 1
    stage(staged, [("responses", body(CHECKIN, "No"))], T0 + timedelta(seconds=45))
    stale_state = {"watermark": T0 + timedelta(seconds=40), "version": 0}  # what a slow writer read earlier
    monkeypatch.setattr(staged, "_read_flush_state", lambda: dict(stale_state))
    with pytest.raises(RuntimeError, match="Assertion failed"):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=90))
    assert resp_rows(staged)[0]["checkinReply"] == "Yes"               # its writes rolled back


def test_alert_after_repeated_failures_and_health(staged, caplog):
    stage(staged, [("users", {"uuid": UUID})], T0)
    for i in range(config.FLUSH_ALERT_AFTER):
        staged.fake.fail_in_script.append("RESPONSES.users")
        with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError):
            staged.run_flush_cycle(now=T0 + timedelta(seconds=60 + i))
    assert any("FLUSH_ALERT" in r.message for r in caplog.records)
    h = staged.flush_health()
    assert h["status"] == "alert" and h["consecutive_failed_flushes"] == config.FLUSH_ALERT_AFTER
    assert h["backlog_calls"] == 1
    staged.run_flush_cycle(now=T0 + timedelta(seconds=120))
    h = staged.flush_health()
    assert h["consecutive_failed_flushes"] == 0 and h["backlog_calls"] == 0


def test_health_sees_a_call_that_arrived_after_its_flush(staged):
    stage(staged, [("users", {"uuid": UUID})], T0)
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    stage(staged, [("users", {"uuid": UUID, "orgID": "9"})], T0 + timedelta(seconds=10))   # late, inside the flushed range
    assert staged.flush_health()["flushes_with_late_calls_24h"] == 1


def test_flush_endpoint_returns_500_for_a_retry(staged):
    stage(staged, [("users", {"uuid": UUID})], T0)
    staged.fake.fail_in_script.append("RESPONSES.users")
    r = staged.app.test_client().post("/tasks/flush")
    assert r.status_code == 500


def test_nothing_to_flush_is_a_noop(staged):
    out = staged.run_flush_cycle(now=T0)
    assert out["status"] in ("ok", "noop") and resp_rows(staged) == []

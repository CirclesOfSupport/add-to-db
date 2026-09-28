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
    fake.create(config.FLUSH_LOG_TABLE, [F("flush_id", "STRING"), F("target", "STRING"), F("started_at", "TIMESTAMP"),
                                         F("finished_at", "TIMESTAMP"), F("from_wm", "TIMESTAMP"),
                                         F("to_wm", "TIMESTAMP"), F("items", "INT64"), F("statements", "INT64"),
                                         F("dead_letters", "INT64"), F("attempts", "INT64"), F("status", "STRING"),
                                         F("error", "STRING"), F("refs", "STRING", mode="REPEATED")])
    fake.create(config.FLUSH_STATE_TABLE, [F("id", "STRING"), F("watermark", "TIMESTAMP"),
                                           F("version", "INT64"), F("updated_at", "TIMESTAMP"),
                                           F("paused_since", "TIMESTAMP")])
    for target in ("responses", "users"):
        fake.insert_raw(config.FLUSH_STATE_TABLE, {"id": f"flush:{target}", "updated_at": None, "version": 0,
                                                   "watermark": datetime(1970, 1, 1, tzinfo=timezone.utc),
                                                   "paused_since": None})
    monkeypatch.setattr(config, "STAGED_TARGETS", {"users", "responses"})
    clock = [0.0]                                  # sleeping advances a fake clock, so retry budgets run out
    monkeypatch.setattr(svc.time_module, "sleep", lambda secs: clock.__setitem__(0, clock[0] + secs))
    monkeypatch.setattr(svc.time_module, "monotonic", lambda: clock[0])
    kicks, tasks = [], []                          # kicks: per-bucket flushes; tasks: every flush-queue task
    def enqueue(bucket, when, kind="flush", body=None):
        tasks.append({"kind": kind, "key": bucket, "when": when, "body": body})
        if kind == "flush":
            kicks.append((bucket, when))
        return f"{kind}-{bucket}"
    monkeypatch.setattr(svc, "enqueue_flush", enqueue)
    monkeypatch.setattr(svc, "_last_kicked_bucket", None)
    monkeypatch.setattr(svc, "is_task_request_authorized", lambda req: True)
    svc.kicks, svc.tasks, svc.clock = kicks, tasks, clock
    return svc


def stage(svc, calls, at):
    """Stage calls exactly as /upsert does, with a chosen receive time."""
    svc.stage_calls(calls, at)


def resp_rows(svc):
    return svc.fake.rows(svc.table("responses"))


def state(svc, target="responses"):
    return next(r for r in svc.fake.rows(config.FLUSH_STATE_TABLE) if r["id"] == f"flush:{target}")


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
    assert out["status"] == "ok" and out["items"] == 4
    assert out["targets"]["responses"]["items"] == 2 and out["targets"]["users"]["items"] == 2
    r = resp_rows(staged)
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["checkinDateTime"] == CHECKIN_UTC
    u = staged.fake.rows(staged.table("users"))
    assert len(u) == 1 and u[0]["checkinrepliestotal"] == 34
    for target in ("responses", "users"):
        assert state(staged, target)["version"] == 1
        assert state(staged, target)["watermark"] == T0 + timedelta(seconds=60 - config.FLUSH_SAFETY_S)
    log = staged.fake.rows(config.FLUSH_LOG_TABLE)
    assert sorted((x["target"], x["status"], x["items"], x["attempts"]) for x in log) == [
        ("responses", "ok", 2, 1), ("users", "ok", 2, 1)]


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
    assert out["targets"]["responses"]["dead_letters"] == 1
    dl = staged.fake.rows(config.DEAD_LETTER_TABLE)
    assert len(dl) == 1 and dl[0]["stage"] == "flush" and dl[0]["target"] == "responses" and dl[0]["request_id"]
    assert resp_rows(staged) == [] and len(staged.fake.rows(staged.table("users"))) == 1


def test_contention_is_retried_inside_the_cycle(staged):
    stage(staged, [("users", {"uuid": UUID, "orgID": "8"}), ("responses", body(CHECKIN, "Yes"))], T0)
    staged.fake.fail_in_script += ["RESPONSES.users", "RESPONSES.users"]      # two collisions, then clear
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert out["targets"]["users"]["attempts"] == 3 and out["targets"]["responses"]["attempts"] == 1
    assert len(staged.fake.rows(staged.table("users"))) == 1
    assert [x["status"] for x in staged.fake.rows(config.FLUSH_LOG_TABLE)] == ["ok", "ok"]   # retries are not failures


def test_users_contention_never_blocks_check_in_rows(staged):
    stage(staged, [("users", {"uuid": UUID, "orgID": "8"}), ("responses", body(CHECKIN, "Yes"))], T0)
    staged.fake.fail_always.add("RESPONSES.users")                           # users contended all cycle
    with pytest.raises(staged.FlushFailed) as info:
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert "users" in info.value.errors and info.value.results["responses"]["items"] == 1
    assert len(resp_rows(staged)) == 1 and staged.fake.rows(staged.table("users")) == []
    assert state(staged, "responses")["version"] == 1 and state(staged, "users")["version"] == 0
    failed = [x for x in staged.fake.rows(config.FLUSH_LOG_TABLE) if x["status"] == "failed"]
    assert [(x["target"], x["attempts"] > 1) for x in failed] == [("users", True)]
    staged.fake.fail_always.clear()                                           # contention over: users catches up
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=90))
    assert out["targets"]["users"]["items"] == 1 and len(staged.fake.rows(staged.table("users"))) == 1


def test_failed_target_writes_nothing_and_keeps_its_calls(staged):
    stage(staged, [("users", {"uuid": UUID, "orgID": "8"})], T0)
    staged.fake.fail_always.add("RESPONSES.users")
    with pytest.raises(staged.FlushFailed):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert staged.fake.rows(staged.table("users")) == []                      # rolled back
    assert staged.fake.rows(config.DEAD_LETTER_TABLE) == []
    staged.fake.fail_always.clear()
    assert staged.run_flush_cycle(now=T0 + timedelta(seconds=90))["targets"]["users"]["items"] == 1


def test_non_retryable_error_is_not_retried(staged, monkeypatch):
    stage(staged, [("responses", body(CHECKIN, "Yes"))], T0)
    calls = []

    def broken(*a, **k):
        calls.append(1)
        raise ValueError("Syntax error: unexpected keyword")
    monkeypatch.setattr(staged, "_flush_target_once", broken)
    with pytest.raises(staged.FlushFailed):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    assert len(calls) == 2                                                    # once per target, no retries


def test_a_second_writer_cannot_commit(staged, monkeypatch):
    stage(staged, [("responses", body(CHECKIN, "Yes"))], T0)
    staged.run_flush_cycle(now=T0 + timedelta(seconds=60))                   # responses version -> 1
    stage(staged, [("responses", body(CHECKIN, "No"))], T0 + timedelta(seconds=45))
    real = staged._read_flush_state
    reads = []

    def stale_once(target):                  # a slow writer: its first read predates the other commit
        reads.append(target)
        if target == "responses" and reads.count("responses") == 1:
            return {"watermark": T0 + timedelta(seconds=40), "version": 0, "paused_since": None}
        return real(target)
    monkeypatch.setattr(staged, "_read_flush_state", stale_once)
    out = staged.run_flush_cycle(now=T0 + timedelta(seconds=90))
    assert out["targets"]["responses"]["attempts"] == 2                     # the stale attempt aborted, retry committed
    r = resp_rows(staged)
    assert len(r) == 1 and r[0]["checkinReply"] == "No"


def test_health_ok_when_clean_alert_only_while_failing(staged, caplog):
    stage(staged, [("users", {"uuid": UUID}), ("responses", body(CHECKIN, "Yes"))], T0)
    staged.run_flush_cycle(now=datetime.now(timezone.utc) + timedelta(seconds=60))
    h = staged.flush_health()
    assert h["status"] == "ok", h
    stage(staged, [("users", {"uuid": UUID, "orgID": "9"})], datetime.now(timezone.utc) + timedelta(seconds=61))
    staged.fake.fail_always.add("RESPONSES.users")
    for i in range(config.FLUSH_ALERT_AFTER):
        with caplog.at_level(logging.WARNING), pytest.raises(staged.FlushFailed):
            staged.run_flush_cycle(now=datetime.now(timezone.utc) + timedelta(seconds=120 + i))
    assert any("FLUSH_ALERT" in r.message for r in caplog.records)
    h = staged.flush_health()
    assert h["status"] == "alert" and h["targets"]["users"]["status"] == "alert"
    assert h["targets"]["responses"]["status"] == "ok"
    assert h["targets"]["users"]["consecutive_failed_flushes"] == config.FLUSH_ALERT_AFTER
    staged.fake.fail_always.clear()
    staged.run_flush_cycle(now=datetime.now(timezone.utc) + timedelta(seconds=200))
    h = staged.flush_health()
    assert h["status"] == "ok", h
    assert h["targets"]["users"]["consecutive_failed_flushes"] == 0 and h["targets"]["users"]["backlog_calls"] == 0


def test_health_sees_a_call_that_arrived_after_its_flush(staged):
    now = datetime.now(timezone.utc)
    stage(staged, [("users", {"uuid": UUID})], now - timedelta(seconds=100))
    staged.run_flush_cycle(now=now)
    stage(staged, [("users", {"uuid": UUID, "orgID": "9"})], now - timedelta(seconds=90))   # late, inside the flushed range
    h = staged.flush_health(now=now)
    assert h["targets"]["users"]["late_calls_not_dead_lettered"] == 1 and h["status"] == "alert"
    assert h["targets"]["responses"]["late_calls_not_dead_lettered"] == 0
    staged.run_flush_cycle(now=now, late_check=True)            # the sweep's flush dead-letters it
    h = staged.flush_health(now=now)
    assert h["targets"]["users"]["late_calls_not_dead_lettered"] == 0 and h["status"] == "ok", h


def test_flush_endpoint_returns_500_for_a_retry(staged):
    stage(staged, [("users", {"uuid": UUID})], T0)
    staged.fake.fail_always.add("RESPONSES.users")
    r = staged.app.test_client().post("/tasks/flush")
    assert r.status_code == 500 and "users" in r.get_json()["failed"]


def test_nothing_to_flush_is_a_noop(staged):
    out = staged.run_flush_cycle(now=T0)
    assert out["status"] in ("ok", "noop") and resp_rows(staged) == []


def test_retry_lines_carry_the_whole_reason(staged, caplog):
    stage(staged, [("users", {"uuid": UUID})], T0)
    staged.fake.fail_in_script.append("RESPONSES.users")
    with caplog.at_level(logging.WARNING):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    line = next(r.getMessage() for r in caplog.records if "contended" in r.getMessage())
    assert line.endswith("Reason: Transaction is aborted due to concurrent update")


def test_flush_kick_requests_the_sweep_flush_under_its_own_name(staged):
    r = staged.app.test_client().post("/tasks/flush-kick")
    assert r.status_code == 200 and staged.kicks == []          # never the name a call in this bucket asks for
    assert [(t["kind"], t["key"], t["body"]) for t in staged.tasks] == [("sweep", r.get_json()["bucket"], {"late_check": True})]


def test_contended_users_gives_up_within_its_short_budget(staged):
    stage(staged, [("users", {"uuid": UUID}), ("responses", body(CHECKIN, "Yes"))], T0)
    staged.fake.fail_always.add("RESPONSES.users")
    start = staged.time_module.monotonic()
    with pytest.raises(staged.FlushFailed):
        staged.run_flush_cycle(now=T0 + timedelta(seconds=60))
    spent = staged.time_module.monotonic() - start          # fake clock: only backoff sleeps advance it
    assert spent <= config.FLUSH_RETRY_BUDGET_S["users"]
    assert config.FLUSH_RETRY_BUDGET_S["responses"] > config.FLUSH_RETRY_BUDGET_S["users"]
    assert len(resp_rows(staged)) == 1

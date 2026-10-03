"""
The staged flush under backlog, lateness and maintenance, and the alerts that reach a person:
  - a flush reads each table's schema once, however many calls it takes;
  - a backlog drains in bounded flushes, each asking for the next at once;
  - the staging append is bounded inside the flush safety window, and a slow append is a 500;
  - a call that becomes visible behind its flush is dead-lettered and alerted, never written;
  - every condition that needs a person logs ADB_ALERT (the alert policy's token);
  - a planned maintenance pause is not an alert, and the flush writes nothing while it lasts.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import pytest

import config
from test_responses_sequences import CHECKIN, UUID, body
from test_staged_hookup import resp_rows, stage, staged  # noqa: F401  (fixture)


def alerts(caplog, kind=None):
    lines = [r.getMessage() for r in caplog.records if "ADB_ALERT" in r.getMessage()]
    return [x for x in lines if kind is None or x.startswith(f"ADB_ALERT {kind}:")]


def sid_body(n, reply="Yes"):
    b = body(CHECKIN, reply)
    b["sessionID"] = f"{UUID}2026-09-18T16%3A01%3A{n:02d}.000000-04%3A00"
    return b


def set_pause(svc, since):
    for target in ("responses", "users"):
        svc.fake.duck.execute(
            f"UPDATE {svc.fake._name(config.flush_state_table(target))} SET paused_since = "
            + ("NULL" if since is None else f"CAST('{since.isoformat()}' AS TIMESTAMPTZ)"))


# --- the flush reads each schema once --------------------------------------------------------

def test_a_flush_reads_each_tables_schema_once(staged):
    now = datetime.now(timezone.utc)
    for n in range(40):
        stage(staged, [("users", {"uuid": f"{UUID[:-2]}{n:02d}", "orgID": "8"}), ("responses", sid_body(n))],
              now - timedelta(seconds=100 - n))
    staged.fake.get_table_calls.clear()
    out = staged.run_flush_cycle(now=now)
    assert out["targets"]["responses"]["items"] == 40 and out["targets"]["users"]["items"] == 40
    calls = staged.fake.get_table_calls
    assert calls.count(staged.table("responses")) == 1 and calls.count(staged.table("users")) == 1, calls
    assert len(resp_rows(staged)) == 40


# --- a backlog drains in bounded flushes -------------------------------------------------------

def test_a_backlog_drains_in_bounded_flushes_each_asking_for_the_next(staged, monkeypatch):
    monkeypatch.setattr(config, "FLUSH_MAX_ITEMS", 5)
    now = datetime.now(timezone.utc)
    for n in range(12):
        stage(staged, [("responses", sid_body(n))], now - timedelta(seconds=200 - n))
    taken = []
    for _ in range(3):
        out = staged.run_flush_cycle(now=now)
        taken.append(out["targets"]["responses"]["items"])
    assert taken == [5, 5, 2]
    drains = [t for t in staged.tasks if t["kind"] == "drain"]
    assert len(drains) == 2                                   # after the 1st and 2nd flush, not after the last
    assert len(resp_rows(staged)) == 12
    assert staged.run_flush_cycle(now=now)["targets"]["responses"]["items"] == 0


def test_the_cap_never_splits_one_receive_time(staged, monkeypatch):
    monkeypatch.setattr(config, "FLUSH_MAX_ITEMS", 3)
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", sid_body(0))], now - timedelta(seconds=100))
    stage(staged, [("responses", sid_body(n)) for n in range(1, 5)], now - timedelta(seconds=99))   # one request, 4 calls
    first = staged.run_flush_cycle(now=now)["targets"]["responses"]
    second = staged.run_flush_cycle(now=now)["targets"]["responses"]
    assert (first["items"], first["more"]) == (1, True)       # stops before the shared receive time
    assert second["items"] == 4                               # takes that receive time whole
    assert len(resp_rows(staged)) == 5


# --- the staging append is bounded -------------------------------------------------------------

def test_the_staging_append_is_bounded_inside_the_safety_window(staged):
    r = staged.app.test_client().post("/upsert", json={"table": "responses", "data": body(CHECKIN, "Yes")})
    assert r.status_code == 202
    call = staged.fake.append_calls[-1]
    assert call["retry"]._timeout <= config.STAGING_APPEND_BUDGET_S
    assert call["timeout"] <= config.STAGING_APPEND_ATTEMPT_S
    assert config.STAGING_APPEND_BUDGET_S + 5 <= config.FLUSH_SAFETY_S


def test_a_slow_append_is_answered_500_and_alerted(staged, caplog):
    staged.fake.before_append.append(lambda: staged.clock.__setitem__(0, staged.clock[0] + config.STAGING_APPEND_BUDGET_S + 1))
    with caplog.at_level(logging.WARNING):
        r = staged.app.test_client().post("/upsert", json={"table": "responses", "data": body(CHECKIN, "Yes")})
    assert r.status_code == 500
    assert alerts(caplog, "STAGING"), [x.getMessage() for x in caplog.records]


# --- a call that lands behind its flush --------------------------------------------------------

def test_a_call_that_lands_behind_its_flush_is_dead_lettered_and_alerted_not_written(staged, caplog):
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", sid_body(1, "No"))], now - timedelta(seconds=100))
    staged.run_flush_cycle(now=now)                                   # watermark now - 20 s
    stage(staged, [("responses", sid_body(2, "Late"))], now - timedelta(seconds=60))   # visible only now, behind it
    for i in range(5):                                                # ordinary flushes never take it
        staged.run_flush_cycle(now=now + timedelta(seconds=30 * (i + 1)))
    assert [r["checkinReply"] for r in resp_rows(staged)] == ["No"]
    with caplog.at_level(logging.WARNING):
        out = staged.run_flush_cycle(now=now + timedelta(seconds=200), late_check=True)   # the sweep's flush
    assert out["targets"]["responses"]["late_dead_lettered"] == 1
    dl = staged.fake.rows(config.DEAD_LETTER_TABLE)
    assert [(d["stage"], d["target"]) for d in dl] == [("late", "responses")] and "Late" in dl[0]["payload"]
    assert alerts(caplog, "LATE")
    assert [r["checkinReply"] for r in resp_rows(staged)] == ["No"]  # never written late
    out = staged.run_flush_cycle(now=now + timedelta(seconds=230), late_check=True)
    assert out["targets"]["responses"]["late_dead_lettered"] == 0 and len(staged.fake.rows(config.DEAD_LETTER_TABLE)) == 1


# --- every condition that needs a person logs ADB_ALERT -----------------------------------------

def test_consecutive_failed_flushes_alert(staged, caplog):
    now = datetime.now(timezone.utc)
    stage(staged, [("users", {"uuid": UUID})], now - timedelta(seconds=100))
    staged.fake.fail_always.add("RESPONSES.users")
    with caplog.at_level(logging.WARNING):
        for i in range(config.FLUSH_ALERT_AFTER):
            with pytest.raises(staged.FlushFailed):
                staged.run_flush_cycle(now=now + timedelta(seconds=i))
    assert len(alerts(caplog, "FLUSH_ALERT")) == 1                  # on the third, not before


def test_a_dead_letter_at_upsert_alerts(staged, caplog):
    with caplog.at_level(logging.WARNING):
        r = staged.app.test_client().post("/upsert", json={"table": "users", "data": {"uuid": UUID, "userWeek": "abc"}})
    assert r.status_code == 400 and alerts(caplog, "SET_ASIDE")


def test_a_dead_letter_at_flush_alerts(staged, caplog):
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", body(CHECKIN, "Yes", userWeek="not-a-number"))], now - timedelta(seconds=100))
    with caplog.at_level(logging.WARNING):
        staged.run_flush_cycle(now=now)
    assert alerts(caplog, "SET_ASIDE") and len(staged.fake.rows(config.DEAD_LETTER_TABLE)) == 1
    # the words read by a person: the alert line and the health field say "set aside", never "dead letter"
    assert not [x for x in alerts(caplog) if "dead" in x.lower()]
    h = staged.flush_health(now=now)
    assert h["set_aside_24h"] == 1 and "dead_letters_24h" not in h


def test_the_set_aside_table_is_adb_set_aside():
    assert config.DEAD_LETTER_TABLE.endswith(".adb_set_aside")


def test_an_old_backlog_alerts_from_the_sweep(staged, caplog):
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", sid_body(1))], now - timedelta(seconds=config.BACKLOG_ALERT_S + 60))
    with caplog.at_level(logging.WARNING):
        r = staged.app.test_client().post("/tasks/flush-kick")
    assert r.status_code == 200 and any(a.startswith("BACKLOG: responses") for a in r.get_json()["alerts"])
    assert alerts(caplog, "BACKLOG")
    assert staged.app.test_client().get("/health/flush").status_code == 503


def test_a_fresh_backlog_does_not_alert(staged, caplog):
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", sid_body(1))], now - timedelta(seconds=30))
    with caplog.at_level(logging.WARNING):
        assert staged.sweep_check(now) == []
    assert alerts(caplog) == []


# --- a planned pause is not an alert ------------------------------------------------------------

def test_a_planned_pause_is_not_an_alert_and_writes_nothing(staged, caplog):
    now = datetime.now(timezone.utc)
    set_pause(staged, now - timedelta(minutes=10))
    stage(staged, [("responses", sid_body(1)), ("users", {"uuid": UUID})], now - timedelta(minutes=20))
    with caplog.at_level(logging.WARNING):
        assert staged.sweep_check(now) == []
        out = staged.run_flush_cycle(now=now)
    assert alerts(caplog) == []
    assert out["targets"]["responses"]["status"] == "paused" and resp_rows(staged) == []
    h = staged.app.test_client().get("/health/flush")
    assert h.status_code == 200 and h.get_json()["status"] == "paused"
    # unpaused, the same backlog alerts; after a flush it does not
    set_pause(staged, None)
    with caplog.at_level(logging.WARNING):
        assert staged.sweep_check(now) and alerts(caplog, "BACKLOG")
    staged.run_flush_cycle(now=now)
    assert staged.sweep_check(now) == [] and len(resp_rows(staged)) == 1


def test_a_pause_left_on_too_long_alerts(staged, caplog):
    now = datetime.now(timezone.utc)
    set_pause(staged, now - timedelta(seconds=config.PAUSE_ALERT_S + 60))
    with caplog.at_level(logging.WARNING):
        got = staged.sweep_check(now)
    assert got and all(a.startswith("PAUSE:") for a in got) and alerts(caplog, "PAUSE")


def test_a_pause_set_mid_flush_stops_the_commit(staged):
    now = datetime.now(timezone.utc)
    stage(staged, [("responses", sid_body(1))], now - timedelta(seconds=100))
    real = staged._read_flush_state
    reads = []

    def pause_after_first_read(target):
        state = real(target)
        reads.append(target)
        if target == "responses" and reads.count("responses") == 1:
            set_pause(staged, now)          # the pause lands between this flush's read and its commit
        return state
    staged._read_flush_state = pause_after_first_read
    out = staged.run_flush_cycle(now=now)
    staged._read_flush_state = real
    assert out["targets"]["responses"]["status"] == "paused" and resp_rows(staged) == []


# --- task names ---------------------------------------------------------------------------------

def test_flush_task_ids_carry_a_hashed_prefix():
    import tasks
    a, b = tasks.flush_task_id("flush", 59700000), tasks.flush_task_id("flush", 59700001)
    assert re.fullmatch(r"[0-9a-f]{12}-flush-59700000", a) and re.fullmatch(r"[0-9a-f]{12}-flush-59700001", b)
    assert a[:12] != b[:12]
    assert tasks.flush_task_id("sweep", 59700000) != a


def test_a_call_after_the_sweep_still_gets_its_bucket_flush(staged):
    c = staged.app.test_client()
    assert c.post("/tasks/flush-kick").status_code == 200
    assert c.post("/upsert", json={"table": "responses", "data": body(CHECKIN, "Yes")}).status_code == 202
    assert [t["kind"] for t in staged.tasks] == ["sweep", "flush"]

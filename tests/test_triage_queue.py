"""
Triage writes on one queue: routing, arrival order, and the TRIAGE_ALERT lines.

The split this unit fixes happens only when two of a message's writes run at the same time; the queue at
max concurrency 1 prevents that, and these tests prove the two things the code owns: every triage write is
sent to that queue (and nothing else is), and run one after another the three writes land on ONE row in
every one of the six arrival orders -- including triage request first, the order TextIt actually sends.
"""
from __future__ import annotations

import importlib
import itertools
import logging
import sys

import pytest

MESSAGE_ID = "20260927013432980502-379281"
MESSAGE = {"message_id": MESSAGE_ID, "uuid": "743aa5a3-d2d8-4bc4-96b8-a118a80aef4a",
           "sessionid": "743aa5a3-d2d8-4bc4-96b8-a118a80aef4a2026-09-27T13:26:20.306337-04:00",
           "message": "test message", "classification": "Other",
           "message_time": "2026-09-27T13:34:32.980428-04:00", "message_day": "2026-09-27",
           "referring_flow_id": "f", "referring_flow_name": "Unrecognized Message"}
TRIAGE = {"message_id": MESSAGE_ID, "triage_request_id": "2026-09-27_13:34:33_743aa5a3-d2d8-4bc4-96b8-a118a80aef4a",
          "triage_request_time": "2026-09-27T13:34:33.157616-04:00"}
DETERMINATION = {"message_id": MESSAGE_ID, "determination": "LowConcern",
                 "determination_time": "2026-09-27T13:34:55.997538-04:00"}
PARTS = {"message": MESSAGE, "triage": TRIAGE, "determination": DETERMINATION}


def _reload_config(monkeypatch, **env):
    for k in ("TRIAGE_QUEUE", "TRIAGE_MAX_ATTEMPTS", "TRIAGE_WAIT_ALERT_S"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    for name in ("config", "tasks"):
        monkeypatch.delitem(sys.modules, name, raising=False)   # restored at teardown: no leak into other tests
    config = importlib.import_module("config")
    tasks = importlib.import_module("tasks")
    return config, tasks


# ---- routing ---------------------------------------------------------------------------------------------

def test_unset_triage_queue_keeps_every_target_on_tasks_queue(monkeypatch):
    config, tasks = _reload_config(monkeypatch)
    for target in config.ALLOWED_TARGETS:
        assert tasks.queue_for(target) == config.TASKS_QUEUE


def test_triage_queue_takes_triage_writes_and_nothing_else(monkeypatch):
    config, tasks = _reload_config(monkeypatch, TRIAGE_QUEUE="add-to-db-triage", TRIAGE_MAX_ATTEMPTS="10")
    assert tasks.queue_for("triage_data") == "add-to-db-triage"
    for target in config.ALLOWED_TARGETS:
        if target != "triage_data":
            assert tasks.queue_for(target) == config.TASKS_QUEUE


def test_enqueue_write_creates_the_task_on_the_triage_queue(monkeypatch):
    config, tasks = _reload_config(monkeypatch, TRIAGE_QUEUE="add-to-db-triage", TRIAGE_MAX_ATTEMPTS="10")
    created = []

    class FakeTasksClient:
        def queue_path(self, project, location, queue):
            return f"projects/{project}/locations/{location}/queues/{queue}"

        def create_task(self, parent, task):
            created.append(parent)

            class T:
                name = f"{parent}/tasks/1"
            return T()

    monkeypatch.setattr(tasks, "_client", FakeTasksClient())
    for path in ("/tasks/upsert", "/tasks/ingest"):
        for part in PARTS.values():
            tasks.enqueue_write(path, "triage_data", part)
    tasks.enqueue_write("/tasks/upsert", "feedback", {"testimonial_id": "t"})
    tasks.enqueue_write("/tasks/upsert", "users", {"uuid": "u"})
    assert created[:6] == ["projects/early-alert-responses/locations/us-east1/queues/add-to-db-triage"] * 6
    assert created[6:] == ["projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes"] * 2


def test_triage_queue_without_explicit_retry_limit_refuses_to_start(monkeypatch):
    with pytest.raises(RuntimeError, match="TRIAGE_MAX_ATTEMPTS"):
        _reload_config(monkeypatch, TRIAGE_QUEUE="add-to-db-triage")


def test_triage_queue_cannot_be_the_shared_queue(monkeypatch):
    with pytest.raises(RuntimeError, match="its own queue"):
        _reload_config(monkeypatch, TRIAGE_QUEUE="add-to-db-writes", TRIAGE_MAX_ATTEMPTS="10")


# ---- arrival order: one row, every order -----------------------------------------------------------------

@pytest.mark.parametrize("order", list(itertools.permutations(PARTS)))
def test_three_writes_one_after_another_land_on_one_row_in_any_order(svc, order):
    for name in order:
        body, status = svc.perform_upsert("triage_data", dict(PARTS[name]))
        assert status == 200 and body["status"] == "ok", body
    rows = svc.fake.rows(svc.table("triage_data"))
    assert len(rows) == 1, f"{order}: {rows}"
    row = rows[0]
    assert row["uuid"] == MESSAGE["uuid"] and row["message"] == MESSAGE["message"]
    assert row["triage_request_id"] == TRIAGE["triage_request_id"]
    assert row["determination"] == "LowConcern"


# ---- TRIAGE_ALERT ----------------------------------------------------------------------------------------

@pytest.fixture
def watched(svc, monkeypatch):
    import config
    monkeypatch.setattr(config, "TRIAGE_QUEUE", "add-to-db-triage")
    monkeypatch.setattr(config, "TRIAGE_MAX_ATTEMPTS", 10)
    monkeypatch.setattr(config, "TRIAGE_WAIT_ALERT_S", 300)
    return svc


def _alerts(caplog):
    return [r.getMessage() for r in caplog.records if "TRIAGE_ALERT" in r.getMessage()]


def test_final_attempt_failure_raises_exhausted(watched, caplog):
    caplog.set_level(logging.ERROR)
    h = {"X-CloudTasks-TaskRetryCount": "9", "X-CloudTasks-TaskETA": "1000"}
    kinds = watched.watch_triage_task("triage_data", TRIAGE, {"error": "BigQuery MERGE failed", "details": "x"}, 500, h, now=1001)
    assert kinds == ["EXHAUSTED"]
    msg = _alerts(caplog)[0]
    assert "TRIAGE_ALERT EXHAUSTED" in msg and "triage request write" in msg and MESSAGE_ID in msg
    assert "ADB_ALERT" not in msg


def test_earlier_attempt_failure_is_silent(watched, caplog):
    caplog.set_level(logging.ERROR)
    for retry in range(9):
        h = {"X-CloudTasks-TaskRetryCount": str(retry), "X-CloudTasks-TaskETA": "1000"}
        assert watched.watch_triage_task("triage_data", TRIAGE, {"error": "e"}, 500, h, now=1001) == []
    assert _alerts(caplog) == []


def test_late_start_raises_backlog(watched, caplog):
    caplog.set_level(logging.ERROR)
    h = {"X-CloudTasks-TaskRetryCount": "0", "X-CloudTasks-TaskETA": "1000"}
    assert watched.watch_triage_task("triage_data", MESSAGE, {"status": "ok"}, 200, h, now=1300) == ["BACKLOG"]
    assert watched.watch_triage_task("triage_data", MESSAGE, {"status": "ok"}, 200, h, now=1299) == []
    assert "message write" in _alerts(caplog)[0] and "test message" not in _alerts(caplog)[0]


def test_acknowledged_but_not_written_raises_dropped(watched, caplog):
    caplog.set_level(logging.ERROR)
    h = {"X-CloudTasks-TaskRetryCount": "0", "X-CloudTasks-TaskETA": "1000"}
    body = {"status": "error", "errors": ["Field 'message_id' cannot be null"]}
    assert watched.watch_triage_task("triage_data", DETERMINATION, body, 200, h, now=1001) == ["DROPPED"]


def test_worker_watches_only_tasks_from_the_triage_queue(watched, caplog, monkeypatch):
    caplog.set_level(logging.ERROR)
    monkeypatch.setattr(watched, "is_task_request_authorized", lambda r: True)
    web = watched.app.test_client()
    old = {"X-CloudTasks-TaskRetryCount": "0", "X-CloudTasks-TaskETA": "1"}   # started ages after it was due
    r = web.post("/tasks/upsert", json={"table": "triage_data", "data": TRIAGE},
                 headers={**old, "X-CloudTasks-QueueName": "add-to-db-writes"})
    assert r.status_code == 200 and _alerts(caplog) == []
    r = web.post("/tasks/upsert", json={"table": "triage_data", "data": MESSAGE},
                 headers={**old, "X-CloudTasks-QueueName": "add-to-db-triage"})
    assert r.status_code == 200 and len(_alerts(caplog)) == 1 and "BACKLOG" in _alerts(caplog)[0]


def test_unset_triage_queue_watches_nothing(svc, caplog, monkeypatch):
    import config
    monkeypatch.setattr(config, "TRIAGE_QUEUE", "")
    caplog.set_level(logging.ERROR)
    monkeypatch.setattr(svc, "is_task_request_authorized", lambda r: True)
    r = svc.app.test_client().post("/tasks/upsert", json={"table": "triage_data", "data": TRIAGE},
                                   headers={"X-CloudTasks-QueueName": "add-to-db-triage",
                                            "X-CloudTasks-TaskETA": "1", "X-CloudTasks-TaskRetryCount": "99"})
    assert r.status_code == 200 and _alerts(caplog) == []

"""tools/prove_flush_timeout.py: the verdicts, with a stand-in for BigQuery (the proof itself runs on DEV)."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import prove_flush_timeout as proof  # noqa: E402

TABLE = "early-alert-responses.DEV.flushfix_timeout_test"


class _Job:
    job_id, state, error_result = "job-a", "RUNNING", None

    def __init__(self, world, kind):
        self.world, self.kind = world, kind

    def result(self, timeout=None):
        w = self.world
        if self.kind == "held":
            w.result_timeout = timeout
            if w.a_finishes:
                return []
            if w.server_stops:
                w.now += w.stops_after
                self.state, self.error_result = "DONE", {"message": "Job timed out after 20s"}
                w.holding = w.stuck
                raise RuntimeError("400 Job timed out after 20s")
            raise TimeoutError("still running")
        if self.kind == "next":
            if w.holding:
                raise RuntimeError("Transaction is aborted due to concurrent update against table " + TABLE)
            w.n += 100
        return [{"n": w.n}] if self.kind == "read" else []

    def cancel(self):
        self.world.cancelled = True
        self.world.holding = self.world.stuck

    def reload(self):
        if self.world.cancelled:
            self.state, self.error_result = "DONE", {"message": "Job execution was cancelled: User requested cancellation"}


class _World:
    def __init__(self, server_stops=False, stuck=False, holds=True, a_finishes=False):
        self.n, self.cancelled, self.dropped, self.created = 0, False, [], []
        self.server_stops, self.stuck, self.a_finishes = server_stops, stuck, a_finishes
        self.holding, self.now, self.stops_after = holds, 0.0, 12.0     # 8 s control + 12 s = the 20 s limit

    def query(self, sql, job_config=None):
        if sql.startswith("CREATE"):
            self.created.append(sql)
        if "CROSS JOIN" in sql:
            self.timeout_ms = int(job_config.job_timeout_ms)
            return _Job(self, "held")
        if sql.startswith("BEGIN TRANSACTION"):
            return _Job(self, "next")
        return _Job(self, "read" if sql.startswith("SELECT") else "other")

    def delete_table(self, table, not_found_ok=False):
        self.dropped.append(table)

    def sleep(self, seconds):
        self.now += seconds

    def clock(self):
        return self.now


def run(world):
    said = []
    code = proof.run_proof(world, TABLE, say=said.append, sleep=world.sleep, clock=world.clock)
    return code, "\n".join(said)


def test_pass_when_our_cancel_frees_the_table():
    w = _World()
    code, text = run(w)
    assert code == 0 and "PROOF PASS" in text and w.cancelled and w.n == 100
    assert "B0 (control, while A runs): REFUSED" in text and "asked to cancel" in text
    assert w.timeout_ms == 20000 and w.result_timeout == pytest.approx(22.0)       # 20 s + 10 s grace - the 8 s already spent
    assert w.dropped == [TABLE]


def test_pass_when_bigquery_stops_the_job_itself():
    w = _World(server_stops=True)
    code, text = run(w)
    assert code == 0 and "BigQuery ended the job" in text and not w.cancelled


def test_fail_when_the_table_stays_held_after_the_cancel():
    w = _World(stuck=True)
    code, text = run(w)
    assert code == 1 and "still refused 30 s after A ended" in text and "PROOF FAIL" in text and w.dropped == [TABLE]


def test_control_failed_when_the_first_transaction_never_held_the_table():
    w = _World(holds=False)
    code, text = run(w)
    assert code == 2 and "CONTROL FAILED" in text and w.dropped == [TABLE]


def test_control_failed_when_the_long_statement_finishes():
    w = _World(a_finishes=True)
    code, text = run(w)
    assert code == 2 and "finished by itself" in text


def test_only_a_dev_table_is_accepted():
    with pytest.raises(SystemExit):
        proof.run_proof(_World(), "early-alert-responses.RESPONSES.x", say=lambda line: None)
    assert proof.table_id().startswith("early-alert-responses.DEV.flushfix_timeout_")


def test_control_failed_when_the_first_transaction_ends_before_its_limit():
    w = _World(server_stops=True)
    w.stops_after = 1.0                                   # an error at 9 s is not the 20 s timeout
    code, text = run(w)
    assert code == 2 and "ended before its limit" in text and w.dropped == [TABLE]

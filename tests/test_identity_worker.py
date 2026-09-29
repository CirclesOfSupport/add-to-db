"""
The offline identity check's worker: the live revision (from git) and this branch,
fed the same triage and testimonial calls, must record byte-identical output; a
one-character change in the SQL or a parameter must be caught.
"""
import json
import os
import subprocess
import sys

import pytest

import config
import test_scope_guard as g
from conftest import FEEDBACK_SCHEMA, TRIAGE_SCHEMA

HERE = os.path.dirname(__file__)
REPO = os.path.dirname(HERE)
WORKER = os.path.join(REPO, "tools", "_identity_worker.py")
BASE = "b4ac22f"
FILES = ("main.py", "bq_writer.py", "config.py", "auth.py", "tasks.py")


def _base_src(tmp_path):
    d = tmp_path / "base_src"
    d.mkdir()
    for name in FILES:
        out = subprocess.run(["git", "-C", REPO, "show", f"{BASE}:src/{name}"], capture_output=True, text=True)
        if out.returncode != 0:
            pytest.skip("git history for the live commit is not available")
        (d / name).write_text(out.stdout, encoding="utf-8")
    return d


def _run(src, tmp_path, label):
    calls = [{"table": "triage_data", "data": g.UNRECOGNIZED}, {"table": "triage_data", "data": g.INITIATE},
             {"table": "triage_data", "data": g.DETERMINATION}, {"table": "feedback", "data": g.TESTIMONIAL},
             {"table": "triage_data", "data": {"message_id": "", "determination": "LowConcern"}}]
    schemas = {config.ALLOWED_TARGETS["triage_data"]: [f.to_api_repr() for f in TRIAGE_SCHEMA],
               config.ALLOWED_TARGETS["feedback"]: [f.to_api_repr() for f in FEEDBACK_SCHEMA]}
    (tmp_path / "calls.json").write_text(json.dumps(calls))
    (tmp_path / "schemas.json").write_text(json.dumps(schemas))
    out = tmp_path / f"{label}.jsonl"
    subprocess.run([sys.executable, WORKER, str(src), str(tmp_path / "calls.json"), str(tmp_path / "schemas.json"), str(out)],
                   check=True, capture_output=True)
    return out.read_text().splitlines()


def test_branch_matches_live_and_mutations_are_caught(tmp_path):
    base = _base_src(tmp_path)
    a = _run(base, tmp_path, "base")
    b = _run(os.path.join(REPO, "src"), tmp_path, "branch")
    assert len(a) == 5 and a == b
    sql_count = sum(1 for line in a for w in json.loads(line)["worker"] for e in w["events"] if "sql" in e)
    assert sql_count == 4                                   # the blank message_id call is rejected at /upsert
    bw = base / "bq_writer.py"
    text = bw.read_text()
    bw.write_text(text.replace("WHEN MATCHED THEN", "WHEN MATCHED  THEN", 1))
    assert _run(base, tmp_path, "mut_sql") != b
    bw.write_text(text.replace("return parsed.astimezone(timezone.utc)", "return parsed.astimezone(timezone.utc).replace(microsecond=1)", 1))
    assert _run(base, tmp_path, "mut_param") != b


# ---- check-in path: staged calls, pinned clock and uuid, flushed under the recorder ----------------------

CUTOVER = "29257265d004196f1003b6ad79218d554c1cab1b"


def _checkin_src(tmp_path, rev, name):
    d = tmp_path / name
    d.mkdir()
    for f in FILES:
        out = subprocess.run(["git", "-C", REPO, "show", f"{rev}:src/{f}"], capture_output=True, text=True)
        if out.returncode != 0:
            pytest.skip("git history for the cutover revision is not available")
        (d / f).write_text(out.stdout, encoding="utf-8")
    return d


def _run_checkin(src, tmp_path, label):
    from conftest import RESPONSE_DATA_SCHEMA, USERS_SCHEMA
    from test_responses_sequences import CHECKIN, UUID, body
    calls = [{"tables": [{"table": "users", "data": {"uuid": UUID, "checkInRepliesTotal": str(30 + n)}},
                         {"table": "responses", "data": body(CHECKIN, "Yes" if n % 2 else "No")}]} for n in range(4)]
    calls.append({"table": "responses", "data": body(CHECKIN, "Yes", userWeek="not-a-number")})
    schemas = {config.ALLOWED_TARGETS["responses"]: [f.to_api_repr() for f in RESPONSE_DATA_SCHEMA],
               config.ALLOWED_TARGETS["users"]: [f.to_api_repr() for f in USERS_SCHEMA]}
    (tmp_path / "calls.json").write_text(json.dumps(calls))
    (tmp_path / "schemas.json").write_text(json.dumps(schemas))
    out = tmp_path / f"{label}.jsonl"
    subprocess.run([sys.executable, WORKER, str(src), str(tmp_path / "calls.json"), str(tmp_path / "schemas.json"), str(out)],
                   check=True, capture_output=True)
    return out.read_text().splitlines()


def test_checkin_replay_is_deterministic_flushes_and_catches_a_flush_change(tmp_path):
    base = _checkin_src(tmp_path, CUTOVER, "cutover_src")
    a = _run_checkin(base, tmp_path, "a")
    assert a == _run_checkin(base, tmp_path, "a2")                      # same code twice: identical (pinned)
    b = _run_checkin(os.path.join(REPO, "src"), tmp_path, "branch")
    assert a == b                                                       # this branch vs the cutover revision
    lines = [json.loads(x) for x in a]
    calls = [x for x in lines if "flush" not in x]
    flushes = [x for x in lines if "flush" in x]
    staged_ids = {e["rows"][0]["request_id"] for c in calls for e in c["ingress_events"] if "insert_rows_json" in e}
    assert staged_ids and all(len(i) == 32 for i in staged_ids)          # the pinned uuid4, not a random one
    assert {f["flush"] for f in flushes} == {"responses", "users"}
    assert sum(len(f["statements"]) for f in flushes) >= 2               # the MERGEs into response_data and users
    assert any("MERGE" in s["sql"] for f in flushes for s in f["statements"])
    # a one-character change in the flush's MERGE builder must be caught
    bw = base / "bq_writer.py"
    text = bw.read_text()
    marker = "def build_batch_merge_query"
    assert marker in text
    head, tail = text.split(marker, 1)
    bw.write_text(head + marker + tail.replace("WHEN MATCHED", "WHEN  MATCHED", 1))
    assert _run_checkin(base, tmp_path, "mut") != b

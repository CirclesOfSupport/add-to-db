"""Gap repair: sources, close-outs, add-to-db's preparation, runs, the plan, the transaction text, the guards."""
import json
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import repair_checkin_gap as G  # noqa: E402
from conftest import RESPONSE_DATA_SCHEMA  # noqa: E402

UTC = timezone.utc
U1, U2 = "11111111-0000-0000-0000-000000000001", "22222222-0000-0000-0000-000000000002"


def body(sid, checkin, reply="No", reply_t="", **extra):
    resp = {"sessionID": sid.replace(":", "%3A"), "uuid": sid[:36], "checkinDateTime": checkin.replace(":", "%3A"),
            "checkinReply": reply, "checkinReplyDateTime": reply_t.replace(":", "%3A"), "userWeek": "3",
            "wellnessDomain": "sleep", "orgCode": "demo"}
    resp.update(extra)
    return json.dumps({"Users": {"uuid": sid[:36]}, "Responses": resp})


def at(s):
    return datetime.fromisoformat(s).astimezone(UTC)


# --- close-outs --------------------------------------------------------------------------------

def test_closeout_is_the_call_fired_as_the_next_checkin_starts():
    old = f"{U1}2026-09-20T13:00:00.000001-04:00"
    new = f"{U1}2026-09-27T13:00:00.000002-04:00"
    calls = [G.parse_call(1, at("2026-09-27T17:00:00.9+00:00"), body(old, "2026-09-20T13:00:00.000001-04:00")),
             G.parse_call(2, at("2026-09-27T17:00:01.2+00:00"), body(new, "2026-09-27T13:00:00.000002-04:00")),
             G.parse_call(3, at("2026-09-27T18:00:00+00:00"), body(old, "2026-09-20T13:00:00.000001-04:00"))]
    G.mark_closeouts(calls)
    assert [c["closeout"] for c in calls] == [True, False, False]   # 3 s from nothing -> a real late call
    assert calls[0]["sid"] == old                                    # url-encoded SessionID decoded


# --- preparation is add-to-db's ----------------------------------------------------------------

def test_prepare_row_matches_plan_target_writes(svc, monkeypatch):
    sid = f"{U1}2026-09-27T13:00:00.5-04:00"
    resp = json.loads(body(sid, "2026-09-27T13:00:00.5-04:00", reply="Yes", reply_t="2026-09-27T13:05:00-04:00",
                           checkinReplyNumerical="7", subscribed=""))["Responses"]
    captured = {}
    real = svc.fold_rows
    monkeypatch.setattr(svc, "fold_rows", lambda rows, *a, **k: captured.setdefault("rows", rows) and real(rows, *a, **k))
    svc.plan_target_writes("responses", [{"data": dict(resp), "ref": 0}])
    row, errors, unknown, guarded = G.prepare_row(svc, RESPONSE_DATA_SCHEMA, resp)
    assert errors == [] and unknown == [] and not guarded
    assert row == captured["rows"][0]
    assert row["checkinDateTime"] == datetime(2026, 9, 27, 17, 0, 0, 500000)       # UTC, naive
    assert row["subscribed"] is None and row["checkinReplyNumerical"] == 7.0


def test_prepare_row_stale_reply_guard_and_unknown_keys(svc):
    sid = f"{U1}2026-09-27T13:00:00-04:00"
    resp = json.loads(body(sid, "2026-09-27T13:00:00-04:00", reply="Yes", reply_t="2026-09-20T13:05:00-04:00",
                           notAColumn="x"))["Responses"]
    row, errors, unknown, guarded = G.prepare_row(svc, RESPONSE_DATA_SCHEMA, resp)
    assert guarded and row["checkinReply"] is None and row["checkinReplyDateTime"] is None
    assert unknown == ["notAColumn"] and "notAColumn" not in row and errors == []


# --- runs --------------------------------------------------------------------------------------

def run(contact, created, value=None, t=None, cat="7-10", flow="F"):
    vals = {"checkinresponse": {"value": value, "category": cat, "time": t}} if t else {"other": {"value": "x"}}
    return {"flow": flow, "uuid": "r", "contact": contact, "created_on": created, "values": vals}


def agreeing(n=25):
    pairs = []
    for i in range(n):
        t = f"2026-09-27T17:{i:02d}:00Z"
        row = {"checkinReply": "Yes", "checkinReplyText": "8", "checkinReplyNumerical": 8.0,
               "checkinReplyDistressed": "No", "checkinReplyDateTime": datetime(2026, 9, 27, 17, i)}
        pairs.append((row, run(U1, "2026-09-27T17:00:00Z", "8", t)))
    return pairs


def test_mapping_is_learned_only_from_unanimous_agreement():
    m = G.learn_mapping(agreeing())
    assert m == {"agreeing": 25, "text": True, "numerical": True, "reply_value": "Yes", "distressed": {"7-10": "No"}}
    pairs = agreeing()
    pairs[0][0]["checkinReplyText"] = "eight"
    assert G.learn_mapping(pairs)["text"] is False
    assert G.learn_mapping(agreeing(5))["text"] is False                 # too few to trust


def test_run_verdicts():
    m = G.learn_mapping(agreeing())
    base = {"checkinReply": "No", "checkinReplyText": None, "checkinReplyNumerical": None,
            "checkinReplyDistressed": None, "checkinReplyDateTime": None}
    row, v, notes = G.apply_run(dict(base), run(U1, "x", "9", "2026-09-27T18:00:00Z"), m, {"F"})
    assert v == "run: reply the call lacks" and notes == []
    assert (row["checkinReply"], row["checkinReplyNumerical"], row["checkinReplyText"],
            row["checkinReplyDistressed"], row["checkinReplyDateTime"]) == ("Yes", 9.0, "9", "No", datetime(2026, 9, 27, 18))
    replied = dict(base, checkinReply="Yes", checkinReplyText="3", checkinReplyNumerical=3.0,
                   checkinReplyDateTime=datetime(2026, 9, 18, 17))
    row, v, _ = G.apply_run(dict(replied), run(U1, "x"), m, {"F"})
    assert v.startswith("run: no reply") and all(row[f] is None for f in G.REPLY_FIELDS)
    assert G.apply_run(dict(replied), run(U1, "x", flow="OLD"), m, {"F"})[1] == "run flow carries no check-in reply result"
    assert G.apply_run(dict(base), None, m, {"F"})[1] == "no run"
    same_t = dict(replied, checkinReplyDateTime=datetime(2026, 9, 27, 18, 0, 1))
    assert G.apply_run(same_t, run(U1, "x", "3", "2026-09-27T18:00:00Z"), m, {"F"})[1] == "agree"


def test_match_run_nearest_within_five_seconds():
    by = G.index_runs([run(U1, "2026-09-27T17:00:04Z"), run(U1, "2026-09-27T17:00:01Z"), run(U1, "2026-09-27T17:00:09Z")])
    got = G.match_run(by, U1, at("2026-09-27T17:00:00+00:00"))
    assert got["created_on"] == "2026-09-27T17:00:01Z"
    assert G.match_run(by, U1, at("2026-09-27T16:59:50+00:00")) is None


class _Resp:
    def __init__(self, code, data=None, text=""):
        self.status_code, self._data, self.text = code, data, text

    def json(self):
        return self._data


def test_fetch_runs_pages_and_waits_as_told_on_429():
    pages = iter([_Resp(429, text='{"detail":"Request was throttled. Expected available in 7 seconds."}'),
                  _Resp(200, {"results": [{"uuid": "r1", "contact": {"uuid": U1}, "created_on": "t"}], "next": "page2"}),
                  _Resp(200, {"results": [{"uuid": "r2", "contact": {"uuid": U2}, "created_on": "t"}], "next": None})])
    slept = []
    out = G.fetch_runs(["F"], at("2026-09-14T00:00:00+00:00"), "tok", sleep=slept.append, get=lambda *a, **k: next(pages))
    assert [r["contact"] for r in out] == [U1, U2] and slept == [10, 1.5]
    with pytest.raises(SystemExit):
        G.fetch_runs(["F"], at("2026-09-14T00:00:00+00:00"), "tok", sleep=slept.append, get=lambda *a, **k: _Resp(500))


# --- the plan end to end on a stub BigQuery ----------------------------------------------------

class _Job:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class _Table:
    schema = RESPONSE_DATA_SCHEMA


class _BQ:
    def __init__(self, calls, staged, stored):
        self.calls, self.staged, self.stored = calls, staged, stored

    def get_table(self, table):
        return _Table()

    def query(self, sql, job_config=None):
        if "webhook_log_detail" in sql:
            return _Job(self.calls)
        if "adb_staging" in sql:
            return _Job([{"sid": s} for s in self.staged])
        if "FARM_FINGERPRINT" in sql:
            wanted = set(job_config.query_parameters[0].values)
            return _Job([dict(r) for r in self.stored if r["SessionID"] in wanted])
        raise AssertionError(sql)


def test_plan_scope_actions_and_groups(svc, monkeypatch, tmp_path):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    monkeypatch.chdir(tmp_path)
    A = f"{U1}2026-09-27T13:00:00.5-04:00"       # in window, no row -> insert
    B = f"{U2}2026-09-27T14:00:00.5-04:00"       # in window, two rows, one wrong -> update + collapse
    C = f"{U1}2026-09-20T13:00:00.5-04:00"       # earlier check-in, only a close-out in the window -> excluded
    D = f"{U2}2026-09-19T14:00:00.5-04:00"       # earlier check-in, a late reply in the window -> repaired
    E = f"33333333-0000-0000-0000-0000000000032026-09-27T15:00:00-04:00"   # add-to-db owns it -> skipped
    t = lambda s: at(s)  # noqa: E731
    calls = [
        {"httplog_id": 1, "fired_at": t("2026-09-27T17:00:00.9+00:00"), "request_body": body(C, "2026-09-20T13:00:00.5-04:00")},
        {"httplog_id": 2, "fired_at": t("2026-09-27T17:00:01+00:00"), "request_body": body(A, "2026-09-27T13:00:00.5-04:00")},
        {"httplog_id": 3, "fired_at": t("2026-09-27T18:00:01+00:00"), "request_body": body(B, "2026-09-27T14:00:00.5-04:00",
                                                                                           "Yes", "2026-09-27T14:30:00-04:00")},
        {"httplog_id": 4, "fired_at": t("2026-09-27T19:00:00+00:00"), "request_body": body(D, "2026-09-19T14:00:00.5-04:00",
                                                                                           "Yes", "2026-09-27T15:00:00-04:00")},
        {"httplog_id": 5, "fired_at": t("2026-09-27T20:00:00+00:00"), "request_body": body(E, "2026-09-27T15:00:00-04:00")},
        {"httplog_id": 6, "fired_at": t("2026-09-27T20:00:01+00:00"), "request_body": json.dumps({"Users": {}, "Responses": {"sessionID": ""}})},
    ]
    stored_b = {"SessionID": B, "checkinDateTime": datetime(2026, 9, 27, 18, 0, 0, 500000), "uuid": U2, "userWeek": 3,
                "wellnessDomain": "sleep", "orgCode": "demo", "checkinReply": "No", "checkinReplyDateTime": None, "gap_fp": 5}
    stored = [stored_b, dict(stored_b, gap_fp=-2),
              dict(stored_b, SessionID=D, checkinDateTime=datetime(2026, 9, 19, 18, 0, 0, 500000), gap_fp=9),
              dict(stored_b, SessionID=C, uuid=U1, checkinDateTime=datetime(2026, 9, 20, 17, 0, 0, 500000), gap_fp=1)]
    plan = G.build_plan(_BQ(calls, [E], stored), "p.DEV.x", [], ["F"])
    ctx = plan.pop("_ctx")
    assert set(ctx["sessions"]) == {A, B, D} and set(ctx["excluded"]) == {C} and ctx["staged"] == [E]
    assert len(ctx["keyless"]) == 1
    assert ctx["sessions"][D]["group"] == "late call" and ctx["sessions"][A]["group"] == "in window"
    by = {s["sid"]: s for s in plan["sessions"]}
    assert by[A]["insert"] and not by[A]["update"]
    assert by[B]["update"] and by[B]["collapse"] and by[B]["fps"] == "-2,5" and by[B]["n"] == 2
    assert by[B]["changed"] == ["checkinReply", "checkinReplyDateTime"]
    assert by[D]["update"] and not by[D]["collapse"]
    assert plan["totals"] == {"insert": 1, "update": 2, "collapse": 1, "extra_rows": 1}
    assert C not in by                                             # a close-out never becomes a repair


# --- the transaction ---------------------------------------------------------------------------

PLAN = {"table": "p.DEV.t", "columns": {"SessionID": "STRING", "checkinDateTime": "DATETIME", "checkinReply": "STRING"},
        "totals": {"insert": 1, "update": 1, "collapse": 1, "extra_rows": 2},
        "sessions": [
            {"sid": "a", "insert": True, "update": False, "collapse": False, "n": 0, "fps": "",
             "row": {"SessionID": "a", "checkinDateTime": "2026-09-27 17:00:00", "checkinReply": "No"}},
            {"sid": "b", "insert": False, "update": True, "collapse": True, "n": 3, "fps": "1,2,3",
             "row": {"SessionID": "b", "checkinDateTime": "2026-09-27 18:00:00", "checkinReply": "Yes"}}]}


def test_apply_script_shape():
    sql, params = G.build_apply_script("p.DEV.t", PLAN, "p.DEV.rows")
    assert sql.startswith("DECLARE gap_before INT64;\nBEGIN TRANSACTION;") and sql.rstrip().endswith("COMMIT TRANSACTION;")
    assert sql.count("USING (SELECT") == 1 and "USING (SELECT * FROM `p.DEV.rows` WHERE `checkinDateTime` IS NOT NULL) S" in sql
    assert "ASSERT @@row_count = 4 AS 'MERGE (ranged)" in sql                      # 1 insert + 3 rows of b
    assert "ASSERT @@row_count = 3 AS 'collapse: deleted rows != 3'" in sql
    assert "ASSERT @@row_count = 1 AS 'collapse: re-inserted rows != 1'" in sql
    assert "gap_before + -1" in sql                                                 # 1 insert - 2 extra rows
    assert sql.index("no longer has exactly") < sql.index("USING (SELECT") and "adb_staging" not in sql
    names = {p.name for p in params}
    assert {"gap_sids", "gap_pre", "gap_dups", "gap_min_ranged", "gap_max_ranged"} <= names


def test_apply_script_refuses_a_changed_merge(monkeypatch):
    monkeypatch.setattr(G, "build_batch_merge_query", lambda *a, **k: "MERGE x USING UNNEST(@other) S")
    with pytest.raises(SystemExit):
        G.build_apply_script("p.DEV.t", PLAN, "p.DEV.rows")


def test_rollback_script_checks_before_and_after():
    applied = {"backup": "p.DEV.bk", "sessions": [{"sid": "a", "before": [0, ""], "after": [1, "7"]},
                                                  {"sid": "b", "before": [3, "1,2,3"], "after": [1, "9"]}]}
    sql, params = G.build_rollback_script("p.DEV.t", applied)
    assert sql.index("changed since the apply") < sql.index("DELETE") < sql.index("INSERT")
    assert "ASSERT @@row_count = 2 AS 'rollback: deleted" in sql and "ASSERT @@row_count = 3 AS 'rollback: restored" in sql


# --- guards ------------------------------------------------------------------------------------

def test_production_needs_the_flag_and_avoids_the_nightly_window():
    with pytest.raises(SystemExit, match="not a DEV table"):
        G.refuse_unless_allowed(G.TABLE, production=False)
    with pytest.raises(SystemExit, match="07:00-09:30 UTC"):
        G.refuse_unless_allowed(G.TABLE, production=True, now=datetime(2026, 9, 29, 8, 15, tzinfo=UTC))
    G.refuse_unless_allowed(G.TABLE, production=True, now=datetime(2026, 9, 29, 15, 0, tzinfo=UTC))
    G.refuse_unless_allowed("early-alert-responses.DEV.x", production=False, now=datetime(2026, 9, 29, 8, 15, tzinfo=UTC))


def test_cli_refuses_production_plan_before_signing_in(tmp_path, monkeypatch):
    p = tmp_path / "plan.json"
    p.write_text(json.dumps({"table": G.TABLE}))
    monkeypatch.setattr(G, "make_client", lambda: (_ for _ in ()).throw(AssertionError("signed in")))
    with pytest.raises(SystemExit, match="not a DEV table"):
        G.main_(["apply", str(p)])
    with pytest.raises(SystemExit, match="--flows is required"):
        G.main_(["list"])


def test_same_treats_blank_as_null_and_numbers_by_value():
    assert G.same("", None) and G.same(None, "  ") and G.same(8, 8.0) and not G.same("No", None)

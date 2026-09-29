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
    def __init__(self, calls, staged, stored, pre=()):
        self.calls, self.staged, self.stored, self.pre = calls, staged, stored, list(pre)

    def get_table(self, table):
        return _Table()

    def query(self, sql, job_config=None):
        if "webhook_log_detail" in sql:
            return _Job(self.pre if "@sids" in sql else self.calls)
        if "adb_staging" in sql:
            return _Job([{"sid": s} for s in self.staged])
        if "FARM_FINGERPRINT" in sql:
            wanted = set(job_config.query_parameters[0].values)
            return _Job([dict(r) for r in self.stored if r["SessionID"] in wanted])
        raise AssertionError(sql)


def _call(i, fired, sid, checkin, **kw):
    return {"httplog_id": i, "fired_at": at(fired), "request_body": body(sid, checkin, **kw)}


def test_plan_scope_actions_and_groups(svc, monkeypatch, tmp_path):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    monkeypatch.chdir(tmp_path)
    A = f"{U1}2026-09-27T13:00:00.5-04:00"       # in window, no row -> insert
    B = f"{U2}2026-09-27T14:00:00.5-04:00"       # in window, two rows, run confirms the reply -> update + collapse
    C = f"{U1}2026-09-20T13:00:00.5-04:00"       # earlier check-in, only a close-out in the window -> excluded
    D = f"{U2}2026-09-19T14:00:00.5-04:00"       # earlier check-in, late reply, no run -> reply not confirmed, kept
    E = "33333333-0000-0000-0000-0000000000032026-09-27T15:00:00-04:00"    # add-to-db owns it -> skipped
    calls = [
        _call(1, "2026-09-27T17:00:00.9+00:00", C, "2026-09-20T13:00:00.5-04:00"),
        _call(2, "2026-09-27T17:00:01+00:00", A, "2026-09-27T13:00:00.5-04:00"),
        _call(3, "2026-09-27T18:00:01+00:00", B, "2026-09-27T14:00:00.5-04:00", reply="Yes",
              reply_t="2026-09-27T14:30:00-04:00"),
        _call(4, "2026-09-27T19:00:00+00:00", D, "2026-09-19T14:00:00.5-04:00", reply="Yes",
              reply_t="2026-09-27T15:00:00-04:00"),
        _call(5, "2026-09-27T20:00:00+00:00", E, "2026-09-27T15:00:00-04:00"),
        {"httplog_id": 6, "fired_at": at("2026-09-27T20:00:01+00:00"),
         "request_body": json.dumps({"Users": {}, "Responses": {"sessionID": ""}})},
    ]
    pre = [_call(0, "2026-09-19T18:00:01+00:00", D, "2026-09-19T14:00:00.5-04:00")]
    stored_b = {"SessionID": B, "checkinDateTime": datetime(2026, 9, 27, 18, 0, 0, 500000), "uuid": U2, "userWeek": 3,
                "wellnessDomain": "sleep", "orgCode": "demo", "checkinReply": "No", "checkinReplyDateTime": None, "gap_fp": 5}
    stored = [stored_b, dict(stored_b, gap_fp=-2),
              dict(stored_b, SessionID=D, checkinDateTime=datetime(2026, 9, 19, 18, 0, 0, 500000), gap_fp=9),
              dict(stored_b, SessionID=C, uuid=U1, checkinDateTime=datetime(2026, 9, 20, 17, 0, 0, 500000), gap_fp=1)]
    runs = [run(U2, "2026-09-27T18:00:01Z", "8", "2026-09-27T18:30:00Z")]
    plan = G.build_plan(_BQ(calls, [E], stored, pre), "p.DEV.x", runs, ["F"])
    ctx = plan.pop("_ctx")
    assert set(ctx["sessions"]) == {A, B, D} and set(ctx["excluded"]) == {C} and ctx["staged"] == [E]
    assert len(ctx["keyless"]) == 1 and ctx["first_missing"] == 0
    assert ctx["sessions"][D]["group"] == "late call" and ctx["sessions"][A]["group"] == "in window"
    assert ctx["sessions"][B]["verdict"] == "agree" and ctx["sessions"][D]["verdict"] == "no run"
    by = {s["sid"]: s for s in plan["sessions"]}
    assert by[A]["insert"] and not by[A]["update"] and "orgCode" in by[A]["write"]
    assert by[B]["update"] and by[B]["collapse"] and by[B]["fps"] == "-2,5" and by[B]["n"] == 2
    assert by[B]["changed"] == ["checkinReply", "checkinReplyDateTime"]
    assert "orgCode" not in by[B]["write"] and "userWeek" not in by[B]["write"]      # other columns kept
    assert D not in by                          # its reply is not run-confirmed and its identity matches
    assert plan["totals"] == {"insert": 1, "update": 1, "collapse": 1, "extra_rows": 1}
    assert C not in by                                             # a close-out never becomes a repair


def test_identity_from_the_first_call_and_other_columns_kept(svc, monkeypatch):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    S = f"{U1}2026-09-27T13:00:00.5-04:00"
    calls = [_call(1, "2026-09-27T17:00:01+00:00", S, "2026-09-27T13:00:00.5-04:00", wellnessDomain="Physical",
                   testimonial="kind words"),
             _call(2, "2026-09-27T23:00:00+00:00", S, "2026-09-27T13:00:00.5-04:00", wellnessDomain="Relational",
                   testimonial="", orgCode="other")]
    stored = [{"SessionID": S, "checkinDateTime": datetime(2026, 9, 27, 17, 0, 0, 500000), "uuid": U1,
               "wellnessDomain": "Relational", "orgCode": "demo", "checkinReply": "No", "gap_fp": 3}]
    plan = G.build_plan(_BQ(calls, [], stored), "p.DEV.x", [], ["F"])
    plan.pop("_ctx")
    (only,) = plan["sessions"]
    assert only["changed"] == ["wellnessDomain"] and only["row"]["wellnessDomain"] == "Physical"
    assert only["write"] == ["SessionID", "checkinDateTime", "wellnessDomain"]   # contactType blank in the first call
    assert "checkinReply" not in only["write"]                    # no run: reply fields untouched


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


# --- review 2026-09-28: interrupted runs are not replies ----------------------------------------
# Four patterns checked against the TextIt API by the reviewer: three runs interrupted by the
# contact's next check-in (value '', category "No Response", time = the interruption) and one real
# reply (a scored reply with text, category "All Responses"). Identifiers here are synthetic.

def result(value, category, t):
    return {"flow": "F", "uuid": "r", "contact": U1, "created_on": "2026-09-25T16:01:09Z",
            "values": {"checkinresponse": {"value": value, "category": category, "time": t}}}


INTERRUPTED = [result("", "No Response", "2026-09-26T14:05:30.616577Z"),
               result("", "No Response", "2026-09-26T18:02:03.703737Z"),
               result("", "No Response", "2026-09-26T18:47:01.179880Z")]
REAL = result("5 = neutral 😐 thanks for asking", "All Responses", "2026-09-26T16:37:23.964784Z")
CALL_NO_REPLY = {"checkinReply": "No", "checkinReplyText": None, "checkinReplyNumerical": None,
                 "checkinReplyDistressed": None, "checkinReplyDateTime": None}


def test_interrupted_run_is_not_a_reply():
    m = G.learn_mapping(agreeing())
    for r in INTERRUPTED:
        assert G.run_reply(r)[0] is False and G.run_reply(r)[3] is None
        row, verdict, _ = G.apply_run(dict(CALL_NO_REPLY), r, m, {"F"})
        assert verdict == "agree" and row == CALL_NO_REPLY          # the stored "No" stays "No"
    assert G.run_reply(result("7", "no response", "2026-09-26T14:05:30Z"))[0] is False   # category, any case
    assert G.run_reply(result("  ", "All Responses", "2026-09-26T14:05:30Z"))[0] is False  # empty value


def test_real_reply_run_fills_the_reply():
    row, verdict, _ = G.apply_run(dict(CALL_NO_REPLY), REAL, G.learn_mapping(agreeing()), {"F"})
    assert verdict == "run: reply the call lacks"
    assert row["checkinReply"] == "Yes" and row["checkinReplyText"] == "5 = neutral 😐 thanks for asking"
    assert row["checkinReplyDateTime"] == datetime(2026, 9, 26, 16, 37, 23, 964784)


def test_list_stops_when_runs_outweigh_calls(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs.json"
    runs.write_text("[]")
    fake = {"totals": {}, "sessions": [], "_ctx": {"sessions": {"x": {"verdict": "run: reply the call lacks"}},
                                                   "excluded": {}, "call_changed": 40, "run_changed": 51}}
    monkeypatch.setattr(G, "build_plan", lambda *a, **k: dict(fake))
    monkeypatch.setattr(G, "preflight", lambda *a, **k: None)
    with pytest.raises(SystemExit, match="runs rule is suspect"):
        G.cmd_list(_BQ([], [], []), ["F"], table="p.DEV.x", runs_file=str(runs))
    assert not list(tmp_path.glob("gap_repair_plan_*.json"))


# --- review 2026-09-28: generated SQL, dry-run forms --------------------------------------------

def test_stored_read_with_only_the_key_is_valid_sql():
    sql = G.read_stored_sql("p.DEV.t", ["SessionID"])
    assert sql == "SELECT SessionID, FARM_FINGERPRINT(TO_JSON_STRING(t)) AS gap_fp FROM `p.DEV.t` t WHERE SessionID IN UNNEST(@s)"
    assert ", ," not in G.read_stored_sql("p.DEV.t", ["SessionID", "uuid"])


def test_dry_run_forms_of_the_apply_script():
    sql, _ = G.build_apply_script("p.DEV.t", PLAN, "p.DEV.rows")
    forms = G.dry_run_forms(sql)
    joined = "\n".join(forms)
    for gone in ("BEGIN", "COMMIT", "DECLARE", "SET gap_before", "@@row_count", "gap_keep", "ASSERT", "CREATE TEMP"):
        assert gone not in joined
    assert any(f.startswith("MERGE") for f in forms) and any(f.startswith("DELETE") for f in forms)
    assert any(f.startswith("INSERT INTO `p.DEV.t` SELECT * FROM (SELECT * EXCEPT(gap_rn)") for f in forms)
    assert any(f.endswith("= 0 + -1") for f in forms)                            # row-count check, variable as 0
    rb, _ = G.build_rollback_script("p.DEV.t", {"backup": "p.DEV.bk", "sessions": [
        {"sid": "a", "before": [0, ""], "after": [1, "7"]}]})
    assert [f.split()[0] for f in G.dry_run_forms(rb)] == ["SELECT", "DELETE", "INSERT", "SELECT"]


class _DryBQ:
    def __init__(self, reject=""):
        self.seen, self.reject = [], reject

    def query(self, sql, job_config=None):
        assert job_config.dry_run is True
        self.seen.append((sql, sorted(p.name for p in job_config.query_parameters)))
        if self.reject and self.reject in sql:
            raise Exception("400 Syntax error: Expected end of input but got \",\" at [1:19]")
        return _Job([])


def test_preflight_dry_runs_each_statement_with_only_its_parameters():
    sql, params = G.build_apply_script("p.DEV.t", PLAN, "p.DEV.rows")
    bq = _DryBQ()
    G.preflight(bq, [(x, params) for x in G.dry_run_forms(sql)])
    merge = next(p for s, p in bq.seen if s.startswith("MERGE"))
    assert merge == ["gap_max_ranged", "gap_min_ranged"]
    with pytest.raises(SystemExit, match="Syntax error"):
        G.preflight(_DryBQ(reject="DELETE"), [(x, params) for x in G.dry_run_forms(sql)])


def test_rollback_without_an_after_state_checks_one_row_each():
    sql, params = G.build_rollback_script("p.DEV.t", {"backup": "p.DEV.bk", "sessions": [
        {"sid": "a", "before": [0, ""], "after": None}]})
    after = next(p for p in params if p.name == "gap_state_after")
    assert after.values[0].struct_values == {"sid": "a", "n": 1, "fps": None}
    assert "p.fps IS NOT NULL AND" in sql


# --- review 2026-09-29: parameter names, list-time dry run, per-row presence ----------------------

VALID_PARAM = r"^[A-Za-z_][A-Za-z0-9_]*$"


def test_no_statement_references_an_invalid_parameter():
    import re
    stmts = G.statement_set("p.DEV.t", G.synthetic_plan(RESPONSE_DATA_SCHEMA))
    shapes = [sql.split()[0] for sql, _ in stmts]
    assert shapes.count("MERGE") == 2 and "DELETE" in shapes and "INSERT" in shapes and "CREATE" in shapes
    for sql, params in stmts:
        for name in re.findall(r"@(\w+)", sql):
            assert re.match(VALID_PARAM, name), (name, sql[:80])
            assert name in {p.name for p in params}, (name, sql[:80])
    assert all("@0" not in sql for sql, _ in stmts)


def test_rollback_dry_run_keeps_its_parameters():
    rb, _ = G.build_rollback_script("p.DEV.t", {"backup": "p.DEV.bk", "sessions": [
        {"sid": "a", "before": [0, ""], "after": [1, "7"]}]})
    forms = G.dry_run_forms(rb)
    assert "@gap_state_after" in forms[0] and "@gap_state_before" in forms[-1]


def test_list_dry_runs_before_reading_anything(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = tmp_path / "runs.json"
    runs.write_text("[]")
    bq = _DryBQ(reject="MERGE")
    bq.get_table = lambda table: _Table()
    with pytest.raises(SystemExit, match="dry run"):
        G.cmd_list(bq, ["F"], table="p.DEV.x", runs_file=str(runs))
    assert all(s for s, _ in bq.seen)                               # only dry runs were sent


def test_loaded_rows_mark_only_the_columns_each_checkin_writes():
    got = {}

    class _L:
        def load_table_from_json(self, data, table, job_config=None):
            got["data"] = data
            return _Job([])
    plan = {"columns": {"SessionID": "STRING", "checkinDateTime": "DATETIME", "wellnessDomain": "STRING", "orgCode": "STRING"},
            "sessions": [
                {"insert": True, "update": False, "write": ["SessionID", "checkinDateTime", "wellnessDomain", "orgCode"],
                 "row": {"SessionID": "a", "checkinDateTime": "2026-09-27 17:00:00", "wellnessDomain": "Physical", "orgCode": "x"}},
                {"insert": False, "update": True, "write": ["SessionID", "wellnessDomain"],
                 "row": {"SessionID": "b", "wellnessDomain": "Physical"}}]}
    assert G.load_rows(_L(), plan, "p.DEV.rows") == 2
    a, b = got["data"]
    assert a[G.PRESENT_FIELD] == "|SessionID|checkinDateTime|wellnessDomain|orgCode|"
    assert b[G.PRESENT_FIELD] == "|SessionID|wellnessDomain|" and b["orgCode"] is None

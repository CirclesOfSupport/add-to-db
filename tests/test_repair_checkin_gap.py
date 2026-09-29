"""Gap repair: bodies and their repair, add-to-db's preparation and fold over the complete call sequence,
the plan, the transaction text, the guards."""
import json
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import repair_checkin_gap as G  # noqa: E402
from conftest import RESPONSE_DATA_SCHEMA  # noqa: E402

UTC = timezone.utc
U1, U2, U3 = ("11111111-0000-0000-0000-000000000001", "22222222-0000-0000-0000-000000000002",
              "33333333-0000-0000-0000-000000000003")


def enc(v):
    return v.replace(":", "%3A").replace("+", "%2B")


def old_body(sid, checkin, reply="No", reply_t="", **extra):
    """The old writer's body: every value url-encoded, pretty-printed."""
    resp = {"sessionID": enc(sid), "uuid": sid[:36], "checkinDateTime": enc(checkin), "checkinReply": reply,
            "checkinReplyDateTime": enc(reply_t), "userWeek": "3", "wellnessDomain": "Physical", "orgCode": "demo"}
    resp.update({k: enc(v) for k, v in extra.items()})
    return json.dumps({"Users": {"uuid": sid[:36]}, "Responses": resp}, indent=2)


def new_data(sid, checkin, reply="No", reply_t="", **extra):
    d = {"uuid": sid[:36], "orgCode": "demo", "userWeek": 3, "sessionID": sid, "contactType": "CheckIn",
         "checkinDateTime": checkin, "wellnessDomain": "Physical", "checkinReplyDateTime": reply_t,
         "checkinReply": reply, "checkinReplyNumerical": None}
    d.update(extra)
    return d


def new_body(sid, checkin, **kw):
    """add-to-db's body, as the live webhook flow sends it."""
    return json.dumps({"tables": [{"table": "users", "data": {"uuid": sid[:36], "orgCode": "demo"}},
                                  {"table": "responses", "data": new_data(sid, checkin, **kw)}]}, indent=2)


def broken(body_text, key, raw):
    """The body the pre-fix template produced: `raw` pasted between the quotes of `key` unescaped."""
    marker = "@@RAW@@"
    obj = json.loads(body_text)
    obj["tables"][1]["data"][key] = marker
    return json.dumps(obj, indent=2).replace(marker, raw)


def at(s):
    return datetime.fromisoformat(s).astimezone(UTC)


def req(i, fired, text, path=G.OLD_PATHS[0], status="HTTP/2.0 200 OK"):
    return {"httplog_id": i, "fired_at": at(fired), "request_path": path, "response_status_line": status,
            "request_body": text}


# --- bodies and their repair --------------------------------------------------------------------

S9 = f"{U1}2026-09-29T12:54:00.675676-04:00"


def test_quote_newline_and_backslash_bodies_repair_and_nothing_else_changes():
    base = new_body(S9, "2026-09-29T12:54:00.676636-04:00")
    for raw in ('Early Alert helps me "bounce my thoughts " off it.', "cares how I am. \nThank you.", "5\\15\\96"):
        text = broken(base, "testimonial", raw)
        with pytest.raises(ValueError):
            json.loads(text)
        rep = G.repair_body(text)
        assert rep["escaped"] == [("testimonial", raw)]
        assert rep["obj"]["tables"][1]["data"]["testimonial"] == raw          # the value as typed
        proof = G.prove_repair(text, rep)
        assert proof["ok"] and proof["outside_identical"] and proof["identical"] == proof["raw_strings"] > 10
        clean = json.loads(base)
        clean["tables"][1]["data"]["testimonial"] = raw
        assert rep["obj"] == clean                                              # every other value untouched


def test_a_body_that_cannot_be_repaired_is_reported():
    assert G.repair_body('{"tables": [ {"table": "responses", "data": {"a": 1,, }}]}') is None
    assert G.repair_body("") is None


def test_body_items_old_new_and_other_tables(svc):
    assert G.body_items(svc, old_body(S9, "2026-09-29T12:54:00-04:00"))[0][0]["sessionID"] == enc(S9)
    items, rep, err = G.body_items(svc, new_body(S9, "2026-09-29T12:54:00-04:00"))
    assert [d["sessionID"] for d in items] == [S9] and rep is None and err is None
    assert G.body_items(svc, json.dumps({"table": "triage_data", "data": {"message_id": "m"}}))[0] == []
    items, rep, err = G.body_items(svc, broken(new_body(S9, "2026-09-29T12:54:00-04:00"), "testimonial", 'a "b" c'))
    assert len(items) == 1 and rep is not None and err is None
    assert G.body_items(svc, "{nope")[2] == "not JSON and not repairable"


# --- preparation and fold are add-to-db's -------------------------------------------------------

def test_prepare_matches_plan_target_writes(svc, monkeypatch):
    resp = json.loads(old_body(S9, "2026-09-27T13:00:00.5-04:00", reply="Yes", reply_t="2026-09-27T13:05:00-04:00",
                               checkinReplyNumerical="7", subscribed=""))["Responses"]
    captured = {}
    real = svc.fold_rows
    monkeypatch.setattr(svc, "fold_rows", lambda rows, *a, **k: captured.setdefault("rows", rows) and real(rows, *a, **k))
    svc.plan_target_writes("responses", [{"data": dict(resp), "ref": 0}])
    row, errors, unknown, keyless, guarded = G.prepare(svc, RESPONSE_DATA_SCHEMA, resp)
    assert errors == [] and unknown == [] and not keyless and not guarded
    assert row == captured["rows"][0]
    assert row["checkinDateTime"] == datetime(2026, 9, 27, 17, 0, 0, 500000)       # UTC, naive
    assert row["subscribed"] is None and row["checkinReplyNumerical"] == 7.0


def test_fold_of_a_mixed_sequence_is_the_writers_fold(svc, monkeypatch):
    """Old-writer and add-to-db calls of one check-in, fed to plan_target_writes in fired order, fold to the
    row the tool plans: last call wins, blank -> NULL, a blank check-in time kept, the stale reply cleared."""
    S = f"{U1}2026-09-27T13:00:00.5-04:00"
    ci = "2026-09-27T13:00:00.5-04:00"
    reqs = [req(1, "2026-09-27T17:00:01+00:00", old_body(S, ci, reply="Yes", reply_t="2026-09-20T09:00:00-04:00")),
            req(2, "2026-09-27T23:00:00+00:00", old_body(S, ci, wellnessDomain="Relational")),
            req(3, "2026-09-29T01:10:00+00:00", new_body(S, "", reply="Yes", reply_t="2026-09-28T21:05:00-04:00",
                                                         checkinReplyNumerical=8, orgCode=""),
                path=G.NEW_PATH, status="HTTP/2.0 202 Accepted")]
    calls = [c for r in reqs for c in G.make_calls(svc, RESPONSE_DATA_SCHEMA, r)]
    assert calls[0]["guarded"] and calls[0]["row"]["checkinReply"] is None           # previous session's reply
    mine = G.fold(RESPONSE_DATA_SCHEMA, calls)[S]
    captured = {}
    real = svc.fold_rows
    monkeypatch.setattr(svc, "fold_rows", lambda rows, *a, **k: captured.setdefault("out", real(rows, *a, **k)))
    datas = [json.loads(old_body(S, ci, reply="Yes", reply_t="2026-09-20T09:00:00-04:00"))["Responses"],
             json.loads(old_body(S, ci, wellnessDomain="Relational"))["Responses"],
             json.loads(new_body(S, "", reply="Yes", reply_t="2026-09-28T21:05:00-04:00", checkinReplyNumerical=8,
                                 orgCode=""))["tables"][1]["data"]]
    svc.plan_target_writes("responses", [{"data": d, "ref": i} for i, d in enumerate(datas)])
    (theirs,) = captured["out"][0].values()
    assert mine == theirs
    assert mine["checkinDateTime"] == datetime(2026, 9, 27, 17, 0, 0, 500000)     # blank never replaces it
    assert mine["wellnessDomain"] == "Physical" and mine["orgCode"] is None        # the last call's values
    assert mine["checkinReply"] == "Yes" and mine["checkinReplyNumerical"] == 8.0


def test_differs_follows_the_merge():
    assert not G.differs({"checkinDateTime": datetime(2026, 9, 27)}, {"checkinDateTime": None}, "checkinDateTime")
    assert G.differs({"wellnessDomain": "Physical"}, {"wellnessDomain": None}, "wellnessDomain")
    assert not G.differs({"wellnessDomain": ""}, {"wellnessDomain": None}, "wellnessDomain")


def test_sid_time_and_groups():
    assert G.sid_time(S9) == at("2026-09-29T16:54:00.675676+00:00")
    assert G.group_of(S9, set()) == "began after the switch (rejected call)"
    assert G.group_of(f"{U1}2026-09-20T13:00:00-04:00", set()) == "began before the window"
    assert G.group_of(f"{U1}2026-09-27T13:00:00-04:00", set()) == "began in the window"


# --- the plan end to end on a stub BigQuery ----------------------------------------------------

class _Job:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class _Table:
    schema = RESPONSE_DATA_SCHEMA


CUTOFF = at("2026-09-29T21:10:02+00:00")


class _BQ:
    def __init__(self, scope, history, staged, stored):
        self.scope, self.history, self.staged, self.stored = scope, history, staged, stored
        self.sql, self.counts = [], []

    def get_table(self, table):
        return _Table()

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        if "MAX(fired_at)" in sql:
            return _Job([{"m": CUTOFF}])
        if "GROUP BY sid" in sql:
            return _Job(self.counts)
        if "STARTS_WITH(response_status_line, 'HTTP/2.0 400') ORDER" in sql and "@ws" not in sql:
            return _Job([r for r in self.scope if r["response_status_line"].startswith("HTTP/2.0 400")])
        if "STARTS_WITH(response_status_line, 'HTTP/2.0 2')" in sql:
            return _Job([r for r in self.history if r["request_path"] == G.NEW_PATH and r["fired_at"] > G.SWITCH_AT
                         and r["response_status_line"].startswith("HTTP/2.0 2")])
        if "adb_staging" in sql:
            return _Job([{"sid": s} for s in self.staged])
        if "IN UNNEST(@sids)" in sql:
            return _Job(self.history)
        if "@ws" in sql:
            return _Job(self.scope)
        if "FARM_FINGERPRINT" in sql:
            wanted = set(job_config.query_parameters[0].values)
            return _Job([dict(r) for r in self.stored if r["SessionID"] in wanted])
        raise AssertionError(sql)


def _stored(sid, ci, fp, **kw):
    r = {"SessionID": sid, "checkinDateTime": ci, "uuid": sid[:36], "userWeek": 3, "orgCode": "demo",
         "wellnessDomain": "Physical", "checkinReply": "No", "checkinReplyDateTime": None, "contactType": None,
         "checkinReplyNumerical": None, "gap_fp": fp}
    r.update(kw)
    return r


def test_plan_folds_every_call_of_every_checkin_in_scope(svc, monkeypatch):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    A = f"{U1}2026-09-27T13:00:00.5-04:00"      # in window, no row -> insert
    B = f"{U2}2026-09-27T14:00:00.5-04:00"      # in window, two rows, a later add-to-db call -> update + collapse
    C = f"{U1}2026-09-20T13:00:00.5-04:00"      # began a week earlier; its call in the window is the one
                                                #   fired as the next check-in starts: an ordinary call now
    D = f"{U3}2026-09-29T12:54:00.5-04:00"      # after the switch, only a rejected (400) call -> insert
    M = f"{U3}2026-09-27T15:00:00-04:00"        # add-to-db staged a call after the log cutoff -> left for later
    ciA, ciB, ciC, ciD, ciM = (x[36:] for x in (A, B, C, D, M))
    scope = [req(2, "2026-09-27T17:00:01+00:00", old_body(A, ciA)),
             req(3, "2026-09-27T18:00:01+00:00", old_body(B, ciB)),
             req(4, "2026-09-27T17:00:00.9+00:00", old_body(C, ciC, reply="Yes", reply_t="2026-09-20T15:00:00-04:00")),
             req(5, "2026-09-27T20:00:00+00:00", old_body(M, ciM)),
             req(6, "2026-09-27T20:00:01+00:00", json.dumps({"Users": {}, "Responses": {"sessionID": ""}})),
             req(9, "2026-09-29T16:54:00.7+00:00", broken(new_body(D, ciD), "testimonial", 'x "y" z'),
                 path=G.NEW_PATH, status="HTTP/2.0 400 Bad Request")]
    history = [req(1, "2026-09-20T17:00:01+00:00", old_body(C, ciC)),
               req(7, "2026-09-29T02:00:00+00:00", new_body(B, ciB, reply="Yes", reply_t="2026-09-28T21:00:00-04:00"),
                   path=G.NEW_PATH, status="HTTP/2.0 202 Accepted"),
               req(8, "2026-09-01T12:00:00+00:00", new_body(A, ciA, wellnessDomain="Test"),
                   path=G.NEW_PATH, status="HTTP/2.0 202 Accepted")] + scope
    history.sort(key=lambda r: (r["fired_at"], r["httplog_id"]))
    stored = [_stored(B, datetime(2026, 9, 27, 18, 0, 0, 500000), 5), _stored(B, datetime(2026, 9, 27, 18, 0, 0, 500000), -2),
              _stored(C, datetime(2026, 9, 20, 17, 0, 0, 500000), 9)]
    bq = _BQ(scope, history, [M], stored)
    plan = G.build_plan(bq, "p.DEV.x")
    ctx = plan.pop("_ctx")
    assert ctx["loaded"]["sids"] == sorted([A, B, C, D, M]) and ctx["moving"] == [M]
    ss = ctx["sessions"]
    assert set(ss) == {A, B, C, D}
    assert [c["id"] for c in ss[C]["calls"]] == [1, 4]                  # the earlier call comes from the log
    assert ss[C]["row"]["checkinReply"] == "Yes" and ss[C]["changed"] == ["checkinReply", "checkinReplyDateTime"]
    assert ss[B]["row"]["checkinReply"] == "Yes" and ss[B]["collapse"] and ss[B]["update"]
    assert ss[A]["insert"] and ss[A]["row"]["wellnessDomain"] == "Physical"    # pre-switch add-to-db test call ignored
    assert [c["endpoint"] for c in ss[A]["calls"]][0].startswith("add-to-db before the switch")
    assert ss[D]["insert"] and ss[D]["rejected"] and ss[D]["calls"][0]["repaired"]
    assert ss[D]["group"] == "began after the switch (rejected call)" and ss[C]["group"] == "began before the window"
    by = {s["sid"]: s for s in plan["sessions"]}
    assert set(by) == {A, B, C, D} and by[B]["fps"] == "-2,5" and by[B]["n"] == 2
    assert plan["totals"] == {"insert": 2, "update": 2, "collapse": 1, "extra_rows": 1}
    assert plan["log_cutoff"] == CUTOFF.isoformat()
    assert all(w == [c for c in [f.name for f in RESPONSE_DATA_SCHEMA] if c in s["row"]] for s in by.values()
               for w in [s["write"]])                                    # every carried column is written


def _world():
    A = f"{U1}2026-09-27T13:00:00.5-04:00"
    B = f"{U2}2026-09-27T14:00:00.5-04:00"
    D = f"{U3}2026-09-29T12:54:00.5-04:00"
    scope = [req(2, "2026-09-27T17:00:01+00:00", old_body(A, A[36:])),
             req(3, "2026-09-27T18:00:01+00:00", old_body(B, B[36:])),
             req(9, "2026-09-29T16:54:00.7+00:00", broken(new_body(D, D[36:]), "testimonial", 'x "y" z'),
                 path=G.NEW_PATH, status="HTTP/2.0 400 Bad Request")]
    history = scope + [req(7, "2026-09-29T02:00:00+00:00", new_body(B, B[36:], reply="Yes",
                                                                   reply_t="2026-09-28T21:00:00-04:00"),
                           path=G.NEW_PATH, status="HTTP/2.0 202 Accepted")]
    history.sort(key=lambda r: (r["fired_at"], r["httplog_id"]))
    stored = [_stored(B, datetime(2026, 9, 27, 18, 0, 0, 500000), 5, checkinReply="Yes", contactType="CheckIn",
                      checkinReplyDateTime=datetime(2026, 9, 29, 1, 0))]
    return A, B, D, scope, history, stored


def test_calls_counts_match_and_mismatch_fails(svc, monkeypatch, capsys):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    A, B, D, scope, history, stored = _world()
    bq = _BQ(scope, history, [], stored)
    bq.counts = [{"sid": A, "old_n": 1, "new_n": 0, "pre_n": 0}, {"sid": B, "old_n": 1, "new_n": 1, "pre_n": 0},
                 {"sid": D, "old_n": 0, "new_n": 1, "pre_n": 0}]
    assert G.cmd_calls(bq) is True
    assert "CALLS PASS: counts equal for 3 of 3" in capsys.readouterr().out
    bq.counts[1]["new_n"] = 2
    assert G.cmd_calls(bq) is False


def test_repaired_proves_each_rejected_body(svc, monkeypatch, capsys):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    A, B, D, scope, history, stored = _world()
    assert G.cmd_repaired(_BQ(scope, history, [], stored)) is True
    out = capsys.readouterr().out
    assert "escaped testimonial (quote)" in out and "REPAIRED PASS: 1 of 1" in out


def test_check_separates_what_the_writer_wrote_from_what_it_lost(svc, monkeypatch, capsys):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    A, B, D, scope, history, stored = _world()
    assert G.cmd_check(_BQ(scope, history, [], stored), table="p.DEV.x") is True
    assert "CHECK PASS: 0 differing columns" in capsys.readouterr().out
    stored[0]["wellnessDomain"] = "Relational"                       # the writer's row is not the fold
    assert G.cmd_check(_BQ(scope, history, [], stored), table="p.DEV.x") is False


def test_list_writes_the_planned_rows_and_the_sample(svc, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(G, "load_svc", lambda client: svc)
    monkeypatch.setattr(G, "preflight", lambda *a, **k: None)
    monkeypatch.chdir(tmp_path)
    A = f"{U1}2026-09-27T13:00:00.5-04:00"
    scope = [req(2, "2026-09-27T17:00:01+00:00", old_body(A, A[36:]))]
    G.cmd_list(_BQ(scope, scope, [], []), table="p.DEV.x")
    out = capsys.readouterr().out
    assert "PLAN: insert 1, update 0, collapse 0" in out and "SAMPLE -- 1 check-ins" in out
    (csv_file,) = tmp_path.glob("gap_repair_planned_*.csv")
    lines = csv_file.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("SessionID,group,action,stored_rows,calls,changed_columns,") and lines[1].startswith(A)
    assert list(tmp_path.glob("gap_repair_sample_*.txt")) and list(tmp_path.glob("gap_repair_plan_*.json"))


def test_sample_is_fixed_and_spread_over_the_actions():
    ss = {}
    for i in range(40):
        ss[f"s{i}"] = {"insert": i < 10, "update": 10 <= i < 30, "collapse": 25 <= i < 30, "rejected": i == 39,
                       "group": "began before the window" if i % 3 == 0 else "began in the window"}
    a, b = G.sample_of(ss), G.sample_of(dict(reversed(list(ss.items()))))
    assert a == b and len(a) == 20 and len(set(a)) == 20
    assert sum(ss[s]["insert"] for s in a) >= 5 and sum(ss[s]["collapse"] for s in a) >= 3 and "s39" in a


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


def test_same_treats_blank_as_null_and_numbers_by_value():
    assert G.same("", None) and G.same(None, "  ") and G.same(8, 8.0) and not G.same("No", None)


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
    bq = _DryBQ(reject="MERGE")
    bq.get_table = lambda table: _Table()
    with pytest.raises(SystemExit, match="dry run"):
        G.cmd_list(bq, table="p.DEV.x")
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

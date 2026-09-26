"""
The responses write path, executed end to end (generated SQL run on DuckDB).

Covers the four check-in-time sequences, a partial body with no check-in-time
key, a row the old writer stored in UTC, and calls with no session ID.
"""
from __future__ import annotations

from datetime import datetime

SID = "8906fdb3-31f4-4c88-af5b-7b536861f7972026-09-18T16%3A01%3A25.005927-04%3A00"
SID_DECODED = "8906fdb3-31f4-4c88-af5b-7b536861f7972026-09-18T16:01:25.005927-04:00"
CHECKIN = "2026-09-18T16%3A01%3A25.006790-04%3A00"
CHECKIN_UTC = datetime(2026, 9, 18, 20, 1, 25, 6790)
UUID = "8906fdb3-31f4-4c88-af5b-7b536861f797"


def call(svc, data, target="responses"):
    body, status = svc.perform_upsert(target, data)
    assert status == 200 and body["status"] == "ok", body
    return body


def rows(svc, target="responses"):
    return svc.fake.rows(svc.table(target))


def body(checkin, reply, **extra):
    d = {"uuid": UUID, "orgID": "8", "orgCode": "wsu", "sessionID": SID,
         "contactType": "CheckIn", "checkinDateTime": checkin, "wellnessDomain": "Relational",
         "checkinReply": reply, "userWeek": "76", "class": "2028"}
    d.update(extra)
    return d


def only_row(svc):
    r = rows(svc)
    assert len(r) == 1, r
    return r[0]


def test_value_then_value(svc):
    call(svc, body(CHECKIN, "No"))
    call(svc, body(CHECKIN, "Yes"))
    r = only_row(svc)
    assert r["checkinDateTime"] == CHECKIN_UTC and r["checkinReply"] == "Yes"
    assert r["SessionID"] == SID_DECODED and r["userWeek"] == 76


def test_blank_then_value_matches_the_stored_null(svc):
    call(svc, body("", "No"))
    assert only_row(svc)["checkinDateTime"] is None
    call(svc, body(CHECKIN, "Yes"))
    r = only_row(svc)
    assert r["checkinDateTime"] == CHECKIN_UTC and r["checkinReply"] == "Yes"


def test_value_then_blank_keeps_the_stored_time(svc):
    call(svc, body(CHECKIN, "No"))
    call(svc, body("", "Yes"))
    r = only_row(svc)
    assert r["checkinDateTime"] == CHECKIN_UTC and r["checkinReply"] == "Yes"


def test_blank_then_blank(svc):
    call(svc, body("", "No"))
    call(svc, body("", "Yes"))
    r = only_row(svc)
    assert r["checkinDateTime"] is None and r["checkinReply"] == "Yes"


def test_partial_body_without_the_key_leaves_other_columns(svc):
    call(svc, body(CHECKIN, "Yes"))
    call(svc, {"uuid": UUID, "orgID": "8", "orgCode": "wsu", "sessionID": SID,
               "referralFollowUpAttempts_str": "%5B%7B%7D%5D"})
    r = only_row(svc)
    assert r["checkinDateTime"] == CHECKIN_UTC and r["checkinReply"] == "Yes"
    assert r["referralFollowUpAttempts_str"] == "[{}]"


def test_row_stored_by_the_old_writer_in_utc_is_matched(svc):
    svc.fake.insert_raw(svc.table("responses"), {
        "SessionID": SID_DECODED, "checkinDateTime": CHECKIN_UTC, "checkinReply": "No", "uuid": UUID})
    call(svc, body(CHECKIN, "Yes"))
    r = only_row(svc)
    assert r["checkinReply"] == "Yes" and r["checkinDateTime"] == CHECKIN_UTC


def test_row_in_another_partition_is_not_matched_by_a_value_call(svc):
    # A same-key row with a DIFFERENT stored time is outside the pruned range:
    # the range still bounds the match (the NULL branch only adds NULL rows).
    svc.fake.insert_raw(svc.table("responses"), {
        "SessionID": SID_DECODED, "checkinDateTime": datetime(2020, 1, 1), "checkinReply": "old"})
    call(svc, body(CHECKIN, "Yes"))
    assert len(rows(svc)) == 2


def test_merge_statement_shape(svc):
    call(svc, body(CHECKIN, "Yes"))
    sql = svc.fake.statements[-1].sql
    assert "(T.`checkinDateTime` BETWEEN @min_dt AND @max_dt OR T.`checkinDateTime` IS NULL)" in sql
    assert "`checkinDateTime` = COALESCE(S.`checkinDateTime`, T.`checkinDateTime`)" in sql
    assert "`checkinReply` = S.`checkinReply`" in sql
    p = svc.fake.statements[-1].params
    assert p["min_dt"].type_ == "DATETIME" and p["min_dt"].value == CHECKIN_UTC == p["max_dt"].value


def test_blank_checkin_merge_has_no_range(svc):
    call(svc, body("", "No"))
    sql = svc.fake.statements[-1].sql
    assert "@min_dt" not in sql and "min_dt" not in svc.fake.statements[-1].params


def test_subscribe_call_without_session_is_inserted(svc):
    sub = {"uuid": UUID, "orgID": "8", "orgCode": "wsu", "sessionID": "", "contactType": "Subscribe",
           "checkinDateTime": CHECKIN, "subscribed": "Yes"}
    b1 = call(svc, sub)
    b2 = call(svc, sub)
    assert b1["operation"] == "insert" == b2["operation"]
    r = rows(svc)
    assert len(r) == 2
    assert all(x["SessionID"] is None and x["contactType"] == "Subscribe" and x["checkinDateTime"] == CHECKIN_UTC for x in r)
    assert svc.fake.statements[-1].sql.strip().startswith("INSERT INTO")


def test_all_blank_call_without_session_is_inserted(svc):
    blank = {"uuid": UUID, "orgID": "134", "orgCode": "va-baa", "zipcode": "81521", "sessionID": "",
             "contactType": "", "checkinDateTime": "", "checkinReply": "", "subscribed": ""}
    assert call(svc, blank)["operation"] == "insert"
    r = only_row(svc)
    assert r["uuid"] == UUID and r["zipcode"] == "81521"
    assert r["SessionID"] is None and r["contactType"] is None and r["checkinDateTime"] is None


def test_call_with_no_session_key_at_all_is_inserted(svc):
    assert call(svc, {"uuid": UUID, "contactType": "Subscribe"})["operation"] == "insert"
    assert len(rows(svc)) == 1


def test_upsert_endpoint_queues_a_keyless_responses_call(svc):
    client = svc.app.test_client()
    resp = client.post("/upsert", json={"tables": [
        {"table": "users", "data": {"uuid": UUID, "checkInRepliesTotal": "33"}},
        {"table": "responses", "data": {"uuid": UUID, "sessionID": "", "contactType": "Subscribe"}},
    ]})
    assert resp.status_code == 202, resp.get_json()
    assert [q[1] for q in svc.queued] == ["users", "responses"]


def test_upsert_endpoint_still_rejects_a_keyless_triage_call(svc):
    client = svc.app.test_client()
    resp = client.post("/upsert", json={"table": "triage_data",
                                        "data": {"message_id": "", "determination": "LowConcern"}})
    assert resp.status_code == 400
    assert "Field 'message_id' cannot be null" in resp.get_json()["errors"]
    assert svc.queued == []


def test_keyless_insert_failure_asks_for_a_retry(svc):
    svc.fake.fail_next.append(RuntimeError("backend error"))
    body_, status = svc.perform_upsert("responses", {"uuid": UUID, "contactType": "Subscribe"})
    assert status == 500 and body_["error"] == "BigQuery INSERT failed"
    assert rows(svc) == []


def test_users_write_is_typed(svc):
    call(svc, {"uuid": UUID, "orgID": "8", "checkInRepliesTotal": "33", "userWeek": "76",
               "class": "2028", "testAccount": ""}, target="users")
    call(svc, {"uuid": UUID, "checkInRepliesTotal": "34"}, target="users")
    r = svc.fake.rows(svc.table("users"))
    assert len(r) == 1
    assert r[0]["checkinrepliestotal"] == 34 and isinstance(r[0]["checkinrepliestotal"], int)
    assert r[0]["userWeek"] == 76 and r[0]["testaccount"] is None
    sql = svc.fake.statements[-1].sql
    assert "@min_dt" not in sql and "COALESCE" not in sql
    assert svc.fake.statements[-1].params["rows"].values[0].struct_types["checkinrepliestotal"] == "INT64"


def test_users_without_uuid_is_still_rejected(svc):
    body_, status = svc.perform_upsert("users", {"uuid": "", "orgID": "8"})
    assert status == 200 and body_["status"] == "error"
    assert "Upsert key field 'uuid' cannot be null" in body_["errors"]
    assert svc.fake.rows(svc.table("users")) == []

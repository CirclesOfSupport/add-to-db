"""
The batched single-writer path (perform_flush), executed on DuckDB.

The calls of one flush arrive in received order; each key's columns take the
last call that carried them, a blank never clears a stored check-in time,
keyless calls are inserted, and an existing row (old-writer UTC, NULL check-in,
or seeded) is matched rather than duplicated.
"""
from __future__ import annotations

from datetime import datetime

from test_responses_sequences import CHECKIN, CHECKIN_UTC, SID, SID_DECODED, UUID, body

import bq_writer


def rows(svc, target="responses"):
    return svc.fake.rows(svc.table(target))


def by_sid(svc):
    out = {}
    for r in rows(svc):
        out.setdefault(r["SessionID"], []).append(r)
    return out


def test_fold_last_call_wins_per_column():
    folded, keyless = bq_writer.fold_rows(
        [{"SessionID": "a", "checkinReply": "No", "checkinDateTime": datetime(2026, 1, 1)},
         {"SessionID": "a", "referralFollowUpAttempts_str": "x"},
         {"SessionID": "a", "checkinReply": "Yes", "checkinDateTime": None},
         {"SessionID": None, "contactType": "Subscribe"}],
        ["SessionID"], ["checkinDateTime"])
    assert folded == {("a",): {"SessionID": "a", "checkinReply": "Yes", "checkinDateTime": datetime(2026, 1, 1),
                               "referralFollowUpAttempts_str": "x"}}
    assert keyless == [{"SessionID": None, "contactType": "Subscribe"}]


def test_stale_first_call_then_real_reply_in_one_flush(svc):
    out = svc.perform_flush("responses", [body(CHECKIN, "No"), body(CHECKIN, "Yes"), body(CHECKIN, "Yes")])
    assert out["keys"] == 1 and out["statements"] == 1
    r = by_sid(svc)[SID_DECODED]
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["checkinDateTime"] == CHECKIN_UTC


def test_later_flush_updates_and_never_duplicates(svc):
    svc.perform_flush("responses", [body(CHECKIN, "No")])
    svc.perform_flush("responses", [body(CHECKIN, "Yes")])
    svc.perform_flush("responses", [body("", "Maybe")])
    r = by_sid(svc)[SID_DECODED]
    assert len(r) == 1 and r[0]["checkinReply"] == "Maybe" and r[0]["checkinDateTime"] == CHECKIN_UTC


def test_partial_body_in_its_own_flush_leaves_other_columns(svc):
    svc.perform_flush("responses", [body(CHECKIN, "Yes")])
    out = svc.perform_flush("responses", [{"uuid": UUID, "sessionID": SID, "referralFollowUpAttempts_str": "%5B%5D"}])
    r = by_sid(svc)[SID_DECODED]
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["referralFollowUpAttempts_str"] == "[]"
    assert "@min_dt" not in svc.fake.statements[-1].sql     # no check-in time -> key only
    assert out["statements"] == 1


def test_mixed_flush_splits_ranged_and_unranged(svc):
    other = SID.replace("8906fdb3", "11111111")
    svc.fake.insert_raw(svc.table("responses"), {"SessionID": other.replace("%3A", ":"), "checkinDateTime": datetime(2026, 9, 1), "checkinReply": "No"})
    out = svc.perform_flush("responses", [body(CHECKIN, "Yes"),
                                          {"uuid": UUID, "sessionID": other, "referralFollowUpAttempts_str": "z"}])
    assert out["statements"] == 2
    s = by_sid(svc)
    assert len(s[SID_DECODED]) == 1 and len(s[other.replace("%3A", ":")]) == 1
    assert s[other.replace("%3A", ":")][0]["checkinReply"] == "No"


def test_existing_null_and_utc_rows_are_matched(svc):
    svc.fake.insert_raw(svc.table("responses"), {"SessionID": SID_DECODED, "checkinDateTime": None, "checkinReply": "No"})
    svc.perform_flush("responses", [body(CHECKIN, "Yes")])
    r = by_sid(svc)[SID_DECODED]
    assert len(r) == 1 and r[0]["checkinDateTime"] == CHECKIN_UTC


def test_range_spans_the_batch(svc):
    later = SID.replace("8906fdb3", "22222222")
    out = svc.perform_flush("responses", [body(CHECKIN, "Yes"),
                                          dict(body("2026-09-25T10%3A00%3A00-04%3A00", "No"), sessionID=later)])
    p = svc.fake.statements[-1].params
    assert p["min_dt"].value == CHECKIN_UTC and p["max_dt"].value == datetime(2026, 9, 25, 14, 0)
    assert out["keys"] == 2 and len(rows(svc)) == 2


def test_keyless_calls_inserted_in_one_statement(svc):
    sub = {"uuid": UUID, "sessionID": "", "contactType": "Subscribe", "checkinDateTime": CHECKIN}
    out = svc.perform_flush("responses", [dict(sub), dict(sub), {"uuid": UUID, "sessionID": "", "zipcode": "81521"}])
    assert out["keyless"] == 3 and out["statements"] == 1
    r = rows(svc)
    assert len(r) == 3 and sorted(str(x["contactType"]) for x in r) == ["None", "Subscribe", "Subscribe"]


def test_users_batch(svc):
    out = svc.perform_flush("users", [{"uuid": UUID, "checkInRepliesTotal": "33", "orgID": "8"},
                                      {"uuid": UUID, "checkInRepliesTotal": "34"}])
    r = svc.fake.rows(svc.table("users"))
    assert out["keys"] == 1 and len(r) == 1 and r[0]["checkinrepliestotal"] == 34 and r[0]["orgID"] == "8"


def test_invalid_item_is_skipped_not_fatal(svc):
    out = svc.perform_flush("users", [{"uuid": "", "orgID": "8"}, {"uuid": UUID, "orgID": "9"}])
    assert out["rows"] == 1 and len(out["skipped"]) == 1

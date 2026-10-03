"""
tools/resubmit_logged_call.py, end to end on DuckDB: a logged call that never landed is read from the
webhook log, repaired if the template broke its JSON, checked part by part against later accepted
calls, sent to the service's own /upsert, flushed by the single writer, and compared with its row.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import resubmit_logged_call as tool  # noqa: E402

import config  # noqa: E402
from test_responses_sequences import CHECKIN, CHECKIN_UTC, SID, SID_DECODED, UUID, body  # noqa: E402
from test_staged_hookup import resp_rows, stage, staged  # noqa: E402,F401  (fixture)

F = bigquery.SchemaField
# three days before the test runs: a re-submitted call is received at the real clock, so "later" calls are
# placed between the lost call and now, whatever day the tests run on
FIRED = (datetime.now(timezone.utc) - timedelta(days=3)).replace(microsecond=0)
URL = f"https://{tool.HOST}{tool.PATH}"
SECRET = "s3cr3t-value-never-shown"


@pytest.fixture
def lost(staged):
    """The service with its single writer, plus the two webhook-log tables the tool reads."""
    fake = staged.fake
    fake.create(tool.LOG, [F("httplog_id", "INT64"), F("fired_at", "TIMESTAMP"), F("status_code", "INT64"),
                           F("is_failure", "BOOL"), F("elapsed_ms", "INT64"), F("webhook_url", "STRING"),
                           F("flow_name", "STRING"), F("flow_uuid", "STRING"), F("ingested_at", "TIMESTAMP")])
    fake.create(tool.DETAIL, [F("httplog_id", "INT64"), F("fired_at", "TIMESTAMP"), F("request_method", "STRING"),
                              F("request_path", "STRING"), F("request_host", "STRING"), F("request_headers", "STRING"),
                              F("request_body", "STRING"), F("response_status_line", "STRING"),
                              F("response_headers", "STRING"), F("response_body", "STRING"),
                              F("detail_fetched_at", "TIMESTAMP")])
    said, sent = [], []

    def log(httplog_id, text, status=500, line="HTTP/2.0 500 Internal Server Error", host=tool.HOST, path=tool.PATH,
            fired=FIRED, flow="LIVE (BG): Webhook to GBQ/Dashboard"):
        fake.insert_raw(tool.LOG, {"httplog_id": httplog_id, "fired_at": fired, "status_code": status,
                                   "is_failure": status != 202, "elapsed_ms": 12000,
                                   "webhook_url": f"https://{host}{path}", "flow_name": flow})
        fake.insert_raw(tool.DETAIL, {"httplog_id": httplog_id, "fired_at": fired, "request_method": "POST",
                                      "request_path": path, "request_host": host, "request_body": text,
                                      "response_status_line": line})

    def http(url, json=None, timeout=None, headers=None):
        """The deployed service, in-process: the request the tool sends goes through the real /upsert."""
        sent.append({"url": url, "json": json, "headers": headers})
        r = staged.app.test_client().post("/upsert", json=json, headers=headers)
        return type("R", (), {"status_code": r.status_code, "text": r.get_data(as_text=True)})()

    def run(*argv, env=None):
        said.clear()
        return tool.main_([str(a) for a in argv], client=fake, service=staged,
                          secret_finder=lambda: tool.find_secret(env={"ADD_TO_DB_SECRET": SECRET} if env is None else env),
                          http=http, say=said.append)

    def flush(at=None):
        """One flush cycle as of `at` (a past moment), or as of a minute from now so calls just received are taken."""
        staged.run_flush_cycle(now=(at or datetime.now(timezone.utc)) + timedelta(seconds=60))

    staged.log, staged.run, staged.said, staged.sent, staged.flush = log, run, said, sent, flush
    return staged


def both(reply="Yes", **extra):
    return {"tables": [{"table": "users", "data": {"uuid": UUID, "orgID": "8", "checkInRepliesTotal": "33"}},
                       {"table": "responses", "data": body(CHECKIN, reply, **extra)}]}


def out(svc) -> str:
    return "\n".join(svc.said)


def part_line(svc, target) -> str:
    return next(x for x in svc.said if x.startswith(f"  {target}"))


# --- rule 1: a users half is never a send ----------------------------------------------------------------

def test_a_users_half_is_never_a_send(lost):
    """No later call and no users row at all: still not sent. And it stays out of a request that sends another part."""
    lost.log(101, json.dumps({"tables": [{"table": "users", "data": {"uuid": UUID, "orgID": "8",
                                                                     "checkInRepliesTotal": "33"}}]}))
    assert lost.run(101, "--post") == 3
    assert "NOT SENT: users is kept by the nightly contact sync" in part_line(lost, "users")
    assert "to send: 0 of 1 part(s)" in out(lost) and "NOTHING TO SEND" in out(lost)
    assert lost.sent == [] and lost.fake.rows(config.STAGING_TABLE) == [] and lost.fake.rows(lost.table("users")) == []
    lost.log(102, json.dumps(both()))
    assert lost.run(102, "--post") == 0
    assert [t["table"] for t in lost.sent[0]["json"]["tables"]] == ["responses"]
    lost.flush()
    assert lost.fake.rows(lost.table("users")) == [] and len(resp_rows(lost)) == 1


def test_a_users_half_costs_no_read(lost):
    lost.log(101, json.dumps({"tables": [{"table": "users", "data": {"uuid": UUID}}]}))
    lost.fake.statements.clear()
    lost.run(101)
    touched = " ".join(j.sql for j in lost.fake.statements)
    assert "adb_staging" not in touched and "RESPONSES.users" not in touched


# --- rule 2: a check-in whose row already equals the call has landed ------------------------------------------

def test_a_check_in_whose_row_already_equals_the_call_is_reported_as_landed(lost):
    """The lost call carries what an earlier call of the session already wrote: landed, never offered as a send."""
    stage(lost, [("responses", body(CHECKIN, "Yes"))], FIRED - timedelta(seconds=27))
    lost.flush(at=FIRED - timedelta(seconds=27))
    lost.log(101, json.dumps(both(reply="Yes")))
    assert lost.run(101, "--post") == 3
    assert "LANDED: the stored row already equals the call in all" in part_line(lost, "responses")
    assert "to send: 0 of 2 part(s)" in out(lost) and lost.sent == []
    assert lost.run(101, "--verify") == 0 and "VERIFY PASS" in out(lost)


def test_landed_is_judged_the_way_the_service_writes_a_carried_over_reply(lost):
    """A call carrying the PREVIOUS session's reply is written as not replied; a row without the reply equals it."""
    stale = body(CHECKIN, "Yes", checkinReplyDateTime="2026-09-11T10%3A00%3A00-04%3A00", checkinReplyNumerical="7")
    stage(lost, [("responses", stale)], FIRED - timedelta(seconds=27))
    lost.flush(at=FIRED - timedelta(seconds=27))
    assert resp_rows(lost)[0]["checkinReply"] is None                      # the service's own guard
    lost.log(101, json.dumps({"tables": [{"table": "responses", "data": stale}]}))
    assert lost.run(101) == 3 and "LANDED" in part_line(lost, "responses")


def test_a_row_that_differs_is_offered_with_the_columns_that_differ(lost):
    stage(lost, [("responses", body(CHECKIN, "No"))], FIRED - timedelta(seconds=27))      # the session's earlier call
    lost.flush(at=FIRED - timedelta(seconds=27))
    lost.log(101, json.dumps(both(reply="Yes")))
    assert lost.run(101) == 0
    line = part_line(lost, "responses")
    assert "SEND: no later call for this SessionID has been accepted since it fired" in line
    assert "columns differ from the stored row (checkinReply: the call has 'Yes', the row has 'No')" in line
    assert "to send: 1 of 2 part(s) (responses)" in out(lost)


def test_a_json_column_is_compared_as_json(lost):
    msg = {"table": "triage_data", "data": {"message_id": "m-json", "message": "hello", "classifier_details": {}}}
    lost.fake.insert_raw(lost.table("triage_data"), {"message_id": "m-json", "message": "hello", "classifier_details": "{}"})
    lost.log(401, json.dumps(msg), status=None, line="Connection Error", flow="LIVE: Unrecognized Message")
    assert lost.run(401, "--post") == 3
    assert "LANDED" in part_line(lost, "triage_data") and lost.sent == [] and lost.queued == []


# --- a valid body ---------------------------------------------------------------------------------

def test_without_post_the_plan_is_shown_and_nothing_is_sent(lost):
    lost.log(101, json.dumps(both()))
    assert lost.run(101) == 0
    text = out(lost)
    assert "body: valid JSON as logged" in text and "to send: 1 of 2 part(s) (responses)" in text
    assert f"SessionID = {SID_DECODED}" in text and "no stored row for this key" in text
    assert "PLAN ONLY: nothing was sent" in text
    assert lost.sent == [] and lost.fake.rows(config.STAGING_TABLE) == []


def test_a_valid_body_is_sent_lands_and_verifies(lost):
    lost.log(101, json.dumps(both()))
    assert lost.run(101, "--post") == 0
    assert "HTTP 202" in out(lost) and "SENT: accepted (202)" in out(lost)
    assert [s["url"] for s in lost.sent] == [URL]
    assert lost.sent[0]["headers"]["X-Webhook-Secret"] == SECRET and SECRET not in out(lost)
    assert lost.sent[0]["json"] == {"tables": [both()["tables"][1]]}        # the logged responses part, as logged
    assert lost.run(101, "--verify") == 0                                   # accepted, not flushed yet: nothing left to send
    assert "SUPERSEDED" in part_line(lost, "responses")
    lost.flush()
    r = resp_rows(lost)
    assert len(r) == 1 and r[0]["checkinReply"] == "Yes" and r[0]["checkinDateTime"] == CHECKIN_UTC
    assert lost.run(101, "--verify") == 0
    assert "VERIFY PASS" in out(lost) and "LANDED" in part_line(lost, "responses")


def test_verify_before_anything_landed_fails(lost):
    lost.log(101, json.dumps(both()))
    assert lost.run(101, "--verify") == 1
    assert "no stored row for this key" in out(lost) and "VERIFY FAIL: a part has not landed" in out(lost)


# --- a body the webhook template broke ---------------------------------------------------------------

BROKEN = """{
  "tables": [
    {
      "table": "users",
      "data": {
        "uuid": "%s"
      }
    },
    {
      "table": "responses",
      "data": {
        "uuid": "%s",
        "sessionID": "%s",
        "checkinDateTime": "%s",
        "orgCode": "She said "keep going" \\ always
and meant it",
        "checkinReply": "Yes",
        "userWeek": "76"
      }
    }
  ]
}""" % (UUID, UUID, SID, CHECKIN)
RAW_VALUE = 'She said "keep going" \\ always\nand meant it'


def test_a_broken_body_is_repaired_proven_sent_and_lands_with_the_raw_text(lost):
    with pytest.raises(ValueError):
        json.loads(BROKEN)
    lost.log(202, BROKEN, status=400, line="HTTP/2.0 400 Bad Request")
    assert lost.run(202, "--post") == 0
    text = out(lost)
    assert "body: repaired -- 1 value(s) escaped (orgCode)" in text
    assert "string values identical to the raw text: 9 of 9" in text and "byte-identical: yes" in text
    assert lost.sent[0]["json"]["tables"][0]["data"]["orgCode"] == RAW_VALUE
    lost.flush()
    assert resp_rows(lost)[0]["orgCode"] == RAW_VALUE and resp_rows(lost)[0]["userWeek"] == 76
    assert lost.run(202, "--verify") == 0 and "LANDED" in part_line(lost, "responses")


def test_a_body_that_cannot_be_repaired_stops_the_run(lost):
    lost.log(203, '{"tables": [{"table": "users", "data": {"uuid": ', status=400, line="HTTP/2.0 400 Bad Request")
    assert lost.run(203, "--post") == 1
    assert "STOPPED: the logged body is not valid JSON and could not be repaired" in out(lost) and lost.sent == []


# --- the service's own 400 --------------------------------------------------------------------------

def test_a_call_the_service_rejects_again_is_reported_not_accepted(lost):
    bad = {"tables": [{"table": "responses", "data": body(CHECKIN, "Yes", userWeek="not-a-number")}]}
    lost.log(301, json.dumps(bad), status=400, line="HTTP/2.0 400 Bad Request")
    assert lost.run(301, "--post") == 1
    text = out(lost)
    assert "could not be compared with a stored row" in part_line(lost, "responses")
    assert "HTTP 400" in text and "NOT ACCEPTED: HTTP 400" in text and "userWeek" in text
    assert resp_rows(lost) == []


# --- a later accepted call supersedes a responses part -----------------------------------------------------

def test_an_earlier_call_of_the_session_does_not_supersede(lost):
    stage(lost, [("responses", body(CHECKIN, "No"))], FIRED - timedelta(seconds=27))   # staged, not yet flushed
    lost.log(101, json.dumps(both(reply="Yes")))
    assert lost.run(101) == 0
    assert "to send: 1 of 2 part(s) (responses)" in out(lost)


def test_a_session_id_logged_plain_and_staged_encoded_is_one_session(lost):
    plain = both()
    plain["tables"][1]["data"]["sessionID"] = SID_DECODED                       # as the template sent it before encoding
    lost.log(101, json.dumps(plain))
    stage(lost, [("responses", body(CHECKIN, "No"))], FIRED + timedelta(minutes=3))    # the later call, url-encoded
    assert lost.run(101) == 3
    assert "responses    SessionID = " + SID_DECODED + "  ->  SUPERSEDED" in out(lost)


def test_a_later_accepted_call_supersedes_and_its_values_stay(lost):
    lost.log(101, json.dumps(both(reply="No")))
    stage(lost, [("responses", body(CHECKIN, "Yes"))], FIRED + timedelta(minutes=5))
    lost.flush(at=FIRED + timedelta(minutes=5))
    assert lost.run(101, "--post") == 3
    assert "SUPERSEDED: 1 later call(s) for this SessionID were accepted" in part_line(lost, "responses")
    assert "NOTHING TO SEND" in out(lost) and lost.sent == []
    assert resp_rows(lost)[0]["checkinReply"] == "Yes"                          # not put back to No


def test_sending_twice_sends_once(lost):
    lost.log(101, json.dumps(both()))
    assert lost.run(101, "--post") == 0
    assert lost.run(101, "--post") == 3                                         # the first send is now the later call
    lost.flush()
    assert lost.run(101, "--post") == 3 and "LANDED" in part_line(lost, "responses")
    assert len(lost.sent) == 1


# --- the other tables are written by key ---------------------------------------------------------------

def test_a_triage_part_with_no_row_is_sent_on_the_per_call_path(lost):
    msg = {"table": "triage_data", "data": {"message_id": "20260930151630123456-000001", "uuid": UUID,
                                            "message": "I need to talk to someone"}}
    lost.log(401, json.dumps(msg), status=None, line=None, flow="LIVE: Unrecognized Message")
    assert lost.run(401, "--post") == 0
    line = part_line(lost, "triage_data")
    assert "SEND: written by key with only the columns this part carries; no stored row for this key" in line
    assert [q[1] for q in lost.queued] == ["triage_data"] and lost.fake.rows(config.STAGING_TABLE) == []


def test_a_triage_part_missing_from_its_row_is_sent_and_names_the_columns(lost):
    lost.fake.insert_raw(lost.table("triage_data"), {"message_id": "m-1", "message": "hello", "determination": "LowConcern"})
    req = {"table": "triage_data", "data": {"message_id": "m-1", "triage_request_id": "req-1"}}
    lost.log(402, json.dumps(req), flow="LIVE: Initiate Triage Review")
    assert lost.run(402) == 0
    assert ("1 of 1 columns differ from the stored row (triage_request_id: the call has 'req-1', the row has None)"
            in part_line(lost, "triage_data"))


# --- what is refused -----------------------------------------------------------------------------------

def test_a_call_that_was_accepted_is_refused(lost):
    lost.log(501, json.dumps(both()), status=202, line="HTTP/2.0 202 Accepted")
    assert lost.run(501, "--post") == 1
    assert "it was accepted, it is not a lost call" in out(lost) and lost.sent == []


def test_a_call_to_another_service_is_refused(lost):
    lost.log(502, json.dumps(both()), host="sheet-service-853176470965.us-east1.run.app", path="/write")
    assert lost.run(502, "--post") == 1
    assert "not to " + tool.HOST + tool.PATH in out(lost) and lost.sent == []


def test_an_id_the_log_does_not_hold_is_refused(lost):
    assert lost.run(999) == 1
    assert "0 rows in the webhook log, expected exactly 1" in out(lost)


def test_a_table_the_service_does_not_upsert_is_refused(lost):
    lost.log(503, json.dumps({"table": "not_a_table", "data": {"x": "1"}}))
    assert lost.run(503, "--post") == 1 and lost.sent == []


# --- --list ---------------------------------------------------------------------------------------------

def test_list_shows_calls_not_answered_202_to_this_service_only(lost):
    lost.log(601, "{}", status=400, line="HTTP/2.0 400 Bad Request", fired=FIRED)
    lost.log(602, "{}", status=202, line="HTTP/2.0 202 Accepted", fired=FIRED + timedelta(seconds=1))
    lost.log(603, "{}", status=None, line=None, fired=FIRED + timedelta(seconds=2), flow="LIVE: Unrecognized Message")
    lost.log(604, "{}", status=500, host="sheet-service-853176470965.us-east1.run.app", path="/write")
    lost.log(605, "{}", status=500, fired=FIRED - timedelta(days=30))                         # before --since
    assert lost.run("--list", "--since", (FIRED - timedelta(days=1)).date().isoformat()) == 0
    lines = [x for x in lost.said if x[:3] in ("601", "602", "603", "604", "605")]
    assert [x[:3] for x in lines] == ["601", "603"]
    assert "status 400" in lines[0] and "status None" in lines[1] and "LIVE: Unrecognized Message" in lines[1]
    assert "2 call(s) to " + tool.HOST + tool.PATH + " since " in out(lost) and "not answered 202" in out(lost)


# --- the secret -----------------------------------------------------------------------------------------

def _described(env):
    return json.dumps({"spec": {"template": {"spec": {"containers": [{"env": env}]}}}})


def test_the_secret_comes_from_the_environment_first():
    assert tool.find_secret(env={"ADD_TO_DB_SECRET": "from-env"}, gcloud=lambda args: pytest.fail("not called")) == "from-env"


def test_the_secret_is_read_from_the_deployed_service():
    calls = []

    def gcloud(args):
        calls.append(args)
        return _described([{"name": "STAGED_TARGETS", "value": "users,responses"},
                           {"name": "WEBHOOK_SECRET", "value": "from-service"}])
    assert tool.find_secret(env={}, gcloud=gcloud) == "from-service"
    assert calls[0][:4] == ["run", "services", "describe", "add-to-db"] and "--region=us-east1" in calls[0]


def test_the_secret_is_read_from_secret_manager_when_the_service_points_there():
    def gcloud(args):
        if args[0] == "run":
            return _described([{"name": "WEBHOOK_SECRET",
                                "valueFrom": {"secretKeyRef": {"name": "add-to-db-webhook-secret", "key": "latest"}}}])
        assert args[:4] == ["secrets", "versions", "access", "latest"] and "--secret=add-to-db-webhook-secret" in args
        return "from-secret-manager\n"
    assert tool.find_secret(env={}, gcloud=gcloud) == "from-secret-manager"


def test_no_readable_secret_stops_the_run():
    with pytest.raises(tool.Stop, match="ADD_TO_DB_SECRET"):
        tool.find_secret(env={}, gcloud=lambda args: _described([{"name": "STAGED_TARGETS", "value": "users"}]))


def test_one_id_or_list_not_both():
    with pytest.raises(SystemExit):
        tool.main_(["101", "--list"], client=object(), say=lambda line: None)
    with pytest.raises(SystemExit):
        tool.main_([], client=object(), say=lambda line: None)
    with pytest.raises(SystemExit):
        tool.main_(["101", "--post", "--verify"], client=object(), say=lambda line: None)

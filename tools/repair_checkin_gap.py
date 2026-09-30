"""
Repair the check-in rows of RESPONSES.response_data that the old writer (get-responses_v2) lost or left
wrong between the partition swap (2026-09-26 06:49 CT, 11:49 UTC) and the switch to add-to-db (the old
writer's last call 2026-09-29 00:56:33 UTC), plus the check-ins of the calls add-to-db answered 400 on
2026-09-29 because the webhook body was not valid JSON.

The rule: the repaired row of a check-in (SessionID) is what add-to-db writes when it receives every
logged call of that check-in in the order the calls were fired -- the service's own preparation of
each call (key casing, typed values, UTC datetimes, blank -> NULL, stale-reply guard) and its own fold
(last call wins per column; a blank check-in time never replaces a stored one), imported from the
service, not rewritten here. Every logged call counts, from both endpoints and whatever it was answered:
  * /get-responses_v2/v2/add (and the maintenance-hold path of the 2026-09-26 swap): the Responses
    object, whose url-encoded values the service's preparation decodes, as it does for every string;
  * /upsert on add-to-db after the switch: the responses item of the body, exactly as sent, including
    the calls the service has already written; a body that is not valid JSON (answered 400) is repaired
    by escaping only the string values that broke it -- every other byte is kept -- and then used.
A check-in is in scope when it began in the window (its identity call, the session id's own time, between the
partition swap and the switch), or has a call rejected after the switch, or began before the window and the fold
of its calls fired before the window equals its one stored row -- so every change the repair makes comes from a
call inside the window. Every other check-in is left untouched and counted. Its calls are taken from the whole
log (bodies are kept from 2026-08-25), whatever the edge.
Calls without a SessionID (sign-up events) are counted and left alone; the users table is out of scope.

    python tools/repair_checkin_gap.py calls
        read-only. Loads the check-ins in scope and every logged call of each; counts them per check-in
        by endpoint and answer, and against an independent count read in SQL; hour-by-hour coverage.
    python tools/repair_checkin_gap.py repaired
        read-only. The /upsert bodies answered 400 since the switch: each one repaired, parsed, and
        proven identical to the raw text in every value except the escaped ones.
    python tools/repair_checkin_gap.py check
        read-only. For every check-in add-to-db has written since the switch: the fold of its complete
        call sequence against the stored row, column by column.
    python tools/repair_checkin_gap.py list [--table T]
        read-only. The plan for the check-ins in scope. Console: summary and a 20-check-in sample.
        Files: gap_repair_list_<stamp>.txt (every change), gap_repair_plan_<stamp>.json (for apply),
        gap_repair_planned_<stamp>.csv (every planned row), gap_repair_sample_<stamp>.txt (the sample
        with every call). They carry subscriber reply text: they stay on this machine.
    python tools/repair_checkin_gap.py history --from YYYY-MM-DD --to YYYY-MM-DD [--table T]
        read-only. The check-ins that began in the slice (CT dates, session id's own time), before the window:
        each rebuilt from every logged call and compared to its stored rows -- no row, more than one row, the
        row equal to its own last call, to an earlier call (a later call lost), or to none; per column, where
        stored differs from the rebuilt check-in and from the last call; previous-session replies kept. Check-ins
        with a load-test call to add-to-db, a call in the repaired window, or a time inside a log hole are
        counted and not diagnosed. Files: history_<from>_<to>_<stamp>.csv and .txt; they stay on this machine.
    python tools/repair_checkin_gap.py backup PLAN --backup DEV.<table>
        copies the current rows of every check-in the plan changes into one new DEV table, checks the copy is
        the rows the list read (count and fingerprints), prints and dry-runs the statement that puts them back.
    python tools/repair_checkin_gap.py rehearse [--keep]
        on a DEV clone of today's RESPONSES.response_data: list, apply, list again, roll back.
    python tools/repair_checkin_gap.py apply PLAN [--production] [--backup DEV.<table made by backup>]
        production only with --production, and never 02:00-04:30 CT (07:00-09:30 UTC). Flush
        maintenance pause on (OPS); drops from the plan any check-in add-to-db has staged a call for
        after the log the list read; backs up the plan's rows to DEV; loads the planned rows to DEV; ONE
        transaction: every listed row still as listed, add-to-db's MERGE, duplicates collapsed to one
        row, exactly one row per check-in, the table's row count moved by exactly inserts - extra rows.
    python tools/repair_checkin_gap.py rollback APPLIED [--production]
        one transaction: every repaired row still as the apply left it, then the backup put back.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from _harness import PROJECT, JobLog, make_client, stamp

from google.cloud import bigquery

import config  # noqa: E402  (src/ is on sys.path via _harness)
from bq_writer import (  # noqa: E402
    BQ_TYPE_MAP, PRESENT_FIELD, apply_stale_reply_guard, build_batch_merge_query, coerce_payload_to_schema,
    fold_rows, is_keyless_row, normalize_payload_to_schema, parse_datetime_like, quote_identifier,
    resolve_key_columns, validate_upsert_keys)

UTC = timezone.utc
TARGET = "responses"
TABLE = f"{PROJECT}.RESPONSES.response_data"
DEV_PREFIX = f"{PROJECT}.DEV."
LOG = f"{PROJECT}.OPS.webhook_log_detail"
STAGING = f"{PROJECT}.OPS.adb_staging"
FLUSH_STATE = f"{PROJECT}.OPS.adb_flush_state"
OLD_PATHS = ["/get-responses_v2/v2/add", "/get-responses_v2-maintenance-hold/v2/add"]
NEW_PATH = "/upsert"
NEW_HOST = "add-to-db-853176470965.us-east1.run.app"
WINDOW_START = datetime(2026, 9, 26, 11, 49, 0, tzinfo=UTC)          # partition swap, 06:49 CT
WINDOW_END = datetime(2026, 9, 29, 0, 57, 0, tzinfo=UTC)             # after the old writer's last call
SWITCH_AT = datetime(2026, 9, 29, 0, 56, 33, 721566, tzinfo=UTC)     # the old writer's last call
LOG_FLOOR = datetime(2026, 8, 25, tzinfo=UTC)                        # the log keeps bodies from here
REPLY_FIELDS = config.STALE_REPLY_FIELDS[TARGET]
CONVENTIONS = config.DATETIME_CONVENTIONS[TARGET]
PRESERVE = config.PRESERVE_ON_BLANK[TARGET]
QUIET_UTC = ((7, 0), (9, 30))       # no production write 07:00-09:30 UTC
SAMPLE_N = 20
SID_SQL = (r"REPLACE(REPLACE(REPLACE(REPLACE(REGEXP_EXTRACT(request_body, r'(?i)\"sessionid\"\s*:\s*\"([^\"]*)\"'), "
           r"'%3A', ':'), '%3a', ':'), '%2B', '+'), '%2b', '+')")
UUID_SQL = r"REGEXP_EXTRACT(request_body, r'\"uuid\"\s*:\s*\"([0-9A-Fa-f-]{36})\"')"
IS_RESP_SQL = ("(request_path IN UNNEST(@old) OR (request_path = @new AND request_host = @host AND "
               r"REGEXP_CONTAINS(request_body, r'\"table\"\s*:\s*\"responses\"')))")


# ---------------------------------------------------------------------------------------------
# bodies: parse, and repair a body the webhook template broke
# ---------------------------------------------------------------------------------------------

_KEY = r"[A-Za-z_][A-Za-z0-9_]*"
# a "key": "value" pair of the webhook template, ending where the template ends a value: the closing
# quote followed by an optional comma, a line break, and the next key or the end of the object
STRING_PAIR = re.compile(r'"(' + _KEY + r')"\s*:\s*"(.*?)"(?=[ \t]*,?[ \t]*\r?\n[ \t]*(?:"' + _KEY + r'"\s*:|[}\]]))',
                         re.S)


def _offending(raw: str) -> bool:
    return '"' in raw or "\\" in raw or any(ord(ch) < 0x20 for ch in raw)


def repair_body(text: str):
    """
    A body that is not valid JSON because a string value was pasted raw (a quote, a backslash or a line
    break typed by a subscriber): escape exactly those values, keep every other byte. Returns
    {"obj", "text", "escaped": [(key, raw)], "spans": [(start, end)]} or None when it still does not parse.
    """
    spans = [(m.start(2), m.end(2), m.group(1), m.group(2)) for m in STRING_PAIR.finditer(text or "")
             if _offending(m.group(2))]
    if not spans:
        return None
    out, pos = [], 0
    for s, e, _, raw in spans:
        out += [text[pos:s], json.dumps(raw, ensure_ascii=False)[1:-1]]
        pos = e
    out.append(text[pos:])
    fixed = "".join(out)
    try:
        obj = json.loads(fixed)
    except ValueError:
        return None
    return {"obj": obj, "text": fixed, "escaped": [(k, raw) for _, _, k, raw in spans],
            "spans": [(s, e) for s, e, _, _ in spans]}


def _string_leaves(obj, key=None):
    """Every (key, string value) of a parsed body, in document order."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _string_leaves(v, k)
    elif isinstance(obj, list):
        for v in obj:
            yield from _string_leaves(v, key)
    elif isinstance(obj, str):
        yield key, obj


def prove_repair(text: str, rep: dict) -> dict:
    """
    Proof that the repair changed nothing but the escaping of the offending values:
      outside   -- the text outside the escaped values is byte-identical before and after;
      values    -- every string value of the parsed body equals the raw characters between its quotes in
                   the original text (the escaped ones included), one for one, in order;
    numbers and nulls sit outside the escaped values, so they parse from identical text.
    """
    before, pos = [], 0
    for s, e in rep["spans"]:
        before.append(text[pos:s])
        pos = e
    before.append(text[pos:])
    after, pos, fixed = [], 0, rep["text"]
    for (s, e), (_, raw) in zip(rep["spans"], rep["escaped"]):
        esc = json.dumps(raw, ensure_ascii=False)[1:-1]
        start = fixed.index(esc, pos)
        after.append(fixed[pos:start])
        pos = start + len(esc)
    after.append(fixed[pos:])
    raw_pairs = [(m.group(1), m.group(2)) for m in STRING_PAIR.finditer(text)]
    parsed = list(_string_leaves(rep["obj"]))
    same_values = sum(1 for (k1, v1), (k2, v2) in zip(raw_pairs, parsed) if k1 == k2 and v1 == v2)
    return {"parses": True, "outside_identical": before == after, "strings": len(parsed),
            "raw_strings": len(raw_pairs), "identical": same_values,
            "ok": before == after and len(parsed) == len(raw_pairs) == same_values}


def body_items(svc, text: str):
    """(responses payloads the body carries, repair or None, error or None) -- as the writer parses bodies."""
    rep = None
    try:
        body = json.loads(text)
    except (TypeError, ValueError):
        rep = repair_body(text or "")
        if rep is None:
            return [], None, "not JSON and not repairable"
        body = rep["obj"]
    if not isinstance(body, dict):
        return [], rep, "not a JSON object"
    if isinstance(body.get("Responses"), dict):              # the old writer's body
        return [body["Responses"]], rep, None
    items, err = svc.normalize_target_requests(body)       # add-to-db's own body parsing
    if err:
        return [], rep, err
    return [i["data"] for i in items if i["target"] == TARGET], rep, None


def _get(d: dict, key: str):
    return next((v for k, v in d.items() if k.lower() == key.lower()), None)


def _aware(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        v = parse_datetime_like(str(value))
    except ValueError:
        return None
    return v if v.tzinfo else v.replace(tzinfo=UTC)


def sid_time(sid: str):
    """A SessionID is the contact uuid followed by the time the session started."""
    return _aware(sid[36:]) if sid and len(sid) > 36 else None


# ---------------------------------------------------------------------------------------------
# preparation and fold: add-to-db's own functions, in plan_target_writes' order
# ---------------------------------------------------------------------------------------------

def load_svc(client):
    """main (the service) imported with our client, for its body parsing and validation."""
    original = bigquery.Client
    bigquery.Client = lambda *a, **k: client
    try:
        import main
    finally:
        bigquery.Client = original
    main.client = client
    return main


def _prepare(svc, schema, data: dict):
    """prepare(), plus the row before the stale-reply guard: what the old writer stored for this call."""
    names = {f.name.lower() for f in schema}
    unknown = sorted(k for k in data if k.lower() not in names)
    normalized, normalize_errors = normalize_payload_to_schema(dict(data), schema)
    coerced, coerce_errors = coerce_payload_to_schema(normalized, schema, CONVENTIONS)
    errors, _warnings = svc.validate_payload(coerced, schema)
    errors = errors + normalize_errors + coerce_errors
    resolved, key_errors = resolve_key_columns(config.UPSERT_KEYS[TARGET], schema)
    row = svc.filter_to_schema(coerced, schema)
    keyless = TARGET in config.KEYLESS_INSERT_TARGETS and is_keyless_row(resolved, row)
    if not keyless:
        errors = errors + key_errors + validate_upsert_keys(resolved, schema, row)
    guarded_row = apply_stale_reply_guard(row, REPLY_FIELDS)
    return guarded_row, errors, unknown, keyless, guarded_row != row, row


def prepare(svc, schema, data: dict):
    """
    One call's row exactly as main.plan_target_writes prepares it, except that a key the table lacks
    is dropped and reported instead of added as a column. (row, errors, unknown_keys, keyless, guarded)
    """
    return _prepare(svc, schema, data)[:5]


def fold(schema, calls: list[dict]) -> dict:
    """add-to-db's fold over prepared calls in fired order -> {SessionID: row}."""
    resolved, _ = resolve_key_columns(config.UPSERT_KEYS[TARGET], schema)
    ordered = sorted((c for c in calls if c["row"] is not None and not c["errors"] and not c["keyless"]),
                     key=lambda c: c["order"])
    folded, _ = fold_rows([c["row"] for c in ordered], resolved, PRESERVE)
    return {key[0]: row for key, row in folded.items()}


# ---------------------------------------------------------------------------------------------
# calls from the log
# ---------------------------------------------------------------------------------------------

def _endpoint(path: str, fired) -> str:
    if path in OLD_PATHS:
        return "old writer" if path == OLD_PATHS[0] else "old writer (maintenance hold)"
    return "add-to-db" if fired > SWITCH_AT else "add-to-db before the switch (not the live writer: excluded)"


def _status(line: str) -> str:
    m = re.search(r"\b(\d{3})\b", line or "")
    return m.group(1) if m else (line or "none").strip()


def make_calls(svc, schema, r) -> list[dict]:
    """One logged request -> one call per responses payload it carries (prepared)."""
    items, rep, err = body_items(svc, r["request_body"])
    base = {"id": r["httplog_id"], "fired_at": r["fired_at"], "endpoint": _endpoint(r["request_path"], r["fired_at"]),
            "status": _status(r["response_status_line"]), "repaired": rep is not None}
    if err:
        return [dict(base, sid=None, raw_sid=None, row=None, snap=None, errors=[err], unknown=[], keyless=False,
                     guarded=False, order=(r["fired_at"], r["httplog_id"], 0))]
    out = []
    for i, data in enumerate(items):
        row, errors, unknown, keyless, guarded, snap = _prepare(svc, schema, data)
        raw_sid = _get(data, "sessionID")
        sid = row.get("SessionID") if not keyless else None
        out.append(dict(base, sid=sid, raw_sid=unquote(str(raw_sid)) if raw_sid is not None else None,
                        row=row, snap=snap, errors=errors, unknown=unknown, keyless=keyless, guarded=guarded,
                        order=(r["fired_at"], r["httplog_id"], i)))
    return out


def _params(**kw):
    out = []
    for name, v in kw.items():
        if isinstance(v, list):
            out.append(bigquery.ArrayQueryParameter(name, "STRING", v))
        elif isinstance(v, datetime):
            out.append(bigquery.ScalarQueryParameter(name, "TIMESTAMP", v))
        else:
            out.append(bigquery.ScalarQueryParameter(name, "STRING", v))
    return bigquery.QueryJobConfig(query_parameters=out)


def log_cutoff(client):
    """The newest add-to-db call the log has ingested (the log is filled hourly): calls are read up to it."""
    sql = f"SELECT MAX(fired_at) m FROM `{LOG}` WHERE fired_at > @sw AND request_path = @new AND request_host = @host"
    return list(client.query(sql, job_config=_params(sw=SWITCH_AT, new=NEW_PATH, host=NEW_HOST)).result())[0]["m"]


COLS = "httplog_id, fired_at, request_path, response_status_line, request_body"


def scope_rows(client):
    """Calls that put a check-in in scope: the old writer's in the window, add-to-db's answered 400."""
    sql = (f"SELECT {COLS} FROM `{LOG}` WHERE fired_at >= @ws AND ((request_path IN UNNEST(@old) AND fired_at < @we) "
           f"OR (request_path = @new AND request_host = @host AND fired_at > @sw "
           f"AND STARTS_WITH(response_status_line, 'HTTP/2.0 400'))) ORDER BY fired_at, httplog_id")
    return client.query(sql, job_config=_params(ws=WINDOW_START, we=WINDOW_END, sw=SWITCH_AT, old=OLD_PATHS,
                                                new=NEW_PATH, host=NEW_HOST)).result()


def since_switch_rows(client, cutoff):
    """add-to-db's accepted responses calls since the switch, up to the log cutoff."""
    sql = (f"SELECT {COLS} FROM `{LOG}` WHERE fired_at > @sw AND fired_at <= @cut AND request_path = @new "
           f"AND request_host = @host AND STARTS_WITH(response_status_line, 'HTTP/2.0 2') "
           r"AND REGEXP_CONTAINS(request_body, r'\"table\"\s*:\s*\"responses\"') ORDER BY fired_at, httplog_id")
    return client.query(sql, job_config=_params(sw=SWITCH_AT, cut=cutoff, new=NEW_PATH, host=NEW_HOST)).result()


def history_rows(client, sids: list[str], floor, cutoff):
    """Every logged call of these check-ins, both endpoints, any answer, from `floor` to the log cutoff."""
    sql = (f"SELECT {COLS} FROM `{LOG}` WHERE fired_at >= @floor AND fired_at <= @cut AND {IS_RESP_SQL} "
           f"AND {SID_SQL} IN UNNEST(@sids) ORDER BY fired_at, httplog_id")
    return client.query(sql, job_config=_params(floor=floor, cut=cutoff, old=OLD_PATHS, new=NEW_PATH, host=NEW_HOST,
                                                sids=sorted(sids))).result()


def sql_counts(client, uuids: list[str], floor, cutoff):
    """Independent count per SessionID (read by pattern in SQL, not by the JSON parser), for the contacts in scope."""
    sql = (f"SELECT {SID_SQL} sid, COUNTIF(request_path IN UNNEST(@old)) old_n, "
           f"COUNTIF(request_path = @new AND fired_at > @sw) new_n, COUNTIF(request_path = @new AND fired_at <= @sw) pre_n "
           f"FROM `{LOG}` WHERE fired_at >= @floor AND fired_at <= @cut AND {IS_RESP_SQL} "
           f"AND {UUID_SQL} IN UNNEST(@uuids) GROUP BY sid")
    return {r["sid"]: (r["old_n"], r["new_n"], r["pre_n"]) for r in client.query(
        sql, job_config=_params(floor=floor, cut=cutoff, sw=SWITCH_AT, old=OLD_PATHS, new=NEW_PATH, host=NEW_HOST,
                                uuids=sorted(uuids))).result()}


def staged_since(client, cutoff) -> set[str]:
    """Check-ins with a call add-to-db staged after the log cutoff: the log does not have their newest call yet."""
    sql = (f"SELECT DISTINCT COALESCE(JSON_VALUE(payload, '$.sessionID'), JSON_VALUE(payload, '$.SessionID'), "
           f"JSON_VALUE(payload, '$.sessionid')) sid FROM `{STAGING}` WHERE target = @t AND received_at > @cut")
    return {unquote(str(r["sid"])) for r in client.query(sql, job_config=_params(t=TARGET, cut=cutoff)).result() if r["sid"]}


def load(client, svc, schema, mode: str) -> dict:
    """
    mode "window": the check-ins in scope of the repair; mode "since switch": every check-in add-to-db
    has written since the switch. Returns the calls of those check-ins (all of them) and context.
    """
    cutoff = log_cutoff(client)
    seed, seed_rows = [], 0
    hours = Counter()
    rows = scope_rows(client) if mode == "window" else since_switch_rows(client, cutoff)
    for r in rows:
        seed_rows += 1
        if mode == "window" and r["request_path"] in OLD_PATHS:
            hours[r["fired_at"].astimezone(UTC).replace(minute=0, second=0, microsecond=0)] += 1
        seed += make_calls(svc, schema, r)
    sids = sorted({c["sid"] for c in seed if c["sid"]})
    starts = [t for t in (sid_time(s) for s in sids) if t is not None]
    floor = max(LOG_FLOOR, min(starts, default=WINDOW_START) - timedelta(minutes=10))
    calls, unknown = [], Counter()
    wanted = set(sids)
    for r in history_rows(client, sids, floor, cutoff):
        for c in make_calls(svc, schema, r):
            if c["sid"] in wanted or (c["sid"] is None and c.get("raw_sid") in wanted):
                calls.append(c)
                unknown.update(c["unknown"])
    calls.sort(key=lambda c: c["order"])
    return {"mode": mode, "cutoff": cutoff, "floor": floor, "seed": seed, "seed_rows": seed_rows, "sids": sids,
            "calls": calls, "unknown": unknown, "hours": hours}


# ---------------------------------------------------------------------------------------------
# stored rows
# ---------------------------------------------------------------------------------------------

def read_stored_sql(table: str, cols: list[str]) -> str:
    sel = ", ".join(["SessionID"] + [quote_identifier(c) for c in cols if c != "SessionID"]
                    + ["FARM_FINGERPRINT(TO_JSON_STRING(t)) AS gap_fp"])
    return f"SELECT {sel} FROM `{table}` t WHERE SessionID IN UNNEST(@s)"


def read_stored(client, table: str, sids: list[str], cols: list[str]) -> dict[str, list[dict]]:
    if not sids:
        return {}
    sql = read_stored_sql(table, cols)
    out = defaultdict(list)
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("s", "STRING", sorted(sids))])
    for r in client.query(sql, job_config=cfg).result():
        out[r["SessionID"]].append(dict(r))
    return out


def same(a, b) -> bool:
    """Stored vs planned, with a blank string and NULL the same (add-to-db writes blank as NULL)."""
    a = None if isinstance(a, str) and a.strip() == "" else a
    b = None if isinstance(b, str) and b.strip() == "" else b
    if a is None or b is None:
        return a is None and b is None
    num = (int, float)
    if isinstance(a, num) and isinstance(b, num) and not isinstance(a, bool) and not isinstance(b, bool):
        return float(a) == float(b)
    return a == b


def differs(stored: dict, planned: dict, col: str) -> bool:
    """Would add-to-db's MERGE change this stored value? A NULL in a preserve column keeps the stored one."""
    if col.lower() in {p.lower() for p in PRESERVE} and planned.get(col) is None:
        return False
    return not same(stored.get(col), planned.get(col))


def fps_of(rows: list[dict]) -> str:
    return ",".join(str(v) for v in sorted(int(r["gap_fp"]) for r in rows))


# ---------------------------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------------------------

def _enc(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    return v


REJECTED = "one of the calls rejected after the switch"


def group_of(sid: str, rejected: set) -> str:
    """By its identity call (the session id's own time); a check-in with a rejected call is its own group."""
    if sid in rejected:
        return REJECTED
    t = sid_time(sid)
    if t is not None and t < WINDOW_START:
        return "began before the window"
    if t is not None and t > SWITCH_AT:
        return "began after the switch"
    return "began in the window"


def pre_window_matches(schema, calls: list[dict], rows: list[dict]) -> str | None:
    """
    Scope rule for a check-in that began before the window: it is repaired only if the fold of its calls fired
    before the window equals its one stored row, so every change the repair makes comes from a call in the window.
    Returns None when it qualifies, else the reason it is left out.
    """
    pre = [c for c in calls if c["fired_at"] < WINDOW_START]
    if not any(c["row"] is not None and not c["errors"] for c in pre):
        return "no call before the window in the log"
    if not rows:
        return "no stored row"
    if len(rows) > 1:
        return "more than one stored row"
    folded = next(iter(fold(schema, pre).values()), None)
    if folded is None:
        return "no call before the window in the log"
    if any(differs(rows[0], folded, c) for c in folded if c != "SessionID"):
        return "stored row differs from its calls before the window"
    return None


def build_plan(client, table: str, loaded: dict | None = None, keep: set | None = None) -> dict:
    """Read-only. Per check-in in scope: the fold of its complete call sequence against its stored rows."""
    svc = load_svc(client)
    schema = list(client.get_table(table).schema)
    loaded = loaded or load(client, svc, schema, "window")
    calls = loaded["calls"]
    moving = staged_since(client, loaded["cutoff"])
    rejected = {c["sid"] for c in list(loaded["seed"]) + calls
                if c["endpoint"] == "add-to-db" and c["status"] == "400" and c["sid"]}
    by_sid = defaultdict(list)
    for c in calls:
        if c["sid"]:
            by_sid[c["sid"]].append(c)
    folded = fold(schema, calls)
    bad = {sid: [e for c in cs for e in c["errors"]] for sid, cs in by_sid.items() if any(c["errors"] for c in cs)}
    in_scope = [sid for sid in loaded["sids"] if sid in folded and sid not in moving]
    cols = [f.name for f in schema]
    stored = read_stored(client, table, in_scope, cols)
    excluded = {}
    if loaded["mode"] == "window":            # the window's scope rule (2026-09-29)
        for sid in list(in_scope):
            g = group_of(sid, rejected)
            if g == "began in the window" or g == REJECTED or sid in (keep or ()):
                continue
            reason = (pre_window_matches(schema, by_sid[sid], stored.get(sid, [])) if g == "began before the window"
                      else "began after the switch")
            if reason:
                excluded[sid] = f"{g}: {reason}"
        in_scope = [sid for sid in in_scope if sid not in excluded]
    sessions = {}
    for sid in in_scope:
        rows, planned = stored.get(sid, []), folded[sid]
        write = [c for c in cols if c in planned]
        changed = sorted({c for c in write if c != "SessionID" for r in rows if differs(r, planned, c)}, key=cols.index)
        sessions[sid] = {"sid": sid, "group": group_of(sid, rejected), "calls": by_sid[sid], "row": planned,
                         "write": write, "stored": rows, "changed": changed, "insert": not rows,
                         "collapse": len(rows) > 1, "update": bool(rows) and bool(changed),
                         "rejected": sid in rejected}
    types = {f.name: f.field_type.upper() for f in schema}
    actions = [s for s in sessions.values() if s["insert"] or s["update"] or s["collapse"]]
    plan = {
        "table": table, "listed_at": datetime.now(UTC).isoformat(), "log_cutoff": loaded["cutoff"].isoformat(),
        "rule": "add-to-db's fold over every logged call of the check-in, in fired order",
        "scope": ("began in the window, or has a call rejected after the switch, or began before the window and the "
                  "fold of its calls before the window equals its stored row"),
        "window": [WINDOW_START.isoformat(), WINDOW_END.isoformat()],
        "columns": {c: types[c] for c in cols if any(c in s["row"] for s in actions)},
        "totals": {"insert": sum(s["insert"] for s in actions), "update": sum(s["update"] for s in actions),
                   "collapse": sum(s["collapse"] for s in actions),
                   "extra_rows": sum(len(s["stored"]) - 1 for s in actions if s["collapse"])},
        "sessions": [{"sid": s["sid"], "group": s["group"], "insert": s["insert"], "update": s["update"],
                      "collapse": s["collapse"], "n": len(s["stored"]), "fps": fps_of(s["stored"]),
                      "changed": s["changed"], "write": s["write"],
                      "row": {c: _enc(v) for c, v in s["row"].items()}} for s in actions],
    }
    plan["_ctx"] = {"loaded": loaded, "sessions": sessions, "moving": sorted(set(loaded["sids"]) & moving),
                    "excluded": excluded,
                    "bad": bad, "no_row_planned": sorted(set(loaded["sids"]) - set(folded)), "schema": schema}
    return plan


# ---------------------------------------------------------------------------------------------
# diagnosis
# ---------------------------------------------------------------------------------------------

def _reply(v):
    return None if v is None or (isinstance(v, str) and v.strip() == "") else v


def diagnose(client, table: str, sessions: dict) -> dict:
    """For the check-ins in scope: no row; more than one row; a stored row differs from the fold; the
    stored reply differs from the fold's; the fold says Yes and no stored row does."""
    cols = sorted({c for s in sessions.values() for c in list(s["row"]) + ["checkinReply"]})
    stored = read_stored(client, table, list(sessions), cols)
    d = Counter(sessions=len(sessions))
    for sid, s in sessions.items():
        rows = stored.get(sid, [])
        planned = _reply(s["row"].get("checkinReply"))
        d["no row"] += not rows
        d["duplicated"] += len(rows) > 1
        d["differs from the fold"] += bool(rows) and any(differs(r, s["row"], c) for r in rows for c in s["write"])
        d["reply != the fold's"] += bool(rows) and any(_reply(r.get("checkinReply")) != planned for r in rows)
        d["Yes missing"] += planned == "Yes" and not any(r.get("checkinReply") == "Yes" for r in rows)
    return d


DIAG_KEYS = ["sessions", "no row", "duplicated", "differs from the fold", "reply != the fold's", "Yes missing"]


def diag_lines(before, after=None) -> list[str]:
    out = [f"  {'':<28} {'before':>8}" + (f" {'after':>8}" if after is not None else "")]
    for k in DIAG_KEYS:
        out.append(f"  {k:<28} {before.get(k, 0):>8}" + (f" {after.get(k, 0):>8}" if after is not None else ""))
    return out


def diag_explained(after) -> bool:
    """After a repair every check-in in scope has exactly one row and it is the fold."""
    return all(after.get(k, 0) == 0 for k in ("no row", "duplicated", "differs from the fold", "reply != the fold's",
                                               "Yes missing"))


# ---------------------------------------------------------------------------------------------
# proofs: calls, repaired, check
# ---------------------------------------------------------------------------------------------

def _short(v, n=40):
    v = "NULL" if v is None else (v.isoformat(sep=" ") if isinstance(v, datetime) else str(v))
    v = v.replace("\r", "\\r").replace("\n", "\\n")
    return v if len(v) <= n else v[:n - 3] + "..."


def _counts(calls) -> Counter:
    return Counter((c["endpoint"], c["status"] + (" (body repaired)" if c["repaired"] else "")) for c in calls)


def cmd_calls(client, out=None) -> bool:
    """Is the call set complete? Every check-in in scope, logged calls vs loaded calls."""
    svc = load_svc(client)
    schema = list(client.get_table(TABLE).schema)
    L = load(client, svc, schema, "window")
    calls, sids = L["calls"], L["sids"]
    keyless_seed = sum(1 for c in L["seed"] if c["keyless"])
    tool = Counter()
    for c in calls:
        if c["sid"] and c["endpoint"] != "add-to-db before the switch (not the live writer: excluded)":
            tool[c["sid"]] += 1
    uuids = sorted({s[:36] for s in sids})
    sql = sql_counts(client, uuids, L["floor"], L["cutoff"])
    mismatch = []
    for sid in sids:
        old_n, new_n, _ = sql.get(sid, (0, 0, 0))
        if old_n + new_n != tool[sid]:
            mismatch.append((sid, old_n + new_n, tool[sid]))
    in_scope = set(sids)
    loaded_ids = {c["id"] for c in calls}
    seed_missing = sorted({c["id"] for c in L["seed"] if c["sid"] and c["id"] not in loaded_ids})
    undecoded = sum(a + b + c for s, (a, b, c) in sql.items() if s and "%" in s)
    pre_switch = sum(v[2] for s, v in sql.items() if s in in_scope)
    early = [s for s in sids if (sid_time(s) or WINDOW_START) < LOG_FLOOR]
    hours = [WINDOW_START.replace(minute=0) + timedelta(hours=i)
             for i in range(int((WINDOW_END - WINDOW_START.replace(minute=0)).total_seconds() // 3600) + 1)]
    counts = [L["hours"].get(h, 0) for h in hours]
    empty = [h.strftime("%m-%d %H:00Z") for h, n in zip(hours, counts) if n == 0]
    p = lambda *a: print(*a, file=out)  # noqa: E731
    p(f"log read up to {L['cutoff'].isoformat()} (the newest add-to-db call ingested); history from {L['floor'].isoformat()}")
    p(f"calls that put a check-in in scope: {L['seed_rows']} requests; without a SessionID (sign-up events, left "
      f"alone) {keyless_seed}")
    p(f"check-ins in scope: {len(sids)} (contacts {len(uuids)})")
    p(f"calls loaded for them: {sum(tool.values())}, by endpoint and answer:")
    for (ep, st), n in sorted(_counts([c for c in calls if c['sid']]).items()):
        p(f"  {ep:<62} {st:<22} {n:>7}")
    p(f"calls the service's preparation rejects (the writer would not write them either): "
      f"{sum(1 for c in calls if c['errors'])}")
    p(f"payload keys the table lacks (dropped, reported): {dict(L['unknown']) or 'none'}")
    p(f"independent count (session id read by pattern in SQL): {sum(a + b for s, (a, b, _) in sql.items() if s in in_scope)} "
      f"calls for these check-ins, {sum(a + b for a, b, _ in sql.values())} "
      f"for these contacts; add-to-db calls before the switch for these check-ins (excluded): {pre_switch}; "
      f"session ids SQL could not decode: {undecoded}")
    p(f"check-ins whose session began before the log's first body (history may be incomplete): {len(early)}")
    p(f"old-writer calls per hour in the window: min {min(counts)}, max {max(counts)}; hours with none: "
      f"{', '.join(empty) or 'none'}")
    p(f"calls that put a check-in in scope but were not found again in its history: {len(seed_missing)}"
      + (f" (first ids {seed_missing[:5]})" if seed_missing else ""))
    p(f"check-ins whose count differs (log vs loaded): {len(mismatch)}")
    for sid, a, b in mismatch[:10]:
        p(f"  {sid[:8]}..{sid[36:]}  log {a}  loaded {b}")
    ok = not mismatch and undecoded == 0 and not seed_missing
    p(f"CALLS {'PASS' if ok else 'FAIL'}: counts equal for {len(sids) - len(mismatch)} of {len(sids)} check-ins")
    return ok


def cmd_repaired(client, out=None) -> bool:
    """Do the rejected bodies repair to valid JSON with no other value changed?"""
    svc = load_svc(client)
    schema = list(client.get_table(TABLE).schema)
    sql = (f"SELECT {COLS} FROM `{LOG}` WHERE fired_at > @sw AND request_path = @new AND request_host = @host "
           f"AND STARTS_WITH(response_status_line, 'HTTP/2.0 400') ORDER BY fired_at, httplog_id")
    rows = list(client.query(sql, job_config=_params(sw=SWITCH_AT, new=NEW_PATH, host=NEW_HOST)).result())
    p = lambda *a: print(*a, file=out)  # noqa: E731
    good = 0
    for r in rows:
        text = r["request_body"] or ""
        try:
            json.loads(text)
            p(f"{r['httplog_id']} {r['fired_at'].isoformat()}  parses as sent (answered 400 for another reason)")
            continue
        except ValueError:
            pass
        rep = repair_body(text)
        if rep is None:
            p(f"{r['httplog_id']} {r['fired_at'].isoformat()}  NOT REPAIRED: still not JSON")
            continue
        proof = prove_repair(text, rep)
        items, _, err = body_items(svc, text)
        sids = [prepare(svc, schema, d)[0].get("SessionID") for d in items]
        good += proof["ok"] and not err
        p(f"{r['httplog_id']} {r['fired_at'].isoformat()}  parses: yes; session {', '.join(s[:8] + '..' + s[36:] for s in sids if s)}")
        for k, raw in rep["escaped"]:
            what = sorted({"quote" if ch == '"' else "backslash" if ch == "\\" else "line break" if ch in "\r\n"
                           else "control" for ch in raw if ch in '"\\' or ord(ch) < 0x20})
            p(f"    escaped {k} ({', '.join(what)}): {_short(raw, 160)}")
        p(f"    string values identical to the raw text: {proof['identical']} of {proof['raw_strings']}; "
          f"text outside the escaped values byte-identical: {'yes' if proof['outside_identical'] else 'NO'}")
    ok = good == len(rows)
    p(f"REPAIRED {'PASS' if ok else 'FAIL'}: {good} of {len(rows)} bodies repaired and proven")
    return ok


def cmd_check(client, table=TABLE, out=None) -> bool:
    """Is this fold the writer's fold? Every check-in add-to-db wrote since the switch."""
    svc = load_svc(client)
    schema = list(client.get_table(table).schema)
    L = load(client, svc, schema, "since switch")
    plan = build_plan(client, table, loaded=L)
    ctx = plan.pop("_ctx")
    sessions = ctx["sessions"]
    cols = [f.name for f in schema]
    post_cols = {}
    for sid, s in sessions.items():
        post_cols[sid] = {k for c in s["calls"] if c["endpoint"] == "add-to-db" and c["status"].startswith("2")
                          and not c["errors"] for k in c["row"]}
    by_post, by_pre, rejected_diff = Counter(), Counter(), Counter()
    examples = []
    compared = [s for s in sessions.values() if s["stored"]]
    for s in compared:
        for c in s["changed"]:
            if s["rejected"]:
                rejected_diff[c] += 1
            elif c in post_cols[s["sid"]]:
                by_post[c] += 1
                if len(examples) < 10:
                    examples.append((s["sid"], c, s["stored"][0].get(c), s["row"].get(c)))
            else:
                by_pre[c] += 1
    guarded = sum(1 for s in compared for c in s["calls"] if c.get("guarded"))
    blank_ci = sum(1 for s in compared if any(c["row"] and "checkinDateTime" in c["row"] and c["row"]["checkinDateTime"]
                                              is None for c in s["calls"]))
    p = lambda *a: print(*a, file=out)  # noqa: E731
    p(f"log read up to {L['cutoff'].isoformat()}; history from {L['floor'].isoformat()}")
    p(f"check-ins add-to-db wrote since the switch: {len(L['sids'])}; with a call staged after the log cutoff "
      f"(not compared) {len(ctx['moving'])}; compared {len(compared)}; with no stored row {sum(1 for s in sessions.values() if not s['stored'])}; "
      f"with more than one stored row {sum(1 for s in compared if len(s['stored']) > 1)}")
    p(f"calls folded: {sum(len(s['calls']) for s in sessions.values())} "
      f"({', '.join(f'{ep} {st} {n}' for (ep, st), n in sorted(_counts([c for s in sessions.values() for c in s['calls']]).items()))})")
    p(f"columns compared per row: {len(cols)}; rows where the check-in time was blank on some call (kept by the "
      f"writer's rule): {blank_ci}; calls whose earlier reply the stale-reply guard cleared: {guarded}")
    p("differing columns carried by a call add-to-db accepted (must be 0): "
      + (", ".join(f"{c} {n}" for c, n in by_post.most_common()) or "none"))
    p("differing columns carried only by the old writer's calls (what it lost; the repair writes them): "
      + (", ".join(f"{c} {n}" for c, n in by_pre.most_common()) or "none"))
    p("differing columns on check-ins with a rejected call (the calls add-to-db could not write): "
      + (", ".join(f"{c} {n}" for c, n in rejected_diff.most_common()) or "none"))
    for sid, c, a, b in examples:
        p(f"  {sid[:8]}..{sid[36:]}  {c}: stored {_short(a)} -> fold {_short(b)}")
    ok = not by_post
    p(f"CHECK {'PASS' if ok else 'FAIL'}: {sum(by_post.values())} differing columns where the writer wrote the "
      f"same calls, over {len(compared)} check-ins")
    return ok


# ---------------------------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------------------------

def _pick(sids, n):
    return sorted(sids, key=lambda s: hashlib.sha1(s.encode()).hexdigest())[:n]


def sample_of(sessions: dict, n=SAMPLE_N) -> list[str]:
    """A fixed-seed sample across actions and groups: inserts, updates, collapses, earlier starts, rejected calls."""
    pools = [
        [s for s, x in sessions.items() if x["insert"]],
        [s for s, x in sessions.items() if x["update"] and not x["collapse"] and x["group"] == "began in the window"],
        [s for s, x in sessions.items() if x["collapse"]],
        [s for s, x in sessions.items() if (x["update"] or x["insert"]) and x["group"] == "began before the window"],
        [s for s, x in sessions.items() if x["rejected"]],
        [s for s, x in sessions.items() if not (x["insert"] or x["update"] or x["collapse"])],
    ]
    quota = [5, 4, 3, 3, 3, 2]
    out = []
    for pool, q in zip(pools, quota):
        out += [s for s in _pick(pool, q + len(out)) if s not in out][:q]
    rest = [s for s in _pick(list(sessions), len(sessions)) if s not in out]
    return (out + rest)[:n]


def sample_lines(s: dict) -> list[str]:
    cols = [c for c in s["write"] if c != "SessionID"]
    varying = [c for c in cols if len({_short(x["row"].get(c), 200) for x in s["calls"] if x["row"]}) > 1]
    shown = [c for c in cols if c in varying or c in s["changed"]] or cols[:6]
    act = "+".join(a for a in ("insert", "update", "collapse") if s[a]) or "no change"
    lines = [f"{s['sid']}  [{s['group']}] {act}; stored rows {len(s['stored'])}; calls {len(s['calls'])}"]
    for x in s["calls"]:
        vals = "; ".join(f"{c}={_short(x['row'].get(c), 30)}" for c in shown if x["row"] and c in x["row"])
        flag = " SET ASIDE: " + "; ".join(x["errors"])[:120] if x["errors"] else ""
        lines.append(f"    call {x['fired_at'].astimezone(UTC).strftime('%m-%d %H:%M:%S')}Z {x['endpoint']} {x['status']}"
                     f"{' repaired' if x['repaired'] else ''}: {vals}{flag}")
    r0 = s["stored"][0] if s["stored"] else {}
    lines.append("    stored: " + ("; ".join(f"{c}={_short(r0.get(c), 30)}" for c in shown) if r0 else "no row"))
    lines.append("    fold:   " + "; ".join(f"{c}={_short(s['row'].get(c), 30)}" for c in shown))
    return lines


def cmd_list(client, table=TABLE, out=None, keep: set | None = None) -> tuple[str, dict]:
    """keep: check-ins a repair already wrote; they stay in scope on the list that verifies it."""
    run = stamp()
    list_path, plan_path = f"gap_repair_list_{run}.txt", f"gap_repair_plan_{run}.json"
    csv_path, sample_path = f"gap_repair_planned_{run}.csv", f"gap_repair_sample_{run}.txt"
    p = lambda *a: print(*a, file=out)  # noqa: E731
    p("dry run of every statement shape (made-up check-ins):")
    preflight(client, statement_set(table, synthetic_plan(client.get_table(table).schema)))
    plan = build_plan(client, table, keep=keep)
    ctx = plan.pop("_ctx")
    sessions, L = ctx["sessions"], ctx["loaded"]
    before = diagnose(client, table, sessions)
    cols = [f.name for f in ctx["schema"]]

    lines = [f"table {table}; rule: {plan['rule']}; log read up to {plan['log_cutoff']}", ""]
    for s in sorted(sessions.values(), key=lambda s: s["sid"]):
        if not (s["insert"] or s["update"] or s["collapse"]):
            continue
        what = "+".join(a for a in ("insert", "update", "collapse") if s[a])
        lines.append(f"{s['sid']}  [{s['group']}] {what}  stored rows {len(s['stored'])}  calls {len(s['calls'])}")
        r0 = s["stored"][0] if s["stored"] else {}
        for c in s["changed"]:
            lines.append(f"      {c}: {_short(r0.get(c), 60)} -> {_short(s['row'].get(c), 60)}")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        planned_cols = [c for c in cols if any(c in s["row"] for s in sessions.values())]
        w.writerow(["SessionID", "group", "action", "stored_rows", "calls", "changed_columns"] + planned_cols[1:])
        for s in sorted(sessions.values(), key=lambda s: s["sid"]):
            act = "+".join(a for a in ("insert", "update", "collapse") if s[a]) or "none"
            w.writerow([s["sid"], s["group"], act, len(s["stored"]), len(s["calls"]), "|".join(s["changed"])]
                       + [_enc(s["row"].get(c)) if s["row"].get(c) is not None else "" for c in planned_cols[1:]])

    picked = sample_of(sessions)
    sample = []
    for sid in picked:
        sample += sample_lines(sessions[sid]) + [""]
    with open(sample_path, "w", encoding="utf-8") as f:
        f.write("\n".join(sample) + "\n")

    t = plan["totals"]
    by_group = Counter(s["group"] for s in sessions.values())
    colc = Counter(c for s in sessions.values() if s["update"] for c in s["changed"])
    nulled = Counter(c for s in sessions.values() if s["update"] for c in s["changed"]
                     if s["row"].get(c) is None and any(_reply(r.get(c)) is not None for r in s["stored"]))
    act_by_group = defaultdict(Counter)
    for s in sessions.values():
        act_by_group[s["group"]].update({"insert": s["insert"], "update": s["update"], "collapse": s["collapse"]})
    summary = [
        f"log read up to {plan['log_cutoff']}; history from {L['floor'].isoformat()}",
        f"check-ins in scope {len(L['sids'])}: planned {len(sessions)}; with a call staged after the log cutoff "
        f"(left for a later list) {len(ctx['moving'])}; every call rejected by the service's preparation "
        f"{len(ctx['no_row_planned'])}; with at least one call so rejected (the rest folded) {len(ctx['bad'])}",
        f"left out by the window's scope rule: {len(ctx['excluded'])} ("
        + "; ".join(f"{k} {v}" for k, v in Counter(ctx["excluded"].values()).most_common()) + ")",
        "  by group: " + "; ".join(f"{g} {n} (insert {act_by_group[g]['insert']}, update {act_by_group[g]['update']}, "
                                   f"collapse {act_by_group[g]['collapse']})" for g, n in by_group.most_common()),
        f"calls folded {sum(len(s['calls']) for s in sessions.values())}: " + ", ".join(
            f"{ep} {st} {n}" for (ep, st), n in sorted(_counts([c for s in sessions.values() for c in s['calls']]).items())),
        f"payload keys the table lacks (dropped, no column added): {dict(L['unknown']) or 'none'}",
        "",
        f"PLAN: insert {t['insert']}, update {t['update']}, collapse {t['collapse']} (extra rows deleted "
        f"{t['extra_rows']}); check-ins changed {len(plan['sessions'])}",
        "columns changed on existing rows: " + (", ".join(f"{c} {n}" for c, n in colc.most_common()) or "none"),
        "non-NULL -> NULL on existing rows: " + (", ".join(f"{c} {n}" for c, n in nulled.most_common()) or "none")
        + " (the writer stores a blank as NULL; a blank check-in time keeps the stored one)",
        "",
        "diagnosis (check-ins planned):",
    ] + diag_lines(before) + [
        "",
        f"full list:        {list_path}", f"plan:             {plan_path}", f"planned rows:     {csv_path}",
        f"sample ({len(picked)}):      {sample_path}",
        "Nothing was written to BigQuery.",
    ]
    with open(list_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines + [""] + summary) + "\n")
    if plan["sessions"]:
        p("dry run of this plan's statements:")
        preflight(client, statement_set(table, plan))
    plan["diagnosis_before"] = dict(before)
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=1, default=str)
    p("\n".join(summary))
    p("")
    p(f"SAMPLE -- {len(picked)} check-ins, every call (columns that vary across the calls or change on the row):")
    p("\n".join(sample))
    plan["_ctx"] = ctx
    return plan_path, plan


# ---------------------------------------------------------------------------------------------
# apply / rollback
# ---------------------------------------------------------------------------------------------

def refuse_unless_allowed(table: str, production: bool, now=None) -> None:
    if not table.startswith(DEV_PREFIX) and not production:
        raise SystemExit(f"refusing to write {table}: it is not a DEV table. Production needs --production; "
                         f"anything else runs on a DEV clone (rehearse).")
    if not table.startswith(DEV_PREFIX):
        now = now or datetime.now(UTC)
        (h0, m0), (h1, m1) = QUIET_UTC
        if (h0, m0) <= (now.hour, now.minute) < (h1, m1):
            raise SystemExit("refusing a production write 07:00-09:30 UTC (02:00-04:30 CT): the nightly chain "
                             "writes response_data then. Run it outside that window.")


def load_rows(client, plan: dict, rows_table: str) -> int:
    """Planned rows for insert/update into a DEV table: every planned column plus add-to-db's presence marker."""
    cols = plan["columns"]
    schema = [bigquery.SchemaField(c, t) for c, t in cols.items()] + [bigquery.SchemaField(PRESENT_FIELD, "STRING")]
    data = []
    for s in plan["sessions"]:
        if not (s["insert"] or s["update"]):
            continue
        rec = {c: s["row"].get(c) for c in cols}
        for c, t in cols.items():
            if t == "DATETIME" and rec[c] is not None:
                rec[c] = str(rec[c]).replace("T", " ")
        rec[PRESENT_FIELD] = "|" + "|".join(s.get("write") or list(cols)) + "|"
        data.append(rec)
    job = client.load_table_from_json(data, rows_table, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_EMPTY", create_disposition="CREATE_IF_NEEDED"))
    job.result()
    return len(data)


def build_apply_script(table: str, plan: dict, rows_table: str) -> tuple[str, list]:
    cols = list(plan["columns"])
    sess = plan["sessions"]
    merge_s = [s for s in sess if s["insert"] or s["update"]]
    dups = [s for s in sess if s["collapse"]]
    pcol = config.PARTITION_COLUMNS["responses"]
    ranged = [s for s in merge_s if s["row"].get(pcol) is not None]
    keyonly = [s for s in merge_s if s["row"].get(pcol) is None]
    preserve = config.PRESERVE_ON_BLANK["responses"]
    params = [
        bigquery.ArrayQueryParameter("gap_sids", "STRING", [s["sid"] for s in sess]),
        bigquery.ArrayQueryParameter("gap_pre", "RECORD", [bigquery.StructQueryParameter(
            "placeholder", bigquery.ScalarQueryParameter("sid", "STRING", s["sid"]),
            bigquery.ScalarQueryParameter("n", "INT64", s["n"]),
            bigquery.ScalarQueryParameter("fps", "STRING", s["fps"])) for s in sess]),
        bigquery.ArrayQueryParameter("gap_dups", "STRING", [s["sid"] for s in dups]),
    ]
    fp_agg = ("STRING_AGG(CAST(FARM_FINGERPRINT(TO_JSON_STRING(t)) AS STRING), ',' "
              "ORDER BY FARM_FINGERPRINT(TO_JSON_STRING(t)))")
    expected_delta = plan["totals"]["insert"] - plan["totals"]["extra_rows"]
    s = ["DECLARE gap_before INT64", "BEGIN TRANSACTION",
         f"SET gap_before = (SELECT COUNT(*) FROM `{table}`)",
         f"ASSERT (SELECT COUNT(*) FROM UNNEST(@gap_pre) p LEFT JOIN (SELECT SessionID, COUNT(*) n, {fp_agg} fps "
         f"FROM `{table}` t WHERE SessionID IN UNNEST(@gap_sids) GROUP BY SessionID) c ON c.SessionID = p.sid "
         f"WHERE IFNULL(c.n, 0) != p.n OR IFNULL(c.fps, '') != p.fps) = 0 "
         f"AS 'a listed check-in no longer has exactly the rows the list read: rerun list'"]
    for label, group, use_range in (("ranged", ranged, True), ("keyonly", keyonly, False)):
        if not group:
            continue
        sql = build_batch_merge_query(table, cols, ["SessionID"], pcol if use_range else None, preserve,
                                      rows_param="gap_rows", min_param=f"gap_min_{label}", max_param=f"gap_max_{label}")
        rows_from = rows_table if rows_table.startswith("(") else f"`{rows_table}`"
        src = (f"(SELECT * FROM {rows_from} WHERE `{pcol}` IS "
               + ("NOT NULL" if use_range else "NULL") + ")")
        if sql.count("UNNEST(@gap_rows)") != 1:
            raise SystemExit("add-to-db's MERGE text changed shape; refusing to adapt it blindly")
        s.append(sql.replace("UNNEST(@gap_rows)", src).strip())
        expected = sum(1 if x["insert"] else x["n"] for x in group)
        s.append(f"ASSERT @@row_count = {expected} AS 'MERGE ({label}) touched a row count other than {expected}'")
        if use_range:
            vals = [datetime.fromisoformat(str(x["row"][pcol])) for x in group]
            params += [bigquery.ScalarQueryParameter(f"gap_min_{label}", "DATETIME", min(vals)),
                       bigquery.ScalarQueryParameter(f"gap_max_{label}", "DATETIME", max(vals))]
    if dups:
        extra_total = sum(x["n"] for x in dups)
        s += [f"CREATE TEMP TABLE gap_keep AS SELECT * EXCEPT(gap_rn) FROM (SELECT t.*, ROW_NUMBER() OVER ("
              f"PARTITION BY SessionID ORDER BY ARRAY_LENGTH(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r':null[,}}]')), "
              f"TO_JSON_STRING(t)) gap_rn FROM `{table}` t WHERE SessionID IN UNNEST(@gap_dups)) WHERE gap_rn = 1",
              f"ASSERT (SELECT COUNT(*) FROM gap_keep) = {len(dups)} AS 'collapse: kept rows != {len(dups)}'",
              f"DELETE FROM `{table}` WHERE SessionID IN UNNEST(@gap_dups)",
              f"ASSERT @@row_count = {extra_total} AS 'collapse: deleted rows != {extra_total}'",
              f"INSERT INTO `{table}` SELECT * FROM gap_keep",
              f"ASSERT @@row_count = {len(dups)} AS 'collapse: re-inserted rows != {len(dups)}'",
              "DROP TABLE gap_keep"]
    s += [f"ASSERT (SELECT COUNT(*) FROM UNNEST(@gap_sids) x LEFT JOIN (SELECT SessionID, COUNT(*) n FROM `{table}` "
          f"WHERE SessionID IN UNNEST(@gap_sids) GROUP BY SessionID) c ON c.SessionID = x WHERE IFNULL(c.n, 0) != 1) = 0 "
          f"AS 'a listed check-in does not have exactly one row'",
          f"ASSERT (SELECT COUNT(*) FROM `{table}`) = gap_before + {expected_delta} "
          f"AS 'the table row count did not move by exactly {expected_delta}'",
          "COMMIT TRANSACTION"]
    return ";\n".join(s) + ";", params


def build_rollback_script(table: str, applied: dict) -> tuple[str, list]:
    sess = applied["sessions"]
    fp_agg = ("STRING_AGG(CAST(FARM_FINGERPRINT(TO_JSON_STRING(t)) AS STRING), ',' "
              "ORDER BY FARM_FINGERPRINT(TO_JSON_STRING(t)))")
    pre = lambda name, key: bigquery.ArrayQueryParameter(name, "RECORD", [bigquery.StructQueryParameter(  # noqa: E731
        "placeholder", bigquery.ScalarQueryParameter("sid", "STRING", x["sid"]),
        bigquery.ScalarQueryParameter("n", "INT64", x[key][0]),
        bigquery.ScalarQueryParameter("fps", "STRING", x[key][1])) for x in sess])
    check = lambda p, msg: (f"ASSERT (SELECT COUNT(*) FROM UNNEST(@{p}) p LEFT JOIN (SELECT SessionID, COUNT(*) n, {fp_agg} fps "  # noqa: E731
                            f"FROM `{table}` t WHERE SessionID IN UNNEST(@gap_sids) GROUP BY SessionID) c ON c.SessionID = p.sid "
                            f"WHERE IFNULL(c.n, 0) != p.n OR (p.fps IS NOT NULL AND IFNULL(c.fps, '') != p.fps)) = 0 AS '{msg}'")
    if any(x["after"] is None for x in sess):
        sess = [dict(x, after=x["after"] or [1, None]) for x in sess]
    backup_from = applied["backup"] if applied["backup"].startswith("(") else f"`{applied['backup']}`"
    n_after = sum(x["after"][0] for x in sess)
    n_before = sum(x["before"][0] for x in sess)
    s = ["BEGIN TRANSACTION",
         check("gap_state_after", "a repaired check-in changed since the apply (add-to-db or someone else wrote it): not rolled back"),
         f"DELETE FROM `{table}` WHERE SessionID IN UNNEST(@gap_sids)",
         f"ASSERT @@row_count = {n_after} AS 'rollback: deleted rows != {n_after}'",
         f"INSERT INTO `{table}` SELECT * FROM {backup_from} WHERE SessionID IN UNNEST(@gap_sids)",
         f"ASSERT @@row_count = {n_before} AS 'rollback: restored rows != {n_before}'",
         check("gap_state_before", "rollback: restored rows are not the rows the list read"),
         "COMMIT TRANSACTION"]
    params = [bigquery.ArrayQueryParameter("gap_sids", "STRING", [x["sid"] for x in sess]),
              pre("gap_state_after", "after"), pre("gap_state_before", "before")]
    return ";\n".join(s) + ";", params


def dry_run_forms(script: str) -> list[str]:
    """
    Each statement of a generated script in a form BigQuery can dry-run on its own: transaction and
    variable statements skipped, ASSERT as SELECT, the temp table's SELECT inlined where it is used,
    the row-count variable as 0. @@row_count asserts are checked by the real run only.
    """
    out, keep = [], None
    for stmt in [x.strip() for x in script.split(";\n") if x.strip()]:
        stmt = stmt.rstrip(";")
        head = stmt.split(None, 2)[:2]
        if head[0] in ("DECLARE", "BEGIN", "COMMIT", "SET") or stmt.startswith("DROP TABLE gap_keep") \
                or "@@row_count" in stmt:
            continue
        if stmt.startswith("CREATE TEMP TABLE gap_keep AS "):
            keep = stmt[len("CREATE TEMP TABLE gap_keep AS "):]
            out.append(keep)
            continue
        if keep is not None:
            stmt = re.sub(r"\bgap_keep\b", f"({keep})", stmt)
        stmt = re.sub(r"(?<![@\w])gap_before\b", "0", stmt)
        m = re.match(r"^ASSERT (.*) AS '[^']*'$", stmt, re.S)
        out.append(f"SELECT {m.group(1)}" if m else stmt)
    return out


def preflight(client, statements: list[tuple[str, list]]) -> None:
    """Dry-run every statement against BigQuery (validated, nothing runs). Any failure stops before writing."""
    bad = []
    for sql, params in statements:
        used = [p for p in params if re.search(rf"@{re.escape(p.name)}\b", sql)]
        try:
            client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False,
                                                                 query_parameters=used))
        except Exception as e:                      # noqa: BLE001  (report every failure, then stop)
            bad.append(f"{sql[:120]!r}: {str(e).splitlines()[0][:300]}")
    print(f"dry run: {len(statements) - len(bad)} of {len(statements)} statements valid")
    if bad:
        raise SystemExit("STOPPED before writing: statements BigQuery rejects in a dry run:\n  " + "\n  ".join(bad))


def statement_set(table: str, plan: dict, rows_table: str | None = None, backup: str | None = None) -> list:
    """Every statement apply and rollback would send, in dry-runnable form, with parameters.
    Without real DEV tables (list time) the loaded rows are a typed empty SELECT and the backup is the table itself."""
    cols = plan["columns"]
    rows_from = rows_table or ("(SELECT " + ", ".join(
        [f"CAST(NULL AS {BQ_TYPE_MAP[t]}) AS {quote_identifier(c)}" for c, t in cols.items()]
        + [f"CAST(NULL AS STRING) AS {PRESENT_FIELD}"]) + " FROM UNNEST([1]) WHERE FALSE)")
    backup_from = backup or f"(SELECT * FROM `{table}` WHERE FALSE)"
    sids = [x["sid"] for x in plan["sessions"]]
    sid_param = [bigquery.ArrayQueryParameter("s", "STRING", sids)]
    sql, params = build_apply_script(table, plan, rows_from)
    rb, rb_params = build_rollback_script(table, {"backup": backup_from, "sessions": [
        {"sid": x["sid"], "before": [x["n"], x["fps"]], "after": [1, ""]} for x in plan["sessions"]]})
    return ([(f"CREATE TABLE `{DEV_PREFIX}adb_gap_repair_backup_dryrun` AS SELECT * FROM `{table}` "
              f"WHERE SessionID IN UNNEST(@s)", sid_param)]
            + [(x, params) for x in dry_run_forms(sql)] + [(x, rb_params) for x in dry_run_forms(rb)]
            + [(read_stored_sql(table, ["SessionID"]), sid_param), (read_stored_sql(table, list(cols)), sid_param)])


def synthetic_plan(schema) -> dict:
    """Three made-up check-ins covering every statement shape: ranged insert, update + collapse, key-only insert."""
    cols = {f.name: f.field_type.upper() for f in schema if f.field_type.upper() not in ("JSON", "RECORD", "STRUCT")}
    names = {c.lower(): c for c in cols}
    write = ["SessionID"] + [names[c.lower()] for c in ("checkinDateTime", "wellnessDomain", "checkinReply") if c.lower() in names]
    pcol = config.PARTITION_COLUMNS["responses"]
    t = "2026-09-27 17:00:00"
    return {"columns": cols, "totals": {"insert": 2, "update": 1, "collapse": 1, "extra_rows": 1}, "sessions": [
        {"sid": "gap-dryrun-a", "insert": True, "update": False, "collapse": False, "n": 0, "fps": "",
         "write": list(cols), "row": {"SessionID": "gap-dryrun-a", pcol: t}},
        {"sid": "gap-dryrun-b", "insert": False, "update": True, "collapse": True, "n": 2, "fps": "1,2",
         "write": write, "row": {"SessionID": "gap-dryrun-b", pcol: t}},
        {"sid": "gap-dryrun-c", "insert": True, "update": False, "collapse": False, "n": 0, "fps": "",
         "write": list(cols), "row": {"SessionID": "gap-dryrun-c", pcol: None}}]}


def job_cost(client, job) -> str:
    secs = (job.ended - job.started).total_seconds() if job.ended and job.started else None
    children = []
    try:
        children = list(client.list_jobs(parent_job=job.job_id))
    except Exception:
        pass
    billed = sum((c.total_bytes_billed or 0) for c in children) if children else (job.total_bytes_billed or 0)
    return (f"{secs:.1f} s, " if secs is not None else "") + f"{billed:,} bytes billed, {len(children)} statements"


def fp_state(client, table, sids) -> dict:
    stored = read_stored(client, table, sids, ["SessionID"])
    return {sid: [len(stored.get(sid, [])), fps_of(stored.get(sid, []))] for sid in sids}


class Pause:
    """The flush maintenance pause (OPS), on for the duration; always cleared."""

    def __init__(self, client, enabled: bool):
        self.client, self.enabled = client, enabled

    def __enter__(self):
        if self.enabled:
            import flush_pause
            flush_pause.set_pause(self.client, FLUSH_STATE, True)
            st = flush_pause.state(self.client, FLUSH_STATE)
            if len(st) != 2 or any(r["paused_since"] is None for r in st):
                flush_pause.set_pause(self.client, FLUSH_STATE, False)
                raise SystemExit("the flush maintenance pause did not take; nothing was written")
            print("flush maintenance pause ON (OPS)")
        return self

    def __exit__(self, *exc):
        if self.enabled:
            import flush_pause
            flush_pause.set_pause(self.client, FLUSH_STATE, False)
            st = flush_pause.state(self.client, FLUSH_STATE)
            ok = len(st) == 2 and all(r["paused_since"] is None for r in st)
            print("flush maintenance pause OFF (OPS)" if ok else
                  "FAIL: the flush maintenance pause is still set. Run: python tools\\flush_pause.py off")
        return False


def _expected_fps(plan: dict) -> dict:
    return {s["sid"]: [s["n"], s["fps"]] for s in plan["sessions"] if s["n"]}


def restore_sql(table: str, backup: str) -> str:
    return (f"BEGIN TRANSACTION;\nDELETE FROM `{table}` WHERE SessionID IN UNNEST(@gap_sids);\n"
            f"INSERT INTO `{table}` SELECT * FROM `{backup}` WHERE SessionID IN UNNEST(@gap_sids);\nCOMMIT TRANSACTION;")


def cmd_backup(client, plan: dict, backup: str) -> bool:
    """
    Copy the current rows of every check-in the plan changes into one DEV table, check the copy is exactly the
    rows the list read (count and fingerprints), and print (and dry-run) the statement that puts them back.
    Writes only the DEV table; refuses any other dataset and an existing table.
    """
    if not backup.startswith(DEV_PREFIX):
        raise SystemExit(f"refusing: the backup must be a DEV table, not {backup}")
    table = plan["table"]
    sids = [x["sid"] for x in plan["sessions"]]
    sid_param = [bigquery.ArrayQueryParameter("s", "STRING", sids)]
    sql = f"CREATE TABLE `{backup}` AS SELECT * FROM `{table}` WHERE SessionID IN UNNEST(@s)"
    preflight(client, [(sql, sid_param)])
    client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=sid_param)).result()
    got = fp_state(client, backup, sids)
    want = _expected_fps(plan)
    rows = sum(v[0] for v in got.values())
    differ = [sid for sid in sids if got[sid] != want.get(sid, [0, ""])]
    print(f"backup {backup}: {rows} rows for {len(sids)} check-ins (the list read {sum(v[0] for v in want.values())} "
          f"rows; {len(sids) - len(want)} check-ins have no row yet and are inserts)")
    print(f"check-ins whose backed-up rows differ from the rows the list read: {len(differ)}"
          + (f" (first {differ[:3]})" if differ else ""))
    rs = restore_sql(table, backup)
    stmts = [x.strip() for x in rs.split(";\n") if x.strip() and not x.strip().startswith(("BEGIN", "COMMIT"))]
    preflight(client, [(x.rstrip(";"), [bigquery.ArrayQueryParameter("gap_sids", "STRING", sids)]) for x in stmts])
    print("restore statement (@gap_sids = the plan's check-ins; the rollback command runs it with its checks):")
    print(rs)
    ok = not differ and rows == sum(v[0] for v in want.values())
    print(f"BACKUP {'PASS' if ok else 'FAIL'}: {rows} rows kept in {backup}")
    return ok


def cmd_apply(client, plan: dict, production: bool, pause: bool | None = None, created: list | None = None,
              backup: str | None = None) -> dict:
    table = plan["table"]
    refuse_unless_allowed(table, production)
    created = created if created is not None else []
    run = stamp()
    made_backup = backup is None
    if backup is None:
        backup = f"{DEV_PREFIX}adb_gap_repair_backup_{run}"
        rows_table = f"{DEV_PREFIX}adb_gap_repair_rows_{run}"
    else:
        if not backup.startswith(DEV_PREFIX):
            raise SystemExit(f"refusing: the backup must be a DEV table, not {backup}")
        rows_table = backup.replace("_backup_", "_rows_") if "_backup_" in backup else backup + "_rows"
    path = f"gap_repair_applied_{run}.json"
    with Pause(client, production if pause is None else pause):
        staged = staged_since(client, datetime.fromisoformat(plan["log_cutoff"]))
        dropped = [s["sid"] for s in plan["sessions"] if s["sid"] in staged]
        if dropped:
            plan = dict(plan, sessions=[s for s in plan["sessions"] if s["sid"] not in staged])
            t = plan["sessions"]
            plan["totals"] = {"insert": sum(s["insert"] for s in t), "update": sum(s["update"] for s in t),
                              "collapse": sum(s["collapse"] for s in t),
                              "extra_rows": sum(s["n"] - 1 for s in t if s["collapse"])}
        print(f"check-ins dropped from the plan because add-to-db has staged a call for them after the log the list "
              f"read: {len(dropped)}")
        sids = [s["sid"] for s in plan["sessions"]]
        sid_param = [bigquery.ArrayQueryParameter("s", "STRING", sids)]
        if made_backup:
            backup_sql = f"CREATE TABLE `{backup}` AS SELECT * FROM `{table}` WHERE SessionID IN UNNEST(@s)"
            preflight(client, [(backup_sql, sid_param)])
            client.query(backup_sql, job_config=bigquery.QueryJobConfig(query_parameters=sid_param)).result()
            created.append(backup)
            nb = list(client.query(f"SELECT COUNT(*) n FROM `{backup}`").result())[0]["n"]
            print(f"backup: {backup} ({nb} rows)")
        else:
            got, want = fp_state(client, backup, sids), _expected_fps(plan)
            bad = [sid for sid in sids if got[sid] != want.get(sid, [0, ""])]
            if bad:
                raise SystemExit(f"STOPPED before writing: {len(bad)} check-ins in {backup} are not the rows the list "
                                 f"read (first {bad[:3]}); nothing was written")
            print(f"backup {backup}: checked, {sum(v[0] for v in got.values())} rows are the rows the list read")
        created.append(rows_table)
        loaded = load_rows(client, plan, rows_table)
        print(f"planned rows loaded: {rows_table} ({loaded} rows)")
        before = {s["sid"]: [s["n"], s["fps"]] for s in plan["sessions"]}
        sql, params = build_apply_script(table, plan, rows_table)
        rb_sql, rb_params = build_rollback_script(table, {"backup": backup, "sessions": [
            {"sid": sid, "before": before[sid], "after": [1, ""]} for sid in sids]})
        preflight(client, [(x, params) for x in dry_run_forms(sql)] + [(x, rb_params) for x in dry_run_forms(rb_sql)]
                  + [(read_stored_sql(table, ["SessionID"]), sid_param)])
        applied = {"table": table, "backup": backup, "rows_table": rows_table, "dropped": dropped,
                   "applied_at": None, "sessions": [{"sid": sid, "before": before[sid], "after": None} for sid in sids]}
        with open(path, "w", encoding="utf-8") as f:            # written before the transaction: rollback needs it
            json.dump(applied, f, indent=1)
        job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        job.result()
        t = plan["totals"]
        print(f"APPLIED in one transaction (committed) on {table}: insert {t['insert']}, update {t['update']}, "
              f"collapse {t['collapse']} (extra rows deleted {t['extra_rows']}); {job_cost(client, job)}")
        applied["applied_at"] = datetime.now(UTC).isoformat()
        after = fp_state(client, table, sids)          # read while the flush is still paused
        for x in applied["sessions"]:
            x["after"] = after[x["sid"]]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(applied, f, indent=1)
    print(f"applied record (rollback needs it): {path}")
    applied["_path"] = path
    return applied


def cmd_rollback(client, applied: dict, production: bool, pause: bool | None = None) -> None:
    table = applied["table"]
    refuse_unless_allowed(table, production)
    with Pause(client, production if pause is None else pause):
        sql, params = build_rollback_script(table, applied)
        job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        job.result()
        print(f"ROLLED BACK {len(applied['sessions'])} check-ins on {table} in one transaction (committed); "
              f"{job_cost(client, job)}")


# ---------------------------------------------------------------------------------------------
# rehearse
# ---------------------------------------------------------------------------------------------

def cmd_rehearse(client, keep=False) -> bool:
    run = stamp()
    clone = f"{DEV_PREFIX}adb_gap_rehearsal_{run}"
    client.query(f"CREATE TABLE `{clone}` CLONE `{TABLE}`").result()
    print(f"clone: {clone} (of {TABLE} as of now)")
    made = [clone]
    try:
        plan_path, plan = cmd_list(client, table=clone)
        plan.pop("_ctx")
        before_fp = {s["sid"]: [s["n"], s["fps"]] for s in plan["sessions"]}
        applied = cmd_apply(client, plan, production=False, pause=False, created=made)
        for sid in applied["dropped"]:
            before_fp.pop(sid, None)
        print("\nrehearsal: list again on the repaired clone (the repair must leave nothing to do):")
        _, again = cmd_list(client, table=clone, keep=set(before_fp))
        again.pop("_ctx")
        t2 = again["totals"]
        diag_before, diag_after = Counter(plan["diagnosis_before"]), Counter(again["diagnosis_before"])
        print("\nrehearsal diagnosis on the clone:")
        print("\n".join(diag_lines(diag_before, diag_after)))
        cmd_rollback(client, applied, production=False, pause=False)
        back = fp_state(client, clone, list(before_fp))
        restored = sum(back[sid] == before_fp[sid] for sid in before_fp)
        idem = t2["insert"] == t2["update"] == t2["collapse"] == 0
        ok = diag_explained(diag_after) and idem and restored == len(before_fp)
        print(f"\nREHEARSAL {'PASS' if ok else 'FAIL'}: after apply no row {diag_after['no row']}, duplicated "
              f"{diag_after['duplicated']}, differs from the fold {diag_after['differs from the fold']} (want 0 each); "
              f"second list: insert {t2['insert']}, update {t2['update']}, collapse {t2['collapse']} (want 0); "
              f"rollback restored {restored} of {len(before_fp)} check-ins exactly")
        return ok
    finally:
        if keep:
            print(f"kept {', '.join(made)}")
        else:
            for t in made:
                client.delete_table(t, not_found_ok=True)
            print(f"dropped {', '.join(made)}")


# ---------------------------------------------------------------------------------------------
# history: what the old writer got wrong before the window (read-only)
# ---------------------------------------------------------------------------------------------

# Spans with no webhook of any kind in the log while traffic ran (read from OPS.webhook_log, httplog_id contiguity):
# a call fired in one of them was not logged, so a check-in that had one cannot be rebuilt exactly.
HOLES = [(datetime(2026, 8, 26, 16, 59, 32, tzinfo=UTC), datetime(2026, 8, 26, 18, 52, 6, tzinfo=UTC), "hole"),
         (datetime(2026, 9, 3, 7, 1, 25, tzinfo=UTC), datetime(2026, 9, 3, 9, 25, 58, tzinfo=UTC), "possible hole")]
DT_TYPES = ("DATETIME", "TIMESTAMP")
H_CLASSES = ["no row", "more than one row", "one row: its own last call (the old contract met)",
             "one row: an earlier call's values (a later call lost: dropped or out of order)",
             "one row: no call's values"]


def ct_day(d: str) -> datetime:
    """Midnight Central time on that date, in UTC. Windows has no time-zone database of its own, so zoneinfo needs
    the tzdata package; pytz (a dev requirement of this repository) carries its own."""
    naive = datetime.strptime(d, "%Y-%m-%d")
    try:
        from zoneinfo import ZoneInfo
        return naive.replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(UTC)
    except Exception:  # noqa: BLE001 -- ZoneInfoNotFoundError, or no zoneinfo
        import pytz
        return pytz.timezone("America/Chicago").localize(naive).astimezone(UTC)


def history_seed(client, start, end, read_to):
    """Check-ins whose session id's own time is in [start, end), found in any logged call up to read_to;
    and the calls without a session id fired in [start, end)."""
    sid_t = r"SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SUBSTR(sid, 37))"
    sql = (f"WITH c AS (SELECT fired_at, {SID_SQL} sid FROM `{LOG}` WHERE fired_at >= @floor AND fired_at < @to "
           f"AND {IS_RESP_SQL}) "
           f"SELECT sid, {sid_t} started, fired_at FROM c WHERE (sid IS NULL OR sid = '') AND fired_at >= @s AND "
           f"fired_at < @e UNION ALL SELECT DISTINCT sid, {sid_t} started, CAST(NULL AS TIMESTAMP) FROM c "
           f"WHERE sid != '' AND {sid_t} >= @s AND {sid_t} < @e")
    rows = list(client.query(sql, job_config=_params(floor=start - timedelta(minutes=10), to=read_to, s=start, e=end,
                                                     old=OLD_PATHS, new=NEW_PATH, host=NEW_HOST)).result())
    return sorted({r["sid"] for r in rows if r["sid"]}), sum(1 for r in rows if not r["sid"])


def _in_hole(t) -> str | None:
    t = _aware(t) if not isinstance(t, datetime) else (t if t.tzinfo else t.replace(tzinfo=UTC))
    return next((kind for a, b, kind in HOLES if t is not None and a <= t <= b), None)


def snapshot_match(snaps: list[dict], row: dict) -> int | None:
    """The index of the last call whose own values equal the stored row in every column the call carried."""
    hit = None
    for i, snap in enumerate(snaps):
        if all(same(row.get(c), v) for c, v in snap.items()):
            hit = i
    return hit


def history_session(sid, calls, rows, folded, types) -> dict:
    """One check-in: which class of the old writer's failure its stored rows show, and in which columns."""
    valid = [c for c in calls if c["row"] is not None and not c["errors"] and not c["keyless"]]
    snaps = [c["snap"] for c in valid]
    out = {"sid": sid, "calls": len(calls), "rows": len(rows), "rejected_calls": sum(bool(c["errors"]) for c in calls),
           "guarded_calls": sum(c["guarded"] for c in valid), "exclude": None, "unlogged": None, "class": None,
           "match": None, "diff": [], "own_diff": [], "stale_reply": False, "time_blanked": False, "carried_then_null": [],
           "yes_missing": False, "reply_differs": False}
    if any(c["endpoint"].startswith("add-to-db") for c in calls):
        out["exclude"] = "a call went to add-to-db (load test)"
        return out
    if any(c["fired_at"] >= WINDOW_START for c in calls):
        out["exclude"] = "a call reaches the repaired window"
        return out
    times = [sid_time(sid)] + [r.get(c) for r in rows for c, t in types.items() if t in DT_TYPES]
    out["unlogged"] = next((k for k in (_in_hole(t) for t in times if t is not None) if k), None)
    if out["unlogged"]:
        return out
    if not valid:
        out["exclude"] = "every call rejected by preparation"
        return out
    new = folded
    old = snaps[-1]                                            # the old contract: the last call's own values
    out["stale_reply"] = any(not same(old.get(c), new.get(c)) for c in REPLY_FIELDS if c in old)
    out["time_blanked"] = old.get("checkinDateTime") is None and new.get("checkinDateTime") is not None
    out["carried_then_null"] = sorted({c for s in snaps for c, v in s.items()
                                       if _reply(v) is not None and new.get(c) is None and c != "SessionID"})
    want = _reply(new.get("checkinReply"))
    out["yes_missing"] = want == "Yes" and not any(r.get("checkinReply") == "Yes" for r in rows)
    out["reply_differs"] = bool(rows) and any(_reply(r.get("checkinReply")) != want for r in rows)
    if not rows:
        out["class"] = H_CLASSES[0]
        return out
    out["diff"] = sorted({c for r in rows for c in new if c != "SessionID" and differs(r, new, c)})
    out["own_diff"] = sorted({c for r in rows for c in old if c != "SessionID" and not same(r.get(c), old.get(c))})
    if len(rows) > 1:
        out["class"] = H_CLASSES[1]
        return out
    out["match"] = snapshot_match(snaps, rows[0])
    out["class"] = (H_CLASSES[2] if out["match"] == len(snaps) - 1 else
                    H_CLASSES[3] if out["match"] is not None else H_CLASSES[4])
    return out


def cmd_history(client, frm: str, to: str, table=TABLE, out=None) -> bool:
    """Read-only. The check-ins that began in [frm, to) (CT dates), rebuilt from every logged call and compared to
    the stored rows: the old writer's failures by class and column. Files stay on this machine."""
    p = lambda *a: print(*a, file=out)  # noqa: E731
    start, end = ct_day(frm), ct_day(to)
    if start < LOG_FLOOR or end > WINDOW_START + timedelta(days=1) or end <= start:
        raise SystemExit("history covers check-ins that began between 2026-08-25 and the repaired window")
    svc = load_svc(client)
    schema = list(client.get_table(table).schema)
    types = {f.name: f.field_type.upper() for f in schema}
    read_to = WINDOW_END
    sids, keyless = history_seed(client, start, end, read_to)
    calls = defaultdict(list)
    wanted = set(sids)
    for r in (history_rows(client, sids, start - timedelta(minutes=10), read_to) if sids else []):
        for c in make_calls(svc, schema, r):
            sid = c["sid"] if c["sid"] in wanted else (c["raw_sid"] if c.get("raw_sid") in wanted else None)
            if sid:
                calls[sid].append(c)
    for cs in calls.values():
        cs.sort(key=lambda c: c["order"])
    folded = fold(schema, [c for cs in calls.values() for c in cs])
    stored = read_stored(client, table, sids, [f.name for f in schema])
    res = [history_session(sid, calls[sid], stored.get(sid, []), folded.get(sid, {}), types) for sid in sids]

    run = stamp()
    csv_path, sum_path = f"history_{frm}_{to}_{run}.csv", f"history_{frm}_{to}_{run}.txt"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["SessionID", "excluded", "unlogged", "class", "calls", "stored_rows", "rejected_calls",
                    "matched_call", "differs_from_fold", "differs_from_last_call", "stale_reply_stored",
                    "checkin_time_blanked", "carried_then_null", "yes_missing", "reply_differs"])
        for x in res:
            w.writerow([x["sid"], x["exclude"] or "", x["unlogged"] or "", x["class"] or "", x["calls"], x["rows"],
                        x["rejected_calls"], "" if x["match"] is None else x["match"] + 1, "|".join(x["diff"]),
                        "|".join(x["own_diff"]), x["stale_reply"], x["time_blanked"], "|".join(x["carried_then_null"]),
                        x["yes_missing"], x["reply_differs"]])
    done = [x for x in res if x["class"]]
    cls = Counter(x["class"] for x in done)
    exc = Counter(x["exclude"] for x in res if x["exclude"])
    unl = Counter(x["unlogged"] for x in res if x["unlogged"])
    col = Counter(c for x in done for c in x["diff"])
    own = Counter(c for x in done for c in x["own_diff"])
    ctn = Counter(c for x in done for c in x["carried_then_null"])
    lines = [
        f"HISTORY {frm} to {to} (CT, session start) -- table {table}; calls read {start.isoformat()} to {read_to.isoformat()}",
        f"check-ins that began in the slice: {len(sids)}; calls without a session id fired in the slice: {keyless}",
        "not diagnosed: " + ("; ".join(f"{k} {n}" for k, n in exc.most_common()) or "none"),
        "unlogged (a session start or stored time inside a log hole): " + ("; ".join(f"{k} {n}" for k, n in unl.items()) or "none"),
        f"diagnosed: {len(done)}",
    ] + [f"  {k:<78} {cls.get(k, 0):>7}" for k in H_CLASSES] + [
        f"  reply differs from the rebuilt check-in                                       {sum(x['reply_differs'] for x in done):>7}",
        f"  rebuilt reply is Yes, no stored row says Yes                                  {sum(x['yes_missing'] for x in done):>7}",
        f"  previous session's reply kept (the old contract has no stale guard)           {sum(x['stale_reply'] for x in done):>7}",
        f"  check-in time blanked by a later call (old contract)                          {sum(x['time_blanked'] for x in done):>7}",
        f"  calls rejected by preparation (the old writer answered 500 on these)          {sum(x['rejected_calls'] for x in res):>7}",
        "stored differs from the rebuilt check-in, by column (check-ins): "
        + (", ".join(f"{c} {n}" for c, n in col.most_common()) or "none"),
        "stored differs from the check-in's own last call, by column (check-ins): "
        + (", ".join(f"{c} {n}" for c, n in own.most_common()) or "none"),
        "a value an earlier call carried is NULL at the end, by column (check-ins): "
        + (", ".join(f"{c} {n}" for c, n in ctn.most_common()) or "none"),
        f"per check-in: {csv_path} (subscriber data: stays on this machine)",
        "Nothing was written to BigQuery.",
    ]
    with open(sum_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    p("\n".join(lines))
    return True


def main_(argv=None):
    for stream in (sys.stdout, sys.stderr):          # reply text carries emoji; never depend on the code page
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("calls", "repaired", "check", "list", "history", "backup", "rehearse", "apply",
                                       "rollback"))
    ap.add_argument("file", nargs="?", help="backup and apply: the plan; rollback: the applied record")
    ap.add_argument("--backup", help="backup: the DEV table to create; apply: the DEV backup made by backup")
    ap.add_argument("--table", default=TABLE, help="list and check: the table to read")
    ap.add_argument("--production", action="store_true")
    ap.add_argument("--keep", action="store_true", help="rehearse: keep the clone, backup and loaded rows")
    ap.add_argument("--from", dest="frm", help="history: first CT date of the slice (YYYY-MM-DD)")
    ap.add_argument("--to", help="history: CT date after the slice (YYYY-MM-DD)")
    args = ap.parse_args(argv)
    if args.action in ("backup", "apply", "rollback"):
        if not args.file:
            raise SystemExit(f"{args.action} needs its file")
        with open(args.file, encoding="utf-8") as f:
            doc = json.load(f)
        if args.action != "backup":
            refuse_unless_allowed(doc["table"], args.production)        # before any sign-in or query
        if args.action == "backup" and not args.backup:
            raise SystemExit("backup needs --backup DEV-table-name")
    started = time.monotonic()
    client = make_client()
    jobs = JobLog(client)
    try:
        ok = _run(client, args, doc if args.action in ("backup", "apply", "rollback") else None)
    finally:
        if args.action == "apply" and args.backup:
            rows_table = args.backup.replace("_backup_", "_rows_") if "_backup_" in args.backup else args.backup + "_rows"
            client.delete_table(rows_table, not_found_ok=True)
            print(f"dropped {rows_table} (the planned rows, loaded for the MERGE only)")
        billed = sum((r["bytes_billed"] or 0) for r in jobs.stats())
        print(f"measured: {(time.monotonic() - started) / 60:.1f} minutes, {billed / 1e9:.2f} GB billed "
              f"({len(jobs.jobs)} BigQuery jobs)")
    if not ok:
        raise SystemExit(1)


def _run(client, args, doc) -> bool:
    if args.action == "calls":
        ok = cmd_calls(client)
    elif args.action == "repaired":
        ok = cmd_repaired(client)
    elif args.action == "check":
        ok = cmd_check(client, table=args.table)
    elif args.action == "list":
        cmd_list(client, table=args.table)
        ok = True
    elif args.action == "history":
        if not (args.frm and args.to):
            raise SystemExit("history needs --from and --to")
        ok = cmd_history(client, args.frm, args.to, table=args.table)
    elif args.action == "backup":
        ok = cmd_backup(client, doc, args.backup)
    elif args.action == "rehearse":
        ok = cmd_rehearse(client, keep=args.keep)
    elif args.action == "apply":
        applied = cmd_apply(client, doc, production=args.production, backup=args.backup)
        print("\nafter the apply: list again (read-only)")
        _, again = cmd_list(client, table=doc["table"], keep={x["sid"] for x in applied["sessions"]})
        again.pop("_ctx")
        before, after = Counter(doc["diagnosis_before"]), Counter(again["diagnosis_before"])
        print("\ndiagnosis, the list read before the apply vs now:")
        print("\n".join(diag_lines(before, after)))
        t2 = again["totals"]
        ok = diag_explained(after) and t2["insert"] == t2["update"] == t2["collapse"] == 0
        print(f"\nAPPLY {'VERIFIED' if ok else 'NOT VERIFIED'}: second list insert {t2['insert']}, update {t2['update']}, "
              f"collapse {t2['collapse']} (want 0); applied record {applied['_path']}")
    else:
        cmd_rollback(client, doc, production=args.production)
        ok = True
    return ok


if __name__ == "__main__":
    main_()

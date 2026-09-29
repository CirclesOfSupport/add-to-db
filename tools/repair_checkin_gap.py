"""
Repair the check-in rows the old writer (get-responses_v2) lost or left wrong between the
response_data partition swap (2026-09-26 06:49 CT, 11:49 UTC) and the switch to add-to-db
(2026-09-28 19:56 CT; its last call 2026-09-29 00:56:33 UTC).

Source of truth, per check-in (SessionID):
  * the check-in's LAST call to the old writer in the window, from OPS.webhook_log_detail, prepared
    with add-to-db's own code (key casing, typed values, UTC datetimes, blank -> NULL, stale-reply
    guard: a reply dated before the check-in is not a reply);
  * TextIt runs of the check-in flow for the reply fields: a run is matched on contact + created_on
    within 5 s of the check-in time; where run and call disagree, the run wins and the row is listed.

Close-out calls are not check-in updates. When a contact's next check-in starts, the flow re-sends
the PREVIOUS check-in's body at the same second, built from contact fields that are already partly
the new check-in's (state, nudges, wellness domain). A call fired within 2 s of the start of another
check-in of the same contact is a close-out; it is never used as a source. A check-in whose only
calls in the window are close-outs is not repaired (its row predates the window: out of scope).

Also out of scope: check-ins with any call in OPS.adb_staging (add-to-db owns them), calls with no
SessionID (sign-up events), the users table, and every row outside the check-ins listed.

    python tools/repair_checkin_gap.py list --flows UUID[,UUID] [--table T] [--runs FILE]
        read-only (BigQuery reads; TextIt runs read with TEXTIT_API_TOKEN). Console: summary only.
        Writes gap_repair_list_<stamp>.txt (every check-in, every change, why), the plan
        gap_repair_plan_<stamp>.json, and the runs it read gap_repair_runs_<stamp>.json (subscriber
        reply text: keep it on this machine; --runs reuses it instead of reading TextIt again).
    python tools/repair_checkin_gap.py rehearse --runs FILE [--keep]
        on a DEV clone of today's RESPONSES.response_data: list, diagnose, apply (backup, one
        transaction), diagnose again, roll back, check every row is back; drops the clone, backup and
        loaded rows (--keep keeps them). Writes nothing outside DEV.
    python tools/repair_checkin_gap.py apply PLAN [--production]
        production only with --production, and never 02:00-04:30 CT (07:00-09:30 UTC: covers the
        nightly chain in daylight and standard time). Sets the flush maintenance pause (OPS), drops
        from the plan any check-in add-to-db has since staged, backs up the plan's rows to DEV, loads
        the planned rows to DEV, then ONE transaction: asserts every listed row is as listed (a row
        add-to-db flushed since the list fails it: rerun list); a call add-to-db stages during the apply
        is flushed after the commit, when the pause clears, so its newer values win; inserts
        and updates with add-to-db's own MERGE; collapses duplicates to one row; asserts exactly one
        row per check-in and the table's row count moved by exactly inserts - extra rows. Clears the
        pause (also on failure), reruns the diagnosis and writes gap_repair_applied_<stamp>.json.
    python tools/repair_checkin_gap.py rollback APPLIED [--production]
        one transaction: asserts every repaired row is still exactly as the apply left it, then puts
        back the backup. Same flags and pause as apply.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from _harness import PROJECT, make_client, stamp

from google.cloud import bigquery

import config  # noqa: E402  (src/ is on sys.path via _harness)
from bq_writer import (  # noqa: E402
    PRESENT_FIELD, apply_stale_reply_guard, build_batch_merge_query, coerce_payload_to_schema,
    normalize_payload_to_schema, parse_datetime_like, quote_identifier)

UTC = timezone.utc
TABLE = f"{PROJECT}.RESPONSES.response_data"
DEV_PREFIX = f"{PROJECT}.DEV."
LOG = f"{PROJECT}.OPS.webhook_log_detail"
STAGING = f"{PROJECT}.OPS.adb_staging"
FLUSH_STATE = f"{PROJECT}.OPS.adb_flush_state"
OLD_PATH = "/get-responses_v2/v2/add"
WINDOW_START = datetime(2026, 9, 26, 11, 49, 0, tzinfo=UTC)     # partition swap, 06:49 CT
WINDOW_END = datetime(2026, 9, 29, 0, 57, 0, tzinfo=UTC)        # after the old writer's last call (00:56:33)
CLOSEOUT_S = 2
RUN_MATCH_S = 5
REPLY_AGREE_S = 2
GATE_MIN = 50       # the list stops if the runs would change more replies than this AND more than the calls do
MAPPING_MIN = 20
REPLY_FIELDS = config.STALE_REPLY_FIELDS["responses"]
CONVENTIONS = config.DATETIME_CONVENTIONS["responses"]
TEXTIT_RUNS = "https://textit.com/api/v2/runs.json"
QUIET_UTC = ((7, 0), (9, 30))       # no production write 07:00-09:30 UTC


# ---------------------------------------------------------------------------------------------
# calls
# ---------------------------------------------------------------------------------------------

def _aware(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        v = parse_datetime_like(str(value))
    except ValueError:
        return None
    return v if v.tzinfo else v.replace(tzinfo=UTC)


def parse_call(httplog_id, fired_at, body_text):
    """One logged call -> dict, or None when the body has no Responses object."""
    try:
        body = json.loads(body_text)
    except (TypeError, ValueError):
        return None
    resp = body.get("Responses") if isinstance(body, dict) else None
    if not isinstance(resp, dict):
        return None
    users = body.get("Users") if isinstance(body.get("Users"), dict) else {}
    get = lambda d, k: next((v for kk, v in d.items() if kk.lower() == k.lower()), None)  # noqa: E731
    sid = unquote(str(get(resp, "sessionID") or "")).strip()
    return {"id": httplog_id, "fired_at": fired_at, "resp": resp, "users": users, "sid": sid,
            "uuid": unquote(str(get(resp, "uuid") or "")).strip(),
            "checkin": _aware(get(resp, "checkinDateTime"))}


def fetch_calls(client) -> list[dict]:
    sql = (f"SELECT httplog_id, fired_at, request_body FROM `{LOG}` WHERE request_path = @p "
           f"AND fired_at >= @s AND fired_at < @e ORDER BY fired_at, httplog_id")
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("p", "STRING", OLD_PATH),
        bigquery.ScalarQueryParameter("s", "TIMESTAMP", WINDOW_START),
        bigquery.ScalarQueryParameter("e", "TIMESTAMP", WINDOW_END)])
    calls = []
    for r in client.query(sql, job_config=cfg).result():
        c = parse_call(r["httplog_id"], r["fired_at"], r["request_body"])
        if c is not None:
            calls.append(c)
    calls.sort(key=lambda c: (c["fired_at"], c["id"]))
    return calls


def mark_closeouts(calls: list[dict]) -> None:
    """call["closeout"] = True when another check-in of the same contact starts within CLOSEOUT_S of it."""
    starts = defaultdict(set)
    for c in calls:
        if c["sid"] and c["uuid"] and c["checkin"] is not None:
            starts[c["uuid"]].add((c["sid"], c["checkin"]))
    for c in calls:
        c["closeout"] = any(sid != c["sid"] and abs((ci - c["fired_at"]).total_seconds()) <= CLOSEOUT_S
                            for sid, ci in starts.get(c["uuid"], ()))


def staged_sessions(client) -> set[str]:
    sql = (f"SELECT DISTINCT COALESCE(JSON_VALUE(payload, '$.sessionID'), JSON_VALUE(payload, '$.SessionID'), "
           f"JSON_VALUE(payload, '$.sessionid')) sid FROM `{STAGING}` WHERE target = 'responses'")
    return {unquote(str(r["sid"])).strip() for r in client.query(sql).result() if r["sid"]}


# ---------------------------------------------------------------------------------------------
# row preparation: add-to-db's own code, in plan_target_writes' order, without schema changes
# ---------------------------------------------------------------------------------------------

def prepare_row(svc, schema, resp: dict):
    """
    (row, errors, unknown_keys, guarded). Same steps as main.plan_target_writes for one item, except
    that a key the table lacks is dropped and reported instead of added as a column.
    """
    names = {f.name.lower() for f in schema}
    unknown = sorted(k for k in resp if k.lower() not in names)
    normalized, n_err = normalize_payload_to_schema(dict(resp), schema)
    coerced, c_err = coerce_payload_to_schema(normalized, schema, CONVENTIONS)
    errors, _ = svc.validate_payload(coerced, schema)
    errors = [e for e in errors + n_err + c_err if "not found in BigQuery schema" not in e]
    row = svc.filter_to_schema(coerced, schema)
    guarded_row = apply_stale_reply_guard(row, REPLY_FIELDS)
    return guarded_row, errors, unknown, guarded_row != row


def load_svc(client):
    """main (for validate_payload / filter_to_schema) imported with our client; nothing else is used."""
    original = bigquery.Client
    bigquery.Client = lambda *a, **k: client
    try:
        import main
    finally:
        bigquery.Client = original
    main.client = client
    return main


# ---------------------------------------------------------------------------------------------
# TextIt runs
# ---------------------------------------------------------------------------------------------

def fetch_runs(flows: list[str], after: datetime, token: str, sleep=time.sleep, get=None) -> list[dict]:
    """Every run of each flow modified after `after` (TextIt filters after/before on modified_on)."""
    import requests
    get = get or requests.get
    out = []
    for flow in flows:
        url = f"{TEXTIT_RUNS}?flow={flow}&after={after.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        pages = 0
        while url:
            resp = get(url, headers={"Authorization": f"Token {token}"}, timeout=60)
            if resp.status_code == 429:
                m = re.search(r"available in (\d+)", resp.text or "")
                sleep((int(m.group(1)) if m else 60) + 3)
                continue
            if resp.status_code != 200:
                raise SystemExit(f"TextIt runs read failed: HTTP {resp.status_code} for flow {flow} "
                                 f"(page {pages + 1}); nothing was written")
            data = resp.json()
            for run in data.get("results", []):
                out.append({"flow": flow, "uuid": run.get("uuid"),
                            "contact": (run.get("contact") or {}).get("uuid"),
                            "created_on": run.get("created_on"), "values": run.get("values") or {}})
            pages += 1
            if pages % 20 == 0:
                print(f"runs: flow {flow}: {pages} pages read")
            url = data.get("next")
            if url:
                sleep(1.5)                         # 2,500 requests/hour, shared with every other job
        print(f"runs: flow {flow}: {sum(1 for r in out if r['flow'] == flow)} runs, {pages} pages")
    return out


def index_runs(runs: list[dict]) -> dict:
    by_contact = defaultdict(list)
    for r in runs:
        created = _aware(r.get("created_on"))
        if r.get("contact") and created is not None:
            by_contact[r["contact"]].append((created, r))
    return by_contact


def match_run(by_contact, uuid: str, checkin):
    if checkin is None:
        return None
    best = None
    for created, run in by_contact.get(uuid, ()):
        d = abs((created - checkin).total_seconds())
        if d <= RUN_MATCH_S and (best is None or d < best[0]):
            best = (d, run)
    return best[1] if best else None


NO_RESPONSE = "no response"


def run_reply(run):
    """
    (replied, value, category, time naive UTC) from the run's checkinresponse result.

    A reply only when the category is not "No Response" and the value is not empty. TextIt writes
    value '' / category "No Response" when the run is interrupted (typically by the contact's next
    check-in); that result's time is the interruption, not a reply.
    """
    res = (run.get("values") or {}).get("checkinresponse")
    if not res:
        return False, None, None, None
    value, cat = res.get("value"), res.get("category")
    t = _aware(res.get("time"))
    replied = (t is not None and str(cat or "").strip().lower() != NO_RESPONSE
               and value is not None and str(value).strip() != "")
    if not replied:
        return False, value, cat, None
    return True, value, cat, t.astimezone(UTC).replace(tzinfo=None)


def _num(value):
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def learn_mapping(pairs: list[tuple[dict, dict]]) -> dict:
    """
    From check-ins where call and run agree that a reply happened at the same time, learn how the run's
    value/category show up in the row. A field is derivable from a run only if EVERY agreeing check-in
    shows the same relation (and there are at least MAPPING_MIN of them).
    """
    rows = [(row, run_reply(run)) for row, run in pairs]
    n = len(rows)
    text_ok = n >= MAPPING_MIN and all((row.get("checkinReplyText") or "").strip() == str(v or "").strip()
                                       for row, (_, v, _, _) in rows)
    num_ok = n >= MAPPING_MIN and all(row.get("checkinReplyNumerical") == _num(v) for row, (_, v, _, _) in rows)
    reply_vals = {row.get("checkinReply") for row, _ in rows}
    yes = next(iter(reply_vals)) if n >= MAPPING_MIN and len(reply_vals) == 1 else None
    seen = defaultdict(Counter)
    for row, (_, _, cat, _) in rows:
        seen[cat][row.get("checkinReplyDistressed")] += 1
    distressed = {cat: next(iter(c)) for cat, c in seen.items() if len(c) == 1 and sum(c.values()) >= MAPPING_MIN}
    return {"agreeing": n, "text": text_ok, "numerical": num_ok, "reply_value": yes, "distressed": distressed}


def apply_run(row: dict, run, mapping: dict, usable_flows: set) -> tuple[dict, str, list[str]]:
    """(row, verdict, notes). The run wins for reply fields where it disagrees."""
    if run is None:
        return row, "no run", []
    if run["flow"] not in usable_flows:
        return row, "run flow carries no check-in reply result", []
    replied, value, cat, t = run_reply(run)
    row_t = row.get("checkinReplyDateTime")
    if not replied and row_t is None:
        return row, "agree", []
    if replied and row_t is not None and abs((row_t - t).total_seconds()) <= REPLY_AGREE_S:
        return row, "agree", []
    new = dict(row)
    notes = []
    if not replied:
        for f in REPLY_FIELDS:
            new[f] = None
        return new, "run: no reply (call carried an earlier reply)", notes
    new["checkinReplyDateTime"] = t
    if mapping["reply_value"] is not None:
        new["checkinReply"] = mapping["reply_value"]
    else:
        notes.append("checkinReply not derivable from runs")
    if mapping["text"]:
        new["checkinReplyText"] = None if value is None or str(value).strip() == "" else str(value)
    else:
        notes.append("checkinReplyText not derivable from runs")
    if mapping["numerical"]:
        new["checkinReplyNumerical"] = _num(value)
    else:
        notes.append("checkinReplyNumerical not derivable from runs")
    if cat in mapping["distressed"]:
        new["checkinReplyDistressed"] = mapping["distressed"][cat]
    else:
        notes.append(f"checkinReplyDistressed not derivable for category {cat!r}")
    verdict = "run: reply the call lacks" if row_t is None else "run: reply at another time"
    return new, verdict, notes


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


def fps_of(rows: list[dict]) -> str:
    return ",".join(str(v) for v in sorted(int(r["gap_fp"]) for r in rows))


# ---------------------------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------------------------

def _enc(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    return v


def build_plan(client, table: str, runs: list[dict], flows: list[str], out=None) -> dict:
    """Read-only. Returns the plan dict (also the list lines and summary under private keys)."""
    svc = load_svc(client)
    schema = list(client.get_table(table).schema)
    calls = fetch_calls(client)
    mark_closeouts(calls)
    staged = staged_sessions(client)
    keyless = [c for c in calls if not c["sid"]]
    by_sid = defaultdict(list)
    for c in calls:
        if c["sid"]:
            by_sid[c["sid"]].append(c)

    usable_flows = {f for f in flows if any(r["flow"] == f and "checkinresponse" in (r["values"] or {}) for r in runs)}
    by_contact = index_runs(runs)

    sessions, excluded, skipped_staged, bad = {}, {}, [], []
    unknown_keys = Counter()
    for sid, cs in by_sid.items():
        if sid in staged:
            skipped_staged.append(sid)
            continue
        kept = [c for c in cs if not c["closeout"]]
        last = (kept or cs)[-1]
        row, errors, unknown, guarded = prepare_row(svc, schema, last["resp"])
        unknown_keys.update(unknown)
        group = "late call" if last["checkin"] is not None and last["checkin"] < WINDOW_START else "in window"
        info = {"sid": sid, "group": group, "last": last, "row": row, "guarded": guarded, "calls": len(cs),
                "closeouts": sum(c["closeout"] for c in cs)}
        if errors:
            bad.append((sid, errors))
            continue
        if not kept:
            excluded[sid] = info
            continue
        sessions[sid] = info

    # runs: learn the mapping on agreeing check-ins, then let runs win where they disagree
    pairs = []
    for s in sessions.values():
        s["run"] = match_run(by_contact, s["last"]["uuid"], s["last"]["checkin"])
        if s["run"] and s["run"]["flow"] in usable_flows:
            replied, _, _, t = run_reply(s["run"])
            rt = s["row"].get("checkinReplyDateTime")
            if replied and rt is not None and abs((rt - t).total_seconds()) <= REPLY_AGREE_S:
                pairs.append((s["row"], s["run"]))
    mapping = learn_mapping(pairs)
    for s in sessions.values():
        s["call_row"] = s["row"]
        s["row"], s["verdict"], s["notes"] = apply_run(s["row"], s["run"], mapping, usable_flows)

    cols = [f.name for f in schema]
    stored = read_stored(client, table, list(sessions) + list(excluded), cols)
    for group in (sessions, excluded):
        for sid, s in group.items():
            rows = stored.get(sid, [])
            s["stored"] = rows
            s["changed"] = sorted({c for c, v in s["row"].items() for r in rows if not same(r.get(c), v)},
                                  key=cols.index)
            s["insert"] = not rows
            s["collapse"] = len(rows) > 1
            s["update"] = bool(rows) and bool(s["changed"])

    call_changed = run_changed = 0
    for s in sessions.values():
        call = _reply(s["call_row"].get("checkinReply"))
        stored_replies = [_reply(r.get("checkinReply")) for r in s["stored"]]
        call_changed += (call == "Yes" and not stored_replies) or any(v != call for v in stored_replies)
        run_changed += _reply(s["row"].get("checkinReply")) != call

    types = {f.name: f.field_type.upper() for f in schema}
    actions = [s for s in sessions.values() if s["insert"] or s["update"] or s["collapse"]]
    plan = {
        "table": table, "listed_at": datetime.now(UTC).isoformat(),
        "window": [WINDOW_START.isoformat(), WINDOW_END.isoformat()],
        "columns": {c: types[c] for c in cols if any(c in s["row"] for s in actions)},
        "totals": {"insert": sum(s["insert"] for s in actions), "update": sum(s["update"] for s in actions),
                   "collapse": sum(s["collapse"] for s in actions),
                   "extra_rows": sum(len(s["stored"]) - 1 for s in actions if s["collapse"])},
        "sessions": [{"sid": s["sid"], "group": s["group"], "insert": s["insert"], "update": s["update"],
                      "collapse": s["collapse"], "n": len(s["stored"]), "fps": fps_of(s["stored"]),
                      "changed": s["changed"], "verdict": s["verdict"],
                      "row": {c: _enc(v) for c, v in s["row"].items()}} for s in actions],
    }
    plan["_ctx"] = {"calls": calls, "keyless": keyless, "sessions": sessions, "excluded": excluded,
                    "staged": skipped_staged, "bad": bad, "unknown": unknown_keys, "mapping": mapping,
                    "usable_flows": usable_flows, "runs": runs, "flows": flows,
                    "call_changed": call_changed, "run_changed": run_changed}
    return plan


# ---------------------------------------------------------------------------------------------
# diagnosis
# ---------------------------------------------------------------------------------------------

def _reply(v):
    return None if v is None or (isinstance(v, str) and v.strip() == "") else v


def diagnose(client, table: str, sessions: dict) -> dict:
    """
    For the check-ins in scope: no row; more than one row; stored reply != the last call's (after the
    stale-reply guard; blank = NULL); last call says Yes and no stored row does; any stored row differs
    from the planned row (calls + runs) in a column the plan carries.
    """
    cols = sorted({c for s in sessions.values() for c in s["row"]})
    stored = read_stored(client, table, list(sessions), cols or ["SessionID"])
    d = Counter(sessions=len(sessions))
    for sid, s in sessions.items():
        rows = stored.get(sid, [])
        last = _reply(s["call_row"].get("checkinReply"))
        d["no row"] += not rows
        d["duplicated"] += len(rows) > 1
        d["reply != last call"] += bool(rows) and any(_reply(r.get("checkinReply")) != last for r in rows)
        d["Yes missing"] += last == "Yes" and not any(r.get("checkinReply") == "Yes" for r in rows)
        d["differs from plan"] += (not rows) or any(not same(r.get(c), v) for r in rows for c, v in s["row"].items())
        planned = _reply(s["row"].get("checkinReply"))
        d[RUN_REPLY] += planned != last
        d[RUN_YES] += last == "Yes" and planned != "Yes"
    return d


RUN_REPLY = "  of which the run changed the reply"
RUN_YES = "  of which the run says no reply"
DIAG_KEYS = ["sessions", "no row", "duplicated", "differs from plan", "reply != last call", RUN_REPLY,
             "Yes missing", RUN_YES]


def diag_lines(before, after=None) -> list[str]:
    out = [f"  {'':<40} {'before':>8}" + (f" {'after':>8}" if after is not None else "")]
    for k in DIAG_KEYS:
        out.append(f"  {k:<40} {before[k]:>8}" + (f" {after[k]:>8}" if after is not None else ""))
    return out


def diag_explained(after) -> bool:
    """After a repair: nothing missing, duplicated or off-plan; every reply difference is the run's."""
    return (after["no row"] == 0 and after["duplicated"] == 0 and after["differs from plan"] == 0
            and after["reply != last call"] == after[RUN_REPLY] and after["Yes missing"] == after[RUN_YES])


# ---------------------------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------------------------

def _short(v, n=40):
    v = "NULL" if v is None else (v.isoformat(sep=" ") if isinstance(v, datetime) else str(v))
    return v if len(v) <= n else v[:n - 3] + "..."


def call_kind(c) -> str:
    resp = c["resp"]
    rt = _aware(resp.get("checkinReplyDateTime"))
    users = {k.lower(): v for k, v in (c["users"] or {}).items()}
    unsub = any(str(users.get(k) or "").strip() for k in ("unsubscribetime",)) or \
        str(users.get("subscribed") or "").strip().lower() == "no"
    if c["closeout"]:
        return "close-out (fired as the contact's next check-in started)"
    if rt is not None and rt >= WINDOW_START:
        return "reply received in the window" + ("; users half: unsubscribed" if unsub else "")
    return "other (no reply in the window)" + ("; users half: unsubscribed" if unsub else "")


def group_report(title, group: dict, lines: list, examples=5):
    act = Counter()
    colc = Counter()
    kinds = Counter()
    for s in group.values():
        act["insert (no row)"] += s["insert"]
        act["update (values differ)"] += s["update"]
        act["collapse (duplicates)"] += s["collapse"]
        act["extra rows deleted"] += (len(s["stored"]) - 1) if s["collapse"] else 0
        act["no change"] += not (s["insert"] or s["update"] or s["collapse"])
        if s["update"]:
            colc.update(s["changed"])
        kinds[call_kind(s["last"])] += 1
    lines.append(f"{title}: {len(group)} check-ins")
    lines.append("  (a) actions: " + ", ".join(f"{k} {v}" for k, v in act.items()))
    lines.append("  (b) columns changed on existing rows: " +
                 (", ".join(f"{c} {n}" for c, n in colc.most_common()) or "none"))
    lines.append("  (c) what the last call was: " + "; ".join(f"{k}: {v}" for k, v in kinds.most_common()))
    ex = [s for s in group.values() if s["update"]][:examples]
    lines.append(f"  (d) {len(ex)} examples, stored -> planned (contact uuid first 8 + SessionID time; run verdict):")
    for s in ex:
        r = s["stored"][0]
        diffs = "; ".join(f"{c}: {_short(r.get(c))} -> {_short(s['row'].get(c))}" for c in s["changed"])
        lines.append(f"      {s['sid'][:8]}..{s['sid'][36:]}  [{s.get('verdict', 'close-out, not repaired')}]  {diffs}")


def cmd_list(client, flows, table=TABLE, runs_file=None, token=None, out=None) -> tuple[str, dict]:
    run = stamp()
    list_path, plan_path = f"gap_repair_list_{run}.txt", f"gap_repair_plan_{run}.json"
    if runs_file:
        with open(runs_file, encoding="utf-8") as f:
            runs = json.load(f)
        runs_path = runs_file
    else:
        if not token:
            raise SystemExit("TEXTIT_API_TOKEN is not set; the runs cross-check needs it (nothing was read or written)")
        runs = fetch_runs(flows, WINDOW_START - timedelta(days=12), token)
        runs_path = f"gap_repair_runs_{run}.json"
        with open(runs_path, "w", encoding="utf-8") as f:
            json.dump(runs, f)
    plan = build_plan(client, table, runs, flows)
    ctx = plan.pop("_ctx")
    sessions, excluded = ctx["sessions"], ctx["excluded"]
    verdicts = Counter(s["verdict"] for s in sessions.values())
    gate = (f"reply changes: from the calls {ctx['call_changed']} check-ins, from the runs {ctx['run_changed']} "
            f"(runs vs calls: " + ", ".join(f"{k} {v}" for k, v in verdicts.most_common()) + ")")
    print(gate, file=out)
    if ctx["run_changed"] > max(GATE_MIN, ctx["call_changed"]):
        raise SystemExit(f"STOPPED: the runs would change more replies ({ctx['run_changed']}) than the calls do "
                         f"({ctx['call_changed']}); the runs rule is suspect. Nothing was written; no plan file.")
    before = diagnose(client, table, sessions)

    lines = [f"table {table}; window {WINDOW_START.isoformat()} -> {WINDOW_END.isoformat()} (calls to {OLD_PATH})", ""]
    for s in sorted(sessions.values(), key=lambda s: s["sid"]):
        if not (s["insert"] or s["update"] or s["collapse"] or s["verdict"].startswith("run:")):
            continue
        what = [a for a in ("insert", "update", "collapse") if s[a]]
        lines.append(f"{s['sid']}  [{s['group']}] {'+'.join(what) or 'no change'}  stored rows {len(s['stored'])}  "
                     f"run: {s['verdict']}" + (f" ({'; '.join(s['notes'])})" if s["notes"] else ""))
        r0 = s["stored"][0] if s["stored"] else {}
        for c in s["changed"]:
            lines.append(f"      {c}: {_short(r0.get(c), 60)} -> {_short(s['row'].get(c), 60)}")

    t = plan["totals"]
    by_group = Counter(s["group"] for s in sessions.values())
    m = ctx["mapping"]
    summary = [
        f"calls in the window {len(ctx['calls'])}; without a SessionID (sign-up events, not repaired) {len(ctx['keyless'])}",
        f"check-ins {len(sessions) + len(excluded) + len(ctx['staged']) + len(ctx['bad'])}: in scope {len(sessions)} "
        f"(in window {by_group['in window']}, checked in earlier with a late call {by_group['late call']}); "
        f"skipped: add-to-db owns them {len(ctx['staged'])}, only close-out calls {len(excluded)}, "
        f"preparation errors {len(ctx['bad'])}",
        f"payload keys the table lacks (dropped, no column added): {dict(ctx['unknown']) or 'none'}",
        "",
        f"PLAN: insert {t['insert']}, update {t['update']}, collapse {t['collapse']} (extra rows deleted {t['extra_rows']}); "
        f"check-ins changed {len(plan['sessions'])}",
        "",
        f"runs read {len(ctx['runs'])}; flows with a check-in reply result: {sorted(ctx['usable_flows']) or 'none'} "
        f"of {ctx['flows']}",
        gate,
        f"mapping learned from {m['agreeing']} agreeing check-ins: reply value {m['reply_value']!r}, "
        f"text {'yes' if m['text'] else 'NO'}, numerical {'yes' if m['numerical'] else 'NO'}, "
        f"distressed by category {m['distressed'] or 'none'}",
        "",
    ]
    grp = []
    group_report("IN WINDOW", {k: v for k, v in sessions.items() if v["group"] == "in window"}, grp)
    group_report("CHECKED IN BEFORE THE WINDOW, LATE CALL IN IT (repaired)",
                 {k: v for k, v in sessions.items() if v["group"] == "late call"}, grp)
    group_report("ONLY CLOSE-OUT CALLS IN THE WINDOW (not repaired: what the close-out would have changed)",
                 excluded, grp)
    summary += grp + ["", "diagnosis (check-ins in scope):"] + diag_lines(before)
    summary += ["", f"full list: {list_path}", f"plan:      {plan_path}", f"runs:      {runs_path}",
                "Nothing was written to BigQuery."]
    with open(list_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines + [""] + summary) + "\n")
    plan["flows"], plan["runs_file"] = flows, runs_path
    plan["diagnosis_before"] = dict(before)
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=1, default=str)
    print("\n".join(summary), file=out)
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
    present = "|" + "|".join(cols) + "|"
    data = []
    for s in plan["sessions"]:
        if not (s["insert"] or s["update"]):
            continue
        rec = {c: s["row"].get(c) for c in cols}
        for c, t in cols.items():
            if t == "DATETIME" and rec[c] is not None:
                rec[c] = str(rec[c]).replace("T", " ")
        rec[PRESENT_FIELD] = present
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
        src = (f"(SELECT * FROM `{rows_table}` WHERE `{pcol}` IS "
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
    n_after = sum(x["after"][0] for x in sess)
    n_before = sum(x["before"][0] for x in sess)
    s = ["BEGIN TRANSACTION",
         check("gap_after", "a repaired check-in changed since the apply (add-to-db or someone else wrote it): not rolled back"),
         f"DELETE FROM `{table}` WHERE SessionID IN UNNEST(@gap_sids)",
         f"ASSERT @@row_count = {n_after} AS 'rollback: deleted rows != {n_after}'",
         f"INSERT INTO `{table}` SELECT * FROM `{applied['backup']}` WHERE SessionID IN UNNEST(@gap_sids)",
         f"ASSERT @@row_count = {n_before} AS 'rollback: restored rows != {n_before}'",
         check("gap_before", "rollback: restored rows are not the rows the list read"),
         "COMMIT TRANSACTION"]
    params = [bigquery.ArrayQueryParameter("gap_sids", "STRING", [x["sid"] for x in sess]),
              pre("gap_after", "after"), pre("gap_before", "before")]
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
        stmt = re.sub(r"\bgap_before\b", "0", stmt)
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


def cmd_apply(client, plan: dict, production: bool, pause: bool | None = None, created: list | None = None) -> dict:
    table = plan["table"]
    refuse_unless_allowed(table, production)
    created = created if created is not None else []
    run = stamp()
    backup = f"{DEV_PREFIX}adb_gap_repair_backup_{run}"
    rows_table = f"{DEV_PREFIX}adb_gap_repair_rows_{run}"
    path = f"gap_repair_applied_{run}.json"
    with Pause(client, production if pause is None else pause):
        staged = staged_sessions(client)
        dropped = [s["sid"] for s in plan["sessions"] if s["sid"] in staged]
        if dropped:
            plan = dict(plan, sessions=[s for s in plan["sessions"] if s["sid"] not in staged])
            t = plan["sessions"]
            plan["totals"] = {"insert": sum(s["insert"] for s in t), "update": sum(s["update"] for s in t),
                              "collapse": sum(s["collapse"] for s in t),
                              "extra_rows": sum(s["n"] - 1 for s in t if s["collapse"])}
        print(f"check-ins dropped from the plan because add-to-db has staged a call for them since the list: {len(dropped)}")
        sids = [s["sid"] for s in plan["sessions"]]
        sid_param = [bigquery.ArrayQueryParameter("s", "STRING", sids)]
        backup_sql = f"CREATE TABLE `{backup}` AS SELECT * FROM `{table}` WHERE SessionID IN UNNEST(@s)"
        preflight(client, [(backup_sql, sid_param)])
        client.query(backup_sql, job_config=bigquery.QueryJobConfig(query_parameters=sid_param)).result()
        created.append(backup)
        nb = list(client.query(f"SELECT COUNT(*) n FROM `{backup}`").result())[0]["n"]
        print(f"backup: {backup} ({nb} rows)")
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

def cmd_rehearse(client, flows, runs_file, keep=False) -> bool:
    run = stamp()
    clone = f"{DEV_PREFIX}adb_gap_rehearsal_{run}"
    client.query(f"CREATE TABLE `{clone}` CLONE `{TABLE}`").result()
    print(f"clone: {clone} (of {TABLE} as of now)")
    made = [clone]
    try:
        plan_path, plan = cmd_list(client, flows, table=clone, runs_file=runs_file)
        plan.pop("_ctx")
        before_fp = {s["sid"]: [s["n"], s["fps"]] for s in plan["sessions"]}
        applied = cmd_apply(client, plan, production=False, pause=False, created=made)
        for sid in applied["dropped"]:
            before_fp.pop(sid, None)
        print("\nrehearsal: list again on the repaired clone (the repair must leave nothing to do):")
        _, again = cmd_list(client, flows, table=clone, runs_file=runs_file)
        again.pop("_ctx")
        t2 = again["totals"]
        diag_before, diag_after = Counter(plan["diagnosis_before"]), Counter(again["diagnosis_before"])
        print("\nrehearsal diagnosis on the clone (check-ins in scope; add-to-db-owned check-ins excluded each time):")
        print("\n".join(diag_lines(diag_before, diag_after)))
        cmd_rollback(client, applied, production=False, pause=False)
        back = fp_state(client, clone, list(before_fp))
        restored = sum(back[sid] == before_fp[sid] for sid in before_fp)
        idem = t2["insert"] == t2["update"] == t2["collapse"] == 0
        ok = diag_explained(diag_after) and idem and restored == len(before_fp)
        print(f"\nREHEARSAL {'PASS' if ok else 'FAIL'}: after apply no row {diag_after['no row']}, duplicated "
              f"{diag_after['duplicated']}, differs from plan {diag_after['differs from plan']} (want 0 each); "
              f"reply != last call {diag_after['reply != last call']} (run-changed {diag_after[RUN_REPLY]}), "
              f"Yes missing {diag_after['Yes missing']} (run says no reply {diag_after[RUN_YES]}); "
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


def main_(argv=None):
    for stream in (sys.stdout, sys.stderr):          # reply text carries emoji; never depend on the code page
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=("list", "rehearse", "apply", "rollback"))
    ap.add_argument("file", nargs="?", help="apply: the plan; rollback: the applied record")
    ap.add_argument("--flows", default="", help="check-in flow UUIDs, comma-separated (list, rehearse)")
    ap.add_argument("--runs", help="runs file written by an earlier list (skips the TextIt read)")
    ap.add_argument("--table", default=TABLE, help="list only: the table to read")
    ap.add_argument("--production", action="store_true")
    ap.add_argument("--keep", action="store_true", help="rehearse: keep the clone, backup and loaded rows")
    args = ap.parse_args(argv)
    flows = [f.strip() for f in args.flows.split(",") if f.strip()]
    if args.action in ("apply", "rollback"):
        if not args.file:
            raise SystemExit(f"{args.action} needs its file")
        with open(args.file, encoding="utf-8") as f:
            doc = json.load(f)
        refuse_unless_allowed(doc["table"], args.production)            # before any sign-in or query
    elif not flows:
        raise SystemExit("--flows is required (the check-in flow UUIDs whose runs carry the replies)")
    if args.action == "rehearse" and not args.runs:
        raise SystemExit("rehearse needs --runs (the file list wrote), so TextIt is read once")
    client = make_client()
    if args.action == "list":
        cmd_list(client, flows, table=args.table, runs_file=args.runs, token=os.environ.get("TEXTIT_API_TOKEN"))
    elif args.action == "rehearse":
        if not cmd_rehearse(client, flows, args.runs, keep=args.keep):
            raise SystemExit(1)
    elif args.action == "apply":
        applied = cmd_apply(client, doc, production=args.production)
        print("\nafter the apply: list again (read-only) with the same runs file")
        _, again = cmd_list(client, doc["flows"], table=doc["table"], runs_file=doc["runs_file"])
        again.pop("_ctx")
        before, after = Counter(doc["diagnosis_before"]), Counter(again["diagnosis_before"])
        print("\ndiagnosis, the list read before the apply vs now (add-to-db-owned check-ins excluded each time):")
        print("\n".join(diag_lines(before, after)))
        t2 = again["totals"]
        ok = diag_explained(after) and t2["insert"] == t2["update"] == t2["collapse"] == 0
        print(f"\nAPPLY {'VERIFIED' if ok else 'NOT VERIFIED'}: second list insert {t2['insert']}, update {t2['update']}, "
              f"collapse {t2['collapse']} (want 0); applied record {applied['_path']}")
        if not ok:
            raise SystemExit(1)
    else:
        cmd_rollback(client, doc, production=args.production)


if __name__ == "__main__":
    main_()

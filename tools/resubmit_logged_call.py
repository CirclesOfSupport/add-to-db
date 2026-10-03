"""
Re-submit ONE logged call that never landed, from the body the webhook log kept.

    python tools/resubmit_logged_call.py --list [--since 2026-09-28]
        calls to this service's /upsert that were not answered 202 (read-only)
    python tools/resubmit_logged_call.py 123456789
        show what would be sent for that httplog id, and why (read-only; sends nothing)
    python tools/resubmit_logged_call.py 123456789 --post
        send it to /upsert and print the answer
    python tools/resubmit_logged_call.py 123456789 --verify
        after a send (a few minutes later): the same read-only plan, passing only when nothing is
        left to send

One id per run. What it does with the id:

1. Reads the call from OPS.webhook_log (when it fired, how it was answered) and its body from
   OPS.webhook_log_detail. A call that was answered 202 is refused: it was accepted, it is not lost.
2. Parses the body. A body that is not valid JSON because a field value was pasted raw (a quote,
   a backslash or a line break typed by a subscriber) is repaired by escaping exactly those values;
   the repair is proven (every other byte identical, every string value equal to the raw text) or
   the run stops.
3. Decides, part by part, what may be sent NOW. A call carries one part per table.
   - A users part is NEVER sent. The users table is brought into line with the messaging platform
     every night by the contact sync, which owns it; an old call's users half would undo that.
   - Every other part is first compared with the stored row for its key, column by column, the way
     the service would write it. If the row already equals what the call carries, the part has
     LANDED and is not sent.
   - If it has not landed and it is a responses part: the single writer keeps the LAST call's value
     for every column, and a re-submitted call is received now, so it would be the last call. It is
     sent only when no later call for the same session has been accepted since the lost call fired;
     when one has, the part is SUPERSEDED: the row carries newer values and the old ones would
     replace them.
   - A part that is offered as a send says why, and names the columns that differ from the stored row.
4. Without --post it stops there, having printed the plan. With --post it sends the parts that
   may be sent in one request to /upsert, with the webhook secret, and prints the status and body.

The webhook secret is read from ADD_TO_DB_SECRET if set; otherwise from the deployed service's
WEBHOOK_SECRET (gcloud run services describe, and Secret Manager if the service reads it from
there). It is never printed. BigQuery reads run as the active gcloud account; every read prints
the bytes it was billed.

Exit codes: 0 done (plan shown / sent and answered 202 / verified), 1 failed, 3 nothing to send
(every part has landed, is superseded, or is a users half).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from google.cloud import bigquery  # noqa: E402

PROJECT = "early-alert-responses"
HOST = "add-to-db-853176470965.us-east1.run.app"
PATH = "/upsert"
URL = f"https://{HOST}{PATH}"
SERVICE, REGION = "add-to-db", "us-east1"
LOG = f"{PROJECT}.OPS.webhook_log"
DETAIL = f"{PROJECT}.OPS.webhook_log_detail"
STAGING = f"{PROJECT}.OPS.adb_staging"
STAGED = ("users", "responses")       # the targets the live service writes through the single writer
ACCEPTED = "202"
DEFAULT_SINCE = "2026-09-28"          # the day this service became the writer for check-in calls

SENT, SUPERSEDED, LANDED, KEPT = "send", "superseded", "landed", "not sent"
NEVER_SENT = {"users": "users is kept by the nightly contact sync, which owns it; a call's users half is never re-sent"}


class Stop(Exception):
    """The run cannot go on; the message says why."""


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


def parse_body(text: str) -> tuple[dict, dict | None]:
    """(the body as an object, the repair or None). Raises Stop when it cannot be read or the repair is not proven."""
    try:
        body = json.loads(text)
        rep = None
    except (TypeError, ValueError):
        rep = repair_body(text or "")
        if rep is None:
            raise Stop("the logged body is not valid JSON and could not be repaired; nothing sent")
        proof = prove_repair(text, rep)
        if not proof["ok"]:
            raise Stop(f"the logged body was repaired but the repair is not proven "
                       f"({proof['identical']} of {proof['raw_strings']} string values identical; text outside the "
                       f"escaped values identical: {proof['outside_identical']}); nothing sent")
        rep["proof"] = proof
        body = rep["obj"]
    if not isinstance(body, dict):
        raise Stop("the logged body is not a JSON object; nothing sent")
    return body, rep


# ---------------------------------------------------------------------------------------------
# reads (BigQuery, read-only)
# ---------------------------------------------------------------------------------------------

def _query(client, sql: str, params: list, say=print) -> list[dict]:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    rows = [dict(r) for r in job.result()]
    billed = getattr(job, "total_bytes_billed", None)
    if billed is not None:
        say(f"    [read: {billed / 1e6:.1f} MB billed]")
    return rows


def _p(name, typ, value):
    return bigquery.ScalarQueryParameter(name, typ, value)


def list_lost(client, since: datetime, say=print) -> list[dict]:
    """Calls to this service's /upsert since `since` that were not answered 202 (no bodies read)."""
    return _query(client,
                  f"SELECT httplog_id, fired_at, status_code, elapsed_ms, flow_name FROM `{LOG}` "
                  f"WHERE fired_at >= @since AND STRPOS(webhook_url, @host) > 0 AND STRPOS(webhook_url, @path) > 0 "
                  f"AND (status_code IS NULL OR status_code != 202) ORDER BY fired_at, httplog_id",
                  [_p("since", "TIMESTAMP", since), _p("host", "STRING", HOST), _p("path", "STRING", PATH)], say)


def read_call(client, httplog_id: int, say=print) -> dict:
    """The logged call: when it fired and how it was answered (webhook_log), and its body (webhook_log_detail)."""
    head = _query(client,
                  f"SELECT httplog_id, fired_at, status_code, elapsed_ms, webhook_url, flow_name FROM `{LOG}` "
                  f"WHERE httplog_id = @id", [_p("id", "INT64", httplog_id)], say)
    if len(head) != 1:
        raise Stop(f"httplog id {httplog_id}: {len(head)} rows in the webhook log, expected exactly 1")
    call = head[0]
    # the call's own second, so only that day's bodies are read when the table is partitioned by fired_at
    detail = _query(client,
                    f"SELECT request_path, request_host, response_status_line, request_body FROM `{DETAIL}` "
                    f"WHERE fired_at BETWEEN @a AND @b AND httplog_id = @id",
                    [_p("a", "TIMESTAMP", call["fired_at"] - timedelta(seconds=1)),
                     _p("b", "TIMESTAMP", call["fired_at"] + timedelta(seconds=1)),
                     _p("id", "INT64", httplog_id)], say)
    if len(detail) != 1:
        raise Stop(f"httplog id {httplog_id}: {len(detail)} bodies in the webhook log detail, expected exactly 1 "
                   f"(a call's body is kept only while its day is kept)")
    call.update(detail[0])
    return call


def later_accepted(client, target: str, key_column: str, key: str, contact: str, fired_at, decode, say=print) -> list:
    """
    The calls for `target` with this key that the service accepted (staged) after `fired_at`, oldest
    first: [{"received_at", "data"}]. The staged payloads that mention the contact are read and matched
    in Python on the decoded key, so an encoded and a plain spelling of one session id are the same session.
    """
    rows = _query(client,
                  f"SELECT received_at, payload FROM `{STAGING}` "
                  f"WHERE target = @t AND received_at > @fired AND STRPOS(payload, @contact) > 0 ORDER BY received_at",
                  [_p("t", "STRING", target), _p("fired", "TIMESTAMP", fired_at), _p("contact", "STRING", contact)], say)
    out = []
    for r in rows:
        try:
            data = json.loads(r["payload"])
        except ValueError:
            continue
        if isinstance(data, dict) and key_of(data, key_column, decode) == key:
            out.append({"received_at": r["received_at"], "data": data})
    return out


# ---------------------------------------------------------------------------------------------
# the plan
# ---------------------------------------------------------------------------------------------

def key_of(data: dict, key_column: str, decode) -> str | None:
    """The call's value for the table's key column (any casing of the name), decoded as the service decodes it."""
    for k, v in data.items():
        if k.lower() == key_column.lower():
            if v is None or (isinstance(v, str) and v.strip() == ""):
                return None
            return decode(v) if isinstance(v, str) else str(v)
    return None


_UUID = re.compile(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo is not None else value


def _as_json(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _same(a, b, field_type: str = "") -> bool:
    if a is None or b is None:
        return a is None and b is None
    if field_type.upper() == "JSON":
        return _as_json(a) == _as_json(b)
    if isinstance(a, datetime) and isinstance(b, datetime):
        return _naive_utc(a) == _naive_utc(b)
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 1e-9
    return a == b or str(a) == str(b)


def expected_row(target: str, data: dict, schema, writer, config) -> dict:
    """The row the service writes for this part: its own key mapping, decoding, typing and reply guard."""
    normalized, e1 = writer.normalize_payload_to_schema(data, schema)
    coerced, e2 = writer.coerce_payload_to_schema(normalized, schema, config.DATETIME_CONVENTIONS.get(target))
    if e1 or e2:
        raise Stop(f"{target}: the body does not prepare cleanly: {'; '.join(e1 + e2)}")
    names = {f.name for f in schema}
    row = {k: v for k, v in coerced.items() if k in names}
    return writer.apply_stale_reply_guard(row, config.STALE_REPLY_FIELDS.get(target))


def compare_with_row(client, target: str, data: dict, key_column: str, key: str, writer, config, say=print) -> dict:
    """
    The stored row(s) for this key against what the call carries, as the service would write it.
    Returns {"rows": how many stored rows, "compared": columns compared, "differing": [(column, call, row)]}.
    A blank in a column the service never clears (config.PRESERVE_ON_BLANK) is not compared.
    """
    table = config.ALLOWED_TARGETS[target]
    schema = list(client.get_table(table).schema)
    types = {f.name: f.field_type for f in schema}
    want = expected_row(target, data, schema, writer, config)
    key_name = next(f.name for f in schema if f.name.lower() == key_column.lower())
    cols = [c for c in want if c != key_name]
    params = [_p("key", "STRING", key)]
    where = f"`{key_name}` = @key"
    pcol = config.PARTITION_COLUMNS.get(target)
    pcol = next((f.name for f in schema if pcol and f.name.lower() == pcol.lower()), None)
    if pcol and want.get(pcol) is not None:           # the session's own days only, not the whole table
        where += f" AND (`{pcol}` BETWEEN @a AND @b OR `{pcol}` IS NULL)"
        params += [_p("a", "DATETIME", want[pcol] - timedelta(days=1)), _p("b", "DATETIME", want[pcol] + timedelta(days=1))]
    stored = _query(client, f"SELECT {', '.join(f'`{c}`' for c in [key_name] + cols)} FROM `{table}` WHERE {where}",
                    params, say)
    preserve = {c.lower() for c in config.PRESERVE_ON_BLANK.get(target, [])}
    compared = [c for c in cols if not (want[c] is None and c.lower() in preserve)]
    differing = []
    if len(stored) == 1:
        differing = [(c, want[c], stored[0][c]) for c in compared if not _same(want[c], stored[0][c], types.get(c, ""))]
    return {"rows": len(stored), "compared": len(compared), "differing": differing}


def _differences(cmp: dict) -> str:
    if cmp["rows"] == 0:
        return "no stored row for this key"
    if cmp["rows"] > 1:
        return f"{cmp['rows']} stored rows for this key"
    shown = "; ".join(f"{c}: the call has {w!r}, the row has {g!r}" for c, w, g in cmp["differing"][:6])
    more = len(cmp["differing"]) - 6
    return f"{len(cmp['differing'])} of {cmp['compared']} columns differ from the stored row ({shown}" + (
        f"; and {more} more)" if more > 0 else ")")


def build_plan(client, call: dict, normalize, writer, config, say=print) -> dict:
    """
    What may be sent now, part by part. normalize is the service's own body parsing
    (normalize_target_requests); writer its bq_writer module; config its config module.
    """
    keys, decode = config.UPSERT_KEYS, writer.decode_webhook_string
    if call.get("request_host") != HOST or call.get("request_path") != PATH:
        raise Stop(f"this call went to {call.get('request_host')}{call.get('request_path')}, not to {HOST}{PATH}; "
                   f"nothing sent")
    status = str(call.get("status_code") or "")
    line = call.get("response_status_line") or ""
    if status == ACCEPTED or f" {ACCEPTED}" in line:
        raise Stop(f"this call was answered 202 ({line or status}): it was accepted, it is not a lost call; nothing sent")
    body, rep = parse_body(call.get("request_body"))
    items, problem = normalize(body)
    if problem:
        raise Stop(f"the logged body is not a request this service takes ({problem}); nothing sent")
    parts = []
    for item in items:
        target, data = item["target"], item["data"]
        if target not in keys:
            raise Stop(f"table '{target}' is not one this service upserts; nothing sent")
        key_column = keys[target][0]
        key = key_of(data, key_column, decode)
        part = {"target": target, "data": data, "key_column": key_column, "key": key, "action": SENT, "why": ""}
        parts.append(part)
        if target in NEVER_SENT:                                   # rule: a users half is never a send
            part["action"], part["why"] = KEPT, NEVER_SENT[target]
            continue
        if key is None:
            part["why"] = "no key: there is no stored row to compare with"
            continue
        try:                                                       # rule: a row that already equals the call has landed
            cmp = compare_with_row(client, target, data, key_column, key, writer, config, say)
        except Stop as stop:
            part["why"] = f"could not be compared with a stored row ({stop})"
            continue
        if cmp["rows"] == 1 and not cmp["differing"]:
            part["action"] = LANDED
            part["why"] = f"the stored row already equals the call in all {cmp['compared']} columns it carries"
            continue
        if target in STAGED:
            contact = _UUID.match(key)
            later = later_accepted(client, target, key_column, key, contact.group(0) if contact else key,
                                   call["fired_at"], decode, say)
            if later:
                part["action"] = SUPERSEDED
                part["why"] = (f"{len(later)} later call(s) for this {key_column} were accepted, the last at "
                               f"{later[-1]['received_at'].isoformat()}; the row carries newer values")
                continue
            part["why"] = f"no later call for this {key_column} has been accepted since it fired; {_differences(cmp)}"
        else:
            part["why"] = f"written by key with only the columns this part carries; {_differences(cmp)}"
    return {"call": call, "repair": rep, "parts": parts,
            "send": {"tables": [{"table": p["target"], "data": p["data"]} for p in parts if p["action"] == SENT]}}


def show_plan(plan: dict, say=print) -> None:
    call = plan["call"]
    say(f"httplog id {call['httplog_id']}  fired {call['fired_at'].isoformat()}  "
        f"answered {call.get('response_status_line') or call.get('status_code') or 'nothing'}  "
        f"flow {call.get('flow_name')}")
    rep = plan["repair"]
    if rep is None:
        say("body: valid JSON as logged")
    else:
        proof = rep["proof"]
        say(f"body: repaired -- {len(rep['escaped'])} value(s) escaped ({', '.join(k for k, _ in rep['escaped'])}); "
            f"string values identical to the raw text: {proof['identical']} of {proof['raw_strings']}; "
            f"text outside the escaped values byte-identical: {'yes' if proof['outside_identical'] else 'NO'}")
    for p in plan["parts"]:
        say(f"  {p['target']:<12} {p['key_column']} = {p['key']}  ->  {p['action'].upper()}: {p['why']}")
    n = len(plan["send"]["tables"])
    say(f"to send: {n} of {len(plan['parts'])} part(s)" + (f" ({', '.join(t['table'] for t in plan['send']['tables'])})" if n else ""))


# ---------------------------------------------------------------------------------------------
# the secret and the post
# ---------------------------------------------------------------------------------------------

def _gcloud(args: list[str]) -> str:
    exe = shutil.which("gcloud.cmd") or shutil.which("gcloud")
    if not exe:
        raise Stop("gcloud is not on PATH")
    out = subprocess.run([exe, *args], capture_output=True, text=True)
    if out.returncode != 0:
        tail = (out.stderr or "").strip().splitlines()
        raise Stop(f"gcloud {' '.join(args[:3])} failed" + (f": {tail[-1]}" if tail else ""))
    return out.stdout


def find_secret(env=os.environ, gcloud=_gcloud) -> str:
    """ADD_TO_DB_SECRET if set; else the deployed service's WEBHOOK_SECRET (plain value or Secret Manager). Never printed."""
    if env.get("ADD_TO_DB_SECRET"):
        return env["ADD_TO_DB_SECRET"]
    described = json.loads(gcloud(["run", "services", "describe", SERVICE, f"--region={REGION}",
                                   f"--project={PROJECT}", "--format=json"]))
    for container in described["spec"]["template"]["spec"]["containers"]:
        for var in container.get("env", []):
            if var.get("name") != "WEBHOOK_SECRET":
                continue
            if var.get("value"):
                return var["value"]
            ref = (var.get("valueFrom") or {}).get("secretKeyRef") or {}
            if ref.get("name"):
                return gcloud(["secrets", "versions", "access", ref.get("key") or "latest",
                               f"--secret={ref['name']}", f"--project={PROJECT}"]).strip()
    raise Stop("the deployed service has no WEBHOOK_SECRET that could be read; set ADD_TO_DB_SECRET and run again")


def post(plan: dict, secret: str, url: str = URL, http=None, say=print) -> int:
    """Send the parts that may be sent, in one request. Returns the HTTP status."""
    if http is None:
        import requests
        http = requests.post
    r = http(url, json=plan["send"], timeout=60,
             headers={"Content-Type": "application/json", "X-Webhook-Secret": secret})
    say(f"POST {url} -> HTTP {r.status_code}")
    say(r.text[:2000])
    return r.status_code


# ---------------------------------------------------------------------------------------------

def main_(argv=None, client=None, service=None, secret_finder=find_secret, http=None, say=print) -> int:
    ap = argparse.ArgumentParser(description="Re-submit one logged call that never landed.")
    ap.add_argument("httplog_id", nargs="?", type=int)
    ap.add_argument("--list", action="store_true", help="list calls to /upsert that were not answered 202")
    ap.add_argument("--since", default=DEFAULT_SINCE, help="with --list: from this UTC date (YYYY-MM-DD)")
    ap.add_argument("--post", action="store_true", help="send the call (without it, only the plan is shown)")
    ap.add_argument("--verify", action="store_true", help="read the rows the call was for and compare them")
    args = ap.parse_args(argv)
    if args.list == (args.httplog_id is not None) or (args.post and args.verify):
        ap.error("give exactly one of: --list, or one httplog id (with at most one of --post / --verify)")
    try:
        if client is None:
            from _harness import make_client
            client = make_client()
        if args.list:
            since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
            rows = list_lost(client, since, say)
            for r in rows:
                say(f"{r['httplog_id']}  {r['fired_at'].isoformat()}  status {r['status_code']}  "
                    f"{r['elapsed_ms']} ms  {r['flow_name']}")
            say(f"{len(rows)} call(s) to {HOST}{PATH} since {args.since} not answered 202")
            return 0
        if service is None:
            from _harness import load_service
            service = load_service(client, {})
        import bq_writer
        import config
        plan = build_plan(client, read_call(client, args.httplog_id, say), service.normalize_target_requests,
                          bq_writer, config, say)
        show_plan(plan, say)
        if args.verify:                      # after a send: nothing may be left to send
            ok = not plan["send"]["tables"]
            say("VERIFY PASS: nothing is left to send" if ok else "VERIFY FAIL: a part has not landed")
            return 0 if ok else 1
        if not plan["send"]["tables"]:
            say("NOTHING TO SEND: every part of this call has landed, is superseded, or is a users half")
            return 3
        if not args.post:
            say("PLAN ONLY: nothing was sent (add --post to send)")
            return 0
        status = post(plan, secret_finder(), http=http, say=say)
        say("SENT: accepted (202)" if status == 202 else f"NOT ACCEPTED: HTTP {status}")
        return 0 if status == 202 else 1
    except Stop as stop:
        say(f"STOPPED: {stop}")
        return 1


if __name__ == "__main__":
    sys.exit(main_())

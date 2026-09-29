"""
Identity gate for the triage-queue change: proves the check-in path (response_data and users through the
staged single writer) is unchanged from the cutover revision, in two parts.

1. SOURCE. Every top-level statement of src/*.py is compared, as a parsed syntax tree, between --base (the
   cutover revision, default 2925726) and this checkout. Only the triage-queue additions may differ:
   config's TRIAGE_* settings, tasks.queue_for and tasks.enqueue_write, and in main the /tasks/upsert
   endpoint plus the four triage-alert helpers. Anything else that differs -- the staging append, the
   flush, the sweep, perform_upsert, the MERGE builder -- fails the gate.

2. RECORDED CALLS. For every webhook call to /upsert recorded in the last --days days that carries one of
   --targets (default users,responses), both versions run in separate processes with BigQuery and Cloud
   Tasks replaced by recorders (STAGED_TARGETS=users,responses, as in production) and must produce
   byte-identical results: the /upsert response, what each stages or queues, and every BigQuery statement's
   SQL text and parameters. With --targets triage_data,feedback it also proves the triage path is unchanged
   while TRIAGE_QUEUE is unset (the switch is an environment variable, not code).

Reads only: the webhook log and the targets' schemas. Nothing is written anywhere.

    python tools/check_identity.py                          # both parts, check-in targets, 7 days
    python tools/check_identity.py --targets triage_data,feedback
    python tools/check_identity.py --source-only            # part 1 only (no BigQuery read)
Exit code 0 = identical.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

from _harness import PROJECT, make_client

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
LOG = f"{PROJECT}.OPS.webhook_log_detail"
BASE = "29257265d004196f1003b6ad79218d554c1cab1b"   # cutover-prereqs head, the revision runbook B deploys
ALLOWED_CHANGES = {
    "config.py": {"SERIALIZED_TARGETS", "TRIAGE_QUEUE", "TRIAGE_MAX_ATTEMPTS", "TRIAGE_WAIT_ALERT_S",
                  "if TRIAGE_QUEUE"},
    "tasks.py": {"queue_for", "enqueue_write"},
    "main.py": {"tasks_upsert", "from_triage_queue", "raise_triage_alert", "triage_part", "watch_triage_task"},
    "auth.py": set(),
    "bq_writer.py": set(),
}
FILES = ("main.py", "bq_writer.py", "config.py", "auth.py", "tasks.py")


def live_commit() -> str | None:
    gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud")
    if not gcloud:
        return None
    out = subprocess.run([gcloud, "run", "services", "describe", "add-to-db", "--region=us-east1",
                          "--format=value(spec.template.spec.containers[0].image)"], capture_output=True, text=True)
    image = out.stdout.strip()
    return image.rsplit(":", 1)[-1] if ":" in image else None


def _top_level(text: str) -> list[tuple[str, str]]:
    """(name, syntax-tree dump) per top-level statement; unnamed statements are named by their source head."""
    out = []
    for node in ast.parse(text).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = node.name
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            name = ",".join(ast.unparse(t) for t in targets)
        elif isinstance(node, ast.If):
            name = f"if {ast.unparse(node.test)}"
        else:
            name = ast.unparse(node).splitlines()[0][:80]
        out.append((name, ast.dump(node, include_attributes=False)))
    return out


def compare_source(base_text: str, branch_text: str, allowed: set) -> dict:
    a, b = _top_level(base_text), _top_level(branch_text)
    da, db = dict(a), dict(b)
    changed = sorted(k for k in da if k in db and da[k] != db[k])
    added = sorted(k for k in db if k not in da)
    removed = sorted(k for k in da if k not in db)
    reordered = [k for k, _ in a if k in db] != [k for k, _ in b if k in da]
    bad = [k for k in changed + added if k not in allowed] + removed + (["(statement order)"] if reordered else [])
    return {"identical": len([k for k in da if k in db and da[k] == db[k]]), "changed": changed,
            "added": added, "removed": removed, "bad": bad}


def source_identity(base: str) -> bool:
    ok = True
    for name, allowed in ALLOWED_CHANGES.items():
        base_text = subprocess.run(["git", "-C", REPO, "show", f"{base}:src/{name}"], capture_output=True,
                                   text=True, check=True).stdout
        with open(os.path.join(REPO, "src", name), encoding="utf-8") as f:
            r = compare_source(base_text, f.read(), allowed)
        print(f"source {name}: {r['identical']} statements identical; changed {r['changed'] or '-'}; "
              f"added {r['added'] or '-'}; removed {r['removed'] or '-'}")
        if r["bad"]:
            ok = False
            print(f"  NOT ALLOWED: {r['bad']}")
    print("SOURCE:", "IDENTICAL outside the triage-queue additions" if ok else "DIFFERENT")
    return ok


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--base", default=BASE, help="commit to compare against (default: the cutover revision)")
    ap.add_argument("--targets", default="users,responses")
    ap.add_argument("--source-only", action="store_true")
    args = ap.parse_args()

    base = args.base
    head = subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(f"base {base[:12]} vs branch {head[:12]}")
    live = live_commit()
    if live:
        print(f"deployed image tag: {live[:12]}{'' if base.startswith(live) or live.startswith(base) else ' (not the base)'}")
    if not source_identity(base):
        sys.exit(1)
    if args.source_only:
        sys.exit(0)
    TARGETS = tuple(t.strip() for t in args.targets.split(",") if t.strip())

    client = make_client()
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    sql = (f"SELECT fired_at, request_body FROM `{LOG}` WHERE request_path = '/upsert' AND fired_at >= @s "
           f"ORDER BY fired_at, httplog_id")
    calls = []
    for r in client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("s", "TIMESTAMP", since)])).result():
        try:
            b = json.loads(r["request_body"])
        except (TypeError, ValueError):
            continue
        tables = [b.get("table")] if "table" in b else [t.get("table") for t in b.get("tables") or [] if isinstance(t, dict)]
        if any(t in TARGETS for t in tables):
            calls.append(b)

    sys.path.insert(0, os.path.join(REPO, "src"))
    import config
    schemas = {config.ALLOWED_TARGETS[t]: [f.to_api_repr() for f in client.get_table(config.ALLOWED_TARGETS[t]).schema]
               for t in TARGETS}

    work = tempfile.mkdtemp(prefix="identity_")
    base_src = os.path.join(work, "base_src")
    os.makedirs(base_src)
    for name in FILES:
        text = subprocess.run(["git", "-C", REPO, "show", f"{base}:src/{name}"], capture_output=True, text=True, check=True).stdout
        with open(os.path.join(base_src, name), "w", encoding="utf-8") as f:
            f.write(text)
    with open(os.path.join(work, "calls.json"), "w") as f:
        json.dump(calls, f)
    with open(os.path.join(work, "schemas.json"), "w") as f:
        json.dump(schemas, f)

    outs = {}
    for label, src in (("base", base_src), ("branch", os.path.join(REPO, "src"))):
        outs[label] = os.path.join(work, f"{label}.jsonl")
        subprocess.run([sys.executable, os.path.join(HERE, "_identity_worker.py"), src,
                        os.path.join(work, "calls.json"), os.path.join(work, "schemas.json"), outs[label]], check=True)

    with open(outs["base"]) as fa, open(outs["branch"]) as fb:
        a, b = fa.read().splitlines(), fb.read().splitlines()
    differ = [i for i, (x, y) in enumerate(zip(a, b)) if x != y] + list(range(min(len(a), len(b)), max(len(a), len(b))))
    kinds, statements, flush_statements, staged = {}, 0, 0, 0
    for line in a:
        d = json.loads(line)
        if "flush" in d:
            flush_statements += len(d["statements"])
            staged += d["calls"]
            continue
        key = f"/upsert {d['ingress_status']}, worker {[w['status'] for w in d['worker']]}"
        kinds[key] = kinds.get(key, 0) + 1
        statements += len([e for e in d["ingress_events"] if "sql" in e or "insert_rows_json" in e])
        statements += len([e for w in d["worker"] for e in w["events"] if "sql" in e or "insert_rows_json" in e])
    print(f"{len(calls)} recorded calls since {since:%Y-%m-%d %H:%M} UTC; {statements} BigQuery statements "
          f"compared at /upsert and /tasks/upsert (staging appends included); {staged} staged calls flushed "
          f"into {flush_statements} flush statements compared")
    print("outcomes (base):", kinds)
    if differ:
        print(f"DIFFERENT on {len(differ)} output lines (calls and flush chunks); first: line {differ[0]}")
        x, y = json.loads(a[differ[0]]), json.loads(b[differ[0]]) if differ[0] < len(b) else {}
        for k in sorted(set(x) | set(y)):
            if x.get(k) != y.get(k):
                print(f"  {k}:\n    base   {json.dumps(x.get(k))[:600]}\n    branch {json.dumps(y.get(k))[:600]}")
    shutil.rmtree(work, ignore_errors=True)
    staged_expected = any(t in ("users", "responses") for t in TARGETS)
    empty = not calls or (staged_expected and flush_statements == 0)
    print("RESULT:", "DIFFERENT" if differ else ("NO CALLS" if not calls else
                                                  ("NO FLUSH STATEMENTS" if empty else "IDENTICAL")))
    sys.exit(0 if (not differ and not empty) else 1)


if __name__ == "__main__":
    main_()

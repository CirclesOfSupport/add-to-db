"""
Offline gate for "triage and testimonial writes are unchanged": for every webhook call
to /upsert recorded in the last --days days that carries triage_data or feedback, the
branch and the live revision must produce byte-identical results -- the /upsert
response, what each queues, and, per queued item, the /tasks/upsert response and
every BigQuery statement's SQL text and parameters.

Reads only: the webhook log, the two tables' schemas, and the live service's image
tag (to know which commit is live). Nothing is written anywhere; the two versions run
in separate processes with BigQuery and Cloud Tasks replaced by recorders.

    python tools/check_triage_identity.py            # live commit from the deployed image
    python tools/check_triage_identity.py --base b4ac22f --days 7
Exit code 0 = identical for every call.
"""
from __future__ import annotations

import argparse
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
TARGETS = ("triage_data", "feedback")
FILES = ("main.py", "bq_writer.py", "config.py", "auth.py", "tasks.py")


def live_commit() -> str | None:
    gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud")
    if not gcloud:
        return None
    out = subprocess.run([gcloud, "run", "services", "describe", "add-to-db", "--region=us-east1",
                          "--format=value(spec.template.spec.containers[0].image)"], capture_output=True, text=True)
    image = out.stdout.strip()
    return image.rsplit(":", 1)[-1] if ":" in image else None


def main_():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--base", default=None, help="commit to compare against (default: the deployed image's)")
    args = ap.parse_args()

    base = args.base or live_commit()
    if not base:
        raise SystemExit("could not read the deployed image tag; pass --base <commit>")
    head = subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(f"live (base) {base[:12]} vs branch {head[:12]}")

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
    kinds = {}
    for line in a:
        d = json.loads(line)
        key = f"/upsert {d['ingress_status']}, worker {[w['status'] for w in d['worker']]}"
        kinds[key] = kinds.get(key, 0) + 1
    statements = sum(len([e for w in json.loads(l)["worker"] for e in w["events"] if "sql" in e]) for l in a)
    print(f"{len(calls)} recorded calls since {since:%Y-%m-%d %H:%M} UTC; {statements} BigQuery statements compared")
    print("outcomes (base):", kinds)
    if differ:
        print(f"DIFFERENT for {len(differ)} calls; first: call {differ[0]}")
        x, y = json.loads(a[differ[0]]), json.loads(b[differ[0]]) if differ[0] < len(b) else {}
        for k in sorted(set(x) | set(y)):
            if x.get(k) != y.get(k):
                print(f"  {k}:\n    base   {json.dumps(x.get(k))[:600]}\n    branch {json.dumps(y.get(k))[:600]}")
    shutil.rmtree(work, ignore_errors=True)
    print("RESULT:", "IDENTICAL" if not differ and calls else ("NO CALLS" if not calls else "DIFFERENT"))
    sys.exit(0 if (not differ and calls) else 1)


if __name__ == "__main__":
    main_()

"""
Worker for check_identity.py: loads ONE version of the service from a source
directory with BigQuery and Cloud Tasks replaced by recorders, sends every recorded
webhook body through /upsert and each queued item through /tasks/upsert, and writes
what the service would have sent (SQL text, parameters, responses) as JSON lines.
Nothing leaves the process: no BigQuery job, no task.

Deterministic in both processes: the receive clock and uuid4 are pinned per call
(call i is received at 2026-01-01 00:00:00 UTC + i seconds; its uuid4 values are
UUID(int = i * 1000 + n)), so a staged request_id or received_at can only differ if
the code differs.

Staged calls (the check-in targets under STAGED_TARGETS) are then FLUSHED under the
recorder: every staging row the replay appended is read back in receive order, as the
live flush reads it (data = the stored payload JSON, ref = the staging row), and put
through plan_target_writes in chunks of FLUSH_MAX_ITEMS per target with the live
prefix "f_"; each chunk's statements (SQL text and parameters) and set-aside calls are
written as one more JSON line.

    python _identity_worker.py SRC_DIR CALLS.json SCHEMAS.json OUT.jsonl
"""
from __future__ import annotations

import json
import sys
import types
import uuid as _uuid
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone

src, calls_path, schemas_path, out_path = sys.argv[1:5]
sys.path.insert(0, src)

from google.cloud import bigquery  # noqa: E402

with open(schemas_path) as f:
    SCHEMAS = {t: [bigquery.SchemaField.from_api_repr(x) for x in fields] for t, fields in json.load(f).items()}


class _Table:
    def __init__(self, table_id, schema):
        self.table_id, self.schema, self.reference = table_id, schema, table_id


class _Job:
    def result(self, *a, **k):
        return []


class Recorder:
    def __init__(self):
        self.events = []

    def get_table(self, table_id):
        table_id = str(table_id)
        if table_id not in SCHEMAS:
            raise RuntimeError(f"no recorded schema for {table_id}")
        return _Table(table_id, list(SCHEMAS[table_id]))

    def update_table(self, table, fields):
        self.events.append({"schema_change": [f.name for f in table.schema]})
        return table

    def query(self, sql, job_config=None, **kwargs):
        params = [p.to_api_repr() for p in (job_config.query_parameters if job_config else [])]
        self.events.append({"sql": sql, "params": params})
        return _Job()

    def insert_rows_json(self, table, rows, **kwargs):
        self.events.append({"insert_rows_json": str(table), "rows": rows})
        return []


rec = Recorder()
bigquery.Client = lambda *a, **k: rec
import config  # noqa: E402
import main  # noqa: E402

main.is_authorized = lambda request: True
main.is_task_request_authorized = lambda request: True
main.time_module.sleep = lambda s: None
queued = []
main.enqueue_write = lambda path, target, data: queued.append((path, target, data)) or "task"
if hasattr(config, "STAGED_TARGETS"):
    config.STAGED_TARGETS = {"users", "responses"}          # the cutover setting must not touch these targets
if hasattr(main, "enqueue_flush"):
    main.enqueue_flush = lambda *a, **k: rec.events.append({"flush_kick": True})

T0 = _datetime(2026, 1, 1, tzinfo=_timezone.utc)


class _PinnedClock(_datetime):
    at = T0

    @classmethod
    def now(cls, tz=None):
        return cls.at if tz is not None else cls.at.replace(tzinfo=None)


_uuid_n = [0]


def _pinned_uuid4():
    _uuid_n[0] += 1
    return _uuid.UUID(int=_uuid_n[0])


if hasattr(main, "_dt"):
    main._dt = _PinnedClock
if hasattr(main, "uuid_module"):
    main.uuid_module = types.SimpleNamespace(uuid4=_pinned_uuid4)
if hasattr(main, "random_module"):
    main.random_module.seed(0)

staged_rows = []   # every row the replay appended to the staging table, in order

web = main.app.test_client()
with open(calls_path) as f:
    calls = json.load(f)
with open(out_path, "w") as out:
    for i, body in enumerate(calls):
        _PinnedClock.at = T0 + _timedelta(seconds=i)
        _uuid_n[0] = i * 1000
        queued.clear()
        rec.events.clear()
        r = web.post("/upsert", json=body)
        staging = getattr(config, "STAGING_TABLE", None)
        staged_rows.extend(row for e in rec.events if staging and e.get("insert_rows_json") == str(staging)
                           for row in e["rows"])
        line = {"i": i, "ingress_status": r.status_code, "ingress_body": r.get_json(),
                "ingress_events": list(rec.events), "queued": [list(q) for q in queued], "worker": []}
        for path, target, data in list(queued):
            rec.events.clear()
            w = web.post(path, json={"table": target, "data": data})
            line["worker"].append({"status": w.status_code, "body": w.get_json(), "events": list(rec.events)})
        out.write(json.dumps(line, sort_keys=True, default=str) + "\n")

    flushed = 0
    if staged_rows and hasattr(main, "plan_target_writes"):
        size = int(getattr(config, "FLUSH_MAX_ITEMS", 300))
        order = list(getattr(config, "FLUSH_TARGET_ORDER", sorted({r["target"] for r in staged_rows})))
        for target in order + sorted({r["target"] for r in staged_rows} - set(order)):
            rows = [r for r in staged_rows if r["target"] == target]
            for k in range(0, len(rows), size):
                chunk = rows[k:k + size]
                items = [{"data": json.loads(r["payload"]), "ref": r} for r in chunk]
                rec.events.clear()
                plan = main.plan_target_writes(target, items, prefix="f_")
                line = {"flush": target, "chunk": k // size, "calls": len(chunk),
                        "rows": plan["rows"], "keys": plan["keys"], "keyless": plan["keyless"],
                        "statements": [{"sql": sql, "params": [p.to_api_repr() for p in params]}
                                       for sql, params in plan["statements"]],
                        "set_aside": [{"ref": d["ref"], "errors": d["errors"]} for d in plan["dead"]],
                        "events": list(rec.events)}
                out.write(json.dumps(line, sort_keys=True, default=str) + "\n")
                flushed += len(plan["statements"])
print(f"{len(calls)} calls written by {src}; {len(staged_rows)} staged calls flushed into {flushed} statements")

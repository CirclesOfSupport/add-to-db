"""
Worker for check_identity.py: loads ONE version of the service from a source
directory with BigQuery and Cloud Tasks replaced by recorders, sends every recorded
webhook body through /upsert and each queued item through /tasks/upsert, and writes
what the service would have sent (SQL text, parameters, responses) as JSON lines.
Nothing leaves the process: no BigQuery job, no task.

    python _identity_worker.py SRC_DIR CALLS.json SCHEMAS.json OUT.jsonl
"""
from __future__ import annotations

import json
import sys

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

web = main.app.test_client()
with open(calls_path) as f:
    calls = json.load(f)
with open(out_path, "w") as out:
    for i, body in enumerate(calls):
        queued.clear()
        rec.events.clear()
        r = web.post("/upsert", json=body)
        line = {"i": i, "ingress_status": r.status_code, "ingress_body": r.get_json(),
                "ingress_events": list(rec.events), "queued": [list(q) for q in queued], "worker": []}
        for path, target, data in list(queued):
            rec.events.clear()
            w = web.post(path, json={"table": target, "data": data})
            line["worker"].append({"status": w.status_code, "body": w.get_json(), "events": list(rec.events)})
        out.write(json.dumps(line, sort_keys=True, default=str) + "\n")
print(f"{len(calls)} calls written by {src}")

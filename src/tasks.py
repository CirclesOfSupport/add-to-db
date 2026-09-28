from __future__ import annotations
import hashlib
import json
from google.cloud import tasks_v2
import config

_client: tasks_v2.CloudTasksClient | None = None


def _get_client() -> tasks_v2.CloudTasksClient:
    global _client
    if _client is None:
        _client = tasks_v2.CloudTasksClient()
    return _client


def enqueue_write(path: str, target: str, data: dict) -> str:
    """
    Durably enqueues a BigQuery write (insert or upsert) to be performed by
    the /tasks/* worker endpoint at `path` on this same Cloud Run service.

    Returns the created task's resource name, which callers can use as
    confirmation the operation is queued (Cloud Tasks persists the task even
    if this instance dies before the worker request completes).
    """
    client = _get_client()
    parent = client.queue_path(config.TASKS_PROJECT, config.TASKS_LOCATION, config.TASKS_QUEUE)

    task = tasks_v2.Task(
        http_request=tasks_v2.HttpRequest(
            http_method=tasks_v2.HttpMethod.POST,
            url=f"{config.SERVICE_URL}{path}",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"table": target, "data": data}).encode(),
            oidc_token=tasks_v2.OidcToken(
                service_account_email=config.TASKS_INVOKER_SERVICE_ACCOUNT,
                audience=config.SERVICE_URL,
            ),
        )
    )

    created = client.create_task(parent=parent, task=task)
    return created.name


def flush_task_id(kind: str, key) -> str:
    """
    Task ID for a flush-queue task. Cloud Tasks' CreateTask reference warns that sequential task IDs
    (or sequential prefixes) increase latency and error rates, and recommends a well-distributed
    prefix such as a hash; the readable part follows it: "<12 hex>-<kind>-<key>".
    """
    readable = f"{kind}-{key}"
    return f"{hashlib.sha256(readable.encode()).hexdigest()[:12]}-{readable}"


def enqueue_flush(bucket, schedule_epoch_s: int, kind: str = "flush", body: dict | None = None) -> str | None:
    """
    Ask for one flush on the flush queue (max concurrency 1), run at `schedule_epoch_s`.

    kind "flush": the flush of receive bucket `bucket`, requested by every staged call in it; the
                  task is named after the bucket, so every later call in the bucket is a no-op.
    kind "sweep": the 5-minute scheduler sweep's flush (named per sweep bucket, so it never takes
                  the name a call in the same bucket would ask for); carries {"late_check": true}.
    kind "drain": the next flush of a backlog the previous flush could not finish (named after the
                  watermark it reached, so a retried flush does not ask twice).
    Returns the task name, or None when a task of that name already exists.
    """
    from google.api_core import exceptions as gexc
    from google.protobuf import timestamp_pb2

    client = _get_client()
    parent = client.queue_path(config.TASKS_PROJECT, config.TASKS_LOCATION, config.FLUSH_QUEUE)
    when = timestamp_pb2.Timestamp(seconds=int(schedule_epoch_s))
    task = tasks_v2.Task(
        name=f"{parent}/tasks/{flush_task_id(kind, bucket)}",
        schedule_time=when,
        http_request=tasks_v2.HttpRequest(
            http_method=tasks_v2.HttpMethod.POST,
            url=f"{config.SERVICE_URL}/tasks/flush",
            headers={"Content-Type": "application/json"},
            body=json.dumps(body or {}).encode(),
            oidc_token=tasks_v2.OidcToken(
                service_account_email=config.TASKS_INVOKER_SERVICE_ACCOUNT,
                audience=config.SERVICE_URL,
            ),
        ),
    )
    try:
        return client.create_task(parent=parent, task=task).name
    except gexc.AlreadyExists:
        return None

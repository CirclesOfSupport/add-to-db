from __future__ import annotations
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

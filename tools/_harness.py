"""
Shared setup for the proof and replay tools: runs the service's own worker code
in-process against DEV copies of the tables. Nothing here touches Cloud Tasks.

Credentials come from the gcloud CLI's ACTIVE account (`gcloud auth
print-access-token`), not from Application Default Credentials, so the tools
run as the account of the active gcloud configuration.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from google.auth import credentials as ga_credentials  # noqa: E402
from google.cloud import bigquery  # noqa: E402

PROJECT = "early-alert-responses"
RESUMABLE_HINT = False   # a resumable tool sets this so the sign-in message says it resumes


class GcloudCliCredentials(ga_credentials.Credentials):
    """Bearer token from `gcloud auth print-access-token`, refreshed every 45 minutes."""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud")
        if not self._gcloud:
            raise SystemExit("gcloud is not on PATH")

    def refresh(self, request):
        with self._lock:
            out = subprocess.run([self._gcloud, "auth", "print-access-token"],
                                 capture_output=True, text=True)
            if out.returncode != 0:
                detail = (out.stderr or "").strip().splitlines()
                raise SystemExit(
                    "\ngcloud could not refresh the access token (the sign-in has probably expired).\n"
                    "Run:  gcloud auth login\n"
                    "then run the same command again"
                    + (" (it resumes where it stopped)." if RESUMABLE_HINT else ".")
                    + (f"\ngcloud said: {detail[-1]}" if detail else ""))
            self.token = out.stdout.strip()
            self.expiry = (datetime.now(timezone.utc) + timedelta(minutes=45)).replace(tzinfo=None)

    @property
    def expired(self):
        return self.expiry is None or datetime.utcnow() >= self.expiry

    @property
    def valid(self):
        return self.token is not None and not self.expired


def make_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT, credentials=GcloudCliCredentials())


class JobLog:
    """Wraps client.query so every job the worker runs is recorded."""

    def __init__(self, client: bigquery.Client):
        self.client = client
        self.jobs: list = []
        self._orig = client.query
        self._lock = threading.Lock()

        def query(sql, job_config=None, **kwargs):
            job = self._orig(sql, job_config=job_config, **kwargs)
            with self._lock:
                self.jobs.append(job)
            return job

        client.query = query

    def stats(self, jobs=None):
        rows = []
        for job in (self.jobs if jobs is None else jobs):
            try:
                job.reload()
            except Exception:
                pass
            ms = None
            if job.started and job.ended:
                ms = int((job.ended - job.started).total_seconds() * 1000)
            rows.append({
                "job_id": job.job_id,
                "statement": (job.statement_type or ""),
                "bytes_processed": job.total_bytes_processed,
                "bytes_billed": job.total_bytes_billed,
                "ms": ms,
                "error": (job.error_result or {}).get("message") if job.error_result else None,
            })
        return rows


def load_service(client: bigquery.Client, dev_targets: dict[str, str]):
    """
    Import the service's main module with `client` as its BigQuery client and the
    given targets pointed at DEV tables. The users_and_responses view refresh is
    disabled so a DEV schema change can never rewrite a RESPONSES view.
    """
    import config

    # main builds its client at import; hand it ours so ADC is never consulted
    original = bigquery.Client
    bigquery.Client = lambda *a, **k: client
    try:
        import main
    finally:
        bigquery.Client = original

    for target, table_id in dev_targets.items():
        if not table_id.startswith(f"{PROJECT}.DEV."):
            raise SystemExit(f"refusing non-DEV table for {target}: {table_id}")
        config.ALLOWED_TARGETS[target] = table_id  # same dict object main imported
    main.client = client
    main.update_users_and_responses_view = lambda *a, **k: None
    main.time_module.sleep = time.sleep
    return main


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

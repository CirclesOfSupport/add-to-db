from __future__ import annotations
import os
from datetime import date, datetime, time
from decimal import Decimal

PROJECT_ID = "early-alert-responses"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Cloud Tasks queue that the /ingest and /upsert webhook endpoints enqueue
# onto, so the webhook can respond as soon as work is durably queued instead
# of waiting for the BigQuery write to finish.
TASKS_PROJECT = os.getenv("TASKS_PROJECT", PROJECT_ID)
TASKS_LOCATION = os.getenv("TASKS_LOCATION", "us-east1")
TASKS_QUEUE = os.getenv("TASKS_QUEUE", "add-to-db-writes")

# Base URL of this Cloud Run service (e.g. https://add-to-db-xxxx-ue.a.run.app).
# Used both as the Cloud Tasks callback target and as the expected OIDC
# audience when verifying that a /tasks/* request really came from Cloud Tasks.
SERVICE_URL = os.getenv("SERVICE_URL", "")

# Service account Cloud Tasks uses to sign the OIDC token on its callback.
# Requests to /tasks/* are rejected unless the token's email matches this.
TASKS_INVOKER_SERVICE_ACCOUNT = os.getenv("TASKS_INVOKER_SERVICE_ACCOUNT", "")

# Only allow approved destinations.
ALLOWED_TARGETS: dict[str, str] = {
    "users": f"{PROJECT_ID}.RESPONSES.users",
    "responses": f"{PROJECT_ID}.RESPONSES.response_data",
    "triage_data": f"{PROJECT_ID}.RESPONSES.triage-message-data",
    "feedback": f"{PROJECT_ID}.RESPONSES.subscriber_feedback",
    "users_copy": f"{PROJECT_ID}.COPY.users",
    "responses_copy": f"{PROJECT_ID}.COPY.response_data",
}

# Staging deployments only: TARGET_TABLES="responses=<project.dataset.table>;users=<...>" points
# those targets at other tables (a DEV copy). Only DEV tables are accepted; unset in production.
for _pair in filter(None, os.getenv("TARGET_TABLES", "").split(";")):
    _target, _, _table = _pair.partition("=")
    _target, _table = _target.strip(), _table.strip()
    if _target not in ALLOWED_TARGETS or not _table.startswith(f"{PROJECT_ID}.DEV."):
        raise RuntimeError(f"TARGET_TABLES: '{_pair}' must name a known target and a {PROJECT_ID}.DEV table")
    ALLOWED_TARGETS[_target] = _table

UPSERT_KEYS: dict[str, list[str]] = {
    "users": ["uuid"],
    "responses": ["SessionID"],
    "triage_data": ["message_id"],
    "feedback": ["testimonial_id"],
    "users_copy": ["uuid"],
    "responses_copy": ["SessionID"],
}

# Partition column per target. When set, the MERGE gets an extra
# `AND T.<col> BETWEEN @min_dt AND @max_dt` predicate so BigQuery can prune
# to the partition(s) the incoming row belongs to. Only DATETIME columns are
# supported; the value comes from the row itself (one row per MERGE, so
# min == max). Targets not listed here get the plain key-only MERGE.
# Harmless on an unpartitioned table (predicate is redundant), so this can
# ship before the table is rebuilt.
PARTITION_COLUMNS: dict[str, str] = {
    "responses": "checkinDateTime",
}

# How each date/time column of a target is stored, where it differs from the
# plain "drop the offset, keep the local wall clock" default. Only targets
# listed here change; every other target keeps the default exactly.
#
# response_data stores its timestamps as the UTC wall clock of the instant the
# payload names (2026-05-26T11:36:27-04:00 is stored as 2026-05-26 15:36:27),
# and its one date-only DATETIME column as midnight of the payload's own local
# date. DATE columns keep the payload's own local date (the default).
#   "utc"            -> convert to UTC, then drop the offset
#   "local_midnight" -> midnight of the payload's local calendar date
# "*" is the convention for any DATETIME column of the target not listed.
DATETIME_CONVENTIONS: dict[str, dict[str, str]] = {
    "responses": {
        "checkinDateTime": "utc",
        "checkinReplyDateTime": "utc",
        "resourceOfferReplyDatetime": "utc",
        "referralFollowUpUtilizedDateTime": "utc",
        "checkinReplyDate": "local_midnight",
        "*": "utc",
    },
}

# Columns a blank payload value must never overwrite. A MERGE that matches a
# stored row keeps the stored value when the payload's is NULL.
PRESERVE_ON_BLANK: dict[str, list[str]] = {
    "responses": ["checkinDateTime"],
}

# Targets that accept a row with no key. Such a row cannot be matched to
# anything, so it is inserted as a new row instead of being rejected
# (sign-up events carry no session ID).
KEYLESS_INSERT_TARGETS: set[str] = {"responses"}

TYPE_CHECKERS = {
    "STRING": lambda v: isinstance(v, str),
    "JSON": lambda v: isinstance(v, (dict, list, str)),
    "INTEGER": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "INT64": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "FLOAT": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "FLOAT64": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "NUMERIC": lambda v: isinstance(v, (int, float, Decimal)) and not isinstance(v, bool),
    "BIGNUMERIC": lambda v: isinstance(v, (int, float, Decimal)) and not isinstance(v, bool),
    "BOOLEAN": lambda v: isinstance(v, bool),
    "BOOL": lambda v: isinstance(v, bool),
    "DATETIME": lambda v: isinstance(v, datetime) and v.tzinfo is None,
    "TIMESTAMP": lambda v: isinstance(v, datetime) and v.tzinfo is not None,
    "DATE": lambda v: isinstance(v, date) and not isinstance(v, datetime),
    "TIME": lambda v: isinstance(v, time),
    "BYTES": lambda v: isinstance(v, (bytes, str))
}

# --- Single-writer hookup for the check-in targets --------------------------
# Targets listed in STAGED_TARGETS are not written per call. /upsert appends
# each validated call to STAGING_TABLE (durable before the 202) stamped with
# its receive time; one flusher at a time (queue FLUSH_QUEUE at max
# concurrency 1, plus a compare-and-set watermark in the same transaction as
# the writes, in a state table of the target's own) folds everything received
# up to (now - FLUSH_SAFETY_S) into one MERGE per target. Empty = every target
# keeps the per-call path.
STAGED_TARGETS: set[str] = {t.strip() for t in os.getenv("STAGED_TARGETS", "").split(",") if t.strip()}
# The single writer's own tables live in OPS (operational objects serving backend jobs), never in
# RESPONSES. An override may point them at DEV (staging deployments and proofs) or OPS, nothing else.
STAGING_TABLE = os.getenv("STAGING_TABLE", f"{PROJECT_ID}.OPS.adb_staging")
DEAD_LETTER_TABLE = os.getenv("DEAD_LETTER_TABLE", f"{PROJECT_ID}.OPS.adb_set_aside")  # calls set aside, not written
FLUSH_LOG_TABLE = os.getenv("FLUSH_LOG_TABLE", f"{PROJECT_ID}.OPS.adb_flush_log")
FLUSH_STATE_TABLE = os.getenv("FLUSH_STATE_TABLE", f"{PROJECT_ID}.OPS.adb_flush_state")
for _name, _table in (("STAGING_TABLE", STAGING_TABLE), ("DEAD_LETTER_TABLE", DEAD_LETTER_TABLE),
                      ("FLUSH_LOG_TABLE", FLUSH_LOG_TABLE), ("FLUSH_STATE_TABLE", FLUSH_STATE_TABLE)):
    if not _table.startswith((f"{PROJECT_ID}.OPS.", f"{PROJECT_ID}.DEV.")):
        raise RuntimeError(f"{_name}: '{_table}' must be a {PROJECT_ID}.OPS or {PROJECT_ID}.DEV table")
FLUSH_QUEUE = os.getenv("FLUSH_QUEUE", "add-to-db-flush")
FLUSH_BUCKET_S = int(os.getenv("FLUSH_BUCKET_S", "30"))      # one flush per this many seconds of traffic
FLUSH_SAFETY_S = int(os.getenv("FLUSH_SAFETY_S", "20"))      # only calls received this long ago are flushed
# Items per target per flush. The 2026-09-28 staging walk measured a flush at ~0.19 s per item (44 s for
# 238 items) and both targets run in one request, so 300 per target keeps one flush request within
# ~120 s at that cost (2 x 300 x 0.19 s + ~4 s fixed). A flush that leaves backlog schedules the next
# one at once (a drain task), so a multi-hour backlog drains in a chain of bounded flushes.
FLUSH_MAX_ITEMS = int(os.getenv("FLUSH_MAX_ITEMS", "300"))
FLUSH_ALERT_AFTER = int(os.getenv("FLUSH_ALERT_AFTER", "3"))  # consecutive failed flushes before FLUSH_ALERT
FLUSH_TARGET_ORDER = ["responses", "users"]   # check-in rows first; each target is its own transaction

# One flush-state table PER TARGET. A flush transaction advances its target's watermark row, and
# BigQuery lets only one transaction at a time change rows in a table, whichever rows they are. With
# both targets' rows in one table, each target's transaction waited on the other's: on 2026-09-30 a
# check-in flush whose MERGE had finished in 5 s then waited 651 s to update its watermark, kept its
# transaction open on response_data past the request timeout, and eight flushes failed behind it.
# The first target in FLUSH_TARGET_ORDER keeps FLUSH_STATE_TABLE; every other staged target has its
# own table -- FLUSH_STATE_TABLE_<TARGET> if set, else "<FLUSH_STATE_TABLE>_<target>" -- holding the
# one row "flush:<target>". The watermark update stays inside the write transaction (compare-and-set
# on version), so nothing else about the single writer changes.
FLUSH_STATE_TABLE_OVERRIDES: dict[str, str] = {
    t: v for t in FLUSH_TARGET_ORDER[1:] if (v := os.getenv(f"FLUSH_STATE_TABLE_{t.upper()}", "").strip())}
for _target, _table in FLUSH_STATE_TABLE_OVERRIDES.items():
    if not _table.startswith((f"{PROJECT_ID}.OPS.", f"{PROJECT_ID}.DEV.")):
        raise RuntimeError(f"FLUSH_STATE_TABLE_{_target.upper()}: '{_table}' must be a {PROJECT_ID}.OPS or "
                           f"{PROJECT_ID}.DEV table")


def flush_state_table(target: str) -> str:
    """The table holding `target`'s watermark row (read at call time, so a tool may repoint FLUSH_STATE_TABLE)."""
    if target == FLUSH_TARGET_ORDER[0]:
        return FLUSH_STATE_TABLE
    return FLUSH_STATE_TABLE_OVERRIDES.get(target) or f"{FLUSH_STATE_TABLE}_{target}"


def check_flush_state_tables(targets=None) -> None:
    """Refuse a configuration in which two staged targets would share a flush-state table."""
    owners: dict[str, str] = {}
    for target in sorted(STAGED_TARGETS if targets is None else targets):
        table = flush_state_table(target)
        if table in owners:
            raise RuntimeError(f"flush state: targets '{owners[table]}' and '{target}' share the table '{table}'; "
                               f"each staged target needs its own")
        owners[table] = target


check_flush_state_tables()
# Per target, per cycle: how long a contended flush keeps retrying before the cycle moves on
# (the queue and the next bucket retry it later). responses covers the ~65 s nightly vamc UPDATE
# on response_data (2026-09-26); users is kept short so a contended users flush cannot hold the
# single writer -- and with it the next check-in flush -- for long.
FLUSH_RETRY_BUDGET_S: dict[str, float] = {
    "responses": float(os.getenv("FLUSH_RETRY_BUDGET_RESPONSES_S", "75")),
    "users": float(os.getenv("FLUSH_RETRY_BUDGET_USERS_S", "20")),
}
# How long one flush transaction may run before it is stopped. On 2026-09-30 one statement inside a
# check-in flush's transaction (the watermark update, after the MERGE had finished) ran 651 s and ended
# in a BigQuery internal error. Nothing bounded it: the request was cut at 300 s, the script went on
# holding its transaction open on response_data, and every check-in flush behind it was cancelled
# until it ended (11 minutes). Each flush script now carries this limit as its BigQuery job timeout,
# and the flusher stops waiting FLUSH_JOB_TIMEOUT_GRACE_S after it and asks BigQuery to cancel the
# job; a transaction that does not commit is rolled back. Both targets run in one request, so the two
# limits and their grace must fit inside the service's request timeout (FLUSH_REQUEST_TIMEOUT_S).
FLUSH_JOB_TIMEOUT_S: dict[str, float] = {
    "responses": float(os.getenv("FLUSH_JOB_TIMEOUT_RESPONSES_S", "140")),
    "users": float(os.getenv("FLUSH_JOB_TIMEOUT_USERS_S", "90")),
}
FLUSH_JOB_TIMEOUT_GRACE_S = float(os.getenv("FLUSH_JOB_TIMEOUT_GRACE_S", "10"))
FLUSH_REQUEST_TIMEOUT_S = float(os.getenv("FLUSH_REQUEST_TIMEOUT_S", "300"))
if sum(FLUSH_JOB_TIMEOUT_S.values()) + 2 * FLUSH_JOB_TIMEOUT_GRACE_S + 30 > FLUSH_REQUEST_TIMEOUT_S:
    raise RuntimeError("FLUSH_JOB_TIMEOUT_*_S: the two flush limits, their grace and 30 s for the reads must fit "
                       "inside FLUSH_REQUEST_TIMEOUT_S")
FLUSH_BACKOFF_BASE_S = float(os.getenv("FLUSH_BACKOFF_BASE_S", "1"))
FLUSH_BACKOFF_CAP_S = float(os.getenv("FLUSH_BACKOFF_CAP_S", "15"))

# The staging append must finish well inside FLUSH_SAFETY_S of the call's receive stamp: a flush only
# reads calls received at least FLUSH_SAFETY_S ago, so an append that becomes visible later than that
# can land behind a flush that already covered its receive time. The whole /upsert, from the receive
# stamp to the end of the append, gets this budget; past it the call is answered 500 (not 202) and
# an ADB_ALERT line is logged. Each append attempt is capped at STAGING_APPEND_ATTEMPT_S.
STAGING_APPEND_BUDGET_S = float(os.getenv("STAGING_APPEND_BUDGET_S", "12"))
STAGING_APPEND_ATTEMPT_S = float(os.getenv("STAGING_APPEND_ATTEMPT_S", "5"))
if STAGING_APPEND_BUDGET_S + 5 > FLUSH_SAFETY_S:
    raise RuntimeError("STAGING_APPEND_BUDGET_S must be at least 5 s shorter than FLUSH_SAFETY_S")

# A staged call that became visible after the flush covering its receive time is found by the late
# check (run by the 5-minute sweep, looking back this far), dead-lettered and alerted -- never
# written late, where it could overwrite a newer call's values.
LATE_CHECK_LOOKBACK_S = int(os.getenv("LATE_CHECK_LOOKBACK_S", str(6 * 3600)))

# Alerting. Every condition that needs a person is logged as one line containing ADB_ALERT; a Cloud
# Monitoring log-match alert policy on that token emails us. The sweep (/tasks/flush-kick, every
# 5 minutes) raises BACKLOG when a target's oldest unflushed call is older than BACKLOG_ALERT_S, unless
# the flush is paused for maintenance (flush_state.paused_since set by tools/flush_pause.py), and
# raises PAUSE when a maintenance pause has lasted longer than PAUSE_ALERT_S (a pause left on by
# mistake). While paused, the flush writes nothing.
BACKLOG_ALERT_S = int(os.getenv("BACKLOG_ALERT_S", str(10 * FLUSH_BUCKET_S)))
PAUSE_ALERT_S = int(os.getenv("PAUSE_ALERT_S", str(4 * 3600)))

# Reply fields a stale carry-over call must not write (see apply_stale_reply_guard).
STALE_REPLY_FIELDS: dict[str, list[str]] = {
    "responses": ["checkinReply", "checkinReplyText", "checkinReplyNumerical",
                  "checkinReplyDateTime", "checkinReplyDistressed"],
}

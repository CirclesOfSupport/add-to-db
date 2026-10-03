# Early Alert: add-to-db

A lightweight Flask webhook service that accepts JSON payloads and writes them to Google BigQuery. Supports both plain inserts and upserts (via BigQuery `MERGE`). Runs as a containerized Cloud Run service built and deployed via Cloud Build.

Webhook requests are validated synchronously but written to BigQuery **asynchronously** via a Cloud Tasks queue — see [Asynchronous Processing](#asynchronous-processing-cloud-tasks) below for why and how.

---

## Architecture

```
Webhook caller (TextIt)
    │  POST /ingest or /upsert
    │  (fast: auth + schema/type validation, then enqueue)
    ▼
Cloud Run (add-to-db)   ←── Cloud Build CI/CD
    │
    │  enqueues an HTTP task, OIDC-signed
    ▼
Cloud Tasks queue (add-to-db-writes, us-east1)
    │
    │  calls back with the OIDC token attached
    ▼
Cloud Run (add-to-db) — POST /tasks/ingest or /tasks/upsert
    │  (schema migration + the actual write)
    ▼
Google BigQuery (early-alert-responses)
```

The webhook caller only ever talks to `/ingest` and `/upsert`. `/tasks/ingest` and `/tasks/upsert` are internal — they're only reachable with a valid OIDC token minted by our own Cloud Tasks queue.

| File | Purpose |
|---|---|
| `src/main.py` | Flask app — route handlers, fast pre-write validation, task enqueueing, and the `/tasks/*` worker endpoints that do the actual insert/upsert |
| `src/config.py` | Allowed tables, upsert keys, type-checker map, and Cloud Tasks settings (queue, location, service URL, invoker service account) |
| `src/auth.py` | Request authorization for webhook callers (shared secret header) and for `/tasks/*` callbacks (Cloud Tasks OIDC token verification) |
| `src/tasks.py` | Builds and enqueues the Cloud Tasks HTTP task that triggers the async write |
| `src/bq_writer.py` | BigQuery helpers — schema expansion, type inference, MERGE query construction, and query parameter construction |
| `Dockerfile` | Container definition (Python 3.12 slim + gunicorn) |
| `cloudbuild.yaml` | Cloud Build pipeline — build, push, deploy to Cloud Run (also sets the Cloud Tasks env vars on deploy) |

---

## Build & Deployment

The service is built and deployed automatically via **Google Cloud Build**.

**Pipeline steps (`cloudbuild.yaml`):**
1. Build the Docker image and tag it with `$COMMIT_SHA`
2. Push the image to Artifact Registry at `us-east1-docker.pkg.dev/$PROJECT_ID/webhook-repo/add-to-db`
3. Deploy to Cloud Run in `us-east1` with:
   - 0 minimum instances (scales to zero)
   - 50 maximum instances (burst capacity for traffic spikes)
   - 50 max concurrent requests per instance (allows multiple requests to queue up on a single instance during bursts, reducing cold starts)
   - 1 worker / 8 threads (gunicorn)
   - Unauthenticated public access (protected by the webhook secret header)
   - The Cloud Tasks env vars (`TASKS_QUEUE`, `TASKS_LOCATION`, `TASKS_INVOKER_SERVICE_ACCOUNT`, and `SERVICE_URL` once known — see below) set via `--update-env-vars`

To trigger a deploy, push a commit to the connected repository branch.

**One-time infra setup (not managed by this repo — run manually against the GCP project before this branch's first deploy):**
- Enable the Cloud Tasks API
- Create the `add-to-db-writes` queue in `us-east1`
- Create a dedicated service account (e.g. `add-to-db-tasks-invoker@...`) and grant it `roles/run.invoker` on the `add-to-db` Cloud Run service, so Cloud Tasks can call back into it
- Grant the Cloud Run service's own runtime service account `roles/cloudtasks.enqueuer` at the project level, so it can enqueue tasks

---

## Asynchronous Processing (Cloud Tasks)

**Why:** the webhook caller only waits ~15 seconds for a response. The BigQuery write (schema migration + `insert_rows`/`MERGE`) occasionally takes longer than that — cold starts on a scale-to-zero Cloud Run service, BigQuery query queueing, or the concurrent-update retry loop in `run_upsert_with_retry` (up to ~2s of retries on top of the query itself). When that happens, the caller reports a false timeout/failure even though the write succeeds moments later.

**How it works:** `/ingest` and `/upsert` split the request into two phases:

1. **Synchronous (in the webhook response):** authenticate, parse the request, look up the current BigQuery schema, and validate the payload's types/required fields against it. This is what still returns an immediate `400` for bad data.
2. **Asynchronous (after the response):** if validation passes, the raw payload is enqueued as a Cloud Tasks HTTP task targeting `/tasks/ingest` or `/tasks/upsert` on this same service, and the webhook responds `202` with `"status": "queued"` and the task name. The queued task is what actually migrates the schema (adds any new columns) and performs the `insert_rows`/`MERGE` write.

Cloud Tasks (rather than, say, a background thread) is used deliberately for two reasons:
- **Durability:** the task is persisted by Cloud Tasks independently of this service's process, so a `202` is a reliable signal even if the instance that enqueued it is later recycled before the write runs.
- **Cloud Run CPU allocation:** by default Cloud Run only allocates CPU while a request is being handled. A background thread kicked off after the response returns could get starved. The Cloud Tasks callback is a normal new HTTP request, so it gets full CPU like any other request.

**Tradeoff:** because the caller already has a `202` before the write happens, an error that only surfaces in the queued worker (e.g., an invalid new column name, or a permanent BigQuery error) is **not** returned to the original caller — it's only visible in Cloud Run logs (`app.logger.error` in the `/tasks/*` handlers). Transient BigQuery errors are retried automatically by Cloud Tasks per the queue's retry policy.

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `WEBHOOK_SECRET` | *(empty)* | Shared secret webhook callers must send in `X-Webhook-Secret`. If unset, all webhook requests are allowed (useful for local dev). |
| `TASKS_PROJECT` | value of `PROJECT_ID` in `config.py` | GCP project containing the Cloud Tasks queue. |
| `TASKS_LOCATION` | `us-east1` | Region of the Cloud Tasks queue. |
| `TASKS_QUEUE` | `add-to-db-writes` | Name of the Cloud Tasks queue that `/ingest`/`/upsert` enqueue onto. |
| `SERVICE_URL` | *(empty)* | Base URL of this Cloud Run service (e.g. `https://add-to-db-xxxx-ue.a.run.app`). Used both as the Cloud Tasks callback target and as the expected OIDC audience when verifying `/tasks/*` requests. **Must** be set for enqueueing to work. |
| `TASKS_INVOKER_SERVICE_ACCOUNT` | *(empty)* | Service account email Cloud Tasks signs its OIDC callback token with. `/tasks/*` requests are rejected unless the token's email matches this. **Must** be set for `/tasks/*` to accept any requests. |

---

## Authentication

There are two distinct auth checks in this service, for two different callers.

**Webhook requests (`/ingest`, `/upsert`)** must include the shared secret in the `X-Webhook-Secret` header:

```
X-Webhook-Secret: <your-secret>
```

The secret is read from the `WEBHOOK_SECRET` environment variable. If the variable is not set, all requests are allowed through (useful for local development).

**Task callbacks (`/tasks/ingest`, `/tasks/upsert`)** are never called by webhook clients — only by our own Cloud Tasks queue. They authenticate via the OIDC `Authorization: Bearer <token>` header Cloud Tasks attaches to the request, which `auth.is_task_request_authorized` verifies against `SERVICE_URL` (as the expected audience) and `TASKS_INVOKER_SERVICE_ACCOUNT` (as the expected signer). If either of those env vars is unset, `/tasks/*` rejects every request with `401` — including local requests, so there's no accidental bypass.

---

## Endpoints

### `GET /`

Health check. Returns `200 OK` when the service is running.

**Response:**
```json
{ "status": "ok" }
```

---

### `POST /ingest`

Validates and queues a single row to be inserted into a BigQuery table. The response confirms the write is durably queued, not that it has completed — see [Asynchronous Processing](#asynchronous-processing-cloud-tasks).

**Single-table request body**

Use this format when inserting one row into one table. This format is still supported for backward compatibility.

```json
{
  "table": "users",
  "data": {
    "field1": "value1",
    "field2": 123
  }
}
```
**Multi-table request body**

Use this format when inserting rows into multiple tables in one HTTP request. Each item in `tables` must include its own `table` and `data` object.

```json
{
  "tables": [
    {
      "table": "users_copy",
      "data": {
        "message_id": "abc123",
        "uuid": "@contact.uuid",
        "test_field": "this is a test"
      }
    },
    {
      "table": "responses_copy",
      "data": {
        "message_id": "abc123",
        "test_field": "this is a test"
      }
    }
  ]
}
```

**Single-table queued response (`202`):**
```json
{
  "status": "queued",
  "operation": "insert",
  "table": "users",
  "table_id": "early-alert-responses.RESPONSES.users",
  "warnings": [],
  "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
}
```
**Multi-table queued response (`202`):**
```json
{
  "status": "queued",
  "operation": "insert",
  "results": [
    {
      "status": "queued",
      "operation": "insert",
      "table": "users_copy",
      "table_id": "early-alert-responses.COPY.users",
      "warnings": [],
      "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
    },
    {
      "status": "queued",
      "operation": "insert",
      "table": "responses_copy",
      "table_id": "early-alert-responses.COPY.response_data",
      "warnings": [],
      "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
    }
  ]
}
```

Note there's no `added_fields` in this response anymore: whether a new BigQuery column actually gets added only happens later, once the queued task runs, so it isn't known at response time. To confirm new fields were added, check Cloud Run logs for the corresponding `/tasks/ingest` invocation.

---

### `POST /upsert`

Validates and queues a single row to be inserted or updated using a BigQuery `MERGE` statement. If a row matching the configured key column(s) already exists, it is updated; otherwise it is inserted. Like `/ingest`, `/upsert` supports both the original single-table request body and the newer multi-table request body, and returns `202` once queued rather than waiting for the `MERGE` to complete — see [Asynchronous Processing](#asynchronous-processing-cloud-tasks).

**Single-table request body**
```json
{
  "table": "users",
  "data": {
    "uuid": "abc-123",
    "name": "Jane Doe"
  }
}
```

**Multi-table request body**
```json
{
  "tables": [
    {
      "table": "users_copy",
      "data": {
        "uuid": "abc-123",
        "name": "Jane Doe"
      }
    },
    {
      "table": "responses_copy",
      "data": {
        "SessionID": "session-123",
        "message_id": "abc123",
        "test_field": "this is a test"
      }
    }
  ]
}
```

**Single-table queued response (`202`):**
```json
{
  "status": "queued",
  "operation": "upsert",
  "table": "users",
  "table_id": "early-alert-responses.RESPONSES.users",
  "warnings": [],
  "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
}
```

**Multi-table queued response (`202`):**
```json
{
  "status": "queued",
  "operation": "upsert",
  "results": [
    {
      "status": "queued",
      "operation": "upsert",
      "table": "users_copy",
      "table_id": "early-alert-responses.COPY.users",
      "warnings": [],
      "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
    },
    {
      "status": "queued",
      "operation": "upsert",
      "table": "responses_copy",
      "table_id": "early-alert-responses.COPY.response_data",
      "warnings": [],
      "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
    }
  ]
}
```

As with `/ingest`, `added_fields` is no longer part of this response — it's only knowable once the queued task actually runs the schema migration.

---

### Internal: `POST /tasks/ingest`, `POST /tasks/upsert`

Not part of the public webhook API — these are the Cloud Tasks callback targets that perform the real work `/ingest` and `/upsert` used to do inline: fetching and migrating the schema, then the actual `insert_rows`/`MERGE`. They accept the same `{"table": ..., "data": ...}` body shape as the single-table `/ingest`/`/upsert` requests.

Requests are rejected with `401` unless they carry a valid OIDC bearer token matching `SERVICE_URL` (audience) and `TASKS_INVOKER_SERVICE_ACCOUNT` (signer) — see [Authentication](#authentication). Validation or write failures return `200` (so Cloud Tasks doesn't retry an error a retry can't fix) but are logged via `app.logger.error`; only transient BigQuery errors return a non-2xx so Cloud Tasks retries with backoff.

---

## Multi-Table Request Behavior
When a request uses the `tables` array, the service validates and enqueues each table item in order. This behavior applies to both `/ingest` and `/upsert`.

| Scenario | Behavior |
|---|---|
| All table items pass validation | Returns `202` with a top-level `results` array containing one `"queued"` result per table. |
| One table is queued and a later table fails validation | The earlier item has already been enqueued (its write will still happen). The request returns a `400` error for the failed table and includes the prior queued items in `queued_results`. |
| A table item fails validation | Processing stops at the failed table. Later table items are not attempted or enqueued. |
| `tables` is empty | Rejected immediately. Returns `400`. |
| `tables` is not a list or cannot be normalized to a list | Rejected immediately. Returns `400`. |
| A table item is missing `table` | Rejected immediately. Returns `400`. |
| A table item has non-object `data` | Rejected immediately. Returns `400`. |
| Both query parameter `table` and body field `tables` are provided | Rejected immediately. Use either the single-table query/body format or the multi-table `tables` array, not both. |

_**NOTE**: Multi-table requests are not atomic across BigQuery tables. If one table is queued and a later table fails validation, the already-queued write is not cancelled — it will still be written to BigQuery asynchronously._

**Partial-success error response example:**

```json
{
  "status": "error",
  "table": "responses_copy",
  "errors": [
    "Missing required field: SessionID"
  ],
  "warnings": [],
  "queued_results": [
    {
      "status": "queued",
      "operation": "insert",
      "table": "users_copy",
      "table_id": "early-alert-responses.COPY.users",
      "warnings": [],
      "task_name": "projects/early-alert-responses/locations/us-east1/queues/add-to-db-writes/tasks/..."
    }
  ]
}
```

---

## Ingest Behavior Reference

The `/ingest` endpoint always performs a straight **INSERT** — it never checks for existing rows. Since writes happen asynchronously (see [Asynchronous Processing](#asynchronous-processing-cloud-tasks)), each scenario below falls into one of two phases:

- **Rejected before queueing** — validated synchronously against the table's current schema; the webhook responds `400` immediately, nothing is enqueued.
- **Queued, then...** — passes synchronous validation, the webhook responds `202` immediately, and the described outcome happens moments later when the queued `/tasks/ingest` task runs. If that step itself fails, the caller's `202` response is unaffected — the failure is only visible in Cloud Run logs.

| Scenario | Behavior |
|---|---|
| **Normal insert, all fields valid** | Queued, then the row is appended to the table. |
| **Table is empty** | Queued, then the row is inserted normally. |
| **Row with same key already exists** | Queued, then a duplicate row is inserted. `/ingest` does not deduplicate — use `/upsert` if uniqueness is required. |
| **Required field missing from input** | Rejected before queueing. Returns `400` with `"Missing required field: <field>"`. |
| **Field value is `null` on a `REQUIRED` column** | Rejected before queueing. Returns `400` with `"Field '<field>' cannot be null"`. |
| **Field value is `null` on a `NULLABLE` column** | Queued, then `NULL` is written to BigQuery. |
| **Wrong data type for a field** | Rejected before queueing. Returns `400` with `"Field '<field>' expected type <TYPE>, got <python_type>"`. |
| **Input contains unknown/extra fields** | Passes synchronous validation with a warning (not an error) — the webhook doesn't know yet whether the field name is even valid. Queued, then the `/tasks/ingest` worker attempts to add it as a new nullable BigQuery column and inserts the row including it. **If the new field name is not a valid BigQuery column name, this fails silently from the caller's point of view** — the `202` response has already been sent; check Cloud Run logs for `/tasks/ingest` errors to catch this. |
| **All fields omitted (empty `data` object)** | Passes validation and is queued only if the table has no `REQUIRED` fields. Otherwise rejected before queueing, `400` for each missing required field. |
| **`data` is not a JSON object (e.g., array or string)** | Rejected immediately. Returns `400` with `"Field 'data' must be a JSON object"`. |
| **`table` is missing or invalid** | Rejected immediately. Returns `400` with `"Missing table"` or `"Invalid table"` and a list of allowed values. |
| **Two inserts in a row with the same data** | Both are queued and both succeed. Two identical rows will exist in the table. |

---

## Upsert Behavior Reference

The `/upsert` endpoint uses a BigQuery `MERGE` statement keyed on the column(s) configured in `UPSERT_KEYS`. As with `/ingest`, writes happen asynchronously — see the phase explanation in [Ingest Behavior Reference](#ingest-behavior-reference) above. "Queued, then..." means the webhook responds `202` immediately and the described outcome happens once the `/tasks/upsert` task runs; a failure at that point is not visible to the caller, only in Cloud Run logs.

| Scenario | Behavior |
|---|---|
| **Key does not exist in table** | Queued, then the row is inserted (`WHEN NOT MATCHED` branch of the MERGE fires). |
| **Key exists once** | Queued, then the existing row is updated with values from `data` (`WHEN MATCHED` branch fires). |
| **Multiple rows exist with the same key** | Queued; the `/tasks/upsert` task's `MERGE` fails since BigQuery cannot update a table row matched more than once. This indicates a data integrity problem in the table — visible only in Cloud Run logs, not to the original caller. |
| **Key field missing from input** | Rejected before queueing. Returns `400` with `"Missing upsert key field: <key>"`. |
| **Key value is `null`** | Rejected before queueing. Returns `400` with `"Upsert key field '<key>' cannot be null"`. |
| **Key value is empty string (`""`)**  | Passes validation (empty string is a valid `STRING`). Queued; BigQuery will match or insert on the empty-string key. |
| **Key exists, other non-key fields omitted** | Queued, then only the fields present in `data` are included in the `UPDATE SET` clause. Omitted fields are left unchanged in the existing row. |
| **Key exists, incoming data is identical to existing row** | Queued, then BigQuery executes the update and overwrites with the same values. No error. Effectively a no-op from a data perspective, but still consumes a slot job. |
| **Key exists, only one field changes** | Queued, then only that row is updated. All other rows and columns are unaffected. |
| **Key exists, incoming value for a field is `null`** | Queued, then the field is set to `NULL` in BigQuery, clearing the previous value. If the column is `REQUIRED`, this is instead caught by synchronous validation and rejected before queueing with `400`. |
| **Input contains unknown/extra fields** | Passes synchronous validation with a warning (not an error). Queued, then the `/tasks/upsert` worker attempts to add it as a new nullable BigQuery column and includes it in the MERGE. **If the new field name is not a valid BigQuery column name, this fails silently from the caller's point of view** — check Cloud Run logs for `/tasks/upsert` errors to catch this. |
| **Wrong data type for a field** | Rejected before queueing. Returns `400` with `"Field '<key>' expected type <TYPE>, got <python_type>"`. |
| **Required non-key field missing on insert** | Rejected before queueing. Returns `400` with `"Missing required field: <field>"`. Note: this fires on both insert and update paths since validation runs before queueing. |
| **Table is empty** | Queued; `WHEN NOT MATCHED` fires and the row is inserted normally. |
| **Two upserts in a row with the same new key** | Both are queued and return `202` immediately. Whichever task's `MERGE` runs first inserts the row; the second matches on the key and updates it. |

### `responses` target specifics

The `responses` target (`RESPONSES.response_data`) differs from the others in four ways; every other target behaves as described above.

| Scenario | Behavior |
|---|---|
| **Date/time values** | Stored in the table's own convention (`DATETIME_CONVENTIONS` in `config.py`): `checkinDateTime`, `checkinReplyDateTime`, `resourceOfferReplyDatetime`, `referralFollowUpUtilizedDateTime` and any other DATETIME column are converted to UTC before the offset is dropped (`2026-05-26T11:36:27-04:00` is stored as `2026-05-26 15:36:27`); `checkinReplyDate` is midnight of the payload's local date; DATE columns keep the payload's local date. Other targets keep the payload's local wall clock. |
| **Partition pruning** | The MERGE carries `(T.checkinDateTime BETWEEN @min_dt AND @max_dt OR T.checkinDateTime IS NULL)` when the payload has a check-in time, so it reads only that day's partition plus the NULL partition. A stored row without a check-in time is still matched and updated rather than duplicated. A payload without a check-in time uses the key alone (a full scan). |
| **Blank check-in time** | Never overwrites a stored one: the update uses `COALESCE(S.checkinDateTime, T.checkinDateTime)` (`PRESERVE_ON_BLANK`). |
| **No session ID** | A payload whose `sessionID` is absent, `null` or blank (sign-ups, some all-blank calls) is inserted as a new row with a NULL `SessionID` instead of being rejected (`KEYLESS_INSERT_TARGETS`). Such rows cannot be updated later. |

---

## Allowed Tables

The `table` field must be one of the following configured values:

| Table name | BigQuery table | Upsert key |
|---|---|---|
| `users` | `early-alert-responses.RESPONSES.users` | `uuid` |
| `responses` | `early-alert-responses.RESPONSES.response_data` | `SessionID` |
| `triage_data` | `early-alert-responses.RESPONSES.triage-message-data` | `message_id` |
| `users_copy` | `early-alert-responses.COPY.users` | `uuid` |
| `responses_copy` | `early-alert-responses.COPY.response_data` | `SessionID` |

To add a new table, update `ALLOWED_TARGETS` and (for upsert support) `UPSERT_KEYS` in `src/config.py`.

---

## Validation

Before responding to the webhook caller (synchronous, can produce an immediate `400`), the service:
- Fetches the live table schema from BigQuery
- Checks that all `REQUIRED` fields are present and non-null
- Validates that each field's value matches the expected BigQuery type
- Treats fields not present in the table schema as warnings, not errors (they might become new columns once queued)
- For upserts, additionally verifies that all configured key columns are present and non-null

After the task is queued and picked up by `/tasks/ingest` or `/tasks/upsert` (asynchronous, failures are logged, not returned to the caller), the service:
- Attempts to add fields not present in the table schema as new nullable BigQuery columns
- Rejects unknown fields only when their names are not valid BigQuery column names — this rejection happens after the caller has already received `202`

**Error response (`400`) — from `/ingest`/`/upsert`, before anything is queued:**
```json
{
  "status": "error",
  "table": "users",
  "errors": ["Missing required field: uuid"],
  "warnings": ["Field not found in BigQuery schema after schema update: extra_field"],
  "queued_results": []
}
```

Errors that depend on the schema migration — like an invalid new BigQuery column name (`"Invalid new field name"`) — can no longer be caught before responding, since that migration now happens in the queued `/tasks/ingest`/`/tasks/upsert` worker. Those failures are logged server-side only; the caller has already received `202`. See the "unknown/extra fields" rows in the [Ingest](#ingest-behavior-reference) and [Upsert](#upsert-behavior-reference) behavior references above.

### Supported BigQuery types

| BigQuery type | Expected Python type |
|---|---|
| `STRING` | `str` |
| `JSON` | `dict` or `list` |
| `INTEGER` | `int` (not `bool`) |
| `FLOAT` | `int` or `float` (not `bool`) |
| `BOOLEAN` | `bool` |
| `DATETIME` / `TIMESTAMP` / `DATE` / `TIME` | `str` (ISO 8601 format) |

---

## Local Development

**Prerequisites:** Python 3.12+, a GCP project with BigQuery and Cloud Tasks access, Application Default Credentials configured.

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export WEBHOOK_SECRET="dev-secret"                       # optional; omit to disable webhook auth
export TASKS_INVOKER_SERVICE_ACCOUNT="add-to-db-tasks-invoker@early-alert-responses.iam.gserviceaccount.com"
export SERVICE_URL="https://add-to-db-xxxx-ue.a.run.app"  # the deployed service's URL, not localhost

# Run the dev server
cd src
flask --app main run --port 8080
```

**Example insert request:**
```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: dev-secret" \
  -d '{"table": "users", "data": {"uuid": "abc-123", "name": "Jane Doe"}}'
```

**Example upsert request:**
```bash
curl -X POST http://localhost:8080/upsert \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: dev-secret" \
  -d '{"table": "users", "data": {"uuid": "abc-123", "name": "Jane Updated"}}'
```

**Tests:** `pip install -r requirements-dev.txt`, then `python -m pytest -q tests` from the repo root. The tests need no GCP access: BigQuery is replaced by DuckDB, which runs the service's generated SQL. `tests/baseline/` holds the `bq_writer` of the revision before the `responses` changes; `tests/test_scope_guard.py` checks that triage and testimonial writes send exactly what it sends.

**Single-writer path for the check-in targets (`STAGED_TARGETS`, off unless set):** `/upsert` validates each call as before, then appends it to the staging table (`STAGING_TABLE`) stamped with its receive time before answering `202`, and asks for a flush of that 30-second bucket (a named task on `FLUSH_QUEUE`, which must run at max concurrency 1). `/tasks/flush` runs `run_flush_cycle`: it reads every call received after the watermark and at least `FLUSH_SAFETY_S` ago, folds them per key in receive order (each column = the last call that carried it; a blank never clears a stored check-in time; a reply whose time is before the session's check-in is written as not replied), and writes everything -- one MERGE per target, rows without a key inserted, calls set aside, a flush-log row, and the watermark advance (compare-and-set on its version) -- in one transaction PER TARGET, each with its own watermark (check-in rows first), so contention on users (the nightly contacts sync) never holds back response_data. Each target's watermark row is in a state table of its own (`FLUSH_STATE_TABLE` = `adb_flush_state` for responses; `adb_flush_state_users` for users, or `FLUSH_STATE_TABLE_USERS`): BigQuery lets only one transaction at a time change rows in a table, so when both rows shared one table each target's flush waited on the other's, and a check-in flush that had already merged its rows could sit with its transaction open on response_data (2026-09-30: a 651 s wait, eight failed flushes behind it, no call lost). A flush transaction now changes rows only in its target table and its own state table; everything else it writes is an INSERT, which runs alongside any other transaction. The service refuses to start if two staged targets are configured to share a state table. Each flush transaction also has a limit on how long it may run (`FLUSH_JOB_TIMEOUT_RESPONSES_S` 140, `FLUSH_JOB_TIMEOUT_USERS_S` 90): the limit is the BigQuery job's own timeout, and if the job is still running 10 s after it the flusher stops waiting, asks BigQuery to cancel it and records a failed flush, which the queue retries. On 2026-09-30 the statement that ran 651 s was the watermark update of a check-in flush whose MERGE had already finished; it ended in a BigQuery internal error, and until it did the script held its transaction open on response_data well past the 300 s request. The two limits and their grace must fit inside the request timeout (`FLUSH_REQUEST_TIMEOUT_S`, 300), or the service refuses to start. `python tools/prove_flush_timeout.py` proves the limit on BigQuery with one DEV table it creates and drops: a transaction that runs past a 20 s limit, a second transaction refused while the first runs (the control), then committed once the first has been stopped, and the row showing the first was rolled back. A contended transaction is retried with full-jitter exponential backoff for up to `FLUSH_RETRY_BUDGET_S` (75 s for responses, 20 s for users, so a contended users flush cannot hold the single writer long); a second writer or a collision aborts only that target's transaction, its calls stay after its watermark, and the queue retries. Calls rejected by validation, or failing pre-flight at flush time, are set aside (not written) in the set-aside table `adb_set_aside` (`DEAD_LETTER_TABLE`). A flush reads each table's schema once and takes at most `FLUSH_MAX_ITEMS` calls per target (300; about 120 s for both targets at the per-call cost we measured before the schema read was made once per flush); when it leaves calls behind it asks for the next flush at once, so a long backlog drains in a chain of short flushes. The staging append is bounded: it must finish within `STAGING_APPEND_BUDGET_S` (12 s) of the receive stamp, well inside the 20 s safety window, or the call is answered `500`. A call that still becomes visible after the flush covering its receive time is found by the late check (run by the 5-minute sweep's flush; each flush-log row lists the calls it took), set aside with stage `late` and never written, since writing it late could overwrite a newer call. `tools/flush_pause.py on|off|status` sets a maintenance pause: while it is set the flush writes nothing, `GET /health/flush` reports `paused` (HTTP 200) and the sweep does not raise the backlog alert (it raises `PAUSE` after 4 hours). Every condition that needs a person logs one line containing `ADB_ALERT` (`FLUSH_ALERT` after `FLUSH_ALERT_AFTER` consecutive failed flushes, `SET_ASIDE` (a call set aside, not written), `LATE`, `STAGING`, `BACKLOG` when the oldest unflushed call is older than `BACKLOG_ALERT_S` and the flush is not paused, `PAUSE`, `SWEEP`); a Cloud Monitoring log-match alert policy on that token emails us. `POST /tasks/flush-kick` is the sweep: it raises the backlog and pause alerts and requests a flush under its own task name. Flush task IDs start with a hash (`<12 hex>-flush-<bucket>`), as Cloud Tasks recommends. `GET /health/flush` reports, per target, backlog, last successful flush, consecutive failures, the pause, and late calls not yet set aside (`late_calls_not_set_aside`), plus calls set aside in 24 h (`set_aside_24h`). Tables: `python tools/staged_ddl.py --apply <DATASET>` creates and verifies them (`--verify` checks only); for a dataset created while both state rows shared `adb_flush_state`, `python tools/staged_ddl.py --split-users-state <DATASET>` creates `adb_flush_state_users` and copies the users row into it, once, replacing nothing -- we run it just before deploying the code that reads the new table (users calls flushed in between are flushed once more and rewrite the same rows). To go back to a version that reads both rows from `adb_flush_state`, we first copy the users watermark back (`UPDATE adb_flush_state SET watermark = (SELECT watermark FROM adb_flush_state_users), version = version + 1 WHERE id = 'flush:users'`), then deploy it. Triage and testimonial targets never use this path.

**Re-submitting a logged call that never landed (`tools/resubmit_logged_call.py`):** when a call to `/upsert` was not answered `202` (a body the service rejected, a timeout, a `500`), its body is still in our webhook log. `python tools/resubmit_logged_call.py --list` shows those calls; `python tools/resubmit_logged_call.py <httplog id>` shows what would be sent and why, and sends nothing; `--post` sends it; `--verify`, a minute or more later, compares the stored rows with the body. One id per run. A body that is not valid JSON because a value was pasted raw is repaired by escaping exactly that value, and the repair is proven before anything is sent. The single writer keeps the last call's value for every column and a re-submitted call is received now, so a users or responses part is sent only when no later call for that contact or session has been accepted since the lost call fired; otherwise the part is reported as superseded and left alone, because sending it would put old values back over newer ones. The webhook secret comes from `ADD_TO_DB_SECRET` or is read from the deployed service; it is never printed.

**Proof tools (real BigQuery, DEV dataset):** `tools/prove_staged.py` runs the single-writer path end to end against DEV copies (staging, flush, stale replies, calls set aside, two writers, a competing writer); `tools/replay_triage_testimonial.py` writes the logged triage and testimonial calls through the live revision's SQL and this branch's worker into DEV copies and compares them row for row; `tools/fix_eastern_rows.py` lists, rehearses on a DEV clone of today's table, applies (one guarded transaction, one UPDATE per column, with a DEV backup; production only with `--production`) and rolls back the correction of rows stored in local time; `tools/prove_unit1.py` runs the `responses` write scenarios against a fresh copy of `response_data` in `DEV` and dry-runs the MERGE against the live table; `tools/replay_webhook_log.py` replays logged check-in calls in-process (not through Cloud Tasks) against DEV copies at queue concurrency and reports ordering, duplicates and failures. Both authenticate as the active `gcloud` account.

**A note on testing the async path locally:** hitting your local `/ingest` or `/upsert` still validates and enqueues a *real* Cloud Tasks task (assuming your ADC has `roles/cloudtasks.enqueuer` on the queue). But `SERVICE_URL` is the callback target Cloud Tasks actually calls — since Cloud Tasks reaches out over the public internet, it can't reach `localhost`. That means the task will always be delivered to the **deployed** Cloud Run service's `/tasks/ingest`/`/tasks/upsert`, not your local process, regardless of which instance enqueued it. To exercise the write logic itself locally, call `/tasks/ingest`/`/tasks/upsert` directly — but note `is_task_request_authorized` requires a real OIDC token whose signer matches `TASKS_INVOKER_SERVICE_ACCOUNT`, so you'll need to mint one (e.g. via `gcloud auth print-identity-token --audiences=$SERVICE_URL --impersonate-service-account=$TASKS_INVOKER_SERVICE_ACCOUNT`) rather than calling it unauthenticated.

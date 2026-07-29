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

**A note on testing the async path locally:** hitting your local `/ingest` or `/upsert` still validates and enqueues a *real* Cloud Tasks task (assuming your ADC has `roles/cloudtasks.enqueuer` on the queue). But `SERVICE_URL` is the callback target Cloud Tasks actually calls — since Cloud Tasks reaches out over the public internet, it can't reach `localhost`. That means the task will always be delivered to the **deployed** Cloud Run service's `/tasks/ingest`/`/tasks/upsert`, not your local process, regardless of which instance enqueued it. To exercise the write logic itself locally, call `/tasks/ingest`/`/tasks/upsert` directly — but note `is_task_request_authorized` requires a real OIDC token whose signer matches `TASKS_INVOKER_SERVICE_ACCOUNT`, so you'll need to mint one (e.g. via `gcloud auth print-identity-token --audiences=$SERVICE_URL --impersonate-service-account=$TASKS_INVOKER_SERVICE_ACCOUNT`) rather than calling it unauthenticated.

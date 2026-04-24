# Early Alert: add-to-db

A lightweight Flask webhook service that accepts JSON payloads and writes them to Google BigQuery. Supports both plain inserts and upserts (via BigQuery `MERGE`). Runs as a containerized Cloud Run service built and deployed via Cloud Build.

---

## Architecture

```
Webhook caller (TextIt)
    │
    ▼
Cloud Run (add-to-db)   ←── Cloud Build CI/CD
    │
    ▼
Google BigQuery (early-alert-responses)
```

| File | Purpose |
|---|---|
| `src/main.py` | Flask app — route handlers, request validation, insert/upsert orchestration |
| `src/config.py` | Allowed targets, upsert keys, and type-checker map |
| `src/auth.py` | Request authorization via shared secret header |
| `src/bq_writer.py` | BigQuery helpers — schema expansion, type inference, MERGE query construction, and query parameter construction |
| `Dockerfile` | Container definition (Python 3.12 slim + gunicorn) |
| `cloudbuild.yaml` | Cloud Build pipeline — build, push, deploy to Cloud Run |

---

## Build & Deployment

The service is built and deployed automatically via **Google Cloud Build**.

**Pipeline steps (`cloudbuild.yaml`):**
1. Build the Docker image and tag it with `$COMMIT_SHA`
2. Push the image to Artifact Registry at `us-east1-docker.pkg.dev/$PROJECT_ID/webhook-repo/add-to-db`
3. Deploy to Cloud Run in `us-east1` with:
   - 0 minimum instances (scales to zero)
   - 50 max concurrent requests per instance
   - 1 worker / 8 threads (gunicorn)
   - Unauthenticated public access (protected by the webhook secret header)

To trigger a deploy, push a commit to the connected repository branch.

---

## Authentication

All requests must include the shared secret in the `X-Webhook-Secret` header.

```
X-Webhook-Secret: <your-secret>
```

The secret is read from the `WEBHOOK_SECRET` environment variable on the Cloud Run service. If the variable is not set, all requests are allowed through (useful for local development).

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

Inserts a single row into a BigQuery table.

**Request body:**
```json
{
  "target": "users",
  "data": {
    "field1": "value1",
    "field2": 123
  }
}
```

**Success response (`200`):**
```json
{
  "status": "ok",
  "operation": "insert",
  "target": "users",
  "table_id": "early-alert-responses.RESPONSES.users",
  "added_fields": [],
  "warnings": []
}
```

---

### `POST /upsert`

Inserts or updates a single row using a BigQuery `MERGE` statement. If a row matching the configured key column(s) already exists, it is updated; otherwise it is inserted.

**Request body:** same shape as `/ingest`

```json
{
  "target": "users",
  "data": {
    "uuid": "abc-123",
    "name": "Jane Doe"
  }
}
```

**Success response (`200`):**
```json
{
  "status": "ok",
  "operation": "upsert",
  "target": "users",
  "table_id": "early-alert-responses.RESPONSES.users",
  "added_fields": [],
  "warnings": []
}
```

---

## Ingest Behavior Reference

The `/ingest` endpoint always performs a straight **INSERT** — it never checks for existing rows. The table below documents how each edge case is handled.

| Scenario | Behavior |
|---|---|
| **Normal insert, all fields valid** | Row is appended to the table. Returns `200`. |
| **Table is empty** | Row is inserted normally. Returns `200`. |
| **Row with same key already exists** | A duplicate row is inserted. `/ingest` does not deduplicate — use `/upsert` if uniqueness is required. |
| **Required field missing from input** | Rejected before hitting BigQuery. Returns `400` with `"Missing required field: <field>"`. |
| **Field value is `null` on a `REQUIRED` column** | Rejected before hitting BigQuery. Returns `400` with `"Field '<field>' cannot be null"`. |
| **Field value is `null` on a `NULLABLE` column** | Accepted. `NULL` is written to BigQuery. |
| **Wrong data type for a field** | Rejected before hitting BigQuery. Returns `400` with `"Field '<field>' expected type <TYPE>, got <python_type>"`. |
| **Input contains unknown/extra fields** | Unknown fields are stripped and ignored. A `warnings` array is returned in the response listing each ignored field. Known fields are inserted normally. |
| **All fields omitted (empty `data` object)** | Passes validation only if the table has no `REQUIRED` fields. Otherwise returns `400` for each missing required field. |
| **`data` is not a JSON object (e.g., array or string)** | Rejected immediately. Returns `400` with `"Field 'data' must be a JSON object"`. |
| **`target` is missing or invalid** | Rejected immediately. Returns `400` with `"Missing target"` or `"Invalid target"` and a list of allowed values. |
| **Two inserts in a row with the same data** | Both succeed. Two identical rows will exist in the table. |

---

## Upsert Behavior Reference

The `/upsert` endpoint uses a BigQuery `MERGE` statement keyed on the column(s) configured in `UPSERT_KEYS`. The table below documents how each edge case is handled.

| Scenario | Behavior |
|---|---|
| **Key does not exist in table** | Row is inserted. `WHEN NOT MATCHED` branch of the MERGE fires. |
| **Key exists once** | Existing row is updated with values from `data`. `WHEN MATCHED` branch fires. |
| **Multiple rows exist with the same key** | BigQuery raises an error — `MERGE` cannot update a target row matched more than once. Request returns `500`. This indicates a data integrity problem in the table. |
| **Key field missing from input** | Rejected before hitting BigQuery. Returns `400` with `"Missing upsert key field: <key>"`. |
| **Key value is `null`** | Rejected before hitting BigQuery. Returns `400` with `"Upsert key field '<key>' cannot be null"`. |
| **Key value is empty string (`""`)**  | Passes validation (empty string is a valid `STRING`). BigQuery will match or insert on the empty-string key. |
| **Key exists, other non-key fields omitted** | Only the fields present in `data` are included in the `UPDATE SET` clause. Omitted fields are left unchanged in the existing row. |
| **Key exists, incoming data is identical to existing row** | BigQuery executes the update and overwrites with the same values. No error. Effectively a no-op from a data perspective, but still consumes a slot job. |
| **Key exists, only one field changes** | Only that row is updated. All other rows and columns are unaffected. |
| **Key exists, incoming value for a field is `null`** | The field is set to `NULL` in BigQuery, clearing the previous value. This will fail if the column is `REQUIRED` (caught by pre-write validation, returns `400`). |
| **Input contains unknown/extra fields** | Unknown fields are stripped and ignored. A `warnings` array is returned in the response listing each ignored field. Known fields are upserted normally. |
| **Wrong data type for a field** | Rejected before hitting BigQuery. Returns `400` with `"Field '<key>' expected type <TYPE>, got <python_type>"`. |
| **Required non-key field missing on insert** | Rejected before hitting BigQuery. Returns `400` with `"Missing required field: <field>"`. Note: this fires on both insert and update paths since validation runs before the MERGE. |
| **Table is empty** | `WHEN NOT MATCHED` fires and the row is inserted normally. |
| **Two upserts in a row with the same new key** | First request inserts the row. Second request matches on the key and updates it. Both return `200`. |

---

## Allowed Targets

The `target` field must be one of the following configured values:

| Target name | BigQuery table | Upsert key |
|---|---|---|
| `users` | `early-alert-responses.RESPONSES.users` | `uuid` |
| `responses` | `early-alert-responses.RESPONSES.response_data` | `SessionID` |
| `triage_data` | `early-alert-responses.RESPONSES.triage-message-data` | `message_id` |
| `users_copy` | `early-alert-responses.COPY.users` | `uuid` |
| `responses_copy` | `early-alert-responses.COPY.response_data` | `SessionID` |

To add a new target, update `ALLOWED_TARGETS` and (for upsert support) `UPSERT_KEYS` in `src/config.py`.

---

## Validation

Before writing to BigQuery, the service:
- Fetches the live table schema from BigQuery
- Checks that all `REQUIRED` fields are present and non-null
- Validates that each field's value matches the expected BigQuery type
- Strips any fields not present in the table schema (with a warning)
- For upserts, additionally verifies that all configured key columns are present and non-null

**Error response (`400`):**
```json
{
  "status": "error",
  "errors": ["Missing required field: uuid"],
  "warnings": ["Unknown field ignored: extra_field"]
}
```

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

**Prerequisites:** Python 3.12+, a GCP project with BigQuery access, Application Default Credentials configured.

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export WEBHOOK_SECRET="dev-secret"   # optional; omit to disable auth

# Run the dev server
cd src
flask --app main run --port 8080
```

**Example insert request:**
```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: dev-secret" \
  -d '{"target": "users", "data": {"uuid": "abc-123", "name": "Jane Doe"}}'
```

**Example upsert request:**
```bash
curl -X POST http://localhost:8080/upsert \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Secret: dev-secret" \
  -d '{"target": "users", "data": {"uuid": "abc-123", "name": "Jane Updated"}}'
```

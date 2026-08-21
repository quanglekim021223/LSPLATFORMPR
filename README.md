# FSA Learning Vendor Ingestion

FastAPI service for scheduled ingestion of learning-vendor data into a raw Bronze layer. The
first delivery implements only LevelUP (Absorb): one token per run, paged Course Catalog,
client-side LinkedIn Learning exclusion, then paged Learning History for the collected course
IDs.

## Runtime flow

```text
FastAPI lifespan / APScheduler (05:00 Asia/Ho_Chi_Minh)
→ run_levelup_ingestion
→ POST /authenticate (one in-memory token)
→ GET /courses page-by-page
→ LocalBronzeWriter + SQLite checkpoint
→ GET /courses/{courseId}/enrollments (bounded concurrency)
→ LocalBronzeWriter + SQLite run summary
```

There is no public manual-trigger endpoint. `GET /health`, `GET /ready`, and
`GET /jobs/levelup/latest` expose operational state without credentials or raw personal data.

## Local setup

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
uvicorn app.main:app --reload
```

Fill the LevelUP values in `.env`; never commit that file. The authentication contract follows
the supplied matrix:

- `POST /authenticate`
- JSON fields `username`, `password`, and `privateKey`
- headers `X-API-Key` and `x-api-version: 2`
- a plain-string token response (JSON `token` / `access_token` envelopes are also accepted)
- subsequent `Authorization` header contains the token exactly as returned by Absorb

Run checks with:

```bash
ruff check .
mypy app
pytest
```

## Scheduler

The scheduler is disabled by default. To enable the daily 05:00 job locally:

```text
SCHEDULER_ENABLED=true
INGESTION_TIME=05:00
INGESTION_TIMEZONE=Asia/Ho_Chi_Minh
```

The in-process scheduler must have exactly one scheduler-bearing service instance. Do not enable
it independently in every Uvicorn worker or replica. For multi-worker production deployments,
keep it disabled and let Fabric, Azure Data Factory, or another external scheduler own the single
job invocation. `max_instances=1`, coalescing, and a five-minute misfire grace prevent overlapping
or accumulated catch-up executions within one process. It is always disabled when `APP_ENV=test`.

## Bronze layout

```text
data/bronze/levelup/
├── course_catalog/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
│   ├── offset=000000.json
│   └── manifest.json
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
    └── course_id=<course-id>/
        ├── offset=000000.json
        └── manifest.json
```

Each page is atomically replaced only after the response succeeds. Manifests are separate from
raw JSON and include offset, record count, sanitized request parameters, fetch time, and SHA-256.
Every response body is stored byte-for-byte as received. If a Course Catalog page still contains
LinkedIn Learning rows, those raw rows remain in Bronze; the client-side filter only prevents their
course IDs from feeding Learning History. A curated catalog belongs in Silver or a separate output.

`BRONZE_STORAGE_TYPE=local` explicitly selects local filesystem storage. This implementation is
not OneLake. `BronzeWriter` is the boundary for a future `OneLakeBronzeWriter` once the team
provides the actual OneLake authentication, workspace/lakehouse identifiers, path contract, and
write semantics.

## Retry, concurrency, and resume

- Timeout, connection failures, HTTP 429, and HTTP 5xx are retried up to
  `HTTP_MAX_RETRIES` times after the initial attempt.
- `Retry-After` is honored; otherwise exponential backoff with jitter is used.
- Other 4xx responses are not retried. A 401 refreshes the shared token and repeats the request
  once.
- At most `LEVELUP_MAX_CONCURRENCY` courses run concurrently via `asyncio.Semaphore`.
- SQLite records every completed page and course. With no explicit `run_id`, the orchestrator
  automatically resumes the newest `failed`, `partial_failure`, or stale `running` run before it
  creates a UUID. Scheduler executions therefore continue incomplete work automatically.
- A SQLite vendor lock prevents overlapping LevelUP jobs. It has a periodically refreshed
  heartbeat and `LEVELUP_LOCK_TTL_SECONDS` expiry. Lock acquisition atomically reclaims stale locks
  and marks an abandoned `running` run failed before resuming it.

## Information still needed from Minh/team

1. Production LevelUP tenant/base URL and confirmation that `Authorization` must contain the raw
   token rather than `Bearer <token>`.
2. Whether the authentication response is always a plain string in every environment.
3. OneLake/Fabric workspace, lakehouse, directory convention, authentication method, and atomic
   commit expectations.
4. Production scheduler owner (single FastAPI instance vs Fabric/ADF/external orchestrator).
5. Retention, encryption, and access-control policy for raw Learning History PII.

SkillUp, DataCamp, Coursera, LinkedIn Learning, Harvard HMM/Spark, and FAMS are intentionally not
implemented in this phase.
# LSPLATFORMPR

# FSA Learning Vendor Ingestion

FastAPI service for scheduled ingestion of learning-vendor data into a raw Bronze layer. It
supports LevelUP (Absorb), SkillUp (iMocha), and DataCamp ingestion domains.

## Runtime flow

```text
FastAPI lifespan / APScheduler (05:00 Asia/Ho_Chi_Minh)
├→ run_levelup_ingestion
│  ├→ POST /authenticate (one in-memory token)
│  ├→ GET /courses page-by-page
│  └→ GET /courses/{courseId}/enrollments (bounded concurrency)
├→ run_skillup_ingestion (three independent concurrent domains)
   ├→ GET /taxonomy
   ├→ GET /employees/skills-profile
   └→ GET /v3/reports
└→ run_datacamp_ingestion (three independent concurrent domains)
   ├→ GET /v1/catalog/live-courses once
   ├→ GET /v1/catalog/archived-courses once
   └→ GET /v1/events page-by-page
→ LocalBronzeWriter + SQLite run summary
```

There is no public manual-trigger endpoint. `GET /health`, `GET /ready`, and
`GET /jobs/levelup/latest`, `GET /jobs/skillup/latest`, and `GET /jobs/datacamp/latest` expose
operational state without credentials or raw personal data.

## Code layout

```text
app/main.py                  application composition and lifespan
app/routers/                 health, readiness, and job-status APIs
app/handlers/                LevelUP, SkillUp, and DataCamp run orchestration
app/vendors/levelup/         LevelUP authentication, catalog, and learning history
app/vendors/skillup/         iMocha client and three SkillUp data domains
app/vendors/datacamp/        DataCamp client, catalogs, and event history
app/helpers/                 shared HTTP retry and secret sanitization
app/config/                  settings and APScheduler configuration
app/repositories/            SQLite checkpoint history and vendor lock
app/storage/                 Bronze writer contract and local implementation
app/models/                  ingestion data models
app/mocks/app.py             single local mock hub entrypoint for every vendor
app/mocks/levelup.py         LevelUP mock routes and data
app/mocks/skillup.py         SkillUp mock routes and data
app/mocks/datacamp.py        DataCamp mock routes and data
```

## Local setup

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
uvicorn app.main:app --reload
```

Fill the vendor values in `.env`; never commit that file. LevelUP authentication follows the
supplied matrix:

- `POST /authenticate`
- JSON fields `username`, `password`, and `privateKey`
- headers `X-API-Key` and `x-api-version: 2`
- a plain-string token response (JSON `token` / `access_token` envelopes are also accepted)
- subsequent `Authorization` header contains the token exactly as returned by Absorb

SkillUp uses `x-api-key: <SKILLUP_API_KEY>` for every request. Its Intelligence and Reports APIs
use separate base URLs configured by `SKILLUP_INTELLIGENCE_BASE_URL` and
`SKILLUP_REPORTS_BASE_URL`. Assessment History always sends a range from
`SKILLUP_ASSESSMENT_START_DATE` through the current UTC time, so scheduled runs retrieve the full
configured history rather than iMocha's seven-day default.

DataCamp sends `Authorization: Bearer <DATACAMP_TOKEN>` and `Accept: application/json` on every
request. Live and archived catalogs are fetched once per run because their pagination contract is
not confirmed. Events use `DATACAMP_EVENTS_PAGE_SIZE` (maximum 1000) and continue until the current
page reaches `meta.numberOfPages`.

Run checks with:

```bash
ruff check .
mypy app
pytest
```

## Local multi-vendor mock demo

The local `.env` points every mock vendor to a path on the same port. Start these two processes in
separate terminals:

```bash
# Terminal 1: shared upstream mock hub
uvicorn app.mocks.app:app --host 127.0.0.1 --port 9000

# Terminal 2: ingestion service and scheduler
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

For a quick scheduler test, set `INGESTION_TIME` in `.env` to a future minute in
`Asia/Ho_Chi_Minh` before starting Terminal 2. At that minute the single scheduler registers and
starts `levelup-daily-ingestion`, `skillup-daily-ingestion`, and
`datacamp-daily-ingestion`. Keep Terminal 2 running, then check the three `/jobs/{vendor}/latest`
endpoints and vendor directories under
`data/bronze/`. Swagger on port `8000` is status-only. The shared mock Swagger at
`http://127.0.0.1:9000/docs` exposes all mock vendors and can grow to include future vendor routers
without adding more processes.

## Scheduler

The scheduler is disabled by default. To enable the daily 05:00 jobs locally:

```text
SCHEDULER_ENABLED=true
INGESTION_TIME=05:00
INGESTION_TIMEZONE=Asia/Ho_Chi_Minh
```

Only vendors with a complete credential configuration are scheduled. LevelUP, SkillUp, and
DataCamp receive separate APScheduler jobs at the same configured time. The in-process scheduler
must have exactly one scheduler-bearing service instance. Do not enable
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

data/bronze/skillup/
├── skill_taxonomy/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
├── skill_inventory/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
└── assessment_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
    ├── offset=000001.json
    └── manifest.json

data/bronze/datacamp/
├── course_catalog_live/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
├── course_catalog_archived/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
    ├── offset=000001.json
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

## Retry, concurrency, and checkpoints

- Timeout, connection failures, HTTP 429, and HTTP 5xx are retried up to
  `HTTP_MAX_RETRIES` times after the initial attempt.
- `Retry-After` is honored; otherwise exponential backoff with jitter is used.
- Other 4xx responses are not retried. For LevelUP, a 401 refreshes the shared token and repeats
  the request once.
- At most `LEVELUP_MAX_CONCURRENCY` courses run concurrently via `asyncio.Semaphore`.
- SkillUp runs its three independent domains concurrently. Each domain writes and checkpoints one
  page at a time, so the full dataset is never accumulated in memory.
- DataCamp also runs its three domains concurrently. Catalog responses are opaque raw JSON and
  intentionally report zero parsed records until their schemas are confirmed. Events are written
  and checkpointed one page at a time.
- SQLite records every completed page, course, domain, and run for status reporting and audit.
  Every scheduled ingestion creates a new `run_id` and starts the vendor from the first page,
  regardless of whether the previous run succeeded or failed. Failed work is not resumed on the
  next schedule.
- SQLite vendor locks prevent overlapping runs per vendor. LevelUP, SkillUp, and DataCamp use
  different lock keys, so their scheduled runs can execute independently.
- At ingestion startup, terminal checkpoint runs older than `CHECKPOINT_RETENTION_DAYS` are deleted
  with their page/course/domain rows. Running or locked runs are kept.

## Information still needed from Minh/team

1. Production LevelUP tenant/base URL and confirmation that `Authorization` must contain the raw
   token rather than `Bearer <token>`.
2. Whether the authentication response is always a plain string in every environment.
3. Exact production response envelope and pagination metadata for SkillUp
   `/employees/skills-profile`; the public documentation currently exposes the per-employee
   endpoint more clearly than this aggregate endpoint.
4. Whether SkillUp response field casing is identical across tenants, especially `hasNextPage`,
   `pageNumber`, and `totalPages`.
5. Exact DataCamp live/archived catalog envelopes and whether either endpoint later exposes
   pagination metadata.
6. Whether DataCamp events are under `events` or `data`, and the exact types/edge cases for
   `meta.numberOfPages` when the result is empty.
7. OneLake/Fabric workspace, lakehouse, directory convention, authentication method, and atomic
   commit expectations.
8. Production scheduler owner (single FastAPI instance vs Fabric/ADF/external orchestrator).
9. Retention, encryption, and access-control policy for raw Learning History PII.

Coursera, LinkedIn Learning, Harvard HMM/Spark, and FAMS are intentionally not
implemented in this phase.

### SkillUp Assessment History date range

The iMocha `GET /v3/reports` contract returns only the most recent seven days when no range is
provided. This service therefore always sends `startDate` from
`SKILLUP_ASSESSMENT_START_DATE` (default `2000-01-01T00:00:00Z`) and sets `endDate` to the current
UTC time. Pagination then retrieves every report in that configured range.
# LSPLATFORMPR

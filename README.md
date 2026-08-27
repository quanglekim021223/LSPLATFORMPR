# FSA Learning Vendor Ingestion

FastAPI service for scheduled ingestion of learning-vendor data into a raw Bronze layer. It
supports LevelUP (Absorb), SkillUp (iMocha), DataCamp, Coursera, LinkedIn Learning, Harvard HMM,
Harvard Spark, and FAMS ingestion domains.

## Runtime flow

```text
FastAPI lifespan / APScheduler (05:00 Asia/Ho_Chi_Minh)
├→ run_levelup_ingestion
│  ├→ POST /authenticate (one in-memory token)
│  ├→ GET /courses page-by-page
│  └→ GET /courses/{courseId}/enrollments (bounded concurrency)
├→ run_skillup_ingestion (three independent concurrent domains)
│  ├→ GET /taxonomy
│  ├→ GET /employees/skills-profile
│  └→ GET /v3/reports
├→ run_datacamp_ingestion (three independent concurrent domains)
│  ├→ GET /v1/catalog/live-courses once
│  ├→ GET /v1/catalog/archived-courses once
│  └→ GET /v1/events page-by-page
├→ run_coursera_ingestion
│  ├→ POST configured token URL once
│  ├→ GET /{org_id}/contents → configured Course Detail path (bounded concurrency)
│  └→ GET /{org_id}/enrollmentReports in parallel with the catalog pipeline
├→ run_linkedin_ingestion
│  ├→ POST configured OAuth token URL once
│  ├→ GET /learningAssets → same endpoint with configured URN query (bounded concurrency)
│  └→ GET /learningActivityReports in concurrent windows of at most 14 days
├→ run_harvard_hmm_ingestion
│  ├→ Catalog: Basic OAuth → GET /api/catalog/{orgKey}?catalogs=HMM
│  └→ History: SFTP backfill missing dates → poll newest HMM CSV
├→ run_harvard_spark_ingestion
│  ├→ Catalog: Basic OAuth → GET /api/catalog/{orgKey}?catalogs=HBR_SPARK
│  └→ History: SFTP backfill missing dates → poll newest Spark CSV
└→ run_fams_ingestion
   └→ GET /api/fsa-reports/training-data once (full or configured filtered mode)
→ LocalBronzeWriter + SQLite run summary
```

There is no public manual-trigger endpoint. `GET /health`, `GET /ready`, and
`GET /jobs/levelup/latest`, `GET /jobs/skillup/latest`, `GET /jobs/datacamp/latest`,
`GET /jobs/coursera/latest`, `GET /jobs/linkedin/latest`, `GET /jobs/harvard-hmm/latest`, and
`GET /jobs/harvard-spark/latest`, and `GET /jobs/fams/latest` expose operational state without
credentials or raw personal data.

## Code layout

```text
backend/src/app/main.py                  application composition and lifespan
backend/src/app/api/v1/                 versioned health, readiness, and job-status APIs
backend/src/app/core/                    runtime config, security, retry, and logging
backend/src/app/config/scheduler.py      APScheduler construction
backend/src/app/clients/                 shared HTTP transport and raw vendor clients
backend/src/app/schemas/                 vendor response and CSV contracts
backend/src/app/models/                  shared ingestion and domain models
backend/src/app/services/                vendor orchestration and domain ingestion
backend/src/app/repositories/            run state, vendor locks, and Bronze writers
backend/src/app/mocks/                   single local mock hub and vendor fixtures
backend/tests/unit/                      isolated client, schema, core, and repository tests
backend/tests/integration/               API, service orchestration, and mock-hub tests
```

## Local setup

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e 'backend[dev]'
cp backend/.env.example backend/.env
cd backend
uvicorn app.main:app --reload
```

Fill the vendor values in `backend/.env`; never commit that file. LevelUP authentication follows the
supplied matrix:

- `POST /authenticate`
- JSON fields `username`, `password`, and `privateKey`
- headers `X-API-Key` and `x-api-version: 2`
- a plain-string token response (JSON `token` / `access_token` envelopes are also accepted)
- subsequent `Authorization` header contains the token exactly as returned by Absorb

LevelUP Course List and Enrollment pages are validated against vendor-specific Pydantic objects
before they are written to Bronze or their checkpoints are marked completed. Missing required keys,
incompatible types, invalid ISO-8601 timestamps, or inconsistent `returnedItems` counts fail the
affected run/course and are not written to Bronze. Contract-valid responses are written
byte-for-byte without re-serialization. Additive vendor fields are retained and produce a
schema-drift warning containing field paths only, never response values.
Fields whose non-null production shape has not yet been supplied (`prices`, cost/time, audience,
goals, and similar nullable fields) remain required keys but temporarily accept their original
value type.

SkillUp uses `x-api-key: <SKILLUP_API_KEY>` for every request. Its Intelligence and Reports APIs
use separate base URLs configured by `SKILLUP_INTELLIGENCE_BASE_URL` and
`SKILLUP_REPORTS_BASE_URL`. Assessment History performs a full initial/monthly sync, starts daily
syncs from the last successful watermark minus `SKILLUP_ASSESSMENT_DAILY_OVERLAP_DAYS` (default
3), and re-reads 90 days weekly.
Skill Taxonomy, Skill Inventory, and Assessment History responses are validated against their
vendor-specific Pydantic contracts before Bronze writes and completed checkpoints. Required field
removal, incompatible types, invalid timestamps, or inconsistent pagination metadata fail only the
affected domain and do not enter Bronze. Valid responses retain their exact original bytes;
additive fields are retained and logged by field path without logging employee or report values.

DataCamp sends `Authorization: Bearer <DATACAMP_TOKEN>` and `Accept: application/json` on every
request. Live and archived catalogs are fetched once per run because their pagination contract is
not present in the supplied response envelope. Catalog items expose `updatedAt`, but the
catalog endpoints do not expose a server-side modified-since filter, so each run still stores the
complete contract-valid raw response. Events use `DATACAMP_EVENTS_PAGE_SIZE` (maximum 1000) and
continue until the current page reaches `meta.numberOfPages`.
Both catalogs and Learning History Events are validated against DataCamp-specific Pydantic
contracts before Bronze writes. Live catalog rows must have `live=true`, archived rows must have
`live=false`, and Events must use the exact `data` plus `meta` envelope with matching page metadata.
Contract-invalid responses fail only their domain and do not enter Bronze. Valid responses keep
their original bytes, while additive fields produce path-only schema-drift warnings.

DataCamp Learning History uses explicit `from`/`to` event windows. The first run reads from
`DATACAMP_EVENTS_START_TIME` through the run start time. Normal daily runs start from the last
successfully stored daily watermark minus `DATACAMP_EVENTS_DAILY_OVERLAP_DAYS` (default 3), every
seven days a reconciliation run re-reads `DATACAMP_EVENTS_LOOKBACK_DAYS` (default 90), and the
first run in each new calendar month re-reads the full configured history. The three watermarks
advance only after every `/v1/events` page has entered Bronze successfully. Explicit `from`/`to`
arguments remain manual overrides and do not alter scheduled watermarks.

Coursera exchanges HTTP Basic credentials for one run-scoped access token at
`COURSERA_TOKEN_URL`, then sends `Authorization: Bearer <token>`. A `401` refreshes the token and
retries that request exactly once. Course List and Learning History use `start`/`limit` and
`paging.next`. Course Detail has no URL embedded in code: an administrator must supply
`COURSERA_CONTENT_DETAIL_PATH_TEMPLATE`, using only `{org_id}` and `{content_id}` placeholders.
For the local mock this is `/{org_id}/contents/{content_id}/detail`; production must use the exact
path from the organization's Coursera contract/Postman configuration.
Token, Course List, Course Detail, and Enrollment Reports are validated against Coursera-specific
Pydantic contracts. The token is held only in memory and is never written to Bronze or logs.
Contract-invalid list, detail, or history responses fail the affected domain and do not enter
Bronze. Valid response bytes remain unchanged; additive fields are retained and logged by field
path only. Course Detail must contain exactly one element whose `contentId` matches the request.
Completion-only enrollment fields (`completedAt`, `grade`, and `contentCertificateUrl`) may be
absent for incomplete enrollments.

LinkedIn Learning exchanges form-encoded `client_id` and `client_secret` for one run-scoped token.
Catalog and activity requests use `Authorization: Bearer <token>`; a `401` refreshes the token and
retries once. Course Catalog runs a full load when no successful catalog watermark exists. Later
runs send the previous successful epoch-millisecond watermark as
`assetFilteringCriteria.lastModifiedAfter`. Catalog requests always use exactly one
`assetFilteringCriteria.assetTypes[0]=COURSE` filter and
`assetRetrievalCriteria.includeRetired=true`, then follow the official `paging.links` entry with
`rel=next`. The catalog watermark advances only after every catalog page and changed-course detail
request succeeds.
Activity History starts with a full sync from `LINKEDIN_HISTORY_START_TIME` (ISO-8601 with timezone
or epoch milliseconds). Daily runs resume from the successful watermark with
`LINKEDIN_HISTORY_DAILY_LOOKBACK_DAYS` of overlap, weekly runs re-read
`LINKEDIN_HISTORY_LOOKBACK_DAYS` (default 90), and the first run in each calendar month re-reads
the full configured history. Every range is split into windows no longer
than 14 days and sends `q=criteria`, `startedAt`, `timeOffset.unit=DAY`, and
`timeOffset.duration` for each window. Daily, weekly, and monthly watermarks advance only after the
complete activity range succeeds.
`LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE` is deliberately blank in `backend/.env.example`; an
administrator must provide the exact query string containing one `{urn}` placeholder. The
production filter is never inferred by code. See the official
[Learning Assets](https://learn.microsoft.com/en-us/linkedin/learning/reference/learningassets)
and [Learning Activity Reports](https://learn.microsoft.com/en-us/linkedin/learning/reference/learning-activity-reports-reference)
contracts. Token, Learning Assets, Asset Detail, and Learning Activity Reports are validated
against LinkedIn-specific Pydantic contracts before Bronze is written. Contract-invalid responses
fail the affected domain and are not stored in Bronze. Valid response bytes remain unchanged;
additive fields are retained and logged by field path only. Asset Detail must contain exactly one
element whose `urn` matches the requested URN.

Harvard HMM and Harvard Spark share the same implementation but use separate vendor names,
credentials, Catalog codes, locks, run summaries, and Bronze directories. Each branch obtains one
Catalog token with HTTP Basic credentials and form scope `hbp.org.api/catalog.read`; a `401`
refreshes the token and retries exactly once. Daily scheduling performs a full Catalog load from
`start=0` only when no successful Catalog watermark exists. Later runs send the previous successful
watermark minus one day as `startDate=YYYYMMDD`, preserving a one-day boundary overlap. HMM and
Spark keep separate watermarks, and a watermark advances only after every Catalog page succeeds.
Token and Catalog responses use shared Harvard Pydantic contracts. HMM and Spark CSV reports use
separate header/row contracts because their column names and shapes differ. Contract-invalid JSON
or CSV fails the affected domain and is not written to Bronze; valid payload bytes are preserved
unchanged. Additive Catalog fields are retained and logged by field path only.

Learning History does not use the Catalog token. It connects with `asyncssh`, and
`HARVARD_SFTP_KNOWN_HOSTS` must point to a trusted OpenSSH known-hosts file. Unknown host keys are
never accepted automatically. For local demos only, `HARVARD_SFTP_MOCK_ENABLED=true` replaces the
network connection with deterministic generated CSV files and therefore does not require SFTP
credentials or a known-hosts file. Keep this setting `false` in production. The report date is the local run date minus
`HARVARD_REPORT_DATE_OFFSET_DAYS`. Set `HARVARD_HMM_HISTORY_START_DATE` and
`HARVARD_SPARK_HISTORY_START_DATE` in `YYYY-MM-DD` format to enable historical backfill. The
first run downloads each dated CSV from that start date through the report cutoff. Every run lists
the remote directory metadata once and compares each expected file's size and modified time with
SQLite. New or changed files are downloaded into a new Bronze run; unchanged files are skipped.
Existing databases gain the metadata columns automatically, and legacy rows are downloaded once to
establish their initial metadata. A missing historical file is attempted once per run and
retried by the next daily run; only the newest expected file is polled at the configured interval
until the earlier of `HARVARD_SFTP_MAX_WAIT_SECONDS` or 07:00. Leaving a start date blank retains
daily-only behavior. One SFTP session is reused for the entire backfill run. CSV rows are validated,
then the original bytes are written unchanged; the manifest contains one `files`
entry per download with remote path/name, size, remote modified time, download time, SHA-256, run
ID, and ingestion date.

Transient SFTP connection errors (timeout, connection reset/lost, and OS network errors) reopen
the session and retry up to `HARVARD_SFTP_MAX_RETRIES` times, default `3`, with backoff. Missing
files continue to use the polling deadline. Authentication and SSH host-key failures are not
retried.

FAMS calls one internal endpoint with `Fsa-Report-Api-Key: <FAMS_API_KEY>`. The scheduled Full mode
still downloads the complete JSON response because the API has no update-time filter. After the
response passes its contract, the job compares an order-independent fingerprint of `classList`
and `studentList` with the previous successful Full run. Changed data is stored as one exact raw
`training_data` page; unchanged data completes successfully with zero new Bronze records and no
duplicate file. Full and each Filtered parameter set keep separate fingerprints.
`FAMS_LOAD_MODE=full` sends no query parameters.
Full mode ignores filter values completely, including their validation. `FAMS_LOAD_MODE=filtered`
sends only non-empty `FAMS_STATUS`, `FAMS_SITE`,
`FAMS_ACTUAL_START_DATE_FROM`, and `FAMS_ACTUAL_START_DATE_TO` values. Dates use `YYYYMMDD`.
Filtered mode requires at least one of these four values; otherwise the job fails before making an
HTTP request so it cannot accidentally become a full pull. Status values must match the documented
FAMS enum, while site values remain free-form comma-separated strings.
There is deliberately no `both` mode, OAuth flow, or separate raw file for `classList` and
`studentList`. A run succeeds only when `success=true`, `data` is an object, and both lists are
arrays. The record count is the combined size of those two lists.
Invalid contract responses are marked failed and are not written to Bronze.

Run checks with:

```bash
ruff check .
mypy src/app
pytest
```

## Runtime logs

Set `LOG_LEVEL=INFO` for concise operational logs: run start, run result, duration, total records by
domain, retry warnings, schema drift, and failures. Failed pages include their domain, offset,
retryability, and sanitized error message. Set `LOG_LEVEL=DEBUG` only when troubleshooting deeply;
it additionally shows every HTTP response and successful Bronze page/file write.

Query parameters, request headers, credentials, tokens, API keys, raw response content, and learner
data are deliberately excluded from these logs. Retry logs include only the safe endpoint path,
status or network error type, attempt number, and wait time.

## Local multi-vendor mock demo

The local `backend/.env` points every mock vendor to a path on the same port. Start these two
processes in separate terminals from the repository root:

```bash
# Terminal 1: shared upstream mock hub
cd backend
uvicorn app.mocks.app:app --host 127.0.0.1 --port 9000

# Terminal 2: ingestion service and scheduler
cd backend
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Mock authentication has independent client-side and vendor-side settings. For example,
`SKILLUP_API_KEY` is what the ingestion client sends, while `MOCK_SKILLUP_API_KEY` is what the mock
vendor accepts. Matching values succeed; different values return `401`, which allows realistic
negative authentication tests. The same pattern covers direct API keys, pre-issued Bearer tokens,
username/password login, and OAuth client credentials. Access tokens returned by the mock token
endpoints come from the corresponding `MOCK_*_ACCESS_TOKEN` settings and are never logged.

The generated Harvard SFTP mock also compares the configured client username/password with its
`MOCK_HARVARD_SFTP_*` values and requires the configured `HARVARD_SFTP_KNOWN_HOSTS` file to contain
the expected mock host key. This checks the application's authentication and trust configuration;
it does not implement an SSH network protocol or replace the real `asyncssh` host-key verification
used when `HARVARD_SFTP_MOCK_ENABLED=false`.

For a quick scheduler test, set `INGESTION_TIME` in `backend/.env` to a future minute in
`Asia/Ho_Chi_Minh` before starting Terminal 2. At that minute the single scheduler registers and
starts `levelup-daily-ingestion`, `skillup-daily-ingestion`, `datacamp-daily-ingestion`,
`coursera-daily-ingestion`, `linkedin-daily-ingestion`, and `fams-daily-ingestion` when their mock
credentials are configured. Harvard Catalog routes are also exposed by this hub, and FAMS is
available under `/fams`. With `HARVARD_SFTP_MOCK_ENABLED=true`, the ingestion process generates
local Harvard CSV responses without another server, so the scheduler also registers
`harvard_hmm-daily-ingestion` and `harvard_spark-daily-ingestion`. Keep Terminal 2 running, then
check the relevant
`/jobs/{vendor}/latest`
endpoints and vendor directories under
`backend/data/bronze/` (the configured `./data/bronze` path is relative to the `backend/`
working directory). Swagger on port `8000` is status-only. The shared mock Swagger at
`http://127.0.0.1:9000/docs` exposes all mock vendors and can grow to include future vendor routers
without adding more processes.

## Scheduler

The scheduler is disabled by default. To enable the daily 05:00 jobs locally:

```text
SCHEDULER_ENABLED=true
INGESTION_TIME=05:00
INGESTION_TIMEZONE=Asia/Ho_Chi_Minh
```

Only vendors with a complete credential configuration are scheduled. LevelUP, SkillUp, DataCamp,
Coursera, LinkedIn, Harvard HMM, Harvard Spark, and FAMS receive separate APScheduler jobs at the
same configured time. The
in-process scheduler must have exactly one scheduler-bearing service instance. Do not enable
it independently in every Uvicorn worker or replica. For multi-worker production deployments,
keep it disabled and let Fabric, Azure Data Factory, or another external scheduler own the single
job invocation. `max_instances=1`, coalescing, and a five-minute misfire grace prevent overlapping
or accumulated catch-up executions within one process. It is always disabled when `APP_ENV=test`.

## Bronze layout

The following local paths are relative to `backend/`:

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

data/bronze/coursera/
├── course_catalog/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
├── course_detail/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
│   └── course_id=<content-id>/
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/

data/bronze/linkedin/
├── course_catalog/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
├── course_detail/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
│   └── course_id=<asset-urn>/
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/

data/bronze/harvard_hmm/
├── course_catalog/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
    ├── harvard_hmm_reporting_YYYYMMDD.csv
    └── manifest.json

data/bronze/harvard_spark/
├── course_catalog/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
└── learning_history/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
    ├── harvard_Spark_reporting_YYYYMMDD.csv
    └── manifest.json

data/bronze/fams/
└── training_data/ingestion_date=YYYY-MM-DD/run_id=<uuid>/
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
- Other 4xx responses are not retried. For LevelUP, Coursera, LinkedIn, and Harvard Catalog, a 401
  refreshes the shared token and repeats the request once.
- At most `LEVELUP_MAX_CONCURRENCY` courses run concurrently via `asyncio.Semaphore`.
- SkillUp runs its three independent domains concurrently. Each domain writes and checkpoints one
  page at a time, so the full dataset is never accumulated in memory. Taxonomy and Skill Inventory
  persist successful sync watermarks and send `LastModifiedOn` and
  `SkillProfileModifiedSince` on later scheduled runs. Assessment History performs an initial full
  sync, re-reads from its daily watermark with a three-day overlap, re-reads the configured 90-day
  window weekly, and repeats the full reconciliation in each new calendar month.
- DataCamp also runs its three domains concurrently. Live and archived catalog counts use the
  length of the contract-valid response `data` list. Events require the documented `data` and
  `meta` objects and are validated, written, and checkpointed one page at a time.
- Coursera authenticates once per run, then runs its catalog pipeline and Learning History in
  parallel. Catalog is loaded fully once; later runs send the successful Catalog watermark as
  `modifiedSinceTimestamp` so Coursera returns only added, removed, or modified content. Course
  Details run only for active changed content, with at most `COURSERA_MAX_CONCURRENCY` requests.
  Learning History is loaded fully on the first run and once per calendar month, uses
  `lastActivityAfter` with `COURSERA_HISTORY_DAILY_OVERLAP_DAYS` for daily deltas, and re-reads
  `COURSERA_HISTORY_LOOKBACK_DAYS` every seven days. Catalog and History keep separate successful watermarks because their timestamps use
  seconds and milliseconds respectively. Every response is contract-validated before its exact
  bytes are stored; `records_count` is the length of validated `elements`.
- LinkedIn follows the same one-token and parallel pipeline pattern. Asset Details use at most
  `LINKEDIN_MAX_CONCURRENCY` requests. Catalog is full on its first successful run and incremental
  afterward using `assetFilteringCriteria.lastModifiedAfter`; its watermark is not advanced when a
  catalog page or detail request fails. History pages are written immediately and use globally
  increasing Bronze offsets so pages from separate 14-day windows cannot overwrite one another.
- Each Harvard job runs Catalog and SFTP History independently in parallel. Contract-valid Catalog
  pages are written immediately and counted from the validated `list`; the first run is full and
  later runs use `startDate` with a one-day overlap. An invalid response does not enter Bronze.
  History lists all remote metadata once, backfills configured dates, and downloads only new or
  metadata-changed CSV files. Missing dates do not discard files that succeeded. One failed branch
  produces `PARTIAL_FAILURE` while preserving the successful branch.
- FAMS makes one request per run and writes the exact response bytes once. It logs only separate
  class/student counts and stores their sum in the run summary. Contract-invalid JSON responses
  are still retained in Bronze but the domain and run are marked failed. API-key and permission
  failures (`401`/`403`) are not retried; timeout, connection, `429`, and `5xx` failures are.
- SQLite records every completed page, course, domain, and run for status reporting and audit.
  Every scheduled ingestion creates a new `run_id` and starts the vendor from the first page,
  regardless of whether the previous run succeeded or failed. Failed work is not resumed on the
  next schedule.
- SQLite vendor locks prevent overlapping runs per vendor. All vendors, including Harvard HMM and
  Harvard Spark, use different lock keys, so their scheduled runs can execute independently.
- At ingestion startup, terminal checkpoint runs older than `CHECKPOINT_RETENTION_DAYS` are deleted
  with their page/course/domain rows. Running or locked runs are kept.

## Information still needed from Minh/team

1. Production LevelUP tenant/base URL and confirmation that `Authorization` must contain the raw
   token rather than `Bearer <token>`.
2. Whether the authentication response is always a plain string in every environment.
3. Non-null production samples for SkillUp fields currently observed only as `null` or empty lists,
   including taxonomy rubrics/tags, nullable validation scores, AI ratings, and `skillPriorirty`.
4. Whether SkillUp Assessment `sections` is omitted when `includeSections` is absent/false, and
   which report fields become null for incomplete assessments.
5. The non-empty item shape of DataCamp `includedInLicenses`.
6. The non-null types of DataCamp Events `assessmentScore` and `knowledgeLevel`, plus empty-result
   behavior for `meta.numberOfPages`.
7. OneLake/Fabric workspace, lakehouse, directory convention, authentication method, and atomic
   commit expectations.
8. Production scheduler owner (single FastAPI instance vs Fabric/ADF/external orchestrator).
9. Retention, encryption, and access-control policy for raw Learning History PII.
10. Production Coursera base URL and exact Course Detail path template from the administrator.
11. Exact production `LINKEDIN_ASSET_DETAIL_QUERY_TEMPLATE` supplied by the LinkedIn administrator.
12. Harvard HMM/Spark Catalog response confirmation for `count` and `list`, including empty-page
    behavior and whether `count` is total across all pages.
13. Trusted Harvard SFTP host-key entry, production credentials, remote timestamps/timezone, and
    confirmation of the exact filename capitalization and delivery cutoff.
14. The earliest historical report date for HMM and Spark, and whether Harvard publishes a file
    for every calendar day or omits weekends/holidays.
15. Production FAMS base URL/API key, IP allowlist, and confirmation that the response field names
    and date-filter semantics match the supplied `fsa-reports-training-data.md` contract.

### SkillUp Assessment History date range

The iMocha `GET /v3/reports` contract returns only the most recent seven days when no range is
provided. The first run sends `startDate` from `SKILLUP_ASSESSMENT_START_DATE` and `endDate` as the
current UTC time to backfill history. Daily runs explicitly request from the last successful daily
watermark minus `SKILLUP_ASSESSMENT_DAILY_OVERLAP_DAYS` (default 3) through the current run start.
Every `SKILLUP_ASSESSMENT_WEEKLY_SYNC_INTERVAL_DAYS` (default 7), the service passes a range
covering `SKILLUP_ASSESSMENT_LOOKBACK_DAYS` (default 90). The first run in each new calendar month
repeats the full configured history. Daily, weekly, and full-sync watermarks advance only after
every report page succeeds; broader runs also advance the narrower scopes they cover.

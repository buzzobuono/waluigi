# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**Waluigi** is a lightweight distributed task orchestrator with a **server-push architecture**: the Boss daemon actively schedules and dispatches tasks to Workers via HTTP, rather than workers polling for work. It is designed for data pipeline DAG execution across distributed clusters, with an optional Catalog service for dataset metadata, schema tracking, and data quality.

## Commands

```bash
# Run servers
wlboss --port 8082 --db-path ./db/waluigi.db
wlworker --boss-url http://localhost:8082 --port 5001 --slots 2
wlcatalog --port 9000 --data-path ./data --db-path ./db/catalog.db
wlconsole --port 8080   # web console with JWT auth, proxies Boss + Catalog

# CLI client
wlctl --url http://localhost:8082 apply -f descriptors/analytics.yaml
wlctl get namespaces
wlctl get jobs
wlctl get tasks
wlctl get tasks -j <job_id>
wlctl get resources
wlctl get workers
wlctl logs <task_id> [-n <lines>] [--follow]
wlctl reset task <task_id>
wlctl reset namespace <ns>
wlctl delete task <task_id>
wlctl delete namespace <ns>

# Tests
pytest tests/
pytest tests/catalog/test_datasets.py   # run a single test file

# Local dev environment
docker compose up
./clean.sh    # remove *.db, *.log, *.json runtime files
./create-docker-images.sh [VERSION]   # build Docker images
```

Config uses `ConfigArgParse` — all CLI flags map to env vars with component prefixes: `WALUIGI_BOSS_*`, `WALUIGI_WORKER_*`, `WALUIGI_CATALOG_*`.

## Architecture

Four independent processes communicate over HTTP:

| Component | Port | Role |
|-----------|------|------|
| Boss | 8082 | Control plane — SQLite state, planner loop, DAG execution |
| Worker | 5001+ | Execution plane — receives pushed tasks, forks subprocesses |
| Catalog | 9000 | Optional — dataset metadata, schema tracking, data quality |
| Console | 8080 | Optional — web UI with JWT auth, proxies Boss + Catalog APIs |

Entry points (defined in `pyproject.toml`): `wlboss`, `wlworker`, `wlcatalog`, `wlconsole`, `wlctl`.

### DAG Execution Flow

1. User submits a Job YAML descriptor via `wlctl apply` or `POST /submit`.
2. Boss claims the job atomically (SQLite `UPDATE ... RETURNING`), then calls `core/engine.py` to recursively build the DAG bottom-up.
3. For each task: verify not already `SUCCESS`, wait for dependencies, allocate named resources.
4. Boss dispatches ready tasks via `HTTP POST /execute` to a Worker (returns 202 if slot available, 429 if full).
5. Worker runs `asyncio.create_subprocess_shell`, injects `WALUIGI_PARAM_*` / `WALUIGI_ATTRIBUTE_*` / `WALUIGI_TASK_ID` / `WALUIGI_JOB_ID` env vars, streams logs back to Boss.
6. Worker reports final status (`SUCCESS` / `FAILED`); Boss releases resources and advances the DAG.

### Key Source Locations

| File | Role |
|------|------|
| `waluigi/boss/__main__.py` | Boss FastAPI app, planner loop, all REST endpoints |
| `waluigi/boss/db.py` | SQLite schema and all DB query functions (`WaluigiDB`) |
| `waluigi/core/engine.py` | Recursive DAG planner: `build()`, `_dispatch()`, resource allocation |
| `waluigi/core/dag.py` | `DAGTask` model, YAML pipeline parser, param resolution (`${parent.params.KEY}`) |
| `waluigi/core/responses.py` | Standard response envelope (`data` + `diagnostic`) |
| `waluigi/core/utils.py` | Logging setup from YAML, Pydantic v1/v2 compat shim |
| `waluigi/worker.py` | Worker FastAPI app, subprocess execution, log streaming, heartbeat |
| `waluigi/cli.py` | `wlctl` command implementations |
| `waluigi/console.py` | Web console FastAPI app — JWT auth (HS256/PBKDF2), user CRUD, transparent proxy to Boss + Catalog |
| `waluigi/catalog/__main__.py` | Catalog FastAPI app — wires up all routers via `include_router` |
| `waluigi/catalog/config.py` | Catalog ConfigArgParse config (port, db-url, data-path, rules-path) |
| `waluigi/catalog/db.py` | Catalog SQLite schema and queries |
| `waluigi/catalog/api/schemas.py` | Pydantic request/response models for all Catalog endpoints |
| `waluigi/catalog/api/routes/` | Separate FastAPI routers (dataset, source, version, schema, browser, chart, dq, lineage, materialize, metadata) |
| `waluigi/catalog/services/` | Service layer between routers and DB (dataset, source, chart, dq, lineage, materialize, metadata, schema, version) |
| `waluigi/catalog/entities.py` | Catalog DB entity dataclasses |
| `waluigi/catalog/utils.py` | Schema inference from files (CSV/Parquet/JSON), ISO timestamp helpers |
| `waluigi/sdk/context.py` | Singleton `context` — reads `WALUIGI_PARAM_*` / `WALUIGI_ATTRIBUTE_*` / `WALUIGI_CONFIG` from env |
| `waluigi/sdk/catalog.py` | `CatalogClient`, `DatasetReader`, `DatasetWriter` |
| `waluigi/sdk/dataquality.py` | `DQManager` — YAML-rule-based data validation |
| `waluigi/sdk/connectors/` | Pluggable storage connectors (local, S3, SFTP, SQL) |
| `waluigi/tasks/` | Built-in reusable task scripts: filter, aggregate, join, merge, pivot, deduplicate, select, add_derived_columns, catalog_* |
| `waluigi/tasks/_io.py` | Helper functions for built-in tasks: `read_input()`, `write_output()` with automatic lineage |
| `waluigi/_deprecated/` | Old code — ignore entirely |

### Task State Machine

`PENDING` → `READY` → `RUNNING` → `SUCCESS` / `FAILED`

Tasks are keyed by `id + params_hash` — the same task ID with different params produces distinct records. `SUCCESS` tasks are never re-run (idempotent resubmit). No automatic retries: failed tasks must be explicitly reset via `wlctl reset`.

### DAG Return Value Semantics (engine.py `build()`)

- Returns `True` → task (and all dependencies) are `SUCCESS`
- Returns `False` → task is blocked (dependency not yet done); parent stays `PENDING`
- Returns `None` → task or dependency `FAILED`; error propagates upward, blocking parent

### YAML Job Descriptor Format

```yaml
kind: Job
metadata:
  namespace: analytics
  workdir: /data/work
spec:
  id: root_task
  name: Root Task
  command: python source/analytics/run.py
  params:
    date: "2024-01-01"
  attributes:
    owner: "data-team"
  resources:
    coin: 1
  requires:
    - id: upstream_task
      command: python source/analytics/upstream.py
      params:
        date: ${parent.params.date}   # inherited from parent at planning time
```

Resources are named pools (e.g., `coin`, `gpu`) defined via `ClusterResources` descriptor and enforced cluster-wide before dispatch:

```yaml
kind: ClusterResources
metadata:
  namespace: default
spec:
  resources:
    coin: 10
    gpu: 2
```

### SQLite Concurrency

Boss uses WAL mode with `busy_timeout=30s`. Multiple Boss replicas can run concurrently — atomic claiming prevents duplicate task execution. Resource allocation uses `BEGIN IMMEDIATE` for serialization. Do not assume single-writer semantics when modifying DB query logic. Each thread gets its own SQLite connection via `threading.local`.

### Worker Subprocess Contract

- Exit code 0 → `SUCCESS`; non-zero → `FAILED`
- Task scripts access inputs via `os.environ["WALUIGI_PARAM_KEY"]` or via `from waluigi.sdk.context import context` (`context.params.KEY`)
- Log buffering: worker sends logs in batches of 5 lines to reduce HTTP overhead
- Worker registers with Boss via heartbeat (configurable interval); `free_slots = SLOTS - active_tasks_count`

## API Response Envelope

All Boss and Catalog endpoints return:

```json
{
  "data": <actual_data>,
  "diagnostic": {
    "result": "OK | WARN | KO",
    "messages": ["..."]
  }
}
```

`OK` = success, `WARN` = success with warnings, `KO` = error (HTTP 4xx/5xx). The SDK's `CatalogClient` unwraps responses and raises `CatalogError` on `KO`.

## Boss REST API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/submit` | Submit a Job or ClusterResources descriptor |
| POST | `/update` | Worker reports task status (RUNNING/SUCCESS/FAILED) |
| POST | `/worker/register` | Worker registers with available slots |
| POST | `/api/logs/{task_id}` | Worker sends task log lines |
| GET | `/api/namespaces` | List namespaces |
| GET | `/api/jobs` | List jobs |
| GET | `/api/tasks` | List tasks (optional `?job_id=`) |
| GET | `/api/tasks/{task_id}/logs` | Get task logs |
| GET | `/api/resources` | List cluster resources |
| GET | `/api/workers` | List registered workers |
| POST | `/api/reset/{scope}/{id}` | Reset task or namespace to PENDING |
| POST | `/api/delete/{scope}/{id}` | Delete task or namespace |

## Catalog REST API

**Sources:**
- `GET/POST /sources` — list or create data sources (local, s3, sql, sftp, api)

**Datasets:**
- `GET/POST /datasets` — list or create datasets
- `GET /datasets/{id}` — get metadata
- `PATCH /datasets/{id}` — update description or status
- `POST /datasets/{id}/_reserve` — Phase 1 of two-phase write: reserve a version slot
- `POST /datasets/{id}/_commit/{version}` — Phase 2: verify file, compute hash, infer schema
- `POST /datasets/{id}/approve` — Mark dataset as approved and publish schema
- `POST /datasets/{id}/materialize` — Fetch a REST API and store result as CSV
- `POST /datasets/{id}/register-virtual` — Register a virtual dataset (SQL query, no file)

**Versions:**
- `GET /datasets/{id}/versions` — List committed versions (newest first)
- `GET /datasets/{id}/_preview/{version}` — Preview rows (CSV/Parquet only)
- `DELETE /datasets/{id}/deprecate/{version}` — Deprecate a version

**Schema:**
- `GET /datasets/{id}/schema` — Get schema with PII flags and status
- `PATCH /datasets/{id}/schema/{column}` — Update column type, PII flag, or description
- `POST /datasets/{id}/schema/publish` — Promote all columns to "published" status

**Metadata & Lineage:**
- `GET/POST /datasets/{id}/metadata/{version}` — Get or set version metadata
- `GET /datasets/{id}/lineage/{version}` — Get upstream/downstream lineage

**Charts:**
- `GET /datasets/{id}/charts` — List chart definitions
- `POST /datasets/{id}/charts` — Create chart definition (ECharts spec)
- `PATCH /datasets/{id}/charts/{chart_id}` — Update chart
- `DELETE /datasets/{id}/charts/{chart_id}` — Delete chart
- `GET /datasets/{id}/charts/{chart_id}/render` — Render chart as ECharts option for a specific version
- `GET /datasets/{id}/charts/{key}/render` — Render chart by key

**Data Quality:**
- `GET /datasets/{id}/expectations` — List DQ expectations
- `POST /datasets/{id}/expectations` — Add expectation
- `PATCH /datasets/{id}/expectations/{exp_id}` — Update expectation
- `DELETE /datasets/{id}/expectations/{exp_id}` — Delete expectation
- `GET /datasets/{id}/dq/{version}` — DQ results for a version
- `GET /dq/rules` — List available DQ rule catalog
- `GET /dq/suites/{path}` — Parse and return a DQ suite YAML

**Browse:**
- `GET /folders/{prefix}/` — List datasets and virtual sub-prefixes (S3 ListObjects semantics)

### Dataset Lifecycle

Two-phase commit for writes:
1. **Reserve** (`_reserve`): creates version record, returns write location
2. **Commit** (`_commit/{version}`): verifies file exists, computes checksum, infers schema, upserts metadata

Schema status lifecycle: `inferred` (automatic) → `draft` (after edit) → `published` (after `schema/publish`)

Dataset status: `draft` → `in_review` → `approved` → `deprecated`

## SDK Usage (Task Scripts)

```python
from waluigi.sdk.context import context   # reads WALUIGI_PARAM_* / WALUIGI_ATTRIBUTE_* / WALUIGI_CONFIG from env
from waluigi.sdk.catalog import CatalogClient

date = context.params.date
job_id = context.job_id   # WALUIGI_JOB_ID

catalog = CatalogClient()   # reads WALUIGI_CATALOG_URL from env

# Read a dataset
reader = catalog.resolve("sales/raw/transactions")
df = reader.read()

# Write a dataset
with catalog.produce("sales/clean/transactions", metadata={"date": date}, inputs=[reader]) as writer:
    writer.write(df)
```

### Built-in Tasks (`waluigi/tasks/`)

Reusable task scripts invocable from YAML descriptors without custom code:

| Task script | Function |
|-------------|----------|
| `filter_dataset.py` | `df.query(where_clause)` |
| `select_columns.py` | Column projection |
| `add_derived_columns.py` | Compute new columns via expressions |
| `aggregate_dataset.py` | Group-by aggregation |
| `join_datasets.py` | Join two datasets |
| `merge_datasets.py` | Union/concat datasets |
| `pivot_dataset.py` | Pivot table |
| `deduplicate_dataset.py` | Drop duplicate rows |
| `catalog_create_dataset.py` | Create Catalog dataset via params |
| `catalog_create_source.py` | Create Catalog source via params |
| `catalog_define_schema.py` | Define/publish schema |
| `catalog_set_expectations.py` | Attach DQ expectations from YAML |
| `catalog_set_charts.py` | Attach chart definitions from YAML |

All built-in tasks use `waluigi/tasks/_io.py` helpers (`read_input()` / `write_output()`) which handle source upsert and lineage automatically.

## Storage Connectors

Located in `waluigi/sdk/connectors/`. All extend `BaseConnector`:

| Connector | Source type | Notes |
|-----------|-------------|-------|
| `LocalConnector` | `local` | Supports CSV, Parquet, JSON, PKL, XLS, Feather, ORC |
| `S3Connector` | `s3` | URI format `s3://bucket/key`; supports MinIO via `endpoint_url` |
| `SQLConnector` | `sql`, `postgresql`, `mysql`, `sqlite` | Location = table or `schema.table`; uses SQLAlchemy |
| `SFTPConnector` | `sftp` | SSH key or password auth via paramiko |

`ConnectorFactory.get(source_type, config)` returns the right connector. Write operations return row count.

## Data Quality

Rules defined as YAML files in `rules/`, evaluated by `DQManager`:

```yaml
formula: "df['col'].notna().sum() / len(df) > threshold"
inputs_schema:
  df: "Input DataFrame"
params_schema:
  threshold: "Minimum proportion (0-1)"
description: "Check completeness"
```

Formulas are validated via AST whitelist before execution (no arbitrary code). `DQManager.run_suite(suite_path, datasets)` returns a `SuiteResult` with score and per-rule pass/fail details.

## Conventions & Key Details

**Task hashing:** `id + sorted(key:value)` pairs from params and attributes — used for idempotency. Same task ID + same params = same record; same ID + different params = distinct record.

**Parameter interpolation:** `${parent.params.KEY}` in child task spec is resolved at `DAGTask` construction time (`core/dag.py`) against the parent's params namespace.

**Version format:** ISO 8601 timestamp with milliseconds (e.g., `2026-04-11T10:00:00.123+00:00`).

**Dataset ID paths:** Forward-slash hierarchical (e.g., `sales/raw/transactions`). The `folders` endpoint uses these as virtual prefixes.

**Metadata keys:** Any string; `sys.*` prefix is reserved for system-managed keys.

**Virtual datasets:** Metadata-only records pointing to external resources (SQL queries, API endpoints) — no physical file.

**Lineage:** Version-specific. Tracks which dataset versions were consumed (upstream) and produced (downstream). External sources are marked `__external__/url`.

**Job locking:** Boss holds a 60-second lock per job while planning; prevents concurrent planners from duplicating work.

**No CI pipeline:** Tests are run manually with `pytest tests/`.

## Tests

`tests/conftest.py` auto-starts a Catalog server for integration tests. Test files:
- `tests/test_worker.py` — Worker unit tests (hash, HTTP helpers, `/execute` endpoint, subprocess, slot management)
- `tests/catalog/test_datasets.py` — Dataset CRUD, lifecycle, find by status
- `tests/catalog/test_sources.py` — Source CRUD and upsert
- `tests/catalog/test_versions.py` — Version list, reserve/commit, deprecate, preview
- `tests/catalog/test_schema.py` — Schema get, patch column, publish
- `tests/catalog/test_metadata.py` — Metadata key-value per version
- `tests/catalog/test_folders.py` — Folder browse (virtual prefixes)
- `tests/catalog/test_dataset_produce.py` — End-to-end produce/resolve via SDK
- `tests/catalog/test_dq.py` — DQ expectations CRUD and results
- `tests/catalog/test_charts.py` — Chart definitions lifecycle and render (ECharts)
- `tests/catalog/test_lineage.py` — Lineage chain (RAW→SILVER→GOLD), version isolation
- `tests/dq_test.py` — Data quality rule formula tests
- `tests/cf_test.py` — Codice fiscale (Italian tax code) coherence rule test
- `tests/describe_rule_test.py` — Rule description tests
- `tests/rules_list_test.py` — Rule listing tests

## Docker / Deployment

`docker-compose.yml` runs 4 services: `boss` (1 replica), `worker` (3 replicas), `catalog` (1 replica), `console` (1 replica). Shared volumes: `/db` (SQLite), `/data` (datasets), `/work` (task working dirs).

Dockerfiles: `Dockerfile.boss`, `Dockerfile.worker`, `Dockerfile.catalog`, `Dockerfile.console`.

Swarm deployment: `deploy-to-swarm.sh` / `clean-from-swarm.sh`.

## Web Console

Two-tier setup:
1. **Static SPA** (`static/`) — Vue-style components served by the Console process. Key views: Namespaces, Jobs, Tasks, Workers, Resources, DAG chart, Log modal, Catalog, Lineage, Dataset preview, Charts.
2. **Console server** (`waluigi/console.py`) — FastAPI app that handles JWT auth (HS256 + PBKDF2 passwords), user management (admin-only CRUD), and acts as an authenticated reverse proxy to Boss and Catalog APIs.

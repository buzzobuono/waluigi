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
pytest tests/catalog/test_catalog.py   # run a single test file

# Local dev environment
docker compose up
./clean.sh    # remove *.db, *.log, *.json runtime files
./create-docker-images.sh [VERSION]   # build Docker images
```

Config uses `ConfigArgParse` — all CLI flags map to env vars with component prefixes: `WALUIGI_BOSS_*`, `WALUIGI_WORKER_*`, `WALUIGI_CATALOG_*`.

## Architecture

Three independent processes communicate over HTTP:

| Component | Port | Role |
|-----------|------|------|
| Boss | 8082 | Control plane — SQLite state, planner loop, DAG execution |
| Worker | 5001+ | Execution plane — receives pushed tasks, forks subprocesses |
| Catalog | 9000 | Optional — dataset metadata, schema tracking, data quality |

Entry points (defined in `pyproject.toml`): `wlboss`, `wlworker`, `wlcatalog`, `wlctl`.

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
| `waluigi/core/task.py` | `DynamicTask` model, param resolution (`${parent.params.KEY}`), task hashing |
| `waluigi/core/responses.py` | Standard response envelope (`data` + `diagnostic`) |
| `waluigi/worker.py` | Worker FastAPI app, subprocess execution, log streaming, heartbeat |
| `waluigi/cli.py` | `wlctl` command implementations |
| `waluigi/catalog/__main__.py` | Catalog FastAPI app, all dataset/source/schema endpoints |
| `waluigi/catalog/db.py` | Catalog SQLite schema and queries |
| `waluigi/catalog/models.py` | Pydantic request/response models for Catalog |
| `waluigi/catalog/entities.py` | Catalog DB entity dataclasses |
| `waluigi/sdk/task.py` | `Task` base class for task scripts (reads env vars) |
| `waluigi/sdk/catalog.py` | `CatalogClient`, `DatasetReader`, `DatasetWriter` |
| `waluigi/sdk/dataquality.py` | `DQManager` — YAML-rule-based data validation |
| `waluigi/sdk/connectors/` | Pluggable storage connectors (local, S3, SFTP, SQL) |
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
- Task scripts access inputs via `os.environ["WALUIGI_PARAM_KEY"]` or via `from waluigi.sdk.task import Task`
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
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import CatalogClient

task = Task()          # reads WALUIGI_PARAM_* and WALUIGI_ATTRIBUTE_* from env
date = task.params.date
job_id = task.job_id   # WALUIGI_JOB_ID

catalog = CatalogClient()   # reads WALUIGI_CATALOG_URL from env

# Read a dataset
reader = catalog.resolve("sales/raw/transactions")
df = reader.read()

# Write a dataset
with catalog.produce("sales/clean/transactions", metadata={"date": date}, inputs=[reader]) as writer:
    writer.write(df)
```

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

**Parameter interpolation:** `${parent.params.KEY}` in child task spec is resolved at `DynamicTask` construction time against the parent's params namespace.

**Version format:** ISO 8601 timestamp with milliseconds (e.g., `2026-04-11T10:00:00.123+00:00`).

**Dataset ID paths:** Forward-slash hierarchical (e.g., `sales/raw/transactions`). The `folders` endpoint uses these as virtual prefixes.

**Metadata keys:** Any string; `sys.*` prefix is reserved for system-managed keys.

**Virtual datasets:** Metadata-only records pointing to external resources (SQL queries, API endpoints) — no physical file.

**Lineage:** Version-specific. Tracks which dataset versions were consumed (upstream) and produced (downstream). External sources are marked `__external__/url`.

**Job locking:** Boss holds a 60-second lock per job while planning; prevents concurrent planners from duplicating work.

**No CI pipeline:** Tests are run manually with `pytest tests/`.

## Tests

`tests/conftest.py` auto-starts a Catalog server for integration tests. Test files:
- `tests/catalog/` — Catalog integration tests (datasets, folders, sources, data produce/resolve)
- `tests/dq_test.py` — Data quality rule tests
- `tests/cf_test.py` — Codice fiscale (Italian tax code) coherence rule test
- `tests/describe_rule_test.py` — Rule description tests
- `tests/rules_list_test.py` — Rule listing tests

## Docker / Deployment

`docker-compose.yml` runs 3 services: `boss` (1 replica), `worker` (3 replicas), `catalog` (1 replica). Shared volumes: `/db` (SQLite), `/data` (datasets), `/work` (task working dirs).

Dockerfiles: `Dockerfile.boss`, `Dockerfile.worker`, `Dockerfile.catalog`.

## Web Console

Static single-page app in `static/`. Vue-style components served directly by Boss. Key views: Namespaces, Jobs, Tasks, Workers, Resources, DAG chart, Log modal, Catalog, Lineage, Dataset preview.

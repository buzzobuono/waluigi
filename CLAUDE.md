# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Maintenance (REQUIRED)

**Whenever you change code, you MUST update the documentation in the same change:**

1. **`doc/`** — the user-facing docs (`architecture.md`, `built-in-tasks.md`, `catalog.md`, `cli.md`, `data-quality.md`, `deployment.md`, `sdk.md`, `task-development.md`, `yaml-reference.md`). Update the file(s) covering the area you touched.
2. **`plugins/waluigi-developer/skills/waluigi-developer/SKILL.md`** — the usage skill. Keep its task/command/workflow references in sync with the code.
3. **`CLAUDE.md`** — this file, when architecture, commands, APIs, or built-in tables change.

A code change is not complete until `doc/`, the skill, and `CLAUDE.md` reflect it. Adding a built-in task, a CLI command, a REST endpoint, or a config flag always implies a matching doc + skill edit.

## What This Project Is

**Waluigi** is a lightweight distributed task orchestrator with a **server-push architecture**: the Boss daemon actively schedules and dispatches tasks to Workers via HTTP, rather than workers polling for work. It is designed for data pipeline DAG execution across distributed clusters, with an optional Catalog service for dataset metadata, schema tracking, and data quality.

## Commands

```bash
# Run servers
wlboss --port 8082 --db-path ./db/waluigi.db
wlworker --boss-url http://localhost:8082 --port 5001 --slots 2 --affinity python
wlcatalog --port 9000 --data-path ./data --db-path ./db/catalog.db
wlconsole --port 8080   # web console with JWT auth, proxies Boss + Catalog

# CLI client (all commands go through wlconsole by default)
wlctl --url http://localhost:8080 login -u admin
wlctl apply -f descriptor.yaml [-n namespace]
wlctl apply-builtins -n <namespace>            # register core built-in TaskDefinitions (idempotent)
wlctl apply-builtins -n <namespace> google     # register Google vendor TaskDefinitions

wlctl get namespaces
wlctl get jobs [-n ns] [-s status]
wlctl get tasks [-n ns] [-j job_id]
wlctl get resources [-n ns]
wlctl get workers
wlctl get taskdefinitions [-n ns]
wlctl get jobdefinitions [-n ns]
wlctl get cronjobs [-n ns]
wlctl get secrets [-n ns]
wlctl get sources [-n ns]
wlctl get datasets [-n ns]
wlctl get versions -d <dataset_id> [-n ns]
wlctl get metadata -d <dataset_id> [-v version] [-n ns]
wlctl get schema -d <dataset_id> [-n ns]

wlctl describe namespace <name>
wlctl describe job <job_id> [-n ns]
wlctl describe task <task_id> [-n ns]
wlctl describe taskdefinition <name> [-n ns]
wlctl describe jobdefinition <name> [-n ns]
wlctl describe cronjob <name> [-n ns]
wlctl describe dataset <id> [-n ns]
wlctl describe source <id> [-n ns]
wlctl describe secret <name> [-n ns]

wlctl logs <task_id> [-n ns] [-l lines] [--follow]
wlctl reset task <task_id> [-n ns]
wlctl reset job <job_id> [-n ns]
wlctl reset namespace <ns>
wlctl cancel job <job_id> [-n ns]
wlctl pause job <job_id> [-n ns]
wlctl resume job <job_id> [-n ns]
wlctl enable cronjob <name> [-n ns]
wlctl disable cronjob <name> [-n ns]

wlctl delete job <job_id> [-n ns]
wlctl delete cronjob <name> [-n ns]
wlctl delete taskdefinition <name> [-n ns]
wlctl delete jobdefinition <name> [-n ns]
wlctl delete namespace <ns>
wlctl delete secret <name> [-n ns]
wlctl delete dataset <id> [-n ns]            # cascade: all versions + physical files
wlctl delete version <version> -d <dataset> [-n ns]  # hard delete single version

wlctl run [cmd] [-f yaml] [-t task_id] [-p KEY=VALUE ...]  # local dev, no cluster needed

# Tests
pytest tests/
pytest tests/catalog/test_datasets.py   # run a single test file

# Local dev environment
docker compose up
./clean.sh    # remove *.db, *.log, *.json runtime files
./create-docker-images.sh [VERSION]   # build Docker images
```

CLI output follows k8s style: `kind/name created` on success, `Error from server (NotFound): msg` on stderr for errors.

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

1. User submits a Job YAML descriptor via `wlctl apply` or `POST /namespaces/{ns}/jobs`.
2. Boss claims the job atomically (SQLite `UPDATE ... RETURNING`), then calls `boss/engine.py` to recursively build the DAG bottom-up.
3. For each task: verify not already `SUCCESS`, wait for dependencies, allocate named resources.
4. Boss resolves `taskRef` types to a command via `TaskDefinition` in DB, applies affinity filtering, then dispatches via `HTTP POST /namespaces/{ns}/dispatch` to a Worker (202 if accepted, 429 if full).
5. Worker runs `asyncio.create_subprocess_shell`, injects env vars, streams logs back to Boss.
6. Worker reports final status (`SUCCESS` / `FAILED`); Boss releases resources and advances the DAG.

### Key Source Locations

| File | Role |
|------|------|
| `waluigi/boss/__main__.py` | Boss FastAPI app entry point |
| `waluigi/boss/engine.py` | Recursive DAG planner: `build()`, `_dispatch()`, resource allocation, affinity filtering |
| `waluigi/boss/planner.py` | Background thread: claims and plans runnable jobs |
| `waluigi/boss/cron_scheduler.py` | Background thread: fires CronJobs on schedule |
| `waluigi/boss/db/engine.py` | SQLite schema (all tables) |
| `waluigi/boss/repositories/` | One repo per entity (job, task, worker, resource, secret, …) |
| `waluigi/boss/services/` | Service layer wrapping repositories |
| `waluigi/boss/api/routes/` | FastAPI routers per resource type |
| `waluigi/commons/dag.py` | `DAGTask` model, YAML pipeline parser (`parse_definition`), `DAGSpec` |
| `waluigi/commons/responses.py` | Standard response envelope (`ok`, `ko`) |
| `waluigi/worker/__main__.py` | Worker FastAPI app, heartbeat loop |
| `waluigi/worker/api/routes/worker_router.py` | `/namespaces/{ns}/dispatch` endpoint |
| `waluigi/worker/services/worker_service.py` | Subprocess execution, log streaming, secret injection |
| `waluigi/worker/config/args.py` | Worker CLI args (slots, affinity, boss-url, …) |
| `waluigi/cli/__main__.py` | `wlctl` argument parser and dispatch |
| `waluigi/cli/commands/` | One module per command group (apply, get, describe, lifecycle, catalog, logs, run) |
| `waluigi/cli/output.py` | Table rendering and k8s-style ok/error output |
| `waluigi/console.py` | Console FastAPI app — JWT auth, user CRUD, reverse proxy to Boss + Catalog |
| `waluigi/catalog/__main__.py` | Catalog FastAPI app — wires up all routers |
| `waluigi/catalog/api/routes/` | Routers: dataset, source, version, schema, browser, chart, dq, lineage, materialize, metadata |
| `waluigi/catalog/services/` | Service layer (dataset, source, version, schema, chart, dq, lineage, materialize, metadata) |
| `waluigi/catalog/repositories/` | One repo per table |
| `waluigi/catalog/entities/` | DB entity dataclasses |
| `waluigi/sdk/context.py` | Singleton `context` — reads `WALUIGI_PARAM_*` / `WALUIGI_ATTRIBUTE_*` / `WALUIGI_CONFIG` from env |
| `waluigi/sdk/catalog.py` | `CatalogClient`, `DatasetReader`, `DatasetWriter` |
| `waluigi/sdk/dataquality.py` | `DQManager` — YAML-rule-based data validation |
| `waluigi/sdk/connectors/` | Pluggable storage connectors (local, S3, SFTP, SQL) |
| `waluigi/tasks/` | Built-in task scripts (Python modules run via `python -m waluigi.tasks.*`) |
| `waluigi/tasks/_io.py` | `read_input()`, `write_output()` helpers with automatic lineage |

### Task State Machine

`PENDING` → `READY` → `RUNNING` → `SUCCESS` / `FAILED`

Tasks are keyed by `id + params_hash` — the same task ID with different params produces distinct records. `SUCCESS` tasks are never re-run (idempotent resubmit). No automatic retries: failed tasks must be explicitly reset via `wlctl reset`.

### DAG Return Value Semantics (`engine.py` `build()`)

- Returns `True` → task (and all dependencies) are `SUCCESS`
- Returns `False` → task is blocked (dependency not yet done or waiting for a worker slot); parent stays `PENDING`
- Returns `None` → task or dependency `FAILED`; error propagates upward
- Returns `"PAUSE"` → all workers saturated; job pauses until next planner tick

### YAML Descriptor Kinds

| Kind | Description |
|------|-------------|
| `Namespace` | Create/configure a namespace |
| `Job` | Submit a job for immediate execution (inline `jobSpec` or via `jobRef`) |
| `JobDefinition` | Reusable job template; referenced by CronJobs or Job `jobRef` |
| `TaskDefinition` | Reusable task type: defines `command`/`script` and `affinity` |
| `CronJob` | Scheduled job (cron expression + `jobRef`) |
| `NamespaceResources` / `ClusterResources` | Define named resource pools |
| `Secret` | Namespace-scoped key-value secrets injected as env vars |
| `Source` | Catalog data source — routed to Catalog, not Boss |
| `User` | Console user (admin only) |

### YAML Job Format

```yaml
kind: Job
metadata:
  name: "my-pipeline"
  namespace: analytics
spec:
  executionPolicy: Ephemeral   # Ephemeral (default) or Stateful
  concurrencyPolicy: Forbid    # Forbid (default), Replace, Allow
  jobSpec:
    tasks:
      - id: extract
        taskRef:
          name: IngestRest      # resolved via TaskDefinition in DB
        config:
          http:
            url: "https://api.example.com/data"
        resources:
          coin: 1
        requires: []

      - id: transform
        taskSpec:               # inline task definition
          command: "python /app/transform.py"
          affinity:
            - python
        params:
          date: "2024-01-01"
        resources:
          coin: 2
        requires:
          - extract
```

Resources are named pools (e.g., `coin`) defined via `ClusterResources`:

```yaml
kind: ClusterResources
metadata:
  namespace: analytics
spec:
  resources:
    coin: 10
```

### TaskDefinition and Affinity

**TaskDefinition** defines a reusable task type. The spec contains:
- `command` — shell command executed by the worker
- `script` — inline Python script (mutually exclusive with command)
- `affinity` — list of capability tags the worker must have (optional)

**Resources are never specified in TaskDefinition** — they are always in the job/task spec, because the same task type can consume different resources depending on the data volume.

**Affinity rules:**
- For `taskRef` tasks: affinity comes from the referenced `TaskDefinition` in DB
- For `taskSpec` inline tasks: affinity goes inside `taskSpec`, not at the outer task level
- The Boss filters workers: `task_affinity ⊆ worker_affinity` (all task tags must be present on the worker)
- Empty task affinity → runs on any worker (including specialized ones)
- Workers register with `--affinity comma,separated,tags`

```yaml
kind: TaskDefinition
metadata:
  name: "IngestRest"
  namespace: analytics
spec:
  command: "python -m waluigi.tasks.ingest_rest"
  affinity:
    - python
```

Built-in task definitions are bundled in the package under `waluigi/tasks/data/`. Two categories:

- **Core** (`builtin-task-definitions.yaml`) — always applicable, no external dependencies
- **Vendor** (`builtin-task-definitions-{vendor}.yaml`) — vendor-specific; apply only when needed

```bash
wlctl apply-builtins -n analytics            # core built-ins
wlctl apply-builtins -n analytics google     # Google vendor built-ins (SendGmail, ...)
```

Both commands are idempotent. When adding a new built-in task, add it to the appropriate YAML file (core or vendor). Vendor files follow the naming convention `builtin-task-definitions-{vendor}.yaml`.

### SQLite Concurrency

Boss uses WAL mode with `busy_timeout=30s`. Multiple Boss replicas can run concurrently — atomic claiming prevents duplicate task execution. Resource allocation uses `BEGIN IMMEDIATE` for serialization. Do not assume single-writer semantics when modifying DB query logic. Each thread gets its own SQLite connection via `threading.local`.

### Worker Subprocess Contract

- Exit code 0 → `SUCCESS`; non-zero → `FAILED`
- Task scripts access inputs via `os.environ["WALUIGI_PARAM_KEY"]` or `from waluigi.sdk.context import context`
- Secrets injected as `WALUIGI_SECRET_{KEY_UPPER}` env vars; also expanded in `WALUIGI_CONFIG` via `${WALUIGI_SECRET_KEY}` placeholders
- Log buffering: worker sends logs in batches of 5 lines to reduce HTTP overhead
- Worker registers with Boss via heartbeat (configurable `--heartbeat` interval)

### Secrets

Secrets are namespace-scoped key-value stores applied via YAML:

```yaml
kind: Secret
metadata:
  namespace: analytics
  name: my-api-keys
spec:
  API_TOKEN: "secret-value"
  DB_PASSWORD: "another-secret"
```

In task config, reference secrets with `${WALUIGI_SECRET_API_TOKEN}`. The worker expands them before running the task. Use `wlctl describe secret <name> -n <ns>` to list keys (values are never shown).

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

`OK` = success, `WARN` = success with warnings, `KO` = error (HTTP 4xx/5xx).

## Boss REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/namespaces` | List namespaces |
| POST | `/namespaces` | Create namespace |
| GET | `/namespaces/{ns}/jobs` | List jobs |
| POST | `/namespaces/{ns}/jobs` | Submit job (Job kind, jobSpec or jobRef) |
| GET | `/namespaces/{ns}/jobs/{job_id}` | Get job |
| POST | `/namespaces/{ns}/jobs/{job_id}/_reset` | Reset job to PENDING |
| POST | `/namespaces/{ns}/jobs/{job_id}/_pause` | Pause job |
| POST | `/namespaces/{ns}/jobs/{job_id}/_resume` | Resume job |
| POST | `/namespaces/{ns}/jobs/{job_id}/_cancel` | Cancel job |
| DELETE | `/namespaces/{ns}/jobs/{job_id}` | Delete job (terminal state only) |
| GET | `/namespaces/{ns}/tasks` | List tasks (optional `?job_id=`) |
| GET | `/namespaces/{ns}/tasks/{task_id}` | Get task |
| PATCH | `/namespaces/{ns}/tasks/{task_id}` | Update task status (worker callback) |
| POST | `/namespaces/{ns}/tasks/{task_id}/logs` | Append task logs (worker callback) |
| GET | `/namespaces/{ns}/tasks/{task_id}/logs` | Get task logs |
| GET | `/namespaces/{ns}/resources` | List namespace resources |
| POST | `/namespaces/{ns}/resources` | Set namespace resources |
| GET | `/namespaces/{ns}/task-definitions` | List task definitions |
| POST | `/namespaces/{ns}/task-definitions` | Upsert task definition |
| DELETE | `/namespaces/{ns}/task-definitions/{id}` | Delete task definition |
| GET | `/namespaces/{ns}/job-definitions` | List job definitions |
| POST | `/namespaces/{ns}/job-definitions` | Upsert job definition |
| DELETE | `/namespaces/{ns}/job-definitions/{id}` | Delete job definition |
| GET | `/namespaces/{ns}/cron-jobs` | List cron jobs |
| POST | `/namespaces/{ns}/cron-jobs` | Upsert cron job |
| POST | `/namespaces/{ns}/cron-jobs/{id}/_enable` | Enable cron job |
| POST | `/namespaces/{ns}/cron-jobs/{id}/_disable` | Disable cron job |
| DELETE | `/namespaces/{ns}/cron-jobs/{id}` | Delete cron job |
| GET | `/namespaces/{ns}/secrets` | List secret names |
| GET | `/namespaces/{ns}/secrets/{name}` | List secret keys (no values) |
| POST | `/namespaces/{ns}/secrets/{name}` | Upsert secret |
| DELETE | `/namespaces/{ns}/secrets/{name}` | Delete secret |
| GET | `/workers` | List registered workers |
| POST | `/workers` | Register/heartbeat worker |

## Catalog REST API

All routes are namespace-scoped under `/namespaces/{namespace}/`.

**Sources:**
- `GET /namespaces/{ns}/sources` — list sources
- `POST /namespaces/{ns}/sources` — create/upsert source
- `GET /namespaces/{ns}/sources/{id}` — get source

**Datasets:**
- `GET /namespaces/{ns}/datasets` — list datasets (optional `?status=`)
- `POST /namespaces/{ns}/datasets` — create dataset
- `GET /namespaces/{ns}/datasets/{id:path}` — get dataset metadata
- `PATCH /namespaces/{ns}/datasets/{id:path}` — update description/status
- `DELETE /namespaces/{ns}/datasets/{id:path}` — **full cascade delete** (all versions, schema, DQ, lineage + physical files)
- `POST /namespaces/{ns}/datasets/{id:path}/_approve` — approve dataset
- `POST /namespaces/{ns}/datasets/{id:path}/_reserve` — Phase 1: reserve version slot
- `POST /namespaces/{ns}/datasets/{id:path}/_commit/{version}` — Phase 2: verify file, compute hash, infer schema
- `POST /namespaces/{ns}/datasets/{id:path}/materialize` — fetch REST API and store as CSV

**Versions:**
- `GET /namespaces/{ns}/datasets/{id:path}/versions` — list committed versions
- `DELETE /namespaces/{ns}/datasets/{id:path}/versions/{version}` — **hard delete** version (sub-records + physical file)

**Schema:**
- `GET /namespaces/{ns}/datasets/{id:path}/schema` — get schema
- `PATCH /namespaces/{ns}/datasets/{id:path}/schema/{column}` — update column
- `POST /namespaces/{ns}/datasets/{id:path}/schema/publish` — publish schema

**Metadata & Lineage:**
- `GET/POST /namespaces/{ns}/datasets/{id:path}/metadata/{version}` — get or set version metadata
- `GET /namespaces/{ns}/datasets/{id:path}/lineage/{version}` — upstream/downstream lineage

**Charts:**
- `GET/POST /namespaces/{ns}/datasets/{id:path}/charts` — list or create charts
- `PATCH/DELETE /namespaces/{ns}/datasets/{id:path}/charts/{chart_id}` — update or delete
- `GET /namespaces/{ns}/datasets/{id:path}/charts/{key}/render` — render as ECharts option

**Data Quality:**
- `GET/POST /namespaces/{ns}/datasets/{id:path}/expectations` — list or add DQ expectations
- `PATCH/DELETE /namespaces/{ns}/datasets/{id:path}/expectations/{exp_id}` — update or delete
- `GET /namespaces/{ns}/datasets/{id:path}/dq/{version}` — DQ results for a version
- `GET /dq/rules` — list available DQ rule catalog (global)
- `GET /dq/suites/{path}` — parse DQ suite YAML (global)

**Browse:**
- `GET /namespaces/{ns}/folders/{prefix:path}/` — list datasets and virtual sub-prefixes

### Dataset Lifecycle

Two-phase commit for writes:
1. **Reserve** (`_reserve`): creates version record, returns write location
2. **Commit** (`_commit/{version}`): verifies file exists, computes checksum, infers schema

Schema status: `inferred` → `draft` → `published`

Dataset status: `draft` → `in_review` → `approved` → `deprecated`

## SDK Usage (Task Scripts)

```python
from waluigi.sdk.context import context   # reads WALUIGI_PARAM_* / WALUIGI_ATTRIBUTE_* / WALUIGI_CONFIG
from waluigi.sdk.catalog import CatalogClient

date = context.params.date
job_id = context.job_id   # WALUIGI_JOB_ID

catalog = CatalogClient()   # reads WALUIGI_CATALOG_URL, WALUIGI_CATALOG_NAMESPACE

# Read a dataset
reader = catalog.read_dataset("raw/transactions")
df = reader.read()

# Write a dataset (two-phase commit)
handle = catalog.create_dataset("clean/transactions", format="parquet", source_id="local")
with handle.create_version(metadata={"date": date}, inputs=[reader]) as writer:
    writer.write(df)
# inputs accepts DatasetReader objects or dicts {"dataset_id": ..., "version": ...}
```

### Built-in Tasks (`waluigi/tasks/`)

These are Python modules invokable from TaskDefinition specs. They are **not auto-registered** — you must apply a `TaskDefinition` in each namespace before using them.

| Module | TaskDefinition name | Function |
|--------|---------------------|----------|
| `waluigi.tasks.ingest_rest` | `IngestRest` | Fetch REST API and store result |
| `waluigi.tasks.filter_dataset` | `FilterDataset` | `df.query(where_clause)` |
| `waluigi.tasks.select_columns` | `SelectColumns` | Column projection |
| `waluigi.tasks.add_derived_columns` | `AddDerivedColumns` | Compute new columns via `expr` (full pandas, `x`=DataFrame) or `mapping` (value→label dict) |
| `waluigi.tasks.transform_dataset` | `TransformDataset` | Inline Python block (`eval`) on `df`; `pd` and `context` pre-injected; `df` reassignable |
| `waluigi.tasks.last_per_group` | `LastPerGroup` | Sort by `order_by`, keep last row per `group_by`; all columns preserved |
| `waluigi.tasks.first_per_group` | `FirstPerGroup` | Sort by `order_by`, keep first row per `group_by`; all columns preserved |
| `waluigi.tasks.aggregate_dataset` | `AggregateDataset` | Group-by aggregation |
| `waluigi.tasks.join_datasets` | `JoinDatasets` | Join two datasets |
| `waluigi.tasks.merge_datasets` | `MergeDatasets` | Union/concat datasets |
| `waluigi.tasks.pivot_dataset` | `PivotDataset` | Pivot table |
| `waluigi.tasks.deduplicate_dataset` | `DeduplicateDataset` | Drop duplicate rows |
| `waluigi.tasks.accumulate_dataset` | `AccumulateDataset` | Append-only fact table, per-date idempotency (reads gold_prev) |
| `waluigi.tasks.accumulate_deduplicate_dataset` | `AccumulateDeduplicateDataset` | Fact table with cross-day dedup by state, keeps oldest date per unique state |
| `waluigi.tasks.upsert_dataset` | `UpsertDataset` | SCD Type 1 dimension, keep-last per business key (reads gold_prev) |
| `waluigi.tasks.catalog_create_source` | `CatalogCreateSource` | Create Catalog source via params |
| `waluigi.tasks.catalog_create_dataset` | `CatalogCreateDataset` | Create Catalog dataset via params |
| `waluigi.tasks.catalog_define_schema` | `CatalogDefineSchema` | Define/publish schema |
| `waluigi.tasks.catalog_set_expectations` | `CatalogSetExpectations` | Attach DQ expectations |
| `waluigi.tasks.catalog_set_charts` | `CatalogSetCharts` | Attach chart definitions (bar, line, pie, histogram, scatter, radar, combo) |
| `waluigi.tasks.reindex_time_series` | `ReindexTimeSeries` | Gap-fill time series; optional `group_by` for multi-series cross-product (day/week/month/year) |
| `waluigi.tasks.send_gmail` | `SendGmail` | Send email via Gmail SMTP + App Password (**vendor: google**) |
| `waluigi.tasks.ingest_google_sheet` | `IngestGoogleSheet` | Ingest public Google Sheet (all sheets or single, parametric coercion/filter) (**vendor: google**) |

All built-in tasks use `waluigi/tasks/_io.py` helpers (`read_input()` / `write_output()`) which handle source upsert and lineage automatically.

## Storage Connectors

Located in `waluigi/sdk/connectors/`. All extend `BaseConnector`:

| Connector | Source type | Notes |
|-----------|-------------|-------|
| `LocalConnector` | `local` | Supports CSV, Parquet, JSON, PKL, XLS, Feather, ORC |
| `S3Connector` | `s3` | URI format `s3://bucket/key`; supports MinIO via `endpoint_url` |
| `SQLConnector` | `sql`, `postgresql`, `mysql`, `sqlite` | Location = table or `schema.table`; uses SQLAlchemy |
| `SFTPConnector` | `sftp` | SSH key or password auth via paramiko |
| `SharePointConnector` | `sharepoint` | Microsoft Graph API app-only auth; CSV + Parquet; chunked upload for files >4 MB |

`ConnectorFactory.get(source_type, config)` returns the right connector, expanding `${VAR}` placeholders in config values against `os.environ` (so `${WALUIGI_SECRET_*}` works in any source config). Write operations return row count. Delete operations clean up physical files (used by dataset and version cascade delete).

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

Formulas are validated via AST whitelist before execution. `DQManager.run_suite(suite_path, datasets)` returns a `SuiteResult` with score and per-rule pass/fail details.

## Conventions & Key Details

**Task hashing:** `id + sorted(key:value)` pairs from params and attributes — used for idempotency. Same task ID + same params = same record; same ID + different params = distinct record.

**Parameter interpolation:** `${parent.params.KEY}` in child task spec is resolved at `DAGTask` construction time (`commons/dag.py`) against the parent's params namespace.

**Version format:** ISO 8601 timestamp with milliseconds (e.g., `2026-04-11T10:00:00.123+00:00`).

**Dataset ID paths:** Forward-slash hierarchical (e.g., `raw/transactions`). Local to the namespace — the full browse path used internally is `{namespace}/{id}`.

**Metadata keys:** Any string; `sys.*` prefix is reserved for system-managed keys.

**Virtual datasets:** Metadata-only records pointing to external resources (SQL queries) — no physical file.

**Lineage:** Version-specific. Tracks which dataset versions were consumed (upstream) and produced (downstream).

**Job locking:** Boss holds a 60-second lock per job while planning; prevents concurrent planners from duplicating work.

**No CI pipeline:** Tests are run manually with `pytest tests/`.

**No backward compatibility:** breaking changes are made directly. No migration scripts — `create_all` creates missing tables; existing tables are never altered.

## Tests

`tests/conftest.py` auto-starts a Catalog server for integration tests. Test files:
- `tests/worker/test_worker.py` — Worker unit tests (hash, dispatch endpoint, subprocess, slot management)
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
- `tests/dq/dq_test.py` — Data quality rule formula tests
- `tests/dq/cf_test.py` — Codice fiscale coherence rule test
- `tests/dq/describe_rule_test.py` — Rule description tests
- `tests/dq/rules_list_test.py` — Rule listing tests

## Docker / Deployment

`docker-compose.yml` runs 4 services: `boss` (1 replica), `worker` (3 replicas), `catalog` (1 replica), `console` (1 replica). Shared volumes: `/db` (SQLite), `/data` (datasets), `/work` (task working dirs).

Dockerfiles: `Dockerfile.boss`, `Dockerfile.worker`, `Dockerfile.catalog`, `Dockerfile.console`.

Swarm deployment: `deploy-to-swarm.sh` / `clean-from-swarm.sh`.

## Web Console

Two-tier setup:
1. **Static SPA** (`waluigi/console/static/`) — Vue-style components. Key views: Namespaces, Jobs, Tasks, Workers, Resources, DAG chart, Log modal, Task Definitions, Job Definitions, Cron Jobs, Secrets, Catalog browser, Dataset detail (schema, DQ, lineage, preview, charts).
2. **Console server** (`waluigi/console.py`) — FastAPI app that handles JWT auth (HS256 + PBKDF2 passwords), user management (admin-only CRUD), and acts as an authenticated reverse proxy to Boss (`/boss/*`) and Catalog (`/catalog/*`) APIs. Enforces namespace access per user token.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**Waluigi** is a lightweight distributed task orchestrator with a **server-push architecture**: the Boss daemon actively schedules and dispatches tasks to Workers via HTTP, rather than workers polling for work. It is designed for data pipeline DAG execution across distributed clusters.

## Commands

```bash
# Run servers
wlboss --port 8082 --db-path ./db/waluigi.db
wlworker --boss-url http://localhost:8082 --port 5001 --slots 2
wlcatalog --port 9000 --data-path ./data --db-path ./db/catalog.db

# CLI client
wlctl --url http://localhost:8082 apply -f descriptors/analytics.yaml
wlctl get jobs
wlctl get tasks
wlctl logs <task_id> --follow
wlctl reset task <task_id>
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

### DAG Execution Flow

1. User submits a Job YAML descriptor via `wlctl apply` or the REST API.
2. Boss claims the job (atomic via SQLite `UPDATE ... RETURNING`), then calls `core/engine.py` to recursively build the DAG bottom-up.
3. For each task: verify not already `SUCCESS`, wait for dependencies, allocate named resources.
4. Boss dispatches ready tasks via `HTTP POST /execute` to a Worker.
5. Worker forks a subprocess, injects `WALUIGI_PARAM_*` / `WALUIGI_ATTRIBUTE_*` env vars, streams logs back to Boss.
6. Worker reports final status (`SUCCESS` / `FAILED`); Boss releases resources and advances the DAG.

### Key Source Locations

- `waluigi/boss/__main__.py` — Boss FastAPI app, planner loop, all REST endpoints
- `waluigi/boss/db.py` — SQLite schema and all DB query functions
- `waluigi/core/engine.py` — Recursive DAG planner algorithm
- `waluigi/core/task.py` — `DynamicTask` model, param resolution (`${parent.params.KEY}`), task hashing
- `waluigi/worker.py` — Worker FastAPI app, subprocess execution, log streaming
- `waluigi/cli.py` — `wlctl` command implementations
- `waluigi/catalog/` — Catalog service (Pydantic models, dataset scanning, data quality rules)
- `waluigi/sdk/` — SDK for task scripts (`Task` base class reads env vars; `CatalogClient`)
- `waluigi/_deprecated/` — Old code, ignore

### Task State Machine

`PENDING` → `READY` → `RUNNING` → `SUCCESS` / `FAILED`

Tasks keyed by `id + params_hash` — the same task ID with different params produces distinct records. `SUCCESS` tasks are never re-run (idempotent resubmit).

### YAML Job Descriptor Format

```yaml
kind: Job
metadata:
  namespace: analytics
  workdir: /data/work
spec:
  id: root_task
  command: python source/analytics/run.py
  params:
    date: "2024-01-01"
  resources:
    coin: 1
  requires:
    - id: upstream_task
      command: python source/analytics/upstream.py
      params:
        date: ${parent.params.date}   # inherited from parent at planning time
```

Resources are named pools (e.g., `coin`, `gpu`) defined via `ClusterResources` descriptor and enforced cluster-wide before dispatch.

### SQLite Concurrency

Boss uses WAL mode. Multiple Boss replicas can run concurrently — atomic claiming prevents duplicate task execution. Do not assume single-writer semantics when modifying DB query logic.

### Worker Subprocess Contract

- Exit code 0 → `SUCCESS`; non-zero → `FAILED`
- Task scripts access inputs via `os.environ["WALUIGI_PARAM_KEY"]` or via `from waluigi.sdk.task import Task`

### Tests

`tests/conftest.py` auto-starts a Catalog server for integration tests. There is no CI pipeline — tests are run manually.

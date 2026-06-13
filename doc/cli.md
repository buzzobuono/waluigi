# CLI Reference (`wlctl`)

`wlctl` is the command-line client for the Waluigi Boss and Catalog. All commands go through the Console proxy if a console URL is configured, enabling namespace access enforcement via JWT.

## Global options

```bash
wlctl [--url <url>] [--namespace <ns>] [--output json] <command>
```

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | `http://localhost:8082` | Console or Boss URL |
| `--namespace` | from `WALUIGI_NAMESPACE` env | Default namespace for commands that require one |
| `--output` | table | Output format: `table` or `json` |

If using the Console, authenticate first:

```bash
wlctl login --url http://console:8080
```

---

## apply

Submit one or more YAML descriptors to the Boss.

```bash
wlctl apply -f <file.yaml>
wlctl apply -f descriptors/jobs/erp-daily.yaml
wlctl apply -f descriptors/full/full.yaml          # multi-document YAML
```

Supported kinds: `Namespace`, `NamespaceResources`, `Job`, `JobDefinition`, `TaskDefinition`, `CronJob`, `User`.

---

## get

Retrieve and display cluster state.

### Namespaces

```bash
wlctl get namespaces
```

### Jobs

```bash
wlctl get jobs [--namespace <ns>]
```

Columns: `JOB_ID`, `STATUS`, `POLICY`, `CONCURRENCY`, `CREATED`.

### Tasks

```bash
wlctl get tasks [--namespace <ns>] [--job-id <job_id>]
```

Columns: `TASK_ID`, `STATUS`, `PARAMS_HASH`, `JOB_ID`, `UPDATED`.

Filter by job:

```bash
wlctl get tasks --namespace analytics --job-id erp-daily@1718100000.0
```

### Workers

```bash
wlctl get workers
```

Columns: `URL`, `STATUS`, `MAX_SLOTS`, `FREE_SLOTS`, `AFFINITY`, `LAST_SEEN`.

### Resources

```bash
wlctl get resources [--namespace <ns>]
```

Columns: `RESOURCE`, `TOTAL`, `AVAILABLE`.

### Task Definitions

```bash
wlctl get task-definitions [--namespace <ns>]
```

### Job Definitions

```bash
wlctl get job-definitions [--namespace <ns>]
```

### CronJobs

```bash
wlctl get cronjobs [--namespace <ns>]
```

Columns: `ID`, `SCHEDULE`, `TIMEZONE`, `ENABLED`, `LAST_RUN`, `NEXT_RUN`.

### Sources (Catalog)

```bash
wlctl get sources [-n <ns>]
```

Columns: `ID`, `TYPE`, `DESCRIPTION`, `CREATED`.

### Datasets (Catalog)

```bash
wlctl get datasets [-n <ns>] [--status draft|in_review|approved|deprecated]
```

Columns: `ID`, `FORMAT`, `SOURCE`, `STATUS`, `DESCRIPTION`, `UPDATED`.

### Versions of a dataset (Catalog)

```bash
wlctl get versions -d <dataset-id> [-n <ns>]
```

Columns: `VERSION`, `STATUS`, `ROWS`, `BYTES`, `CREATED`.

### Schema of a dataset (Catalog)

```bash
wlctl get schema -d <dataset-id> [-n <ns>]
```

Columns: `COLUMN`, `TYPE`, `PII`, `STATUS`, `DESCRIPTION`.

---

## describe

Show detailed information about a definition or job.

### Job Definition

```bash
wlctl describe jobdefinition <name> [--namespace <ns>]
```

### Task Definition

```bash
wlctl describe taskdefinition <name> [--namespace <ns>]
```

### Job

```bash
wlctl describe job <job_id> [--namespace <ns>]
```

### Dataset (Catalog)

```bash
wlctl describe dataset <dataset-id> [-n <ns>]
```

Displays format, source, status, description, dates.

### Source (Catalog)

```bash
wlctl describe source <source-id> [-n <ns>]
```

Displays type, config, description, dates.

---

## preview

Show the first N rows of a Catalog dataset version.

```bash
wlctl preview <dataset-id> [-n <ns>] [-v <version>] [-l <rows>]
```

```bash
wlctl preview web/raw/raw_web -n analytics
wlctl preview web/raw/raw_web -n analytics --version 2026-06-13T10:00:00.000+00:00 --lines 50
```

If `--version` is omitted, the latest committed version is used.

---

## lineage

Show upstream (inputs) and downstream (consumers) for a dataset version.

```bash
wlctl lineage <dataset-id> [-n <ns>] [-v <version>]
```

```bash
wlctl lineage web/clean/clean_web -n analytics
```

---

## dq

Show data quality check results for a dataset version.

```bash
wlctl dq <dataset-id> [-n <ns>] [-v <version>]
```

```bash
wlctl dq web/clean/clean_web -n analytics
```

Displays overall score and per-rule PASS/FAIL with individual scores.

---

## logs

Stream or display task logs.

```bash
wlctl logs <task_id> [--namespace <ns>] [-n <lines>] [--follow]
```

| Option | Description |
|--------|-------------|
| `-n` | Show last N lines (default: 20) |
| `--follow` | Stream live logs (polls until task completes) |

```bash
wlctl logs extract_erp@1718100000.0 --follow
wlctl logs transform_erp@1718100000.0 -n 50
```

---

## reset

Reset a task, job, or entire namespace to `PENDING` status. Allows rerunning failed (or completed) work.

### Reset a task

```bash
wlctl reset task <task_id> [--namespace <ns>]
```

### Reset a job

Resets all tasks in the job to `PENDING`:

```bash
wlctl reset job <job_id> [--namespace <ns>]
```

### Reset a namespace

Resets all tasks in the namespace to `PENDING`:

```bash
wlctl reset namespace <namespace>
```

---

## delete

Delete a resource. Terminal state (`SUCCESS`, `FAILED`, `CANCELLED`) is required for jobs and namespaces.

### Delete a job

Deletes the job and all associated tasks and logs:

```bash
wlctl delete job <job_id> [--namespace <ns>]
```

Returns an error if the job is still active. Use `reset` + wait, or `cancel` first.

### Delete a cronjob

```bash
wlctl delete cronjob <id> [--namespace <ns>]
```

### Delete a TaskDefinition

```bash
wlctl delete taskdefinition <name> [--namespace <ns>]
```

### Delete a JobDefinition

```bash
wlctl delete jobdefinition <name> [--namespace <ns>]
```

### Delete a namespace

Deletes the namespace and all its jobs, tasks, logs, task dependencies, and resource definitions:

```bash
wlctl delete namespace <namespace>
```

---

## Job lifecycle commands

### Pause a running job

Prevents further task dispatches. Tasks already running are not interrupted.

```bash
wlctl pause job <job_id> [--namespace <ns>]
```

### Resume a paused job

```bash
wlctl resume job <job_id> [--namespace <ns>]
```

### Cancel a job

Marks the job and all non-terminal tasks as `CANCELLED`:

```bash
wlctl cancel job <job_id> [--namespace <ns>]
```

---

## CronJob lifecycle

```bash
wlctl enable cronjob <id> [--namespace <ns>]
wlctl disable cronjob <id> [--namespace <ns>]
```

---

## run

Run a task locally for development and testing — no Boss, no Worker, no cluster required.
The command injects the same environment variables that a Worker would inject, so the same
task scripts that run in production work unchanged on the developer's machine.

### Direct command

```bash
wlctl run "python pipeline/extract.py" \
    --params date=2026-06-12 source=ERP \
    --namespace analytics \
    --catalog-url http://localhost:9000
```

### From a YAML descriptor

Extract a task's command/script and config directly from a `Job` or `JobDefinition` file:

```bash
wlctl run --file descriptors/jobs/erp-daily.yaml --task extract \
    --params date=2026-06-12
```

Job-level `spec.params` defaults are merged in; `--params` values take precedence.
The task `config` block is read from the YAML and passed as `WALUIGI_CONFIG` automatically.

### Options

| Flag | Description |
|------|-------------|
| `cmd` | Shell command to run directly (positional, optional) |
| `-f` / `--file` | YAML descriptor (`Job` or `JobDefinition`) |
| `-t` / `--task` | Task ID to extract from `--file` |
| `-p` / `--params KEY=VALUE` | Override or supply task params (repeatable) |
| `-n` / `--namespace` | Catalog namespace (`WALUIGI_CATALOG_NAMESPACE`) |
| `--catalog-url` | Catalog URL (`WALUIGI_CATALOG_URL`) |

### Environment variables injected

| Variable | Value |
|----------|-------|
| `WALUIGI_TASK_ID` | `local-run` |
| `WALUIGI_JOB_ID` | `local-run` |
| `WALUIGI_PARAM_<KEY>` | Each merged param (uppercased) |
| `WALUIGI_CONFIG` | Task config JSON (from YAML descriptor) |
| `WALUIGI_CATALOG_NAMESPACE` | From `--namespace` or pre-existing env var |
| `WALUIGI_CATALOG_URL` | From `--catalog-url` or pre-existing env var |
| `PYTHONUNBUFFERED` | `1` |

### Inline script tasks

`wlctl run --file` also supports tasks with a `taskSpec.script` block (inline Python):

```yaml
- id: process
  taskSpec:
    script: |
      from waluigi.sdk.context import context
      print(f"date={context.params.date}")
```

```bash
wlctl run --file job.yaml --task process --params date=2026-06-12
```

---

## Output format

By default all commands print a human-readable table. Use `--output json` for machine-readable output:

```bash
wlctl get jobs --namespace analytics --output json
```

---

## Configuration

`wlctl` reads configuration from:

1. CLI flags (`--url`, `--namespace`)
2. Environment variables: `WALUIGI_CTL_URL`, `WALUIGI_NAMESPACE`
3. Saved session (after `wlctl login`)

---

## Common workflows

### Submit and monitor a job

```bash
wlctl apply -f descriptors/jobs/erp-daily.yaml
wlctl get jobs --namespace analytics
wlctl get tasks --namespace analytics --job-id erp-daily
wlctl logs extract_erp --follow
```

### Reset and rerun a failed pipeline

```bash
# See which tasks failed
wlctl get tasks --namespace analytics --job-id erp-daily

# Reset the specific failed task
wlctl reset task transform_erp --namespace analytics

# Or reset the entire job
wlctl reset job erp-daily --namespace analytics
```

### Inspect a worker

```bash
wlctl get workers
```

### Apply resources before first use

```bash
wlctl apply -f descriptors/namespaces/analytics.yaml
wlctl apply -f descriptors/resources/resources.yaml
wlctl get resources --namespace analytics
```

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

---

## describe

Show detailed information about a definition or job.

### Job Definition

```bash
wlctl describe job-definition <name> [--namespace <ns>]
```

Displays metadata and a task dependency tree.

### Task Definition

```bash
wlctl describe task-definition <name> [--namespace <ns>]
```

Displays metadata and script/command.

### Job

```bash
wlctl describe job <job_id> [--namespace <ns>]
```

Displays job metadata and a flat task list with dependency info.

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

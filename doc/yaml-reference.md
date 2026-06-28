# YAML Descriptor Reference

All resources are submitted via `wlctl apply -f <file>.yaml` or `POST` to the Boss API. A YAML file can contain a single document or multiple documents separated by `---`.

---

## Job

Defines a DAG execution. Tasks are either inline (`jobSpec`) or reference a stored `JobDefinition` (`jobRef`).

```yaml
kind: Job
metadata:
  name: <string>               # job name / ID (required)
  namespace: <string>          # target namespace (required)
spec:
  executionPolicy: <string>    # Ephemeral | Stateful  (default: Ephemeral)
  concurrencyPolicy: <string>  # Forbid | Replace | Allow  (default: Forbid)
  params: <dict>               # run-time parameters, inherited by all tasks
  attributes: <dict>           # custom metadata (string values), inherited by all tasks

  # One of:
  jobRef:
    name: <string>             # JobDefinition name to instantiate
  jobSpec:
    tasks: <list[Task]>        # inline task list (see Task schema below)
```

### ExecutionPolicy

| Value | Behaviour |
|-------|-----------|
| `Ephemeral` | Task IDs are suffixed with `@{timestamp}`. Each submission is a fresh instance. A job can be resubmitted any number of times without blocking. |
| `Stateful` | Task IDs are canonical (no suffix). `job_id = metadata.name`. State is tracked across submissions. `SUCCESS` tasks are never re-run. |

### ConcurrencyPolicy (Stateful only)

| Value | Behaviour |
|-------|-----------|
| `Forbid` | Reject new submission if a job with the same name is PENDING, READY, or RUNNING. |
| `Replace` | Cancel the active job and start a new one. |
| `Allow` | Submit regardless; multiple instances may run concurrently. |

### Task schema

```yaml
- id: <string>                 # unique task ID within the job (required)
  requires: <list[string]>     # IDs of tasks this depends on (default: [])
  resources: <dict>            # resource consumption, e.g. {coin: 1, gpu: 1}
  params: <dict>               # task-level params (override/extend job params)
  attributes: <dict>           # task-level attributes (override/extend job attributes)
  config: <dict>               # arbitrary config passed as WALUIGI_CONFIG JSON

  # Exactly one of:
  taskSpec:
    command: <string>          # shell command to execute
    script: <string>           # inline Python script (mutually exclusive with command)
    affinity: <list[string]>   # worker capability tags required (inline tasks only)

  taskRef:
    name: <string>             # TaskDefinition name; affinity comes from the TaskDefinition
```

> **Affinity placement:** for `taskSpec` tasks, declare `affinity` inside `taskSpec`. For `taskRef` tasks, affinity is defined in the `TaskDefinition` in the database and cannot be set in the job YAML.

### Parameter inheritance

Job-level `params` and `attributes` flow down to all tasks. Task-level `params`/`attributes` add or override the inherited values.

### Full example

```yaml
kind: Job
metadata:
  name: erp-daily
  namespace: analytics
spec:
  executionPolicy: Stateful
  concurrencyPolicy: Forbid
  params:
    date: "2026-06-12"
  attributes:
    owner: data-team
  jobSpec:
    tasks:
      - id: extract
        taskSpec:
          command: "python pipeline/extract.py"
          affinity:
            - python
        params:
          source: ERP
        resources:
          coin: 1

      - id: transform
        taskSpec:
          command: "python pipeline/transform.py"
          affinity:
            - python
        requires:
          - extract
        resources:
          coin: 2

      - id: load
        taskSpec:
          command: "python pipeline/load.py"
          affinity:
            - python
        requires:
          - transform
        resources:
          coin: 1
```

---

## JobDefinition

Reusable pipeline template. Instantiated by a `Job` via `jobRef`.

```yaml
kind: JobDefinition
metadata:
  name: <string>               # definition name (required, unique per namespace)
  namespace: <string>          # target namespace (required)
spec:
  tasks: <list[Task]>          # same schema as Job.spec.jobSpec.tasks
```

A `JobDefinition` holds the pipeline structure; a `Job` supplies the runtime `params` and `attributes`. This enables running the same pipeline with different dates, configurations, or environments.

### Example

```yaml
kind: JobDefinition
metadata:
  name: erp-analytics-pipeline
  namespace: analytics
spec:
  tasks:
    - id: filter_high_value
      taskRef:
        name: FilterDataset
      config:
        input:
          dataset: analytics/erp/clean/erp
        output:
          dataset: analytics/erp/filtered/high_value
          source_id: analytics-local
          format: parquet
        where: "value > 1000"
      resources:
        coin: 1

    - id: add_derived
      taskRef:
        name: AddDerivedColumns
      requires:
        - filter_high_value
      config:
        input:
          dataset: analytics/erp/filtered/high_value
        output:
          dataset: analytics/erp/enriched/derived
          source_id: analytics-local
          format: parquet
        columns:
          - name: value_k
            expr: "value / 1000"
      resources:
        coin: 1
```

---

## TaskDefinition

Reusable script or command. Referenced by tasks via `taskRef.name`.

```yaml
kind: TaskDefinition
metadata:
  name: <string>               # definition name (required, unique per namespace)
  namespace: <string>          # target namespace (required)
spec:
  command: <string>            # shell command (optional)
  script: <string>             # inline Python script (optional; mutually exclusive)
  affinity: <list[string]>     # worker capability tags required (optional)
```

> **Resources in TaskDefinition:** resource consumption is a job-level concern and is never set in a `TaskDefinition`. Declare `resources` on the task in the `Job` or `JobDefinition` YAML.

### Example — custom script

```yaml
kind: TaskDefinition
metadata:
  name: send-report
  namespace: analytics
spec:
  script: |
    from waluigi.sdk.context import context
    recipient = context.params.recipient
    print(f"Sending report to {recipient}")
  affinity:
    - python
```

### Example — built-in task type

Built-in task types (FilterDataset, AggregateDataset, etc.) are shipped as Python modules. They are **not** automatically available — you must apply the corresponding `TaskDefinition` to each namespace where you want to use them:

```bash
wlctl apply-builtins -n <namespace>
```

Built-in task definitions are bundled in the package at `waluigi/tasks/data/builtin-task-definitions.yaml`. Once applied, tasks can reference them via `taskRef.name`.

### Example — referencing a built-in

```yaml
kind: Job
metadata:
  name: etl
  namespace: analytics
spec:
  jobSpec:
    tasks:
      - id: filter_orders
        taskRef:
          name: FilterDataset     # must be applied as TaskDefinition in this namespace
        config:
          input: {dataset: sales/raw/orders}
          output: {dataset: sales/clean/orders, source_id: analytics-local, format: parquet}
          where: "status == 'completed'"
        resources:
          coin: 1
```

---

## Namespace

Creates an execution and data isolation boundary. All jobs, tasks, definitions, and resources belong to a namespace.

```yaml
kind: Namespace
metadata:
  name: <string>               # namespace ID (required)
  description: <string>        # human-readable description (optional)
```

### Example

```yaml
kind: Namespace
metadata:
  name: analytics
  description: "Analytics data pipeline namespace"
```

---

## NamespaceResources

Defines named resource pools for a namespace. The Boss enforces these limits before dispatching any task.

```yaml
kind: NamespaceResources
metadata:
  namespace: <string>          # target namespace (required)
spec:
  <resource_name>: <float>     # e.g. coin, gpu, memory, api_slots
```

Resources are arbitrary named counters. Common conventions:

| Name | Meaning |
|------|---------|
| `coin` | Generic execution slot (model CPU shares) |
| `gpu` | GPU unit |
| `pdc` | PDC-specific slot |

Tasks declare consumption with matching names under `resources:`. The Boss acquires the declared amount before dispatch and releases it on task completion or failure.

### Example

```yaml
kind: NamespaceResources
metadata:
  namespace: analytics
spec:
  coin: 10.0
  gpu: 2.0
```

---

## CronJob

Schedules recurring job submissions. The Boss runs an internal scheduler that evaluates cron expressions and submits the referenced JobDefinition at the configured times.

```yaml
kind: CronJob
metadata:
  name: <string>               # cronjob ID (required, unique per namespace)
  namespace: <string>          # target namespace (required)
spec:
  schedule: <string>           # cron expression (required)
                               # format: "minute hour dom month dow"
                               # e.g. "0 6 * * *" = daily at 06:00
  timezone: <string>           # IANA timezone (default: UTC)
                               # e.g. "Europe/Rome", "America/New_York"
  enabled: <bool>              # enable/disable without deleting (default: true)
  executionPolicy: <string>    # Ephemeral | Stateful (default: Ephemeral)
  concurrencyPolicy: <string>  # Forbid | Replace | Allow (default: Forbid)
  params: <dict>               # runtime params; values support strftime codes
  attributes: <dict>           # runtime attributes; values support strftime codes
  jobRef:
    name: <string>             # JobDefinition to submit
```

### Dynamic params with strftime

Parameter values containing `%` codes are interpolated with the current datetime at trigger time:

| Code | Example output |
|------|---------------|
| `%Y-%m-%d` | `2026-06-12` |
| `%Y-%m-%dT%H:%M:%S` | `2026-06-12T06:00:00` |
| `%H:%M` | `06:00` |
| `%Y` | `2026` |

### Example

```yaml
kind: CronJob
metadata:
  name: daily-etl
  namespace: analytics
spec:
  schedule: "0 6 * * *"
  timezone: Europe/Rome
  enabled: true
  executionPolicy: Ephemeral
  concurrencyPolicy: Forbid
  params:
    date: "%Y-%m-%d"
    run_ts: "%Y-%m-%dT%H:%M:%S"
  jobRef:
    name: erp-analytics-pipeline
```

---

## User

Manages Console users. Applied via `wlctl apply` (admin only) or the Console UI.

```yaml
kind: User
metadata:
  name: <string>               # username (required)
  displayName: <string>        # full name (optional)
spec:
  namespaces: <list[string]>   # accessible namespaces; use ["*"] for admin
  password: <string>           # plain text (hashed before storage; optional if prompted)
```

### Example

```yaml
kind: User
metadata:
  name: alice
  displayName: Alice Rossi
spec:
  namespaces:
    - analytics
    - simulation
```

---

## YAML anchors and reuse

YAML anchors (`&`) and aliases (`*`) reduce repetition in complex descriptors. Waluigi encourages their use for source definitions and shared configs:

```yaml
x-sources:
  local: &local
    id: analytics-local
    type: LOCAL
    description: Local storage for analytics

x-config:
  base: &base_cfg
    catalog_source: analytics-local
    format: parquet

kind: Job
metadata:
  name: my-job
  namespace: analytics
spec:
  jobSpec:
    tasks:
      - id: task_a
        taskRef:
          name: FilterDataset
        config:
          input:
            dataset: analytics/raw/orders
          output:
            dataset: analytics/clean/orders
            source_id: analytics-local
          where: "status == 'completed'"
        resources:
          coin: 1

      - id: task_b
        taskSpec:
          script: |
            from waluigi.sdk.context import context
            print(context.config.catalog_source)
        requires:
          - task_a
        config:
          <<: *base_cfg              # merge anchor
          extra_field: value
        resources:
          coin: 1
```

---

## Multi-document YAML

A single file can contain multiple resource descriptors separated by `---`. Resources are applied in order:

```yaml
kind: Namespace
metadata:
  name: test

---

kind: NamespaceResources
metadata:
  namespace: test
spec:
  coin: 2.0

---

kind: TaskDefinition
metadata:
  name: hello
  namespace: test
spec:
  script: |
    print("hello")

---

kind: Job
metadata:
  name: hello-job
  namespace: test
spec:
  jobSpec:
    tasks:
      - id: hello
        taskRef:
          name: hello
        resources:
          coin: 1
```

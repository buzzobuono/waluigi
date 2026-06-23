# Task Development

A task is the smallest unit of work in Waluigi. It is a subprocess forked by a Worker: any executable that reads environment variables and exits with code 0 (success) or non-zero (failure).

---

## Three ways to define a task

### 1. Shell command (`taskSpec.command`)

Any shell command. Parameters are available as environment variables.

```yaml
- id: extract
  taskSpec:
    command: "python pipeline/extract.py"
    affinity:
      - python
  params:
    date: "2026-06-12"
    source: ERP
  resources:
    coin: 1
```

The worker runs the command with `asyncio.create_subprocess_shell`, capturing stdout+stderr and streaming logs back to the Boss.

### 2. Inline Python script (`taskSpec.script`)

A Python script embedded directly in the YAML. Useful for short tasks or for keeping pipeline definition and logic in a single file.

```yaml
- id: process
  taskSpec:
    script: |
      from waluigi.sdk.context import context
      import pandas as pd

      date = context.params.date
      df = pd.read_parquet(f"/data/raw/{date}.parquet")
      df_clean = df.dropna()
      df_clean.to_parquet(f"/data/clean/{date}.parquet")
      print(f"Processed {len(df_clean)} rows")
  params:
    date: "2026-06-12"
  resources:
    coin: 1
```

The worker executes this as:

```bash
python -c "import os; exec(os.environ['WALUIGI_SCRIPT'])"
```

The script content is injected via the `WALUIGI_SCRIPT` environment variable.

Use YAML anchors to keep large scripts readable:

```yaml
x-scripts:
  process: &process_script |
    from waluigi.sdk.context import context
    # ... script body ...

kind: Job
metadata:
  name: my-job
  namespace: analytics
spec:
  jobSpec:
    tasks:
      - id: process
        taskSpec:
          script: *process_script
        resources:
          coin: 1
```

### 3. Built-in task type (`taskRef.name`)

Reference one of Waluigi's built-in reusable task types. No code to write — just provide a `config` block.

```yaml
- id: filter_orders
  taskRef:
    name: FilterDataset
  config:
    input:
      dataset: sales/raw/orders
    output:
      dataset: sales/clean/orders
      source_id: local
      format: parquet
    where: "status == 'completed'"
  resources:
    coin: 1
```

→ See [built-in-tasks.md](built-in-tasks.md) for all available types.

---

## Environment variables injected by the Worker

The Worker injects these environment variables before forking the subprocess:

| Variable | Source | Example |
|----------|--------|---------|
| `WALUIGI_PARAM_<KEY>` | task `params` (uppercase) | `WALUIGI_PARAM_DATE=2026-06-12` |
| `WALUIGI_ATTRIBUTE_<KEY>` | task `attributes` (uppercase) | `WALUIGI_ATTRIBUTE_OWNER=data-team` |
| `WALUIGI_TASK_ID` | task ID | `extract@1718000000.123` |
| `WALUIGI_JOB_ID` | job ID | `erp-daily@1718000000.123` |
| `WALUIGI_CONFIG` | task `config` dict (JSON string) | `{"input": {...}, "where": "..."}` |
| `WALUIGI_SCRIPT` | inline script content | (set only for `taskSpec.script`) |
| `WALUIGI_CATALOG_URL` | Catalog URL | `http://catalog:9000` |
| `WALUIGI_CATALOG_NAMESPACE` | current namespace | `analytics` |
| `PYTHONUNBUFFERED` | always `1` | (enables real-time log streaming) |

Task params are uppercased: `date` → `WALUIGI_PARAM_DATE`, `source_id` → `WALUIGI_PARAM_SOURCE_ID`.

---

## SDK context object

The SDK `context` singleton provides a clean interface to the injected environment:

```python
from waluigi.sdk.context import context

# Parameters (from task params)
date        = context.params.date         # WALUIGI_PARAM_DATE
source      = context.params.source       # WALUIGI_PARAM_SOURCE

# Attributes (from task attributes)
owner       = context.attributes.owner    # WALUIGI_ATTRIBUTE_OWNER

# Config (from task config dict)
cfg         = context.config              # SimpleNamespace from WALUIGI_CONFIG JSON
limit       = context.config.limit        # context.config.<field>

# System IDs
task_id     = context.task_id             # WALUIGI_TASK_ID
job_id      = context.job_id             # WALUIGI_JOB_ID
namespace   = context.namespace           # WALUIGI_CATALOG_NAMESPACE
```

`context.params`, `context.attributes`, and `context.config` are all `SimpleNamespace` objects. Accessing an undefined attribute raises `AttributeError` — use `getattr(context.params, 'key', default)` for optional params.

```python
rows_per_source = int(getattr(context.params, 'rows_per_source', 10))
```

---

## Working with the Catalog

Use `CatalogClient` to read and write datasets. The namespace is automatically set from `WALUIGI_CATALOG_NAMESPACE`.

```python
from waluigi.sdk.catalog import CatalogClient
from waluigi.sdk.context import context

catalog = CatalogClient()   # reads WALUIGI_CATALOG_URL + WALUIGI_CATALOG_NAMESPACE

# ── READ ──────────────────────────────────────────────────────
reader = catalog.read_dataset("sales/raw/orders")
df = reader.read()

# ── TRANSFORM ────────────────────────────────────────────────
df_clean = df[df["status"] == "completed"].copy()

# ── WRITE (two-phase commit) ─────────────────────────────────
handle = catalog.create_dataset(
    "sales/clean/orders",
    format="parquet",
    source_id="local",
    description="Completed orders only",
)

with handle.create_version(
    metadata={"date": context.params.date},
    inputs=[reader],                # records lineage automatically
) as writer:
    writer.write(df_clean)

if writer.skipped:
    print(f"Skipped — same metadata, existing version: {writer.version}")
else:
    print(f"Written: {writer.dataset_id} @ {writer.version} ({len(df_clean)} rows)")
```

→ Full SDK reference: [sdk.md](sdk.md)

---

## Language-agnostic tasks

Any executable works — bash scripts, R, Java, compiled binaries. Params arrive as environment variables:

```bash
#!/bin/bash
echo "Processing date: $WALUIGI_PARAM_DATE"
echo "Source: $WALUIGI_PARAM_SOURCE"
Rscript analysis.R "$WALUIGI_PARAM_DATE"
```

```yaml
- id: r_analysis
  taskSpec:
    command: "bash run_r.sh"
  params:
    date: "2026-06-12"
    source: ERP
  resources:
    coin: 2
```

---

## Task config (`config:`)

The `config` block passes arbitrary structured data to a task without polluting `params`. It is available as `context.config` (a `SimpleNamespace`) or as raw JSON via `WALUIGI_CONFIG`.

```yaml
- id: extract
  taskSpec:
    command: "python extract.py"
  config:
    catalog_source: analytics-local
    expectations:
      - rule_id: expect_column_values_to_not_be_null
        inputs: {x: "this.value"}
  resources:
    coin: 1
```

```python
cfg = context.config
source = cfg.catalog_source                  # "analytics-local"
expectations = cfg.expectations              # list of dicts
```

For built-in tasks (`taskRef`), the entire `config` block is the task's input specification (see [built-in-tasks.md](built-in-tasks.md)).

---

## Task dependencies

```yaml
tasks:
  - id: extract               # no requires → runs first
    taskSpec:
      command: python extract.py
    resources: {coin: 1}

  - id: clean
    taskSpec:
      command: python clean.py
    requires:
      - extract               # waits for extract to complete
    resources: {coin: 1}

  - id: report
    taskSpec:
      command: python report.py
    requires:
      - clean                 # waits for clean
    resources: {coin: 1}
```

Fan-out (multiple dependents):

```yaml
  - id: extract
  - id: transform_a
    requires: [extract]
  - id: transform_b
    requires: [extract]
  - id: load
    requires: [transform_a, transform_b]   # waits for both
```

The Boss handles shared dependencies correctly (diamond patterns) via a memo cache — a shared upstream task is evaluated exactly once per planning cycle.

---

## Resource declarations

Tasks declare the resources they consume. The Boss acquires these before dispatch and releases them on completion or failure. If the declared resources exceed what is available, the task waits.

```yaml
resources:
  coin: 1        # 1 generic slot
  gpu: 1         # 1 GPU unit
```

Resources must be defined in `NamespaceResources` before they can be consumed. See [yaml-reference.md](yaml-reference.md#namespaceresources).

---

## Affinity

Declare which worker capabilities a task requires. For `taskSpec` tasks, put `affinity` inside `taskSpec`. For `taskRef` tasks, affinity is defined in the `TaskDefinition`.

```yaml
- id: train
  taskSpec:
    command: python train.py
    affinity:
      - python
      - gpu
  resources:
    gpu: 1
```

The Boss dispatches the task only to workers that declare at least these tags. If no matching worker is available, the task waits (the planner retries on the next tick).

```bash
# Worker registration
wlworker --affinity python,pandas,gpu --slots 4
```

> **Note:** `affinity` at the outer task level (outside `taskSpec`) is ignored. Always declare it inside `taskSpec` or in the `TaskDefinition`.

---

## Best practices

**Keep tasks small and focused.** A task should do one thing. Chain tasks via `requires` rather than building monolithic scripts.

**Use `context.params` for runtime values, `config` for structural config.** Params change per run (date, environment). Config is structural and stable (column lists, connector references).

**Write idempotent tasks.** The Boss may retry (after `wlctl reset`) or reuse the same task across Stateful job runs. Writing to a well-defined output path/version makes reruns safe.

**Exit with code 0 only on full success.** If your script encounters a data quality failure or missing upstream, raise an exception or `sys.exit(1)`. The Boss will mark the task FAILED and halt the downstream DAG.

**Use the two-phase Catalog commit.** The `with handle.create_version(...) as writer:` context manager handles reserve, write, commit, and rollback on error atomically.

**Log meaningfully.** Everything printed to stdout or stderr is captured and sent to the Boss log store. `print(f"Loaded {len(df)} rows")` is visible in `wlctl logs`.

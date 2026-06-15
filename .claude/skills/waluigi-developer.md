# Waluigi Developer

You are working on **Waluigi**, a lightweight distributed task orchestrator with a server-push architecture. This skill gives you operational mastery: you can design pipelines, test them locally, deploy them to a cluster, and implement the full medallion architecture (Bronze → Silver → Gold) with data quality checks.

---

## Cluster components

| Process | Port | Purpose |
|---------|------|---------|
| `wlboss` | 8082 | Control plane — DAG planner, SQLite state |
| `wlworker` | 5001+ | Execution plane — runs subprocesses |
| `wlcatalog` | 9000 | Dataset metadata, schema, lineage, DQ |
| `wlconsole` | 8080 | JWT auth proxy, web UI |
| `wlctl` | CLI | Your main tool — always points to the console |

All `wlctl` commands go through the Console. Authenticate first:
```bash
wlctl --url http://localhost:8080 login -u admin
```

---

## 1. One-time namespace setup

Every pipeline lives in a namespace. Do this once per namespace:

```bash
# 1. Create namespace
wlctl apply -f - <<'EOF'
kind: Namespace
metadata:
  name: analytics
  description: "Analytics pipelines"
EOF

# 2. Define resource pools (adjust totals to your cluster)
wlctl apply -f - <<'EOF'
kind: NamespaceResources
metadata:
  namespace: analytics
spec:
  coin: 10.0
EOF

# 3. Apply all built-in task definitions
wlctl apply -f descriptors/task-definitions/builtin-task-definitions.yaml -n analytics

# 4. Verify
wlctl get namespaces
wlctl get task-definitions -n analytics
```

Workers must register with `--affinity python` to pick up built-in tasks:
```bash
wlworker --boss-url http://localhost:8082 --port 5001 --slots 4 --affinity python
```

---

## 2. Pipeline development workflow: local → cluster

This is the recommended cycle. **Always test locally before applying to the cluster.**

### Step 1 — Design the YAML

Write a `Job` or `JobDefinition` YAML. Start simple: one or two tasks. Use `taskSpec.script` for portability (no shared filesystem mount required — scripts are injected via env var).

```yaml
# my-pipeline.yaml
kind: Job
metadata:
  name: orders-pipeline
  namespace: analytics
spec:
  executionPolicy: Ephemeral
  params:
    date: "2026-06-15"
  jobSpec:
    tasks:
      - id: ingest
        taskSpec:
          script: |
            from waluigi.sdk.context import context
            import pandas as pd
            date = context.params.date
            df = pd.DataFrame({"order_id": [1, 2, 3], "date": [date]*3, "amount": [100, 200, 150]})
            df.to_parquet(f"/tmp/orders_{date}.parquet", index=False)
            print(f"Ingested {len(df)} rows for {date}")
          affinity:
            - python
        resources:
          coin: 1

      - id: clean
        taskSpec:
          script: |
            from waluigi.sdk.context import context
            import pandas as pd
            date = context.params.date
            df = pd.read_parquet(f"/tmp/orders_{date}.parquet")
            df = df[df["amount"] > 0]
            print(f"Clean rows: {len(df)}")
          affinity:
            - python
        requires:
          - ingest
        resources:
          coin: 1
```

### Step 2 — Run a single task locally

No Boss, no Worker, no cluster needed:

```bash
wlctl run -f my-pipeline.yaml -t ingest --params date=2026-06-15
```

You see exactly what the Worker would see: stdout, stderr, exit code. Fix until it passes.

### Step 3 — Run the full pipeline locally

```bash
wlctl run -f my-pipeline.yaml --params date=2026-06-15
```

Tasks execute sequentially in dependency order. Stops immediately on first failure.

```
[wlctl run job] tasks: 2  ingest → clean
── Task 1/2: ingest ──────────────────────
Ingested 3 rows for 2026-06-15
✓ ingest  (0.4s)
── Task 2/2: clean ───────────────────────
Clean rows: 3
✓ clean  (0.2s)
[wlctl run job] completed — 2/2 tasks OK
```

### Step 4 — Apply to the cluster

```bash
wlctl apply -f my-pipeline.yaml
```

### Step 5 — Monitor execution

```bash
# Job status
wlctl get jobs -n analytics

# Task-level detail (add --job-id for a specific job)
wlctl get tasks -n analytics --job-id orders-pipeline@1718100000.0

# Live logs
wlctl logs ingest@1718100000.0 --follow

# If a task fails, inspect logs then reset
wlctl reset task ingest@1718100000.0 -n analytics

# Reset the entire job
wlctl reset job orders-pipeline@1718100000.0 -n analytics
```

### Step 6 — Iterate

- Tweak the script → re-run locally with `wlctl run`
- When stable → re-apply with `wlctl apply`
- For `Stateful` jobs: reset the affected task, the Boss replans automatically

---

## 3. YAML patterns reference

### Affinity — CRITICAL rule

```yaml
# CORRECT: affinity goes INSIDE taskSpec
- id: my_task
  taskSpec:
    command: python script.py
    affinity:
      - python
      - gpu

# WRONG: outer-level affinity is silently ignored
- id: my_task
  taskSpec:
    command: python script.py
  affinity:        # ← this has NO effect
    - python
```

For `taskRef` tasks, affinity is defined in the `TaskDefinition` in the DB — you cannot set it in the job YAML.

### Inline script task (preferred — no mount required)

```yaml
- id: transform
  taskSpec:
    script: |
      from waluigi.sdk.context import context
      import pandas as pd

      date  = context.params.date
      limit = int(getattr(context.params, 'limit', 1000))

      df = pd.read_parquet(f"/tmp/raw_{date}.parquet")
      df = df.head(limit)
      df.to_parquet(f"/tmp/clean_{date}.parquet", index=False)
      print(f"Done: {len(df)} rows")
    affinity:
      - python
  params:
    limit: "500"
  resources:
    coin: 1
```

Scripts access params via `context.params.<key>` (always lowercase). Optional params: `getattr(context.params, 'key', default)`.

### Shell command task

```yaml
- id: run_r
  taskSpec:
    command: "Rscript /app/analysis.R"
    affinity:
      - r
  resources:
    coin: 2
```

### taskRef task (built-in or custom TaskDefinition)

```yaml
- id: filter_orders
  taskRef:
    name: FilterDataset        # must be applied as TaskDefinition in this namespace
  config:
    input:
      dataset: analytics/bronze/orders
      source: &local {id: local, type: local}
    output:
      dataset: analytics/silver/orders
      format: parquet
      source: *local
    where: "amount > 0 and status == 'completed'"
  resources:
    coin: 1
```

### JobDefinition (reusable template)

```yaml
kind: JobDefinition
metadata:
  name: orders-etl
  namespace: analytics
spec:
  tasks:
    - id: ingest
      taskSpec:
        script: |
          from waluigi.sdk.context import context
          import pandas as pd
          date = context.params.date
          print(f"Ingesting date={date}")
        affinity:
          - python
      resources:
        coin: 1

    - id: clean
      taskSpec:
        script: |
          from waluigi.sdk.context import context
          print(f"Cleaning date={context.params.date}")
        affinity:
          - python
      requires:
        - ingest
      resources:
        coin: 1
```

Run it via a Job with `jobRef`:
```yaml
kind: Job
metadata:
  name: orders-etl-run
  namespace: analytics
spec:
  executionPolicy: Stateful
  concurrencyPolicy: Forbid
  params:
    date: "2026-06-15"
  jobRef:
    name: orders-etl
```

### CronJob (scheduled)

```yaml
kind: CronJob
metadata:
  name: daily-orders-etl
  namespace: analytics
spec:
  schedule: "0 6 * * *"        # every day at 06:00
  timezone: Europe/Rome
  enabled: true
  executionPolicy: Ephemeral
  concurrencyPolicy: Forbid
  params:
    date: "%Y-%m-%d"           # strftime: interpolated at trigger time
    run_ts: "%Y-%m-%dT%H:%M:%S"
  jobRef:
    name: orders-etl
```

Enable / disable without deleting:
```bash
wlctl enable cronjob daily-orders-etl -n analytics
wlctl disable cronjob daily-orders-etl -n analytics
```

### Custom TaskDefinition

```yaml
kind: TaskDefinition
metadata:
  name: send-report
  namespace: analytics
spec:
  script: |
    from waluigi.sdk.context import context
    recipient = context.params.recipient
    print(f"Sending report for {context.params.date} to {recipient}")
  affinity:
    - python
  # NOTE: never put resources here — declare them in the Job task
```

Then reference it:
```yaml
- id: notify
  taskRef:
    name: send-report
  params:
    recipient: "team@company.com"
  resources:
    coin: 1
```

### Parameter inheritance

Job-level `params` flow to all tasks. Task-level `params` add or override:
```yaml
spec:
  params:
    date: "2026-06-15"        # inherited by all tasks
    env: production
  jobSpec:
    tasks:
      - id: extract
        params:
          source: ERP          # merged: date + env + source available
```

### Secrets injection

```yaml
kind: Secret
metadata:
  name: api-keys
  namespace: analytics
spec:
  API_TOKEN: "secret-value"
  DB_PASSWORD: "another-secret"
```

In task config use `${WALUIGI_SECRET_API_TOKEN}`. In scripts use `os.environ["WALUIGI_SECRET_API_TOKEN"]`.

---

## 4. Built-in task types

Apply to namespace first:
```bash
wlctl apply -f descriptors/task-definitions/builtin-task-definitions.yaml -n analytics
```

All built-in tasks automatically use `CatalogClient` for I/O and record lineage. They all require `affinity: [python]` (already set in the TaskDefinition).

### Common source pattern (YAML anchor)

```yaml
x-sources:
  local: &local
    id: local
    type: local
```

Reuse `*local` in every `input.source` and `output.source`.

---

### FilterDataset

Keeps rows matching a pandas `.query()` expression.

```yaml
- id: filter_completed
  taskRef:
    name: FilterDataset
  config:
    input:
      dataset: analytics/bronze/orders   # read from
      source: *local
    output:
      dataset: analytics/silver/orders   # write to
      format: parquet
      source: *local
    where: "status == 'completed' and amount > 0"
  resources:
    coin: 1
```

---

### SelectColumns

Project a subset of columns.

```yaml
- id: project_fields
  taskRef:
    name: SelectColumns
  config:
    input:
      dataset: analytics/bronze/orders
      source: *local
    output:
      dataset: analytics/silver/orders_slim
      format: parquet
      source: *local
    columns:
      - order_id
      - date
      - customer_id
      - amount
  resources:
    coin: 1
```

---

### AddDerivedColumns

Compute new columns with pandas eval. Later columns can reference earlier ones.

```yaml
- id: enrich
  taskRef:
    name: AddDerivedColumns
  config:
    input:
      dataset: analytics/silver/orders
      source: *local
    output:
      dataset: analytics/silver/orders_enriched
      format: parquet
      source: *local
    columns:
      - name: revenue
        expr: "quantity * unit_price * (1 - discount)"
      - name: margin_pct
        expr: "gross_profit / revenue * 100"
  resources:
    coin: 1
```

---

### AggregateDataset

Group-by aggregation. Functions: `sum`, `mean`, `count`, `min`, `max`, `std`, `first`, `last`.

```yaml
- id: agg_by_region
  taskRef:
    name: AggregateDataset
  config:
    input:
      dataset: analytics/silver/orders_enriched
      source: *local
    output:
      dataset: analytics/gold/report_by_region
      format: parquet
      source: *local
    group_by:
      - region
      - category
    agg:
      revenue: sum
      quantity: sum
      order_id: count
  resources:
    coin: 1
```

---

### JoinDatasets

Horizontal join (`pd.merge`). How: `inner` | `left` | `right` | `outer`.

```yaml
- id: join_products
  taskRef:
    name: JoinDatasets
  config:
    left:
      dataset: analytics/silver/orders
      source: *local
    right:
      dataset: analytics/silver/products
      source: *local
    join:
      columns: product_id
      how: left
      suffixes: ["_order", "_product"]
    output:
      dataset: analytics/silver/orders_with_products
      format: parquet
      source: *local
  resources:
    coin: 1
```

---

### MergeDatasets

Vertical concatenation (`pd.concat`) of multiple datasets. Adds a `source_label` column when `label` is set.

```yaml
- id: merge_regions
  taskRef:
    name: MergeDatasets
  config:
    inputs:
      - dataset: analytics/silver/orders_north
        label: north
        source: *local
      - dataset: analytics/silver/orders_south
        label: south
        source: *local
    output:
      dataset: analytics/silver/orders_all
      format: parquet
      source: *local
  resources:
    coin: 1
```

---

### PivotDataset

Pivot table or unpivot (melt).

```yaml
# Pivot
- id: pivot_revenue
  taskRef:
    name: PivotDataset
  config:
    input:
      dataset: analytics/gold/report_by_region
      source: *local
    output:
      dataset: analytics/gold/pivot_region_category
      format: parquet
      source: *local
    mode: pivot
    index: region
    columns: category
    values: revenue
    aggfunc: sum
    fill_value: 0
  resources:
    coin: 1

# Unpivot (melt)
- id: unpivot
  taskRef:
    name: PivotDataset
  config:
    input: {dataset: analytics/gold/pivot, source: *local}
    output: {dataset: analytics/gold/unpivoted, format: parquet, source: *local}
    mode: unpivot
    id_vars: [region]
    value_vars: [electronics, food, apparel]
    var_name: category
    value_name: revenue
  resources:
    coin: 1
```

---

### DeduplicateDataset

Remove duplicate rows. `keep`: `first` | `last` | `false` (drop all duplicates).

```yaml
- id: dedup_customers
  taskRef:
    name: DeduplicateDataset
  config:
    input:
      dataset: analytics/bronze/customers
      source: *local
    output:
      dataset: analytics/silver/customers
      format: parquet
      source: *local
    subset:
      - customer_id
    keep: first
  resources:
    coin: 1
```

---

### CatalogCreateSource

Register or update a data source (idempotent).

```yaml
- id: create_source
  taskRef:
    name: CatalogCreateSource
  config:
    id: local
    type: local
    description: "Local filesystem"
    config: {}
  resources:
    coin: 1
```

---

### CatalogCreateDataset

Register or update a dataset entry (idempotent).

```yaml
- id: create_dataset
  taskRef:
    name: CatalogCreateDataset
  config:
    dataset: analytics/bronze/orders
    source_id: local
    format: parquet
    description: "Raw orders from ERP"
  resources:
    coin: 1
```

---

### CatalogDefineSchema

Set semantic metadata on schema columns and optionally publish them.

```yaml
- id: define_schema
  taskRef:
    name: CatalogDefineSchema
  config:
    dataset: analytics/bronze/orders
    publish: true
    columns:
      - name: customer_id
        logical_type: string
        description: "Customer identifier"
        pii: true
        pii_type: direct
      - name: amount
        logical_type: float
        description: "Order amount in EUR"
        nullable: false
  resources:
    coin: 1
```

---

### CatalogSetExpectations

Replace all DQ expectations on a dataset (idempotent, full replace).

Available rule IDs:
| Rule ID | Inputs | Params |
|---------|--------|--------|
| `expect_column_values_to_not_be_null` | `x` | — |
| `expect_column_values_to_be_unique` | `x` | — |
| `expect_column_values_to_be_between` | `x` | `min_val`, `max_val` |
| `expect_column_values_to_be_in_set` | `x` | `allowed_values` (list) |
| `expect_column_values_to_match_regex` | `x` | `pattern` |
| `expect_column_values_to_be_of_type` | `x` | `target_type` (`int`,`float`,`str`,…) |
| `expect_column_value_lengths_to_be_between` | `x` | `min_length`, `max_length` |
| `expect_column_mean_to_be_between` | `x` | `min_val`, `max_val` |
| `expect_column_pair_values_a_to_be_greater_than_b` | `a`, `b` | — |
| `expect_compound_columns_to_be_unique` | `a`, `b` | — |
| `expect_column_values_to_be_in_another_dataset` | `x` | *(refs `other`)* |

```yaml
- id: set_dq
  taskRef:
    name: CatalogSetExpectations
  config:
    dataset: analytics/silver/orders
    expectations:
      - rule_id: expect_column_values_to_not_be_null
        inputs: {x: "this.order_id"}
        tolerance: 1.0
      - rule_id: expect_column_values_to_be_between
        inputs: {x: "this.amount"}
        params: {min_val: 0, max_val: 1000000}
        tolerance: 0.99
      - rule_id: expect_column_values_to_be_of_type
        inputs: {x: "this.amount"}
        params: {target_type: "float"}
        tolerance: 1.0
  resources:
    coin: 1
```

`tolerance: 1.0` = all rows must pass. `tolerance: 0.99` = at most 1% of rows can fail.

---

### CatalogSetCharts

Attach ECharts visualisations to a dataset. Types: `bar`, `line`, `pie`, `histogram`, `scatter`.

```yaml
- id: set_charts
  taskRef:
    name: CatalogSetCharts
  config:
    dataset: analytics/gold/report_by_region
    charts:
      - key: revenue_by_region
        title: Revenue by Region
        spec:
          type: bar
          x: {field: region, label: Region}
          y: {field: revenue, agg: sum, label: "Total Revenue"}

      - key: category_share
        title: Category Share
        spec:
          type: pie
          x: {field: category}
          y: {field: revenue, agg: sum}

      - key: amount_dist
        title: Amount Distribution
        spec:
          type: histogram
          x: {field: amount, label: Amount}
          bins: 20
  resources:
    coin: 1
```

---

## 5. Catalog: writing datasets from a custom script

Use the SDK inside a `taskSpec.script`. The two-phase commit (reserve → write → commit) is handled automatically by the context manager.

```yaml
- id: produce_clean_orders
  taskSpec:
    script: |
      import pandas as pd
      from waluigi.sdk.context import context
      from waluigi.sdk.catalog import CatalogClient

      catalog = CatalogClient()   # reads WALUIGI_CATALOG_URL + WALUIGI_CATALOG_NAMESPACE
      date = context.params.date

      # READ (latest committed version)
      reader = catalog.read_dataset("analytics/bronze/orders")
      df = reader.read()

      # TRANSFORM
      df = df[df["amount"] > 0].copy()
      df["revenue"] = df["amount"] * df["quantity"]

      # WRITE (two-phase commit — reserve → write → commit)
      handle = catalog.create_dataset(
          "analytics/silver/orders",
          format="parquet",
          source_id="local",
          description="Cleaned orders with revenue",
      )
      with handle.create_version(
          metadata={"date": date, "job_id": context.job_id},
          inputs=[reader],           # records lineage automatically
          force=False,               # skip if identical metadata exists (idempotent)
      ) as writer:
          writer.write(df)

      if writer.skipped:
          print(f"Skipped — version already exists for date={date}")
      else:
          print(f"Written {len(df)} rows → {writer.dataset_id}@{writer.version}")
    affinity:
      - python
  resources:
    coin: 1
```

---

## 6. Medallion architecture: Bronze → Silver → Gold

This is the recommended pipeline pattern for Waluigi + Catalog. Each layer is a separate dataset with its own schema, DQ expectations, and lineage.

```
Bronze  = raw ingestion — one version per run, no transformation
Silver  = cleaned, validated, DQ-checked
Gold    = aggregated, business-ready, with charts
```

### Full example JobDefinition

```yaml
kind: JobDefinition
metadata:
  name: orders-medallion
  namespace: analytics
spec:
  tasks:

    # ── BRONZE: raw ingestion ─────────────────────────────────────────────────
    - id: bronze_ingest
      taskSpec:
        script: |
          import pandas as pd
          import json
          from waluigi.sdk.context import context
          from waluigi.sdk.catalog import CatalogClient

          catalog = CatalogClient()
          date = context.params.date
          cfg  = context.config

          # Simulate fetching raw data (replace with actual API/DB call)
          df = pd.DataFrame({
              "order_id":   [f"ORD-{i:04d}" for i in range(100)],
              "date":       [date] * 100,
              "customer_id":[f"C{i % 20:03d}" for i in range(100)],
              "amount":     [float(i * 10 + 5) for i in range(100)],
              "quantity":   [i % 10 + 1 for i in range(100)],
              "status":     ["completed" if i % 5 != 0 else "cancelled" for i in range(100)],
          })

          handle = catalog.create_dataset(
              "orders/bronze/raw",
              format="parquet",
              source_id="local",
              description="Raw orders from ERP — unvalidated",
          )
          with handle.create_version(
              metadata={"date": date, "job_id": context.job_id, "source": "erp"},
          ) as writer:
              writer.write(df)

          print(f"Bronze: ingested {len(df)} raw rows for date={date}")
        affinity:
          - python
      resources:
        coin: 1

    # ── BRONZE: set DQ expectations (run once, idempotent) ───────────────────
    - id: bronze_expectations
      taskRef:
        name: CatalogSetExpectations
      config:
        dataset: orders/bronze/raw
        expectations:
          - rule_id: expect_column_values_to_not_be_null
            inputs: {x: "this.order_id"}
            tolerance: 1.0
          - rule_id: expect_column_values_to_not_be_null
            inputs: {x: "this.date"}
            tolerance: 1.0
          - rule_id: expect_column_values_to_be_between
            inputs: {x: "this.amount"}
            params: {min_val: 0, max_val: 9999999}
            tolerance: 1.0
      requires:
        - bronze_ingest
      resources:
        coin: 1

    # ── SILVER: clean + validate ──────────────────────────────────────────────
    - id: silver_clean
      taskSpec:
        script: |
          import pandas as pd
          from waluigi.sdk.context import context
          from waluigi.sdk.catalog import CatalogClient

          catalog = CatalogClient()
          date = context.params.date

          reader = catalog.read_dataset("orders/bronze/raw")
          df = reader.read()
          original_rows = len(df)

          # Clean
          df = df[df["status"] == "completed"].copy()
          df = df[df["amount"] > 0]
          df["revenue"] = df["amount"] * df["quantity"]
          df = df.dropna(subset=["order_id", "customer_id"])
          df = df.drop_duplicates(subset=["order_id"])

          print(f"Silver: {len(df)}/{original_rows} rows after cleaning")

          handle = catalog.create_dataset(
              "orders/silver/clean",
              format="parquet",
              source_id="local",
              description="Completed, deduplicated orders with revenue",
          )
          with handle.create_version(
              metadata={"date": date, "job_id": context.job_id},
              inputs=[reader],
          ) as writer:
              writer.write(df)

          print(f"Silver written → {writer.dataset_id}@{writer.version}")
        affinity:
          - python
      requires:
        - bronze_ingest
      resources:
        coin: 1

    # ── SILVER: DQ expectations ───────────────────────────────────────────────
    - id: silver_expectations
      taskRef:
        name: CatalogSetExpectations
      config:
        dataset: orders/silver/clean
        expectations:
          - rule_id: expect_column_values_to_not_be_null
            inputs: {x: "this.order_id"}
            tolerance: 1.0
          - rule_id: expect_column_values_to_be_unique
            inputs: {x: "this.order_id"}
            tolerance: 1.0
          - rule_id: expect_column_values_to_be_between
            inputs: {x: "this.amount"}
            params: {min_val: 0.01, max_val: 9999999}
            tolerance: 1.0
          - rule_id: expect_column_values_to_be_of_type
            inputs: {x: "this.revenue"}
            params: {target_type: "float"}
            tolerance: 1.0
      requires:
        - silver_clean
      resources:
        coin: 1

    # ── SILVER: schema definition ─────────────────────────────────────────────
    - id: silver_schema
      taskRef:
        name: CatalogDefineSchema
      config:
        dataset: orders/silver/clean
        publish: true
        columns:
          - name: order_id
            logical_type: string
            description: "Unique order identifier"
            nullable: false
          - name: customer_id
            logical_type: string
            description: "Customer identifier"
            pii: true
            pii_type: indirect
          - name: amount
            logical_type: float
            description: "Order amount in EUR"
            nullable: false
          - name: revenue
            logical_type: float
            description: "Revenue = amount × quantity"
      requires:
        - silver_clean
      resources:
        coin: 1

    # ── GOLD: aggregate ───────────────────────────────────────────────────────
    - id: gold_aggregate
      taskRef:
        name: AggregateDataset
      config:
        input:
          dataset: orders/silver/clean
          source: &local {id: local, type: local}
        output:
          dataset: orders/gold/by_customer
          format: parquet
          description: "Revenue and order count by customer"
          source: *local
        group_by:
          - customer_id
        agg:
          revenue: sum
          order_id: count
          amount: mean
      requires:
        - silver_clean
      resources:
        coin: 1

    # ── GOLD: charts ──────────────────────────────────────────────────────────
    - id: gold_charts
      taskRef:
        name: CatalogSetCharts
      config:
        dataset: orders/gold/by_customer
        charts:
          - key: revenue_by_customer
            title: Revenue by Customer
            spec:
              type: bar
              x: {field: customer_id, label: Customer}
              y: {field: revenue, agg: sum, label: "Total Revenue"}
          - key: order_count
            title: Orders per Customer
            spec:
              type: bar
              x: {field: customer_id}
              y: {field: order_id, agg: sum, label: "# Orders"}
      requires:
        - gold_aggregate
      resources:
        coin: 1
```

### Run the medallion pipeline

```bash
# Test all tasks locally first
wlctl run -f orders-medallion.yaml --params date=2026-06-15

# Apply JobDefinition to cluster
wlctl apply -f orders-medallion.yaml

# Trigger a run
wlctl apply -f - <<'EOF'
kind: Job
metadata:
  name: orders-run
  namespace: analytics
spec:
  executionPolicy: Ephemeral
  params:
    date: "2026-06-15"
  jobRef:
    name: orders-medallion
EOF

# Monitor
wlctl get tasks -n analytics
```

### Inspect Catalog results

```bash
# List datasets by layer
wlctl get datasets -n analytics

# View versions of a dataset
wlctl get versions -d orders/silver/clean -n analytics

# View schema
wlctl get schema -d orders/silver/clean -n analytics

# View DQ results (latest version)
wlctl dq dataset orders/silver/clean -n analytics

# View lineage
wlctl lineage dataset orders/silver/clean -n analytics

# Describe a dataset
wlctl describe dataset orders/silver/clean -n analytics

# Preview rows
wlctl preview dataset orders/gold/by_customer -n analytics -l 20
```

---

## 7. Complete CLI quick reference

```bash
# Authentication
wlctl --url http://localhost:8080 login -u admin

# Apply any descriptor
wlctl apply -f descriptor.yaml [-n namespace]

# Inspect
wlctl get namespaces
wlctl get jobs [-n ns] [-s status]
wlctl get tasks [-n ns] [--job-id job_id]
wlctl get workers
wlctl get resources [-n ns]
wlctl get taskdefinitions [-n ns]
wlctl get jobdefinitions [-n ns]
wlctl get cronjobs [-n ns]
wlctl get secrets [-n ns]
wlctl get sources [-n ns]
wlctl get datasets [-n ns] [--status draft|in_review|approved|deprecated]
wlctl get versions -d <dataset_id> [-n ns]
wlctl get schema -d <dataset_id> [-n ns]

# Describe
wlctl describe job <job_id> [-n ns]
wlctl describe task <task_id> [-n ns]
wlctl describe taskdefinition <name> [-n ns]
wlctl describe jobdefinition <name> [-n ns]
wlctl describe dataset <id> [-n ns]
wlctl describe source <id> [-n ns]
wlctl describe secret <name> [-n ns]

# Logs
wlctl logs <task_id> [-n ns] [-n lines] [--follow]

# Job lifecycle
wlctl reset task <task_id> [-n ns]
wlctl reset job <job_id> [-n ns]
wlctl reset namespace <ns>
wlctl pause job <job_id> [-n ns]
wlctl resume job <job_id> [-n ns]
wlctl cancel job <job_id> [-n ns]

# CronJob lifecycle
wlctl enable cronjob <name> [-n ns]
wlctl disable cronjob <name> [-n ns]

# Delete
wlctl delete job <job_id> [-n ns]
wlctl delete cronjob <name> [-n ns]
wlctl delete taskdefinition <name> [-n ns]
wlctl delete jobdefinition <name> [-n ns]
wlctl delete namespace <ns>
wlctl delete secret <name> [-n ns]
wlctl delete dataset <id> [-n ns]
wlctl delete version <version> -d <dataset_id> [-n ns]

# Local run (no cluster needed)
wlctl run -f job.yaml -t <task_id> [--params KEY=VALUE ...]
wlctl run -f job.yaml [--params KEY=VALUE ...]
wlctl run "python script.py" --params date=2026-06-15 -n analytics

# Catalog
wlctl preview dataset <id> [-n ns] [-v version] [-l rows]
wlctl lineage dataset <id> [-n ns] [-v version]
wlctl dq dataset <id> [-n ns] [-v version]
```

---

## 8. Common troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Task stuck in `PENDING` | No worker with matching affinity | `wlctl get workers` — check affinity column; start worker with `--affinity python` |
| Task goes to `FAILED` immediately | Script error or unknown `taskRef` | `wlctl logs <task_id>` — read the exception |
| `taskRef` fails with "Unknown task type" | TaskDefinition not applied in namespace | `wlctl apply -f descriptors/task-definitions/builtin-task-definitions.yaml -n <ns>` |
| Affinity set but ignored | `affinity` at outer task level, not inside `taskSpec` | Move `affinity` inside `taskSpec:` block |
| Catalog write fails | Source not created yet | Apply `CatalogCreateSource` first or add `create_source` task before the write |
| Version already exists, skipped | Metadata dedup triggered (`force=False`) | Pass `force=True` to `create_version()` or change the metadata dict |
| `wlctl run` fails but cluster works | `WALUIGI_CATALOG_URL` not set locally | `export WALUIGI_CATALOG_URL=http://localhost:9000` |
| Job ID suffix `@timestamp` | `executionPolicy: Ephemeral` (default) | Use `Stateful` for canonical IDs |

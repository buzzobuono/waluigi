# Built-in Task Types

Waluigi ships with reusable task types that cover common data transformation patterns. Reference them in a job with `taskRef.name` and provide a `config` block — no Python code required.

All built-in tasks use `CatalogClient` internally to read inputs and write outputs via the two-phase commit protocol. Lineage is recorded automatically.

## Setup: applying built-in TaskDefinitions

Built-in task types are Python modules in the `waluigi.tasks` package. They are **not** auto-registered — you must apply the corresponding `TaskDefinition` to each namespace where you want to use them:

```bash
wlctl apply -f descriptors/task-definitions/builtin-task-definitions.yaml -n analytics
```

The `descriptors/task-definitions/builtin-task-definitions.yaml` file in the repo contains all built-in definitions. Each definition sets `affinity: [python]` — make sure at least one worker registers with `--affinity python`.

Once applied, tasks can reference built-in types via `taskRef.name` without any custom code.

---

## Source definition

Most tasks reference one or more sources. A source can be declared inline or via a YAML anchor:

```yaml
x-sources:
  local: &local
    id: analytics-local
    type: LOCAL
    description: Local filesystem storage

kind: Job
...
  tasks:
    - id: my_task
      taskRef:
        name: FilterDataset
      config:
        input:
          dataset: analytics/raw/orders
          source: *local                 # reuse anchor
        output:
          dataset: analytics/clean/orders
          format: parquet
          source: *local
```

Source types: `LOCAL`, `S3`, `SQL`, `SFTP`. See [sdk.md](sdk.md#connectors) for connector config fields.

---

## FilterDataset

Keeps rows matching a pandas query expression.

```yaml
taskRef:
  name: FilterDataset
config:
  input:
    dataset: <string>          # input dataset ID
    source: <source>           # source definition
  output:
    dataset: <string>          # output dataset ID
    format: <string>           # parquet | csv | json  (default: parquet)
    description: <string>      # optional
    source: <source>
  where: <string>              # pandas query expression
```

**Example:**

```yaml
- id: filter_completed
  taskRef:
    name: FilterDataset
  config:
    input:
      dataset: sales/raw/orders
      source: *local
    output:
      dataset: sales/clean/orders
      format: parquet
      source: *local
    where: "status == 'completed' and value > 0"
  resources:
    coin: 1
```

The `where` string is passed to `df.query(where)`. Refer to [pandas query syntax](https://pandas.pydata.org/docs/user_guide/indexing.html#the-query-method).

---

## SelectColumns

Projects a subset of columns.

```yaml
taskRef:
  name: SelectColumns
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
  columns: <list[string]>      # columns to retain
```

**Example:**

```yaml
- id: select_fields
  taskRef:
    name: SelectColumns
  config:
    input:
      dataset: sales/raw/orders
      source: *local
    output:
      dataset: sales/projected/orders
      format: parquet
      source: *local
    columns:
      - order_id
      - date
      - customer_id
      - product_id
      - quantity
      - unit_price
  resources:
    coin: 1
```

---

## AddDerivedColumns

Computes new columns using pandas eval expressions. Columns are added sequentially — later expressions can reference columns added earlier.

```yaml
taskRef:
  name: AddDerivedColumns
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
  columns:
    - name: <string>           # new column name
      expr: <string>           # pandas eval expression
```

**Example:**

```yaml
- id: add_financials
  taskRef:
    name: AddDerivedColumns
  config:
    input:
      dataset: sales/clean/orders
      source: *local
    output:
      dataset: sales/enriched/orders
      format: parquet
      source: *local
    columns:
      - name: revenue
        expr: "quantity * unit_price * (1 - discount)"
      - name: gross_profit
        expr: "revenue - quantity * cost_price"
      - name: margin_pct
        expr: "gross_profit / revenue * 100"
  resources:
    coin: 1
```

Expressions are evaluated with `df.eval(expr, inplace=False)` and appended to the DataFrame.

---

## AggregateDataset

Group-by aggregation.

```yaml
taskRef:
  name: AggregateDataset
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
  group_by: <list[string]>     # grouping columns
  agg:
    <column>: <function>       # sum | mean | count | min | max | std | first | last
```

**Example:**

```yaml
- id: agg_by_region
  taskRef:
    name: AggregateDataset
  config:
    input:
      dataset: sales/enriched/orders
      source: *local
    output:
      dataset: sales/report/by_region
      format: parquet
      source: *local
    group_by:
      - region
      - category
    agg:
      revenue: sum
      gross_profit: sum
      quantity: sum
      order_id: count
  resources:
    coin: 1
```

---

## JoinDatasets

Horizontal join of two datasets (`pd.merge`).

```yaml
taskRef:
  name: JoinDatasets
config:
  left:
    dataset: <string>
    source: <source>
  right:
    dataset: <string>
    source: <source>
  join:
    columns: <string | list[string]>  # join key(s)
    how: <string>                     # inner | left | right | outer  (default: inner)
    suffixes: <list[string]>          # default: ["_x", "_y"]
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
```

**Example:**

```yaml
- id: join_orders_products
  taskRef:
    name: JoinDatasets
  config:
    left:
      dataset: sales/clean/orders
      source: *local
    right:
      dataset: simulation/clean/products
      source: *local
    join:
      columns: product_id
      how: left
      suffixes: ["_order", "_product"]
    output:
      dataset: sales/joined/orders_products
      format: parquet
      source: *local
  resources:
    coin: 1
```

---

## MergeDatasets

Vertical concatenation of multiple datasets (`pd.concat`).

```yaml
taskRef:
  name: MergeDatasets
config:
  inputs:
    - dataset: <string>
      label: <string>        # optional; adds a "source_label" column
      source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
```

**Example:**

```yaml
- id: merge_executive_report
  taskRef:
    name: MergeDatasets
  config:
    inputs:
      - dataset: sales/report/top_regions
        label: top_regions
        source: *local
      - dataset: sales/report/top_categories
        label: top_categories
        source: *local
      - dataset: sales/report/top_products
        label: top_products
        source: *local
    output:
      dataset: sales/report/executive
      format: parquet
      description: "Executive summary"
      source: *local
  resources:
    coin: 1
```

---

## PivotDataset

Pivot table or unpivot (melt). Mode is controlled by `mode:`.

### Pivot mode (default)

```yaml
taskRef:
  name: PivotDataset
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    source: <source>
  mode: pivot                  # default
  index: <string | list>       # row labels
  columns: <string>            # column labels
  values: <string>             # values to aggregate
  aggfunc: <string>            # sum | mean | count | min | max  (default: sum)
  fill_value: <any>            # fill for missing combinations (default: 0)
```

**Example — pivot:**

```yaml
- id: pivot_revenue
  taskRef:
    name: PivotDataset
  config:
    input:
      dataset: sales/report/by_region
      source: *local
    output:
      dataset: sales/report/pivot_region_category
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
```

### Unpivot mode

```yaml
taskRef:
  name: PivotDataset
config:
  input: {...}
  output: {...}
  mode: unpivot
  id_vars: <list[string]>      # columns to keep as-is
  value_vars: <list[string]>   # columns to melt (optional; all others if absent)
  var_name: <string>           # name for variable column  (default: variable)
  value_name: <string>         # name for value column  (default: value)
```

---

## DeduplicateDataset

Removes duplicate rows.

```yaml
taskRef:
  name: DeduplicateDataset
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    source: <source>
  subset: <list[string]>       # columns to consider (optional; all if absent)
  keep: <string>               # first | last | false  (default: first)
                               # false = drop all duplicates
```

**Example:**

```yaml
- id: dedup_products
  taskRef:
    name: DeduplicateDataset
  config:
    input:
      dataset: simulation/raw/products
      source: *local
    output:
      dataset: simulation/clean/products
      format: parquet
      source: *local
    subset:
      - product_id
    keep: first
  resources:
    coin: 1
```

---

## AccumulateDataset

Append-only **fact table** with per-date idempotency — the canonical built-in for daily fact tables in a Bronze→Silver→Gold (medallion) architecture. Each run reads the previous output (gold) version, drops the rows for the current date, appends today's input, and writes a new output version.

Idempotency is two-layered: re-running the same day removes that day's rows from the previous gold before appending (row-level), and `write_output` reserves with `force=False` so identical metadata skips the write entirely (version-level). Lineage records **both** inputs: today's input and the previous gold version.

```yaml
taskRef:
  name: AccumulateDataset
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
  date_column: <string>        # date partition column in the dataframe (default: "date")
  date_param: <string>         # job param holding today's date value   (default: "date")
```

**Behaviour:**

| Scenario | Result |
|----------|--------|
| First run (no previous gold) | Writes today's input as the first gold version |
| Normal run | Removes rows where `date_column == date_param`, appends input, writes a new version |
| Same day re-run (same params) | `force=False` skips the write — no duplicate version |
| `date_column` absent from input | The column is added automatically with the `date_param` value |
| Empty input | Still writes a gold version (history only) — does not fail |

**Example:**

```yaml
- id: accumulate_orders
  taskRef:
    name: AccumulateDataset
  config:
    input:
      dataset: bronze/myapp/orders_raw
      source: *local
    output:
      dataset: gold/myapp/orders_all
      format: parquet
      description: "Orders accumulated — all days"
      source: *local
    date_column: date
    date_param: date
  resources:
    coin: 2
  requires:
    - bronze_ingest
```

---

## UpsertDataset

**SCD Type 1** dimension table — the canonical built-in for daily dimension tables in a medallion architecture. Each run reads the previous output (gold) version, concatenates today's input, and keeps the last record per business `key` (`keep="last"`), so newer rows win on a key collision.

Records that disappear from the source are **not** deleted — they remain in the output. Same-day re-runs are idempotent via `force=False` on identical metadata. Lineage records both today's input and the previous gold version.

```yaml
taskRef:
  name: UpsertDataset
config:
  input:
    dataset: <string>
    source: <source>
  output:
    dataset: <string>
    format: <string>
    description: <string>
    source: <source>
  key: <string | list[string]>  # business key column(s) — required; list = composite key
```

**Behaviour:**

| Scenario | Result |
|----------|--------|
| First run | Writes input after internal dedup by `key` |
| Normal run | Concat previous gold + today → dedup `keep="last"` → new version |
| Existing key, updated data | Overwrites the previous record (today wins) |
| New key | Added normally |
| Key removed from source | **Retained** in gold — the task only updates/adds, never deletes |
| Key column missing from input | Fails with an explicit `KeyError` |

**Example:**

```yaml
- id: upsert_clienti
  taskRef:
    name: UpsertDataset
  config:
    input:
      dataset: bronze/myapp/clienti_raw
      source: *local
    output:
      dataset: gold/myapp/clienti
      format: parquet
      description: "Customer master — latest version per customer"
      source: *local
    key:
      - IdCliente                # list supports composite keys
  resources:
    coin: 1
  requires:
    - bronze_ingest
```

---

## CatalogCreateSource

Registers or updates a data source. Idempotent (upsert by ID).

```yaml
taskRef:
  name: CatalogCreateSource
config:
  id: <string>                 # unique source ID (required)
  type: <string>               # local | s3 | sql | sftp | api
  description: <string>        # optional
  config: <dict>               # connector-specific config (optional)
```

**Example:**

```yaml
- id: create_source
  taskRef:
    name: CatalogCreateSource
  config:
    id: analytics-local
    type: local
    description: Local filesystem for analytics namespace
    config: {}
  resources:
    coin: 1
```

---

## CatalogCreateDataset

Registers or updates a dataset in the Catalog. Idempotent.

```yaml
taskRef:
  name: CatalogCreateDataset
config:
  dataset: <string>            # dataset ID path (required)
  source_id: <string>          # source to associate (optional)
  format: <string>             # parquet | csv | json  (default: parquet)
  description: <string>        # optional
```

**Example:**

```yaml
- id: create_dataset
  taskRef:
    name: CatalogCreateDataset
  config:
    dataset: analytics/erp/raw/transactions
    source_id: analytics-local
    format: parquet
    description: "Raw ERP transaction data"
  resources:
    coin: 1
```

---

## CatalogDefineSchema

Sets semantic metadata on dataset schema columns. If columns are not specified, only the publish flag is applied.

```yaml
taskRef:
  name: CatalogDefineSchema
config:
  dataset: <string>            # dataset ID (required)
  publish: <bool>              # promote all columns to "published" (default: false)
  columns:                     # optional; updates specific columns
    - name: <string>
      logical_type: <string>   # integer | float | string | date | ...
      description: <string>
      nullable: <bool>
      pii: <bool>
      pii_type: <string>       # none | direct | indirect | sensitive
      pii_notes: <string>
```

**Example:**

```yaml
- id: define_schema
  taskRef:
    name: CatalogDefineSchema
  config:
    dataset: analytics/erp/raw/transactions
    publish: true
    columns:
      - name: customer_id
        logical_type: string
        description: "Customer identifier"
        pii: true
        pii_type: direct
      - name: amount
        logical_type: float
        description: "Transaction amount in EUR"
        nullable: false
  resources:
    coin: 1
```

---

## CatalogSetExpectations

Replaces all DQ expectations on a dataset. **Idempotent** — always replaces the full list.

```yaml
taskRef:
  name: CatalogSetExpectations
config:
  dataset: <string>            # dataset ID (required)
  expectations:                # replaces all existing
    - rule_id: <string>        # rule ID from the rules catalog
      inputs: <dict>           # input mappings: {input_name: "this.<column>"}
      params: <dict>           # optional rule parameters
      tolerance: <float>       # 0.0–1.0, fraction of rows that can fail (default: 1.0)
```

**Example:**

```yaml
- id: set_expectations
  taskRef:
    name: CatalogSetExpectations
  config:
    dataset: analytics/erp/raw/transactions
    expectations:
      - rule_id: expect_column_values_to_not_be_null
        inputs: {x: "this.transaction_id"}
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

→ See [data-quality.md](data-quality.md) for available rule IDs and their parameters.

---

## CatalogSetCharts

Upserts chart definitions on a dataset. Existing charts with the same `key` are updated; others are added.

```yaml
taskRef:
  name: CatalogSetCharts
config:
  dataset: <string>            # dataset ID (required)
  charts:                      # upserted by key
    - key: <string>            # unique chart key
      title: <string>
      spec:
        type: <string>         # bar | line | pie | histogram | scatter
        x:
          field: <string>      # column name
          label: <string>      # optional axis label
        y:
          field: <string>
          agg: <string>        # sum | mean | count | min | max
          label: <string>
        bins: <int>            # histogram only
```

**Example:**

```yaml
- id: set_charts
  taskRef:
    name: CatalogSetCharts
  config:
    dataset: analytics/erp/report/summary
    charts:
      - key: revenue_by_category
        title: Revenue by Category
        spec:
          type: bar
          x: {field: category, label: Category}
          y: {field: revenue, agg: sum, label: "Total Revenue"}

      - key: category_share
        title: Category Share
        spec:
          type: pie
          x: {field: category}
          y: {field: revenue, agg: sum}

      - key: amount_distribution
        title: Amount Distribution
        spec:
          type: histogram
          x: {field: amount, label: Amount}
          bins: 20
  resources:
    coin: 1
```

---

## FetchHttp

Calls an HTTP JSON endpoint, flattens nested objects, and writes the result as a Catalog dataset.
Supports pagination via `next` link or `page` query parameter.

```yaml
- id: fetch_users
  taskRef:
    name: FetchHttp
  params:
    url: "https://api.example.com/v1/users"
  config:
    dataset_id:   "api/users/raw"        # Catalog dataset path
    source_id:    "api-local"            # Catalog source for local storage
    format:       "parquet"              # parquet (default), csv, json
    description:  "Users from external API"
    # optional
    headers:      {}                     # HTTP request headers
    params:       {}                     # extra query params
    data_key:     "data"                 # key containing list in response (auto-detected)
    next_key:     "next"                 # key for next-page URL
    page_param:   "page"                 # query param for page-number pagination
    page_size:    100                    # value for page_size query param
  resources:
    coin: 1
```

Nested fields are flattened with underscore separator:

| Raw JSON | Flat column |
|----------|-------------|
| `address.city` | `address_city` |
| `company.name` | `company_name` |
| `geo.lat` | `address_geo_lat` |

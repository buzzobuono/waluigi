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

## Sources

Sources must be registered in the catalog before tasks can write to them. Use `CatalogCreateSource` (as a task in the same job, or via a separate setup job) to register them:

```yaml
- id: setup_source
  taskRef:
    name: CatalogCreateSource
  config:
    id: analytics-local
    type: local
    config: {}
```

Reads do not require a `source` in the task config — the source is already stored in the catalog alongside the dataset. Only **writes** require a `source_id` field pointing to a pre-registered source.

Source types: `local`, `s3`, `sql`, `sftp`, `sharepoint`. See [sdk.md](sdk.md#connectors) for connector config fields.

---

## FilterDataset

Keeps rows matching a pandas query expression.

```yaml
taskRef:
  name: FilterDataset
config:
  input:
    dataset: <string>          # input dataset ID
  output:
    dataset: <string>          # output dataset ID
    source_id: <string>        # pre-registered source ID
    format: <string>           # parquet | csv | json  (default: parquet)
    description: <string>      # optional
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
    output:
      dataset: sales/clean/orders
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
    description: <string>
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
    output:
      dataset: sales/projected/orders
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
    description: <string>
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
    output:
      dataset: sales/enriched/orders
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
    description: <string>
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
    output:
      dataset: sales/report/by_region
      source_id: analytics-local
      format: parquet
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
  right:
    dataset: <string>
  join:
    columns: <string | list[string]>  # join key(s)
    how: <string>                     # inner | left | right | outer  (default: inner)
    suffixes: <list[string]>          # default: ["_x", "_y"]
  output:
    dataset: <string>
    source_id: <string>               # pre-registered source ID
    format: <string>
    description: <string>
```

**Example:**

```yaml
- id: join_orders_products
  taskRef:
    name: JoinDatasets
  config:
    left:
      dataset: sales/clean/orders
    right:
      dataset: simulation/clean/products
    join:
      columns: product_id
      how: left
      suffixes: ["_order", "_product"]
    output:
      dataset: sales/joined/orders_products
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>      # pre-registered source ID
    format: <string>
    description: <string>
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
      - dataset: sales/report/top_categories
        label: top_categories
      - dataset: sales/report/top_products
        label: top_products
    output:
      dataset: sales/report/executive
      source_id: analytics-local
      format: parquet
      description: "Executive summary"
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
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
    output:
      dataset: sales/report/pivot_region_category
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
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
    output:
      dataset: simulation/clean/products
      source_id: analytics-local
      format: parquet
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
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
    description: <string>
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
    output:
      dataset: gold/myapp/orders_all
      source_id: analytics-local
      format: parquet
      description: "Orders accumulated — all days"
    date_column: date
    date_param: date
  resources:
    coin: 2
  requires:
    - bronze_ingest
```

---

## AccumulateDeduplicateDataset

Fact table with **cross-day deduplication by state** — a variant of `AccumulateDataset` for operational-funnel / state-history tables. Instead of appending today's snapshot verbatim, it keeps a single row per unique state (all columns **except** `date_column`), dated with the *first* day that state was observed. Rows that do not change day-to-day are not duplicated, so the dataset grows only on real state changes.

Each run concatenates the previous gold + today's input (prev first), sorts by `date_column`, and drops duplicates on every column except the date with `keep="first"` (oldest date wins). Same-day re-runs are idempotent: the dedup absorbs the repeat and `force=False` skips identical metadata. Lineage records both inputs.

```yaml
taskRef:
  name: AccumulateDeduplicateDataset
config:
  input:
    dataset: <string>
  output:
    dataset: <string>
    source_id: <string>        # pre-registered source ID
    format: <string>
    description: <string>
  date_column: <string>        # date column used for ordering / partition (default: "date")
  date_param: <string>         # job param holding today's date value      (default: "date")
```

**`AccumulateDataset` vs `AccumulateDeduplicateDataset`:**

| | `AccumulateDataset` | `AccumulateDeduplicateDataset` |
|---|---|---|
| Unchanged rows day-to-day | kept (duplicated in gold) | collapsed to one row with the first-seen date |
| Growth | linear (N rows/day) | only on real state changes |
| Use case | pure daily snapshot | operational funnel / state history |

**Example:**

```yaml
- id: accumulate_applications
  taskRef:
    name: AccumulateDeduplicateDataset
  config:
    input:
      dataset: bronze/universo/applications_raw
    output:
      dataset: gold/universo/applications_all
      source_id: analytics-local
      format: parquet
      description: "Applications with state-change history"
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
  output:
    dataset: <string>
    source_id: <string>           # pre-registered source ID
    format: <string>
    description: <string>
  key: <string | list[string]>    # business key column(s) — required; list = composite key
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
    output:
      dataset: gold/myapp/clienti
      source_id: analytics-local
      format: parquet
      description: "Customer master — latest version per customer"
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

## IngestRest

Calls a JSON REST API, flattens nested objects, and writes the result as a Catalog dataset. Supports pagination via `next` link or `page` query parameter.

```yaml
taskRef:
  name: IngestRest
config:
  output:
    dataset: <string>          # output dataset ID
    source_id: <string>        # pre-registered source ID
    format: <string>           # parquet | csv | json  (default: parquet)
    description: <string>      # optional
  http:
    url: <string>              # required — endpoint URL
    method: <string>           # GET (default) | POST
    headers: <dict>            # HTTP request headers — ${VAR} placeholders expanded
    params: <dict>             # query-string params
    body: <dict>               # request body for POST
    data_key: <string>         # key in JSON response containing the list (auto-detected)
    next_key: <string>         # key for next-page URL  (default: "next")
    page_param: <string>       # query param for page-number pagination
    page_size: <int>           # value for page_size query param
```

Nested JSON fields are flattened with underscore separator: `{"address": {"city": "Rome"}}` → column `address_city`.

**Example:**

```yaml
- id: ingest_users
  taskRef:
    name: IngestRest
  config:
    output:
      dataset: bronze/api/users
      source_id: analytics-local
      format: parquet
      description: "Users from external API"
    http:
      url: "https://api.example.com/v1/users"
      method: GET
      headers:
        Authorization: "Bearer ${WALUIGI_SECRET_API_TOKEN}"
      next_key: next
      page_size: 100
  resources:
    coin: 1
```

---

## SharePointExport

Publishes a Catalog dataset to a SharePoint document library via the Microsoft Graph API (app-only OAuth2). Designed for Microsoft 365 environments — no Azure subscription required beyond the app registration.

**Azure AD prerequisites:**
1. Register an app in [Azure AD](https://portal.azure.com) → App registrations
2. Add **Application** permission: `Sites.ReadWrite.All` (not delegated)
3. Click "Grant admin consent"
4. Create a client secret → store it as a Waluigi Secret

```yaml
taskRef:
  name: SharePointExport
config:
  input:
    dataset: <string>          # Catalog dataset to publish
  sharepoint:
    tenant_id: <string>        # Azure AD tenant GUID or "contoso.onmicrosoft.com"
    client_id: <string>        # App registration Application (client) ID
    site_id: <string>          # SharePoint site ID — get from Graph Explorer
                               # (omit if you provide site_url)
    site_url: <string>         # e.g. "https://contoso.sharepoint.com/sites/DataTeam"
                               # Used to auto-resolve site_id when site_id is absent
    drive_id: <string>         # Document library ID (optional — defaults to root drive)
    folder: <string>           # Destination folder path, e.g. "PowerBI/Gold"
    filename: <string>         # Filename override (default: last segment of dataset id + ext)
    format: <string>           # csv (default) | parquet
```

The client secret is read from `WALUIGI_SECRET_CLIENT_SECRET`. Store it as a Waluigi Secret in the namespace with key `CLIENT_SECRET` (or `client_secret`). Files larger than 4 MB are uploaded via Graph API upload sessions automatically.

**How to find `site_id`:**
In [Graph Explorer](https://developer.microsoft.com/graph/graph-explorer) call `GET https://graph.microsoft.com/v1.0/sites/{hostname}:{/relative-path}` (e.g. `…/sites/contoso.sharepoint.com:/sites/DataTeam`). The `id` field in the response is your `site_id`.

**Example:**

```yaml
kind: Secret
metadata:
  namespace: analytics
  name: sharepoint
spec:
  CLIENT_SECRET: "your-azure-client-secret"
---
# In the job task list:
- id: publish_revenue
  taskRef:
    name: SharePointExport
  requires:
    - gold_revenue
  config:
    input:
      dataset: gold/kpi_revenue
    sharepoint:
      tenant_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      client_id: "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
      site_url:  "https://contoso.sharepoint.com/sites/DataTeam"
      folder:    "PowerBI/Gold"
      format:    csv
  resources:
    coin: 1
```

In Power BI Service, point the SharePoint connector at the `PowerBI/Gold` folder and configure a daily scheduled refresh. No on-premises gateway required.

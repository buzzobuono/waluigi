# SDK Reference

The Waluigi SDK (`waluigi.sdk`) provides the Python interface for task scripts to interact with the runtime environment and the Catalog.

---

## context

`context` is a singleton that reads the environment variables injected by the Worker. Import it once at the top of any task script.

```python
from waluigi.sdk.context import context
```

### Properties

| Property | Type | Source env var | Description |
|----------|------|----------------|-------------|
| `context.params` | `SimpleNamespace` | `WALUIGI_PARAM_*` | Task params (lowercase keys) |
| `context.attributes` | `SimpleNamespace` | `WALUIGI_ATTRIBUTE_*` | Task attributes (lowercase keys) |
| `context.config` | `SimpleNamespace` | `WALUIGI_CONFIG` (JSON) | Task config dict |
| `context.task_id` | `str` | `WALUIGI_TASK_ID` | Current task ID |
| `context.job_id` | `str` | `WALUIGI_JOB_ID` | Current job ID |
| `context.namespace` | `str` | `WALUIGI_CATALOG_NAMESPACE` | Current namespace |

### Usage

```python
from waluigi.sdk.context import context

# Required params
date   = context.params.date
source = context.params.source

# Optional params with default
limit = int(getattr(context.params, 'limit', 1000))

# Attributes
owner = context.attributes.owner

# Structured config
catalog_source = context.config.catalog_source
expectations   = context.config.expectations       # list of dicts
```

Key-access is via attribute syntax. Keys in `params`/`attributes` are lowercased (e.g., `WALUIGI_PARAM_SOURCE_ID` → `context.params.source_id`). Accessing a non-existent key raises `AttributeError`; use `getattr(context.params, 'key', default)` for optional values.

---

## CatalogClient

The primary interface for dataset I/O, source management, and metadata.

```python
from waluigi.sdk.catalog import CatalogClient

# From within a task (env vars are auto-injected)
catalog = CatalogClient()

# Explicit configuration
catalog = CatalogClient(
    url="http://catalog:9000",
    namespace="analytics",
)
```

`url` defaults to `WALUIGI_CATALOG_URL`. `namespace` defaults to `WALUIGI_CATALOG_NAMESPACE`.

### Sources

```python
# List
sources = catalog.list_sources()

# Get
source = catalog.get_source("local-storage")

# Create or update (upsert)
catalog.create_source(
    id="pg-dwh",
    type="sql",
    config={"url": "postgresql+psycopg2://user:pw@host/db"},
    description="PostgreSQL DWH",
)
```

### Datasets

```python
# List (with optional filters)
datasets = catalog.list_datasets()
datasets = catalog.list_datasets(status="approved")
datasets = catalog.list_datasets(description="orders")

# Get
dataset = catalog.get_dataset("sales/clean/orders")

# Create
handle = catalog.create_dataset(
    id="sales/clean/orders",
    format="parquet",               # parquet | csv | json
    source_id="local",
    description="Completed orders",
)
```

### Reading a dataset

`read_dataset` returns a `DatasetReader` for the latest committed version (or a specific one):

```python
reader = catalog.read_dataset("sales/raw/orders")
df = reader.read()

# Specific version
reader = catalog.read_dataset("sales/raw/orders", version="2026-06-12T06:00:00.000+00:00")
```

`reader.version` — version string  
`reader.dataset_id` — full dataset ID  
`reader.location` — physical file location  
`reader.fmt` — `DatasetFormat` enum

### Writing a dataset (two-phase commit)

```python
handle = catalog.create_dataset("sales/clean/orders", format="parquet", source_id="local")

with handle.create_version(
    metadata={"date": "2026-06-12", "run_id": context.job_id},
    inputs=[reader_a, reader_b],    # record lineage
    force=False,                    # skip if identical metadata already exists
) as writer:
    writer.write(df_clean)

# After context exit:
print(writer.version)       # "2026-06-12T06:00:00.123+00:00"
print(writer.dataset_id)    # "sales/clean/orders"
print(writer.skipped)       # True if metadata dedup triggered
```

The context manager:
1. Calls `_reserve` → gets write location and version
2. Writes the file via the connector
3. Calls `_commit` → verifies file, computes checksum, infers schema, records lineage
4. On exception → calls `_fail` to roll back the version

### Browsing

```python
result = catalog.list_folders("sales/")
# {"prefixes": ["sales/clean/", "sales/raw/"], "datasets": [...]}
```

---

## Storage Connectors

Connectors abstract the physical storage layer. They are instantiated internally by `CatalogClient` based on the source type and config. You interact with them indirectly via `DatasetReader.read()` and `DatasetWriter.write()`.

### LocalConnector (`type: local`)

Reads/writes files on the local filesystem, rooted at the Catalog's `--data-path`.

```python
# Source creation
catalog.create_source(id="local", type="local", config={})
```

Supported formats: `csv`, `tsv`, `parquet`, `json`

No special config required.

### S3Connector (`type: s3`)

Reads/writes files on Amazon S3 or any S3-compatible endpoint (MinIO, Ceph, etc.).

```python
catalog.create_source(
    id="s3-prod",
    type="s3",
    config={
        "aws_access_key_id":     "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region_name":           "eu-west-1",
        "endpoint_url":          "http://minio:9000",   # omit for real S3
    },
)
```

**Location format:** `s3://bucket/key/path.parquet`

Supported formats: `csv`, `parquet`, `json`, `pkl`, `xls`, `xlsx`

### SQLConnector (`type: sql`)

Reads/writes database tables via SQLAlchemy.

```python
catalog.create_source(
    id="pg-dwh",
    type="sql",
    config={
        "url": "postgresql+psycopg2://user:password@host:5432/database",
    },
)
```

**SQLAlchemy DSN examples:**

| Database | DSN format |
|----------|-----------|
| PostgreSQL | `postgresql+psycopg2://user:pw@host/db` |
| MySQL | `mysql+pymysql://user:pw@host/db` |
| SQLite | `sqlite:////absolute/path/to/db.sqlite` |
| SQL Server | `mssql+pyodbc://user:pw@host/db?driver=ODBC+Driver+17+for+SQL+Server` |

**Location format:** Table name (e.g., `public.orders`) or schema-qualified name.

**Virtual datasets:** Location can be a full `SELECT` query used as a subquery.

### SFTPConnector (`type: sftp`)

Reads/writes files on a remote SFTP server.

```python
catalog.create_source(
    id="sftp-archive",
    type="sftp",
    config={
        "host":         "sftp.example.com",
        "port":         22,
        "username":     "data_user",
        # Either password or key:
        "password":     "secret",
        "key_filename": "/home/user/.ssh/id_rsa",
    },
)
```

**Location format:** Remote file path (e.g., `/data/archive/orders.csv`)

Supported formats: `csv`, `parquet`, `json`

---

## DQManager

Runs a DQ suite against one or more DataFrames.

```python
from waluigi.sdk.dataquality import DQManager

dq = DQManager()

result = dq.run_suite(
    suite_path="/rules/suites/sales_dq_suite.yaml",
    datasets={"this": df},          # dataset inputs keyed by name
)

print(result.success)               # True only if all rules pass
print(f"{result.passed}/{result.total} rules passed (score: {result.score:.2f})")

for r in result.results:
    status = "✅" if r.success else "❌"
    print(f"{status} {r.rule_id}: score={r.score:.2f}")
    if not r.success and r.failed_indices:
        print(f"   Failed rows: {r.failed_indices[:5]}")
```

### SuiteResult attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `success` | `bool` | True only if all rules pass |
| `score` | `float` | `passed / total` (0.0–1.0) |
| `total` | `int` | Total number of rules |
| `passed` | `int` | Rules that passed |
| `failed` | `int` | Rules that failed |
| `results` | `list[RuleResult]` | Per-rule details |

### RuleResult attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `rule_id` | `str` | Rule identifier |
| `success` | `bool` | Pass/fail |
| `score` | `float \| None` | Fraction of passing rows |
| `failed_indices` | `list` | Row indices that failed |
| `error` | `str \| None` | Exception message if rule errored |

→ See [data-quality.md](data-quality.md) for rule and suite YAML formats.

---

## Complete task example

A full ETL task using the SDK:

```python
# pipeline/clean_orders.py
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import CatalogClient
from waluigi.sdk.dataquality import DQManager

catalog = CatalogClient()
date = context.params.date

# ── Read ──────────────────────────────────────────────────────────
reader = catalog.read_dataset("sales/raw/orders")
df = reader.read()
print(f"Read {len(df)} rows from {reader.dataset_id}@{reader.version}")

# ── Transform ─────────────────────────────────────────────────────
df = df[df["date"] == date].copy()
df["revenue"] = df["quantity"] * df["unit_price"] * (1 - df["discount"])
df = df.dropna(subset=["order_id", "customer_id"])

# ── Quality check ─────────────────────────────────────────────────
dq = DQManager()
result = dq.run_suite("/rules/suites/orders_suite.yaml", datasets={"this": df})
if not result.success:
    raise ValueError(f"DQ failed: {result.failed}/{result.total} rules failed")

# ── Write ─────────────────────────────────────────────────────────
handle = catalog.create_dataset(
    "sales/clean/orders",
    format="parquet",
    source_id="local",
    description="Cleaned and validated orders",
)

with handle.create_version(
    metadata={"date": date, "job_id": context.job_id},
    inputs=[reader],
) as writer:
    writer.write(df)

if writer.skipped:
    print(f"Skipped — version already exists: {writer.version}")
else:
    print(f"Written {len(df)} rows → {writer.dataset_id}@{writer.version}")
```

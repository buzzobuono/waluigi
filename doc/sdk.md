# SDK Reference

The Waluigi SDK (`waluigi.sdk`) provides the Python interface for task scripts to interact with the runtime environment and the Catalog.

---

## context

`context` is a singleton populated at import time from the environment variables injected by the Worker (or `wlrun`). Import it once at the top of any task script.

```python
from waluigi.sdk.context import context
```

### Properties

| Property | Type | Source env var | Description |
|----------|------|----------------|-------------|
| `context.params` | `SimpleNamespace` | `WALUIGI_PARAM_*` | Task params — lowercase keys |
| `context.attributes` | `SimpleNamespace` | `WALUIGI_ATTRIBUTE_*` | Task attributes — lowercase keys |
| `context.config` | `SimpleNamespace` | `WALUIGI_CONFIG` (JSON) | Task config dict, deeply converted to `SimpleNamespace` |

Keys in `params` and `attributes` are lowercased: `WALUIGI_PARAM_SOURCE_ID` → `context.params.source_id`.

**Runtime values not on `context`** — read directly from the environment:

```python
import os

job_id    = os.environ.get("WALUIGI_JOB_ID",  "unknown")
task_id   = os.environ.get("WALUIGI_TASK_ID", "unknown")
namespace = os.environ.get("WALUIGI_CATALOG_NAMESPACE", "")
```

### Secrets

Secrets stored in Waluigi are injected as `WALUIGI_SECRET_{KEY_UPPER}` env vars:

```python
import os

api_token  = os.environ["WALUIGI_SECRET_API_TOKEN"]
db_passwd  = os.environ.get("WALUIGI_SECRET_DB_PASSWORD", "")
```

Secrets can also be referenced in `config` with `${WALUIGI_SECRET_KEY}` placeholders — the Worker expands them before serializing `WALUIGI_CONFIG`.

### Usage

```python
from waluigi.sdk.context import context
import os

# Required params
date   = context.params.date
source = context.params.source

# Optional param with default
limit = int(getattr(context.params, 'limit', 1000))

# Structured config (dict → SimpleNamespace recursively)
url       = context.config.http.url
page_size = context.config.http.page_size

# Config list stays as a plain Python list
for exp in context.config.expectations:   # list[dict]
    print(exp["rule_id"])

# Runtime env vars
job_id = os.environ.get("WALUIGI_JOB_ID", "unknown")

# Secrets
token = os.environ["WALUIGI_SECRET_API_TOKEN"]
```

Accessing a non-existent attribute raises `AttributeError`; use `getattr(context.params, 'key', default)` for optional values.

---

## CatalogClient

The primary interface for dataset I/O, source management, and metadata.

```python
from waluigi.sdk.catalog import CatalogClient

# From within a task — env vars are auto-injected by the Worker
catalog = CatalogClient()

# Explicit configuration (useful for testing)
catalog = CatalogClient(
    url="http://catalog:9000",
    namespace="analytics",
)
```

`url` defaults to `WALUIGI_CATALOG_URL` (default: `http://localhost:9000`).  
`namespace` defaults to `WALUIGI_CATALOG_NAMESPACE`.

A module-level singleton is also available for convenience:

```python
from waluigi.sdk.catalog import catalog   # pre-built CatalogClient()
```

Use the singleton in task scripts that don't need custom configuration.

### Exceptions

| Exception | When |
|-----------|------|
| `CatalogError` | Catalog returns `result: KO` or HTTP 4xx/5xx |
| `CatalogWarning` | Catalog returns `result: WARN` (logged, not raised) |

```python
from waluigi.sdk.catalog import CatalogClient, CatalogError

try:
    reader = catalog.read_dataset("missing/dataset")
except CatalogError as e:
    print(f"Dataset not found: {e}")
```

### Sources

```python
# List
sources = catalog.list_sources()           # list[dict]

# Get
source = catalog.get_source("local")       # dict

# Create or update (upsert by id)
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
dataset = catalog.get_dataset("sales/clean/orders")   # dict

# Create or upsert — returns a DatasetHandle
handle = catalog.create_dataset(
    id="sales/clean/orders",
    format="parquet",          # parquet | csv | json | tsv
    source_id="local",
    description="Completed orders",
)
```

### Reading a dataset

`read_dataset` returns a `DatasetReader` for the latest committed version (or a specific one).

```python
# Latest version
reader = catalog.read_dataset("sales/raw/orders")
df = reader.read()                        # → pd.DataFrame

# Specific version
reader = catalog.read_dataset(
    "sales/raw/orders",
    version="2026-06-12T06:00:00.000+00:00",
)

# Pagination (useful for large datasets)
df_page = reader.read(limit=10000, offset=0)
```

**DatasetReader attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `reader.dataset_id` | `str` | Full dataset ID |
| `reader.version` | `str` | ISO 8601 timestamp string |
| `reader.location` | `str` | Physical file or table location |
| `reader.format` | `DatasetFormat` | Enum value |

### Writing a dataset (two-phase commit)

```python
import os
from waluigi.sdk.catalog import catalog

handle = catalog.create_dataset(
    "sales/clean/orders",
    format="parquet",
    source_id="local",
)

with handle.create_version(
    metadata={"date": "2026-06-12", "job_id": os.environ.get("WALUIGI_JOB_ID")},
    inputs=[{"dataset_id": reader.dataset_id, "version": reader.version}],
    force=False,   # skip write if identical metadata already committed (idempotent)
) as writer:
    writer.write(df_clean)

# After the with block:
print(writer.version)    # "2026-06-12T06:00:00.123+00:00"
print(writer.dataset_id) # "sales/clean/orders"
print(writer.skipped)    # True if metadata dedup triggered (force=False)
```

The context manager implements the two-phase commit:

1. `_reserve` → gets write location + version string
2. `writer.write(data)` → writes via the connector
3. `_commit` on clean exit → verifies file, computes checksum, infers schema, records lineage
4. `_fail` on exception → rolls back the version record

**`inputs` format** — list of dicts recording upstream lineage:

```python
# Single input
inputs=[{"dataset_id": reader.dataset_id, "version": reader.version}]

# Multiple inputs
inputs=[
    {"dataset_id": reader_a.dataset_id, "version": reader_a.version},
    {"dataset_id": reader_b.dataset_id, "version": reader_b.version},
]

# External source (no catalog version)
inputs=[{"dataset_id": "__external__/https://api.example.com", "version": "latest"}]
```

**`writer.write(data)` accepted types:**

| Type | Notes |
|------|-------|
| `pd.DataFrame` | Most common |
| `pa.Table` | PyArrow table |
| `list[dict]` | Converted to DataFrame |
| `dict[str, list]` | Converted to DataFrame |
| `Iterator[pd.DataFrame]` | Streaming — chunks written incrementally |
| `Iterator[list[dict]]` | Streaming |

Returns the number of rows written (`int`). Returns `0` if `writer.skipped`.

### Versions

```python
# List committed versions (newest first)
versions = catalog.list_versions("sales/clean/orders")
# [{"version": "2026-06-12T...", "location": "...", "rows": 1500, ...}, ...]

# Version metadata
meta = catalog.get_version_metadata("sales/clean/orders", version="2026-06-12T...")
```

### Lineage

```python
lineage = catalog.get_lineage("sales/clean/orders", version="2026-06-12T...")
# {"upstream": [...], "downstream": [...]}
```

### Browsing

```python
result = catalog.list_folders("sales/")
# {"prefixes": ["sales/clean/", "sales/raw/"], "datasets": [...]}

result = catalog.list_folders("")   # root
```

### DQ Expectations (programmatic)

```python
# List
expectations = catalog.list_expectations("sales/clean/orders")

# Add
catalog.add_expectation(
    "sales/clean/orders",
    rule_id="expect_column_values_to_not_be_null",
    inputs={"x": "this.order_id"},
    tolerance=1.0,
)
```

---

## DatasetHandle

Returned by `catalog.create_dataset()`. Use to produce versions and manage DQ/charts.

```python
handle = catalog.create_dataset("gold/kpi", format="parquet", source_id="local")

# Replace all DQ expectations
handle.set_expectations([
    {"rule_id": "expect_column_values_to_not_be_null",
     "inputs":  {"x": "this.id"},
     "tolerance": 1.0},
    {"rule_id": "expect_column_values_to_be_between",
     "inputs":  {"x": "this.amount"},
     "params":  {"min_val": 0, "max_val": 1_000_000},
     "tolerance": 0.99},
])

# Upsert a chart (by key)
handle.set_chart(
    key="revenue_by_region",
    title="Revenue by Region",
    spec={"type": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "agg": "sum"}},
)

# Open a writer
with handle.create_version(metadata={"date": "2026-06-12"}) as writer:
    writer.write(df)
```

---

## Storage Connectors

Connectors abstract the physical storage layer. Instantiated automatically by `CatalogClient` based on source type and config. You interact with them indirectly via `DatasetReader.read()` and `DatasetWriter.write()`.

### LocalConnector (`type: local`)

Reads and writes files on the Catalog server's local filesystem (rooted at `--data-path`).

```python
catalog.create_source(id="local", type="local", config={})
```

Supported formats: `csv`, `tsv`, `parquet`, `json`

Streaming is supported for all formats: Parquet uses a `ParquetWriter`; CSV/TSV appends chunks; JSON uses JSON-Lines.

### S3Connector (`type: s3`)

Reads and writes objects on Amazon S3 or any S3-compatible store (MinIO, Ceph, Garage, etc.).

```python
catalog.create_source(
    id="s3-prod",
    type="s3",
    config={
        "aws_access_key_id":     "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region_name":           "eu-west-1",
        "endpoint_url":          "http://minio:9000",   # omit for real AWS S3
    },
)
```

**Location format:** `s3://bucket/path/to/file.parquet`

Supported formats: `csv`, `parquet`, `json`, `pkl` / `pickle`, `xls` / `xlsx`

### SQLConnector (`type: sql`)

Reads and writes database tables via SQLAlchemy. Location is a table name (optionally schema-qualified) or a raw `SELECT` query.

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

| Database | DSN |
|----------|-----|
| PostgreSQL | `postgresql+psycopg2://user:pw@host/db` |
| MySQL | `mysql+pymysql://user:pw@host/db` |
| SQLite | `sqlite:////absolute/path/to/file.db` |
| SQL Server | `mssql+pyodbc://user:pw@host/db?driver=ODBC+Driver+17+for+SQL+Server` |

**Location:** table name (e.g. `orders` or `public.orders`) or a `SELECT` query for virtual datasets.

Each committed version writes to an auto-named table `{dataset_last_segment}__{version_compact}`.

### SFTPConnector (`type: sftp`)

> **Note:** The SFTP connector is implemented (`waluigi/sdk/connectors/sftp.py`) but not currently registered in `ConnectorFactory`. Register it manually if needed:
> ```python
> from waluigi.sdk.connectors.factory import ConnectorFactory
> from waluigi.sdk.connectors.sftp import SFTPConnector
> from waluigi.catalog.api.schemas import SourceType
> ConnectorFactory.register(SourceType.SFTP, SFTPConnector)
> ```

```python
catalog.create_source(
    id="sftp-archive",
    type="sftp",
    config={
        "host":         "sftp.example.com",
        "port":         22,
        "username":     "data_user",
        "password":     "secret",         # or use key_filename
        "key_filename": "/home/user/.ssh/id_rsa",
    },
)
```

**Location:** remote file path (e.g. `/data/archive/orders.csv`)

Supported formats: `csv`, `parquet`, `json`

### Custom connector

```python
from waluigi.sdk.connectors.base import BaseConnector
from waluigi.sdk.connectors.factory import ConnectorFactory
from waluigi.catalog.api.schemas import SourceType

class MyConnector(BaseConnector):
    def exists(self, location): ...
    def checksum(self, location): ...
    def resolve_location(self, dataset_id, version, format, data_path): ...
    def write(self, location, format, data): ...
    def delete(self, location): ...
    def read(self, location, format, limit=None, offset=0): ...
    def infer_schema(self, location): ...

ConnectorFactory.register(SourceType.LOCAL, MyConnector)  # override an existing type
```

---

## DQManager

Runs a DQ suite against one or more DataFrames. Used server-side by the Catalog, but available for direct use in advanced task scripts.

```python
from waluigi.sdk.dataquality import DQManager

dq = DQManager(rules_path="/path/to/rules/")   # directory containing .yaml rule files

result = dq.run_suite(
    suite_path="/path/to/suites/orders_suite.yaml",
    datasets={"this": df},    # keys match the dataset references in suite inputs
)

print(result.success)          # True only if ALL rules pass
print(result.score)            # passed/total as float (0.0–1.0)
print(f"{result.passed}/{result.total} rules passed")

for r in result.results:
    status = "✅" if r.success else "❌"
    print(f"{status} {r.rule_id}: score={r.score:.2%}")
    if not r.success:
        if r.error:
            print(f"   Error: {r.error}")
        elif r.failed_indices:
            print(f"   Failed row indices: {r.failed_indices[:5]}")

# Utility methods
dq.print_report(result)                # formatted summary to stdout
dq.describe_rule("expect_column_values_to_not_be_null")
dq.list_rules()
```

### SuiteResult

| Attribute | Type | Description |
|-----------|------|-------------|
| `success` | `bool` | `True` only if every rule passed |
| `score` | `float` | `passed / total` (0.0–1.0) |
| `total` | `int` | Total rules evaluated |
| `passed` | `int` | Rules that passed |
| `failed` | `int` | Rules that failed |
| `results` | `list[RuleResult]` | Per-rule detail |

### RuleResult

| Attribute | Type | Description |
|-----------|------|-------------|
| `rule_id` | `str` | Rule identifier |
| `success` | `bool` | Pass/fail relative to `tolerance` |
| `score` | `float \| None` | Fraction of rows that passed |
| `failed_indices` | `list` | Row indices that failed (empty if all passed) |
| `error` | `str \| None` | Exception message if the rule errored |

→ See [data-quality.md](data-quality.md) for rule and suite YAML formats.

---

## Complete task example

```python
# pipeline/clean_orders.py
import os
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import catalog   # module-level singleton

date   = context.params.date
job_id = os.environ.get("WALUIGI_JOB_ID", "unknown")

# ── Read ──────────────────────────────────────────────────────────────────────
reader = catalog.read_dataset("sales/raw/orders")
df = reader.read()
print(f"Read {len(df)} rows from {reader.dataset_id}@{reader.version}")

# ── Transform ─────────────────────────────────────────────────────────────────
df = df[df["date"] == date].copy()
df["revenue"] = df["quantity"] * df["unit_price"] * (1 - df["discount"])
df = df.dropna(subset=["order_id", "customer_id"])

# ── Write ─────────────────────────────────────────────────────────────────────
handle = catalog.create_dataset(
    "sales/clean/orders",
    format="parquet",
    source_id="local",
    description="Cleaned and validated orders",
)

with handle.create_version(
    metadata={"date": date, "job_id": job_id},
    inputs=[{"dataset_id": reader.dataset_id, "version": reader.version}],
    force=False,    # idempotent: skip if same metadata already committed
) as writer:
    writer.write(df)

if writer.skipped:
    print(f"Skipped — version already exists: {writer.version}")
else:
    print(f"Written {len(df)} rows → {writer.dataset_id}@{writer.version}")
```

### Streaming write (large datasets)

```python
def generate_chunks():
    for chunk_df in pd.read_csv("huge.csv", chunksize=50_000):
        yield chunk_df

handle = catalog.create_dataset("big/table", format="parquet", source_id="local")
with handle.create_version(metadata={"date": date}) as writer:
    rows = writer.write(generate_chunks())   # writes incrementally via ParquetWriter
print(f"Written {rows} rows")
```

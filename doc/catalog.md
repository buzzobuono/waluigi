# Catalog Service

The Catalog is an optional but powerful companion to the orchestrator. It tracks dataset metadata, physical versions, schema, data quality results, lineage, and charts — all accessible via REST API and the Console UI.

All Catalog resources are scoped to a namespace: `/namespaces/{namespace}/...`.

---

## Core concepts

```
Namespace
  └── Source (storage backend: local, S3, SQL, SFTP)
        └── Dataset (logical data asset: id path, format, status)
              └── Version (physical snapshot: ISO timestamp, location, checksum)
                    ├── Schema (column definitions, types, PII flags)
                    ├── Lineage (upstream/downstream versions)
                    ├── DQ results (per-version quality score)
                    └── Metadata (key-value pairs)
              ├── Expectations (DQ rules attached to the dataset)
              └── Charts (ECharts visualisation definitions)
```

---

## Sources

A source represents a storage backend. It holds the connection config for a connector. Every dataset belongs to a source.

### Source types and config

| Type | Description | Config fields |
|------|-------------|---------------|
| `local` | Local filesystem (relative to `--data-path`) | none |
| `s3` | Amazon S3 or S3-compatible (MinIO) | `aws_access_key_id`, `aws_secret_access_key`, `region_name`, `endpoint_url` |
| `sql` | Any SQL database via SQLAlchemy | `url` (DSN string) |
| `sftp` | SFTP server | `host`, `port`, `username`, `password` or `key_filename` |
| `api` | REST API (for materialisation) | connection details per endpoint |

### Source API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/sources` | List sources |
| `POST` | `/namespaces/{ns}/sources` | Register or update (upsert by ID) |
| `GET` | `/namespaces/{ns}/sources/{id}` | Get source details |
| `PATCH` | `/namespaces/{ns}/sources/{id}` | Update source |
| `DELETE` | `/namespaces/{ns}/sources/{id}` | Delete source |

### Create source (SDK)

```python
catalog.create_source(
    id="pg-dwh",
    type="sql",
    config={"url": "postgresql+psycopg2://user:pw@host/db"},
    description="PostgreSQL DWH",
)
```

---

## Datasets

A dataset is a logical data asset identified by a hierarchical path (e.g., `sales/raw/orders`). It tracks the format, lifecycle status, associated source, and DQ suite.

### Dataset lifecycle

```
draft ──► in_review ──► approved ──► deprecated
```

| Status | Meaning |
|--------|---------|
| `draft` | Created; not yet reviewed |
| `in_review` | Submitted for review |
| `approved` | Approved; schema is published |
| `deprecated` | No longer in use |

`_approve` publishes the schema (all columns transition to `published` status) and moves the dataset to `approved`.

### Dataset ID paths

Dataset IDs use forward-slash hierarchy: `namespace/area/name` (e.g., `analytics/erp/clean/transactions`). The `/folders/{prefix}/` endpoint provides S3-style virtual prefix navigation.

### Dataset API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets` | List datasets (filter: `?status=`, `?description=`) |
| `POST` | `/namespaces/{ns}/datasets` | Create dataset |
| `GET` | `/namespaces/{ns}/datasets/{id}` | Get dataset metadata |
| `PATCH` | `/namespaces/{ns}/datasets/{id}` | Update description, status, dq_suite |
| `DELETE` | `/namespaces/{ns}/datasets/{id}` | Delete dataset |
| `POST` | `/namespaces/{ns}/datasets/{id}/_approve` | Approve + publish schema |

---

## Versions (two-phase commit)

Writing to a dataset is a two-phase operation to ensure consistency: reserve a slot, write the file externally, then commit.

```
Phase 1: POST /_reserve
  → returns: version (timestamp), write_location

  [client writes the file to write_location]

Phase 2: POST /_commit/{version}
  → Boss verifies file, computes SHA-256 checksum, infers schema
  → records lineage, DQ results
  → returns: committed version
```

The SDK handles both phases transparently:

```python
handle = catalog.create_dataset("sales/clean/orders", format="parquet", source_id="local")
with handle.create_version(metadata={"date": "2026-06-12"}, inputs=[reader]) as writer:
    writer.write(df)
# commit happens automatically on context exit
```

If the `with` block raises an exception, the version is marked `FAILED` and rolled back.

### Metadata deduplication

Pass `metadata` to `create_version()`. If a version with identical metadata already exists and `force=False`, the commit is skipped (`writer.skipped = True`). Useful for idempotent reruns.

### Version API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/versions` | List versions (newest first) |
| `POST` | `/namespaces/{ns}/datasets/{id}/_reserve` | Phase 1: reserve |
| `POST` | `/namespaces/{ns}/datasets/{id}/_commit/{version}` | Phase 2: commit |
| `POST` | `/namespaces/{ns}/datasets/{id}/_fail/{version}` | Mark version as failed |
| `GET` | `/namespaces/{ns}/datasets/{id}/_preview/{version}` | Preview rows |
| `DELETE` | `/namespaces/{ns}/datasets/{id}/_deprecate/{version}` | Deprecate version |
| `POST` | `/namespaces/{ns}/datasets/{id}/_register-virtual` | Register a virtual dataset |

---

## Schema

On commit, Waluigi infers schema automatically from the data file (column names, physical types). Schema can then be enriched with semantic metadata.

### Schema column status

```
inferred ──► draft ──► published
```

| Status | Meaning |
|--------|---------|
| `inferred` | Automatically detected on commit |
| `draft` | Manually edited but not yet published |
| `published` | Approved and visible in the Console |

`POST /schema/publish` transitions all columns to `published`.

### Schema API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/schema` | Get full schema |
| `PATCH` | `/namespaces/{ns}/datasets/{id}/schema/{column}` | Update column metadata |
| `DELETE` | `/namespaces/{ns}/datasets/{id}/schema/{column}` | Delete column |
| `POST` | `/namespaces/{ns}/datasets/{id}/schema/publish` | Publish all columns |

### Column metadata fields

| Field | Type | Description |
|-------|------|-------------|
| `logical_type` | string | Semantic type: `integer`, `float`, `string`, `date`, `timestamp`, … |
| `description` | string | Human-readable column description |
| `nullable` | bool | Whether the column can contain nulls |
| `pii` | bool | Whether the column contains personal data |
| `pii_type` | string | `none` \| `direct` \| `indirect` \| `sensitive` |
| `pii_notes` | string | Additional PII context |
| `tags` | list[string] | Arbitrary tags |

---

## Lineage

Lineage is recorded at the version level. When a task writes a dataset using the SDK with `inputs=[reader, ...]`, the Catalog records:

- Which upstream versions were read (input lineage)
- Which task and job produced this version

```python
reader_a = catalog.read_dataset("sales/raw/orders")
reader_b = catalog.read_dataset("products/master")

with handle.create_version(metadata={...}, inputs=[reader_a, reader_b]) as writer:
    writer.write(df_result)
# lineage: sales/clean/orders@version ← sales/raw/orders@v1, products/master@v2
```

### Lineage API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/lineage/{version}` | Get upstream + downstream |

**Response:**

```json
{
  "dataset_id": "analytics/clean/orders",
  "version": "2026-06-12T06:00:00.000+00:00",
  "upstream": [
    {"dataset_id": "analytics/raw/orders", "version": "...", "task_id": "extract"}
  ],
  "downstream": [
    {"dataset_id": "analytics/report/summary", "version": "...", "task_id": "aggregate"}
  ]
}
```

---

## Data Quality

DQ expectations define the quality rules for a dataset. Results are computed and stored per version during the commit phase.

### Attaching expectations

Via the API or SDK, or via the `CatalogSetExpectations` built-in task.

```python
catalog.add_expectation(
    dataset_id="sales/clean/orders",
    rule_id="expect_column_values_to_not_be_null",
    inputs={"x": "this.order_id"},
    tolerance=1.0,
)
```

### DQ results

Results are stored per version and include an overall score (`passed / total`) and per-rule pass/fail details.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/expectations` | List expectations |
| `POST` | `/namespaces/{ns}/datasets/{id}/expectations` | Add expectation |
| `PATCH` | `/namespaces/{ns}/datasets/{id}/expectations/{id}` | Update |
| `DELETE` | `/namespaces/{ns}/datasets/{id}/expectations/{id}` | Delete |
| `GET` | `/namespaces/{ns}/datasets/{id}/dq/{version}` | Get DQ results for version |
| `GET` | `/dq/rules` | List available rule catalog |
| `GET` | `/dq/suite?path=...` | Parse suite YAML |

→ See [data-quality.md](data-quality.md) for rule formats and available rules.

---

## Charts

Charts are ECharts-based visualisation definitions attached to a dataset. They are rendered on demand against the latest (or specified) version.

Supported chart types: `bar`, `line`, `pie`, `histogram`, `scatter`.

```python
catalog.create_chart(
    dataset_id="analytics/report/summary",
    key="revenue_by_region",
    title="Revenue by Region",
    spec={
        "type": "bar",
        "x": {"field": "region"},
        "y": {"field": "revenue", "agg": "sum"},
    },
)
```

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/charts` | List charts |
| `POST` | `/namespaces/{ns}/datasets/{id}/charts` | Create chart |
| `PATCH` | `/namespaces/{ns}/datasets/{id}/charts/{chart_id}` | Update |
| `DELETE` | `/namespaces/{ns}/datasets/{id}/charts/{chart_id}` | Delete |
| `GET` | `/namespaces/{ns}/datasets/{id}/charts/{id}/render` | Render (ECharts option JSON) |

---

## Dataset Browse (Folders)

Datasets are browsable via a virtual folder hierarchy based on the ID path.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/folders/{prefix}/` | List datasets and sub-prefixes |

```
GET /namespaces/analytics/folders/erp/
→ {
    "prefixes": ["erp/clean/", "erp/raw/", "erp/report/"],
    "datasets": []
  }

GET /namespaces/analytics/folders/erp/clean/
→ {
    "prefixes": [],
    "datasets": [
      {"id": "erp/clean/transactions", "format": "parquet", "status": "approved", ...}
    ]
  }
```

---

## Metadata

Per-version key-value metadata (arbitrary string values). System keys with prefix `sys.` are reserved.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/datasets/{id}/versions/{version}/metadata` | Get all metadata |
| `POST` | `/namespaces/{ns}/datasets/{id}/versions/{version}/metadata` | Set key |
| `DELETE` | `/namespaces/{ns}/datasets/{id}/versions/{version}/metadata/{key}` | Delete key |

---

## Materialisation

Fetch a REST API endpoint and store the result as a CSV version of a dataset. Useful for API-sourced datasets.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/namespaces/{ns}/datasets/{id}/_materialize` | Fetch and store |

```json
{
  "source_id": "local",
  "base_url": "https://api.example.com",
  "endpoint": "/v1/orders",
  "params": {"limit": 1000},
  "description": "Orders snapshot"
}
```

---

## Virtual datasets

Virtual datasets point to a SQL query or external resource without a physical file. Register via `_register-virtual`:

```json
{
  "source_id": "pg-dwh",
  "location": "SELECT * FROM finance.orders WHERE year = 2026",
  "format": "sql",
  "description": "Finance orders 2026"
}
```

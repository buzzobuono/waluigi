"""
Waluigi Catalog v2
==================
Data catalog service — datasets, virtual sources, schema governance, lineage.

Dataset identity
----------------
Every dataset has a single slash-separated id, e.g. "sales/raw/sales_raw".
There are no separate collection entities. Navigation is virtual — listing
by prefix, exactly like S3 object storage.

Response contract (always):
    {
        "data":       <payload | null>,
        "diagnostic": {
            "result":   "OK" | "WARN" | "KO",
            "messages": ["..."]
        }
    }

HTTP status codes:
    200 / 201  →  OK or WARN  (operation succeeded, possibly with warnings)
    404        →  KO          (resource not found)
    409        →  KO          (state conflict)
    422        →  KO          (unprocessable)
    500        →  KO          (unexpected server error)
"""

import os
import sys
import csv
import socket
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import configargparse
import httpx
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from waluigi.core.catalog_db import CatalogDB
from waluigi.responses import ok, warn, ko

# ---------------------------------------------------------------------------
# App & config
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Waluigi Catalog v2",
    description=__doc__,
    version="2.0.0",
)

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_CATALOG_")
p.add("--port",         type=int, default=9000)
p.add("--host",         default=socket.gethostname())
p.add("--bind-address", default="0.0.0.0")
p.add("--db-path",      default=os.path.join(os.getcwd(), "db/catalog.db"))
p.add("--data-path",    default=os.path.join(os.getcwd(), "data"))
p.add("--scan",         action="store_true", default=False)
p.add("--scan-path",    default=None)
p.add("--scan-prefix",  default=None,
      help="Dataset id prefix to assign scanned files")
args = p.parse_args()

DATA_PATH = args.data_path
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out",
}


def log(msg: str):
    print(f"[Catalog 📦] {msg}", flush=True)


try:
    db = CatalogDB(args.db_path)
    log(f"Database ready: {args.db_path}")
except Exception as e:
    log(f"❌ Critical DB error: {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _version_id() -> str:
    return _now()


def _local_path(dataset_id: str, version: str, fmt: str) -> str:
    safe_ver = version.replace(":", "-")
    ext = f".{fmt}" if fmt else ""
    d = os.path.join(DATA_PATH, dataset_id)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{safe_ver}{ext}")


def _compute_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _infer_schema(path: str, fmt: str) -> list[dict]:
    try:
        if fmt in ("csv", "tsv"):
            df = pd.read_csv(path, sep="\t" if fmt == "tsv" else ",", nrows=1000)
        elif fmt == "parquet":
            df = pd.read_parquet(path)
        elif fmt in ("xls", "xlsx"):
            df = pd.read_excel(path, nrows=1000)
        else:
            return []

        type_map = {
            "int64": "integer", "int32": "integer",
            "float64": "decimal", "float32": "decimal",
            "bool": "boolean", "datetime64[ns]": "datetime",
            "object": "string",
        }
        return [
            {"name": col,
             "physical_type": str(df[col].dtype),
             "logical_type":  type_map.get(str(df[col].dtype), "string")}
            for col in df.columns
        ]
    except Exception:
        return []


def _safe_json_value(v):
    import numpy as np
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (float, int, np.number)):
        if not np.isfinite(float(v)):
            return None
        return v
    return v


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SourceUpdateRequest(BaseModel):
    type:        Optional[str]            = None
    config:      Optional[Dict[str, Any]] = None
    description: Optional[str]            = None

class DatasetUpdateRequest(BaseModel):
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None
    owner:        Optional[str]       = None
    status:       Optional[str]       = None

class SourceCreateRequest(BaseModel):
    id:          str            = Field(...,  example="pg-dwh")
    type:        str            = Field(...,  example="sql")
    config:      Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None

class ReserveRequest(BaseModel):
    format:       str            = Field("",        example="csv")
    task_id:      str            = Field("unknown", example="ingest_sales")
    job_id:       str            = Field("unknown", example="job/daily")
    source_id:    Optional[str] = None
    description:  Optional[str] = None
    owner:        Optional[str] = None
    tags:         Optional[List[str]] = None
        
class LineageRef(BaseModel):
    dataset_id: str = Field(..., example="finance/erp/fatture")
    version:    str = Field(..., example="2026-04-11T10:00:00+00:00")


class CommitRequest(BaseModel):
    rows:          Optional[int]            = None
    columns:       Optional[Dict[str, Any]] = Field(None, alias="schema")
    inputs:        List[LineageRef]         = Field(default_factory=list)
    business_meta: Dict[str, str]           = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class VirtualRegisterRequest(BaseModel):
    source_id:    str            = Field(...,   example="pg-dwh")
    location:     str            = Field(...,   example="SELECT * FROM finance.fatture")
    format:       str            = Field("sql", example="sql")
    task_id:      str            = Field("unknown")
    job_id:       str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None
    owner:        Optional[str] = None
    tags:         Optional[List[str]] = None


class SchemaColumnPatch(BaseModel):
    logical_type: Optional[str]       = None
    nullable:     Optional[bool]      = None
    pii:          Optional[bool]      = None
    pii_type:     Optional[str]       = None
    pii_notes:    Optional[str]       = None
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None


class SchemaPublishRequest(BaseModel):
    published_by: str = Field("anonymous", example="mario.rossi")


class ApproveRequest(BaseModel):
    approved_by: str  = Field(...,  example="mario.rossi")
    notes:       str  = Field("",   example="PII verified, schema confirmed")


class MetadataSetRequest(BaseModel):
    key:   str = Field(..., example="source")
    value: str = Field(..., example="SAP_EXTRACT")


class MaterializeRequest(BaseModel):
    base_url:     str            = Field(..., example="https://api.example.com")
    endpoint:     str            = Field(..., example="/v1/orders")
    params:       Dict[str, Any] = Field(default_factory=dict)
    task_id:      str            = Field("unknown")
    job_id:       str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None


class ScanRequest(BaseModel):
    data_path: Optional[str] = None
    prefix:    Optional[str] = None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _model_dump(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    else:
        return obj.dict(exclude_none=True)

def _extract_items(body) -> list:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "results", "items", "records",
                    "content", "entries", "rows"):
            if key in body and isinstance(body[key], list):
                return body[key]
        values = [v for v in body.values() if isinstance(v, list)]
        if len(values) == 1:
            return values[0]
    return []


def _next_url(body, base_url: str, endpoint: str, page: int) -> str | None:
    if not isinstance(body, dict):
        return None
    for key in ("next", "next_page", "nextPage", "nextCursor"):
        val = body.get(key)
        if val and isinstance(val, str):
            return val if val.startswith("http") else f"{base_url}{val}"
    total = body.get("total_pages") or body.get("pages") or body.get("totalPages")
    if total and page < int(total):
        return f"{base_url}{endpoint}"
    return None


def _flatten(obj, prefix="", sep="_") -> dict:
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep))
        elif isinstance(v, list):
            out[key] = (str(v) if (v and isinstance(v[0], dict))
                        else ",".join(str(i) for i in v))
        else:
            out[key] = v
    return out


async def _fetch_and_write(base_url: str, endpoint: str,
                           params: dict,
                           output_path: str) -> tuple[int, list[dict]]:
    records, page = [], 1
    next_url = f"{base_url}{endpoint}"

    async with httpx.AsyncClient(timeout=30) as client:
        while next_url:
            call_params = {**params, "page": page} if page > 1 else params
            r = await client.get(next_url, params=call_params)
            r.raise_for_status()
            body  = r.json()
            items = _extract_items(body)
            if not items:
                break
            records.extend([_flatten(item) for item in items])
            next_url = _next_url(body, base_url, endpoint, page)
            if next_url:
                page += 1
            else:
                break

    if not records:
        return 0, []

    fieldnames = list(dict.fromkeys(k for row in records for k in row.keys()))
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    schema_cols = [{"name": k, "physical_type": "string",
                    "logical_type": "string"} for k in fieldnames]
    return len(records), schema_cols


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def _scan(data_path: str, prefix: str = None) -> int:
    log(f"🔍 Scanning {data_path} ...")
    count = 0
    for root, dirs, files in os.walk(data_path):
        dirs.sort()
        for filename in sorted(files):
            ext = os.path.splitext(filename)[1].lower()
            if ext not in SCANNABLE_EXTENSIONS:
                continue

            filepath = os.path.join(root, filename)
            fmt      = ext.lstrip(".")
            rel_dir  = os.path.relpath(root, data_path).replace(os.sep, "/")
            name     = os.path.splitext(filename)[0]
            version  = name.replace("-", ":", 2)

            if prefix:
                dataset_id = f"{prefix.strip('/')}/{rel_dir}/{name}".replace("//", "/")
            else:
                dataset_id = f"{rel_dir}/{name}".replace("//", "/")

            try:
                file_hash = _compute_hash(filepath)
                schema    = _infer_schema(filepath, fmt)
                db.create_dataset(dataset_id)
                db.reserve(dataset_id, version, filepath, fmt,
                           "scanner", "scan")
                result = db.commit(dataset_id, version, file_hash, None,
                                   {c["name"]: c["physical_type"] for c in schema})
                if result and not result["skipped"]:
                    db.upsert_schema_columns(dataset_id, schema)
                count += 1
                log(f"  ✅ {dataset_id}@{version[:19]} [{fmt}]")
            except Exception as e:
                log(f"  ⚠️  Skipped {filepath}: {e}")

    log(f"🏁 Scan complete — {count} dataset(s) registered.")
    return count


# ===========================================================================
# Routes — Browse (S3-style prefix listing)
# ===========================================================================

@app.get("/folders/{prefix:path}/", tags=["Browse"],
         summary="List datasets and virtual sub-prefixes under a prefix",
         description=(
             "Trailing slash distinguishes browse from dataset access. "
             "Returns direct child datasets and deeper virtual prefixes, "
             "exactly like S3 ListObjects with a delimiter."
         ))
async def list_prefix(prefix: str):
    return ok(db.list_prefix(prefix))


# ===========================================================================
# Routes — Sources
# ===========================================================================

@app.get("/sources", tags=["Sources"],
         summary="List sources")
async def list_sources():
    return ok(db.list_sources())


@app.post("/sources", tags=["Sources"],
          summary="Register a new source",
          status_code=201)
async def create_source(body: SourceCreateRequest):
    created = db.create_source(body.id, body.type, body.config, body.description)
    if not created:
        return ko(f"Source '{body.id}' already exists", 409)
    return ok(db.get_source(body.id))


@app.get("/sources/{id}", tags=["Sources"],
         summary="Get a source details")
async def get_source(id: str):
    src = db.get_source(id)
    if not src:
        return ko("Source not found", 404)
    return ok(src)


@app.patch("/sources/{id}", tags=["Sources"],
           summary="Update a source")
async def update_source(id: str, body: SourceUpdateRequest):
    updated = db.update_source(id, **_model_dump(body))
    if not updated:
        return ko("Source not found", 404)
    return ok(db.get_source(id))


@app.delete("/sources/{id}", tags=["Sources"],
            summary="Delete a source")
async def delete_source(id: str):
    deleted = db.delete_source(id)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": id})

# ===========================================================================
# Routes — Datasets
# ===========================================================================


@app.get("/datasets", tags=["Datasets"],
    summary="List datasets",
    description="status: draft | in_review | approved | deprecated"
)
async def find_datasets(status: str | None = Query(default=None, example="draft"), 
                        description: str | None = Query(default=None, example="sales dataset")):
    valid = {"draft", "in_review", "approved", "deprecated"}
    if status and status not in valid:
        return ko(f"Invalid status. Must be one of: {', '.join(sorted(valid))}", 422)
    if not status and not description:
        return ok(db.list_datasets())
    return ok(db.find_datasets(status=status, description=description))


@app.get("/datasets/{dataset_id:path}", tags=["Datasets"],
           summary="Get a dataset detail")
async def get_dataset(dataset_id: str):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    msgs = []
    if dataset.get("status") != "approved":
        msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
    return warn(dataset, msgs) if msgs else ok(dataset)


@app.patch("/datasets/{dataset_id:path}", tags=["Datasets"],
           summary="Update a dataset")
async def update_dataset(dataset_id: str, body: DatasetUpdateRequest):
    updated = db.update_dataset(dataset_id,
                                **_model_dump(body))
    if not updated:
        return ko("Dataset not found", 404)
    return ok(db.get_dataset(dataset_id))


@app.delete("/datasets/{id}", tags=["Datasets"],
            summary="Delete a dataset")
async def delete_source(id: str):
    deleted = db.delete_dataset(id)
    if not deleted:
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})
        
######

@app.get("/datasets/{dataset_id:path}/preview/{version}",
         tags=["Versions"],
         summary="Preview rows of a local version")
async def preview(dataset_id: str, version: str,
                  limit: int = 10, offset: int = 0):
    import numpy as np
    
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    if record.get("source_type") and record["source_type"] != "local":
        return ko("Preview only available for local versions", 422)

    path = record["location"]
    fmt  = (record.get("format") or "").lower()
    if not os.path.exists(path):
        return ko(f"File not found: {path}", 404)

    try:
        if fmt == "csv":
            df = pd.read_csv(path,
                             skiprows=range(1, offset + 1),
                             nrows=limit, dtype=str)
        elif fmt == "parquet":
            print(fmt)
            df = pd.read_parquet(path).iloc[offset: offset + limit]
        else:
            return ko(f"Preview not supported for format '{fmt}'", 422)

        clean = [{k: _safe_json_value(v) for k, v in row.items()}
                 for row in df.to_dict(orient="records")]

        return ok({
            "dataset_id": dataset_id,
            "version":    version,
            "columns":    df.columns.tolist(),
            "rows":       clean,
            "pagination": {"limit": limit, "offset": offset,
                           "count": len(clean)},
        })
    except Exception as e:
        return ko(str(e), 500)


@app.get("/datasets/{dataset_id:path}/versions", tags=["Datasets"],
         summary="List all committed versions (newest first)")
async def list_versions(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok({"dataset_id": dataset_id,
               "versions":   db.get_history(dataset_id)})


@app.get("/datasets/{dataset_id:path}/resolve", tags=["Datasets"],
         summary="Resolve connection info for the latest committed version")
async def resolve(dataset_id: str):
    latest = db.get_latest(dataset_id)
    if not latest:
        return ko("Dataset not found or no committed version", 404)

    source_type = latest.get("source_type") or "local"
    source_cfg  = latest.get("source_config") or {}

    if source_type == "local":
        connection_info = {"path": latest["location"]}
    elif source_type == "s3":
        connection_info = {
            "uri":          latest["location"],
            "endpoint_url": source_cfg.get("endpoint_url"),
            "region":       source_cfg.get("region"),
        }
    elif source_type == "sql":
        connection_info = {
            "dsn":   source_cfg.get("dsn"),
            "query": latest["location"],
        }
    elif source_type == "sftp":
        connection_info = {
            "host":        source_cfg.get("host"),
            "port":        source_cfg.get("port", 22),
            "remote_path": latest["location"],
        }
    else:
        connection_info = {"url": latest["location"]}

    schema    = db.get_schema(dataset_id)
    pii_cols  = [c["column_name"] for c in schema if c.get("pii")]

    data = {
        "dataset_id":      dataset_id,
        "version":         latest["version"],
        "source_type":     source_type,
        "format":          latest.get("format"),
        "rows":            latest.get("rows"),
        "committed_at":    latest.get("committed_at"),
        "connection_info": connection_info,
        "schema_status":   schema[0]["status"] if schema else None,
        "pii_columns":     pii_cols,
    }

    msgs = []
    if latest_dataset := db.get_dataset(dataset_id):
        ds_status = latest_dataset.get("status", "draft")
        data["dataset_status"]  = ds_status
        data["approved_by"]     = latest_dataset.get("approved_by")
        data["approved_at"]     = latest_dataset.get("approved_at")
        if ds_status != "approved":
            msgs.append(
                f"Dataset status is '{ds_status}' — "
                "schema has not been approved by a data steward")
    if pii_cols:
        msgs.append(f"Dataset contains PII columns: {', '.join(pii_cols)}")
    return warn(data, msgs) if msgs else ok(data)


@app.get("/datasets/{dataset_id:path}/lineage/{version}", tags=["Datasets"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str,
                      version: str):
    record = (db.get_version(dataset_id, version) if version
              else db.get_latest(dataset_id))
    if not record:
        return ko("Dataset version not found", 404)

    ver = record["version"]
    return ok({
        "dataset_id": dataset_id,
        "version":    ver,
        "upstream":   db.get_upstream(dataset_id, ver),
        "downstream": db.get_downstream(dataset_id, ver),
    })


@app.get("/datasets/{dataset_id:path}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(dataset_id: str):
    if not db.dataset_exists(dataset_id):
        return ko("Dataset not found", 404)
    columns   = db.get_schema(dataset_id)
    pii_count = sum(1 for c in columns if c.get("pii"))
    inferred  = [c["column_name"] for c in columns
                 if c.get("status") == "inferred"]
    msgs = []
    if pii_count:
        msgs.append(f"{pii_count} column(s) flagged as PII")
    if inferred:
        msgs.append(
            f"{len(inferred)} column(s) still 'inferred' — "
            "review before publishing")
    data = {
        "dataset_id": dataset_id,
        "columns":    columns,
        "summary": {
            "total":     len(columns),
            "pii":       pii_count,
            "inferred":  len(inferred),
            "draft":     sum(1 for c in columns if c.get("status") == "draft"),
            "published": sum(1 for c in columns if c.get("status") == "published"),
        },
    }
    return warn(data, msgs) if msgs else ok(data)


@app.patch("/datasets/{dataset_id:path}/schema/{column_name}",
           tags=["Schema"],
           summary="Edit a column's semantic metadata and PII flags")
async def patch_schema_column(dataset_id: str, column_name: str,
                               body: SchemaColumnPatch,
                               editor: str = Query("anonymous")):
    if not db.dataset_exists(dataset_id):
        return ko("Dataset not found", 404)
    updates = _model_dump(body)
    updated = db.update_schema_column(dataset_id, column_name,
                                      editor, **updates)
    if not updated:
        return ko("Column not found in schema", 404)
    # Any schema edit promotes dataset to in_review
    db.set_in_review(dataset_id)
    col  = next((c for c in db.get_schema(dataset_id)
                 if c["column_name"] == column_name), None)
    msgs = []
    if col and col.get("pii") and col.get("pii_type") == "none":
        msgs.append("PII flag set but pii_type is 'none' — "
                    "set it to: direct | indirect | sensitive")
    return warn(col, msgs) if msgs else ok(col)


@app.post("/datasets/{dataset_id:path}/schema/publish",
          tags=["Schema"],
          summary="Publish schema — promotes all columns to 'published'")
async def publish_schema(dataset_id: str, body: SchemaPublishRequest):
    if not db.dataset_exists(dataset_id):
        return ko("Dataset not found", 404)
    result = db.publish_schema(dataset_id, body.published_by)
    msgs   = result["breaking_changes"] + result["warnings"]
    data   = {
        "dataset_id":      dataset_id,
        "published_at":    result["published_at"],
        "published_by":    body.published_by,
        "breaking_changes": result["breaking_changes"],
        "warnings":        result["warnings"],
    }
    if result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking changes vs previous schema"] + msgs)
    return warn(data, msgs) if msgs else ok(data)


@app.post("/datasets/{dataset_id:path}/approve",
          tags=["Datasets"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest):
    dataset = db.dataset_exists(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    if dataset.get("status") == "deprecated":
        return ko("Cannot approve a deprecated dataset", 409)

    # Publish schema atomically with approval
    schema_result = db.publish_schema(dataset_id, publisher=body.approved_by)
    approved      = db.approve_dataset(dataset_id, body.approved_by)
    if not approved:
        return ko("Approval failed", 500)

    msgs = schema_result["breaking_changes"] + schema_result["warnings"]
    data = {
        "dataset_id":      dataset_id,
        "status":          "approved",
        "approved_by":     body.approved_by,
        "notes":           body.notes,
        "schema_published_at":  schema_result["published_at"],
        "breaking_changes":     schema_result["breaking_changes"],
        "warnings":             schema_result["warnings"],
    }
    log(f"Approved {dataset_id} by {body.approved_by}")
    if schema_result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking schema changes on approval"] + msgs)
    return warn(data, msgs) if msgs else ok(data)






@app.get("/datasets/{dataset_id:path}", tags=["Datasets"],
         summary="Get dataset entity + latest version")
async def get_dataset____(dataset_id: str):
    dataset = db.exists_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    schema  = db.get_schema(dataset_id)
    summary = {
        "total":     len(schema),
        "inferred":  sum(1 for c in schema if c.get("status") == "inferred"),
        "draft":     sum(1 for c in schema if c.get("status") == "draft"),
        "published": sum(1 for c in schema if c.get("status") == "published"),
        "pii":       sum(1 for c in schema if c.get("pii")),
    }
    msgs = []
    if dataset.get("status") != "approved":
        msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
    data = {"dataset": dataset, "latest": db.get_latest(dataset_id),
            "schema_summary": summary}
    return warn(data, msgs) if msgs else ok(data)


# ===========================================================================
# Routes — Version lifecycle
# ===========================================================================

@app.post("/datasets/{dataset_id:path}/reserve", tags=["Versions"],
          summary="Reserve a new local version (phase 1 of 2-phase write)",
          status_code=201)
async def reserve(dataset_id: str, body: ReserveRequest):
    version = _version_id()
    path    = _local_path(dataset_id, version, body.format)
    try:
        db.create_dataset(dataset_id,
                          description=body.description,
                          owner=body.owner,
                          tags=body.tags)
        if not db.reserve(dataset_id, version, path, body.format,
                          body.task_id, body.job_id,
                          source_id=body.source_id):
            return ko("Version already exists", 409)
        log(f"Reserved {dataset_id}@{version}")
        return ok({"dataset_id": dataset_id,
                   "version":    version,
                   "path":       path})
    except Exception as e:
        return ko(str(e), 500)


@app.post("/datasets/{dataset_id:path}/commit/{version}",
          tags=["Versions"],
          summary="Commit a reserved version (phase 2 of 2-phase write)")
async def commit(dataset_id: str, version: str, body: CommitRequest):
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    if record["status"] != "reserved":
        return ko(f"Cannot commit — status is '{record['status']}'", 409)

    path = record["location"]
    if not os.path.exists(path):
        return ko(f"File not found at: {path}", 422)

    try:
        file_hash = _compute_hash(path)
        inferred  = _infer_schema(path, record.get("format", ""))
        schema_kv = (body.columns
                     or {c["name"]: c["physical_type"] for c in inferred})
        result    = db.commit(dataset_id, version, file_hash,
                              body.rows, schema_kv)

        if result is None:
            return ko("Commit failed — version may already be committed", 409)

        if result["skipped"]:
            log(f"Skipped {dataset_id} — identical to latest")
            return ok({"dataset_id": dataset_id,
                       "version":    result["version"],
                       "skipped":    True,
                       "reason":     "Identical to latest committed version"})

        # sys.* metadata — written by server
        db.set_system_metadata(dataset_id, version, {
            "format":       record.get("format"),
            "rows":         body.rows,
            "hash":         file_hash,
            "task_id":      record.get("produced_by_task"),
            "job_id":       record.get("produced_by_job"),
            "committed_at": _now(),
        })

        # Business metadata from task
        for k, v in (body.business_meta or {}).items():
            if not k.startswith("sys."):
                db.set_metadata(dataset_id, version, k, v)

        # Schema
        db.upsert_schema_columns(dataset_id, inferred)
        diff = db.diff_schema_against_inferred(dataset_id, inferred)

        # Lineage
        if body.inputs:
            db.insert_lineage(dataset_id, version,
                              [_model_dump(i) for i in body.inputs])

        log(f"Committed {dataset_id}@{version} hash={file_hash[:8]}…")

        all_warnings = diff["breaking"] + diff["warnings"]
        data = {
            "dataset_id": dataset_id,
            "version":    version,
            "path":       path,
            "hash":       file_hash,
            "rows":       body.rows,
            "skipped":    False,
        }
        if diff["breaking"]:
            return warn(data, ["⚠️ Schema breaking changes"] + all_warnings)
        if all_warnings:
            return warn(data, all_warnings)
        return ok(data)

    except Exception as e:
        log(f"❌ Commit error: {e}")
        return ko(str(e), 500)


@app.post("/datasets/{dataset_id:path}/fail/{version}",
          tags=["Versions"],
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str):
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    db.fail(dataset_id, version)
    return ok({"dataset_id": dataset_id,
               "version":    version,
               "status":     "failed"})


@app.delete("/datasets/{dataset_id:path}/deprecate/{version}",
            tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str):
    if not db.deprecate(dataset_id, version):
        return ko("Version not found", 404)
    log(f"Deprecated {dataset_id}@{version}")
    return ok({"dataset_id": dataset_id,
               "version":    version,
               "status":     "deprecated"})


# ===========================================================================
# Routes — Virtual datasets
# ===========================================================================

@app.post("/datasets/{dataset_id:path}/register-virtual",
          tags=["Virtual"],
          summary="Register a virtual dataset version (no local file)",
          status_code=201)
async def register_virtual(dataset_id: str, body: VirtualRegisterRequest):
    src = db.get_source(body.source_id)
    if not src:
        return ko(f"Source '{body.source_id}' not found. "
                  f"Register it first via POST /sources.", 422)
    version = _version_id()
    try:
        db.create_dataset(dataset_id,
                          display_name=body.display_name,
                          description=body.description,
                          owner=body.owner,
                          tags=body.tags)
        db.commit_virtual(dataset_id, version, body.source_id,
                          body.location, body.format,
                          body.task_id, body.job_id)
        log(f"Virtual {dataset_id}@{version} [{src['type']}]")
        return ok({"dataset_id":  dataset_id,
                   "version":     version,
                   "source_id":   body.source_id,
                   "source_type": src["type"],
                   "location":    body.location,
                   "format":      body.format})
    except Exception as e:
        return ko(str(e), 500)


# ===========================================================================
# Routes — Version metadata
# ===========================================================================

@app.get("/datasets/{dataset_id:path}/metadata/{version:path}",
         tags=["Metadata"],
         summary="Get all metadata for a version")
async def get_metadata(dataset_id: str, version: str):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    return ok(db.get_metadata(dataset_id, version))


@app.post("/datasets/{dataset_id:path}/metadata/{version:path}",
          tags=["Metadata"],
          summary="Set a metadata key on a version")
async def set_metadata(dataset_id: str, version: str,
                       body: MetadataSetRequest):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    if body.key.startswith("sys."):
        return ko("sys.* keys are reserved for the server", 422)
    db.set_metadata(dataset_id, version, body.key, body.value)
    return ok({"key": body.key, "value": body.value})


@app.delete("/datasets/{dataset_id:path}/metadata/{version:path}/{key}",
            tags=["Metadata"],
            summary="Delete a metadata key from a version")
async def delete_metadata(dataset_id: str, version: str, key: str):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    if not db.delete_metadata(dataset_id, version, key):
        return ko("Key not found or protected (sys.*)", 404)
    return ok({"key": key, "deleted": True})


# ===========================================================================
# Routes — Materialize
# ===========================================================================

@app.post("/datasets/{dataset_id:path}/materialize",
          tags=["Materialize"],
          summary="Fetch a REST API and store result as a local CSV version",
          status_code=201)
async def materialize(dataset_id: str, body: MaterializeRequest):
    version = _version_id()
    path    = _local_path(dataset_id, version, "csv")
    try:
        db.create_dataset(dataset_id,
                          display_name=body.display_name,
                          description=body.description)
        db.reserve(dataset_id, version, path,
                   "csv", body.task_id, body.job_id)

        rows, schema_cols = await _fetch_and_write(
            body.base_url, body.endpoint, body.params, path)

        if rows == 0:
            db.fail(dataset_id, version)
            return ko("No records returned from endpoint", 422)

        file_hash = _compute_hash(path)
        schema_kv = {c["name"]: c["physical_type"] for c in schema_cols}
        result    = db.commit(dataset_id, version,
                              file_hash, rows, schema_kv)

        if result is None:
            db.fail(dataset_id, version)
            return ko("Commit failed", 409)

        if result["skipped"]:
            try:
                os.remove(path)
            except Exception:
                pass
            return ok({"dataset_id": dataset_id,
                       "version":    result["version"],
                       "rows":       rows,
                       "skipped":    True,
                       "reason":     "Identical to latest committed version",
                       "source_url": f"{body.base_url}{body.endpoint}"})

        db.upsert_schema_columns(dataset_id, schema_cols)
        db.insert_lineage(dataset_id, version, [{
            "dataset_id": f"__external__/{body.base_url}{body.endpoint}",
            "version":    "live",
        }])
        log(f"Materialized {dataset_id}@{version} rows={rows}")
        return ok({"dataset_id": dataset_id,
                   "version":    version,
                   "path":       path,
                   "rows":       rows,
                   "hash":       file_hash,
                   "skipped":    False,
                   "source_url": f"{body.base_url}{body.endpoint}"})

    except httpx.HTTPError as e:
        db.fail(dataset_id, version)
        return ko(f"HTTP error: {e}", 502)
    except Exception as e:
        try:
            db.fail(dataset_id, version)
        except Exception:
            pass
        return ko(str(e), 500)


# ===========================================================================
# Routes — Scan
# ===========================================================================

@app.post("/scan", tags=["Scan"],
          summary="Scan a filesystem path and register all dataset files found")
async def scan_api(body: ScanRequest):
    data_path = body.data_path or DATA_PATH
    if not os.path.exists(data_path):
        return ko(f"Path not found: {data_path}", 404)
    count = _scan(data_path, body.prefix)
    return ok({"scanned": count, "data_path": data_path})


# ===========================================================================
# Entrypoint
# ===========================================================================

def main():
    if args.scan:
        _scan(args.scan_path or DATA_PATH, args.scan_prefix)
        return

    log("Waluigi Catalog v2")
    log(f"  Binding : {args.bind_address}:{args.port}")
    log(f"  URL     : http://{args.host}:{args.port}")
    log(f"  DB      : {args.db_path}")
    log(f"  Data    : {args.data_path}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()

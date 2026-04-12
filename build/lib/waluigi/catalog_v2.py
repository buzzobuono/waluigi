"""
Waluigi Catalog v2
==================
Data catalog service — collections, virtual sources, schema governance, lineage.

Response contract (always):
    {
        "data":       <payload | null>,
        "diagnostic": {
            "result":   "OK" | "WARN" | "KO",
            "messages": ["..."]
        }
    }

HTTP status codes:
    200 / 201  →  result OK or WARN  (operation succeeded)
    404        →  result KO          (resource not found)
    409        →  result KO          (state conflict)
    422        →  result KO          (unprocessable, e.g. file missing)
    500        →  result KO          (unexpected server error)
"""

import os
import sys
import csv
import json
import socket
import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import configargparse
import httpx
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from waluigi.core.catalog_db_v2 import CatalogDB


# ---------------------------------------------------------------------------
# App & Config
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Waluigi Catalog v2",
    description=__doc__,
    version="2.0.0",
)

p = configargparse.ArgParser(auto_env_var_prefix="WALUIGI_CATALOG_")
p.add("--port",            type=int, default=9000)
p.add("--host",            default=socket.gethostname())
p.add("--bind-address",    default="0.0.0.0")
p.add("--db-path",         default=os.path.join(os.getcwd(), "db/catalog.db"))
p.add("--data-path",       default=os.path.join(os.getcwd(), "data"))
p.add("--scan",            action="store_true", default=False)
p.add("--scan-path",       default=None)
p.add("--scan-collection", default=None)
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
# Response helpers
# ---------------------------------------------------------------------------

def ok(data: Any, messages: list[str] = None) -> JSONResponse:
    return JSONResponse({
        "data": data,
        "diagnostic": {"result": "OK", "messages": messages or []},
    })


def warn(data: Any, messages: list[str]) -> JSONResponse:
    return JSONResponse({
        "data": data,
        "diagnostic": {"result": "WARN", "messages": messages},
    })


def ko(messages: list[str] | str, status: int = 400) -> JSONResponse:
    if isinstance(messages, str):
        messages = [messages]
    return JSONResponse(
        {
            "data": None,
            "diagnostic": {"result": "KO", "messages": messages},
        },
        status_code=status,
    )


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _version_id() -> str:
    return _now()


def _local_path(collection: str, dataset_id: str, version: str, fmt: str) -> str:
    safe_ver = version.replace(":", "-")
    ext = f".{fmt}" if fmt else ""
    d = os.path.join(DATA_PATH, collection, dataset_id)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{safe_ver}{ext}")


def _compute_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _infer_schema(path: str, fmt: str) -> list[dict]:
    """Return list of {name, physical_type, logical_type} dicts."""
    try:
        if fmt in ("csv", "tsv"):
            sep = "\t" if fmt == "tsv" else ","
            df = pd.read_csv(path, sep=sep, nrows=1000)
        elif fmt == "parquet":
            df = pd.read_parquet(path)
        elif fmt in ("xls", "xlsx"):
            df = pd.read_excel(path, nrows=1000)
        else:
            return []

        type_map = {
            "int64": "integer", "int32": "integer",
            "float64": "decimal", "float32": "decimal",
            "bool": "boolean",
            "datetime64[ns]": "datetime",
            "object": "string",
        }
        return [
            {
                "name": col,
                "physical_type": str(df[col].dtype),
                "logical_type": type_map.get(str(df[col].dtype), "string"),
            }
            for col in df.columns
        ]
    except Exception:
        return []


def _safe_json_value(v):
    """Sanitize a single cell for JSON serialization."""
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

class CollectionUpdateRequest(BaseModel):
    description: Optional[str] = None
    owner:       Optional[str] = None
    tags:        Optional[List[str]] = None


class SourceCreateRequest(BaseModel):
    id:          str            = Field(...,  example="pg-dwh")
    type:        str            = Field(...,  example="sql")
    config:      Dict[str, Any] = Field(default_factory=dict,
                                        example={"dsn": "postgresql://user:pass@host/db"})
    description: Optional[str] = Field(None, example="Main data warehouse")


class SourceUpdateRequest(BaseModel):
    type:        Optional[str]            = None
    config:      Optional[Dict[str, Any]] = None
    description: Optional[str]            = None


class DatasetUpdateRequest(BaseModel):
    display_name: Optional[str]       = None
    description:  Optional[str]       = None
    owner:        Optional[str]       = None
    tags:         Optional[List[str]] = None


class ReserveRequest(BaseModel):
    format:   str = Field("",        example="csv")
    task_id:  str = Field("unknown", example="ingest_fatture")
    job_id:   str = Field("unknown", example="job/daily")
    source_id: Optional[str] = Field(None, example="local")

    # optional dataset-level info (used to upsert the entity on first reserve)
    display_name: Optional[str]       = None
    description:  Optional[str]       = None
    owner:        Optional[str]       = None
    tags:         Optional[List[str]] = None


class LineageRef(BaseModel):
    collection: str = Field(..., example="finance/erp")
    dataset_id: str = Field(..., example="raw_fatture")
    version:    str = Field(..., example="2026-04-11T10:00:00+00:00")


class CommitRequest(BaseModel):
    rows:          Optional[int]            = None
    columns:       Optional[Dict[str, Any]] = Field(None, alias="schema")
    inputs:        List[LineageRef]         = Field(default_factory=list)
    business_meta: Dict[str, str]           = Field(default_factory=dict,
        description="Free key-value metadata from the task (stored without prefix).")

    model_config = {"populate_by_name": True}


class VirtualRegisterRequest(BaseModel):
    source_id:   str            = Field(...,  example="pg-dwh")
    location:    str            = Field(...,  example="SELECT * FROM finance.fatture")
    format:      str            = Field("sql", example="sql")
    task_id:     str            = Field("unknown")
    job_id:      str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None
    owner:        Optional[str] = None
    tags:         Optional[List[str]] = None


class SchemaColumnPatch(BaseModel):
    logical_type: Optional[str]       = None
    nullable:     Optional[bool]      = None
    pii:          Optional[bool]      = None
    pii_type:     Optional[str]       = None   # none|direct|indirect|sensitive
    pii_notes:    Optional[str]       = None
    description:  Optional[str]       = None
    tags:         Optional[List[str]] = None


class SchemaPublishRequest(BaseModel):
    published_by: str = Field("anonymous", example="mario.rossi")


class SchemaContractColumn(BaseModel):
    name:         str             = Field(...,  example="email")
    logical_type: Optional[str]  = Field(None, example="string")
    nullable:     Optional[bool]  = None
    pii:          Optional[bool]  = None
    pii_type:     Optional[str]  = Field(None, example="direct")
    pii_notes:    Optional[str]  = None
    description:  Optional[str]  = None
    tags:         Optional[List[str]] = None


class SchemaContractRequest(BaseModel):
    columns:      List[SchemaContractColumn] = Field(
        ..., description="Column definitions applied automatically at every commit.")
    auto_publish: bool = Field(
        True, description="Publish schema automatically when all columns are covered.")


class MetadataSetRequest(BaseModel):
    key:   str = Field(..., example="owner")
    value: str = Field(..., example="data-engineering")


class MaterializeRequest(BaseModel):
    base_url:  str            = Field(...,   example="https://api.example.com")
    endpoint:  str            = Field(...,   example="/v1/orders")
    params:    Dict[str, Any] = Field(default_factory=dict)
    task_id:   str            = Field("unknown")
    job_id:    str            = Field("unknown")
    display_name: Optional[str] = None
    description:  Optional[str] = None


class ScanRequest(BaseModel):
    data_path:  Optional[str] = None
    collection: Optional[str] = None


# ---------------------------------------------------------------------------
# Materialization helpers (REST API → CSV)
# ---------------------------------------------------------------------------

def _extract_items(body) -> list:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "results", "items", "records", "content",
                    "entries", "rows"):
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
            out[key] = str(v) if (v and isinstance(v[0], dict)) else ",".join(str(i) for i in v)
        else:
            out[key] = v
    return out


async def _fetch_and_write(base_url: str, endpoint: str,
                           params: dict, output_path: str) -> tuple[int, list[dict]]:
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

    schema_cols = [{"name": k, "physical_type": "string", "logical_type": "string"}
                   for k in fieldnames]
    return len(records), schema_cols


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def _scan(data_path: str, collection: str | None = None) -> int:
    log(f"🔍 Scanning {data_path} ...")
    count = 0
    for root, dirs, files in os.walk(data_path):
        dirs.sort()
        for filename in sorted(files):
            ext = os.path.splitext(filename)[1].lower()
            if ext not in SCANNABLE_EXTENSIONS:
                continue

            filepath   = os.path.join(root, filename)
            fmt        = ext.lstrip(".")
            dataset_id = os.path.basename(root)
            rel        = os.path.relpath(os.path.dirname(root), data_path)
            coll       = collection or (
                rel.replace(os.sep, "/") if rel != "." else "root"
            )
            version    = os.path.splitext(filename)[0].replace("-", ":", 2)

            try:
                file_hash = _compute_hash(filepath)
                schema    = _infer_schema(filepath, fmt)
                db.ensure_collection(coll)
                db.ensure_dataset(coll, dataset_id)
                # Reserve + immediate commit (scanned files are already written)
                db.reserve(coll, dataset_id, version, filepath, fmt,
                           "scanner", "scan", source_id=None)
                result = db.commit(coll, dataset_id, version,
                                   file_hash, None, {c["name"]: c["physical_type"] for c in schema})
                if result and not result["skipped"]:
                    db.upsert_schema_columns(coll, dataset_id, schema)
                count += 1
                log(f"  ✅ {coll}/{dataset_id}@{version[:19]} [{fmt}]")
            except Exception as e:
                log(f"  ⚠️  Skipped {filepath}: {e}")

    log(f"🏁 Scan complete — {count} dataset(s) registered.")
    return count


# ===========================================================================
# Routes — Collections
# ===========================================================================

@app.get("/collections", tags=["Collections"],
         summary="List root collections")
async def list_root_collections():
    return ok(db.list_collection_children(parent=None))


@app.get("/collections/{path:path}/children", tags=["Collections"],
         summary="List direct children of a collection")
async def list_collection_children(path: str):
    node = db.get_collection(path)
    if not node:
        return ko("Collection not found", 404)
    return ok({"collection": node, "children": db.list_collection_children(path)})


@app.get("/collections/{path:path}/datasets", tags=["Collections"],
         summary="List datasets in a collection")
async def list_collection_datasets(path: str, recursive: bool = False):
    node = db.get_collection(path)
    if not node:
        return ko("Collection not found", 404)
    datasets = db.list_datasets_in_collection(path, recursive)
    return ok({"collection": path, "datasets": datasets})


@app.patch("/collections/{path:path}", tags=["Collections"],
           summary="Update collection description, owner or tags")
async def update_collection(path: str, body: CollectionUpdateRequest):
    updated = db.update_collection(path, **body.model_dump(exclude_none=True))
    if not updated:
        return ko("Collection not found", 404)
    return ok(db.get_collection(path))


# ===========================================================================
# Routes — Sources
# ===========================================================================

@app.get("/sources", tags=["Sources"],
         summary="List all registered sources")
async def list_sources():
    return ok(db.list_sources())


@app.post("/sources", tags=["Sources"],
          summary="Register a new physical source / connector",
          status_code=201)
async def create_source(body: SourceCreateRequest):
    created = db.create_source(body.id, body.type,
                               body.config, body.description)
    if not created:
        return ko(f"Source '{body.id}' already exists", 409)
    return ok(db.get_source(body.id))


@app.get("/sources/{id}", tags=["Sources"],
         summary="Get source details")
async def get_source(id: str):
    src = db.get_source(id)
    if not src:
        return ko("Source not found", 404)
    return ok(src)


@app.patch("/sources/{id}", tags=["Sources"],
           summary="Update source config or description")
async def update_source(id: str, body: SourceUpdateRequest):
    updated = db.update_source(id, **body.model_dump(exclude_none=True))
    if not updated:
        return ko("Source not found", 404)
    return ok(db.get_source(id))


@app.delete("/sources/{id}", tags=["Sources"],
            summary="Delete a source")
async def delete_source(id: str):
    deleted = db.delete_source(id)
    if not deleted:
        return ko("Source not found", 404)
    return ok({"id": id, "deleted": True})


# ===========================================================================
# Routes — Datasets (logical entity)
# ===========================================================================

@app.get("/datasets/{collection:path}/{id}", tags=["Datasets"],
         summary="Get dataset entity + latest version")
async def get_dataset(collection: str, id: str):
    dataset = db.get_dataset(collection, id)
    if not dataset:
        return ko("Dataset not found", 404)
    latest = db.get_latest(collection, id)
    return ok({"dataset": dataset, "latest": latest})


@app.patch("/datasets/{collection:path}/{id}", tags=["Datasets"],
           summary="Update dataset display name, description, owner or tags")
async def update_dataset(collection: str, id: str, body: DatasetUpdateRequest):
    updated = db.update_dataset(collection, id,
                                **body.model_dump(exclude_none=True))
    if not updated:
        return ko("Dataset not found", 404)
    return ok(db.get_dataset(collection, id))


@app.get("/datasets/{collection:path}/{id}/versions", tags=["Datasets"],
         summary="List all committed versions (newest first)")
async def list_versions(collection: str, id: str):
    history = db.get_history(collection, id)
    if history is None:
        return ko("Dataset not found", 404)
    return ok({"collection": collection, "dataset_id": id, "versions": history})


@app.get("/datasets/{collection:path}/{id}/resolve", tags=["Datasets"],
         summary="Resolve connection info for the latest committed version",
         description=(
             "Returns all the information a client needs to read the dataset "
             "using native libraries. For local/s3/sftp sources this is a "
             "location path. For sql sources this includes the dsn and query. "
             "Credentials are NEVER returned — the client is expected to use "
             "its own environment / IAM roles."
         ))
async def resolve(collection: str, id: str,
                  prefer: Optional[str] = Query(None, example="sql")):
    latest = db.get_latest(collection, id)
    if not latest:
        return ko("Dataset not found or no committed version", 404)

    source_type = latest.get("source_type") or "local"
    source_cfg  = latest.get("source_config") or {}

    connection_info: dict = {}

    if source_type == "local":
        connection_info = {"path": latest["location"]}
    elif source_type == "s3":
        connection_info = {
            "uri":          latest["location"],
            "endpoint_url": source_cfg.get("endpoint_url"),
            "region":       source_cfg.get("region"),
            # credentials: use env AWS_ACCESS_KEY_ID / IAM
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
            # credentials: use env / ssh-agent
        }
    elif source_type == "api":
        connection_info = {"url": latest["location"]}

    schema  = db.get_schema(collection, id)
    pii_cols = [c["column_name"] for c in schema if c.get("pii")]

    msgs = []
    if pii_cols:
        msgs.append(f"Dataset contains PII columns: {', '.join(pii_cols)}")

    data = {
        "collection":      collection,
        "dataset_id":      id,
        "version":         latest["version"],
        "source_type":     source_type,
        "format":          latest.get("format"),
        "rows":            latest.get("rows"),
        "committed_at":    latest.get("committed_at"),
        "connection_info": connection_info,
        "schema_status":   schema[0]["status"] if schema else None,
        "pii_columns":     pii_cols,
    }

    return warn(data, msgs) if pii_cols else ok(data)


@app.get("/datasets/{collection:path}/{id}/lineage", tags=["Datasets"],
         summary="Get upstream and downstream lineage for the latest version")
async def get_lineage(collection: str, id: str,
                      version: Optional[str] = Query(None)):
    if version:
        record = db.get_version(collection, id, version)
    else:
        record = db.get_latest(collection, id)

    if not record:
        return ko("Dataset version not found", 404)

    ver = record["version"]
    return ok({
        "collection": collection,
        "dataset_id": id,
        "version":    ver,
        "upstream":   db.get_upstream(collection, id, ver),
        "downstream": db.get_downstream(collection, id, ver),
    })


# ===========================================================================
# Routes — Version lifecycle
# ===========================================================================

@app.post("/datasets/{collection:path}/{id}/reserve", tags=["Versions"],
          summary="Reserve a new local dataset version (phase 1 of 2-phase write)",
          status_code=201)
async def reserve(collection: str, id: str, body: ReserveRequest):
    version = _version_id()
    path    = _local_path(collection, id, version, body.format)

    try:
        db.ensure_collection(collection)
        db.ensure_dataset(collection, id,
                          display_name=body.display_name,
                          description=body.description,
                          owner=body.owner,
                          tags=body.tags)
        ok_reserve = db.reserve(collection, id, version, path,
                                body.format, body.task_id, body.job_id,
                                source_id=body.source_id)
        if not ok_reserve:
            return ko("Version already exists", 409)

        log(f"Reserved {collection}/{id}@{version}")
        return ok({
            "collection": collection,
            "dataset_id": id,
            "version":    version,
            "path":       path,
        })
    except Exception as e:
        return ko(str(e), 500)


@app.post("/datasets/{collection:path}/{id}/{version}/commit", tags=["Versions"],
          summary="Commit a reserved version (phase 2 of 2-phase write)",
          description=(
              "Computes SHA-256 of the written file, finalizes the version. "
              "If content is identical to the latest committed version the slot "
              "is dropped and skipped=true is returned. "
              "If the published schema has breaking changes a WARN is raised."
          ))
async def commit(collection: str, id: str, version: str,
                 body: CommitRequest):
    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)
    if record["status"] != "reserved":
        return ko(f"Cannot commit — status is '{record['status']}'", 409)

    path = record["location"]
    if not os.path.exists(path):
        return ko(f"File not found at expected path: {path}", 422)

    try:
        file_hash = _compute_hash(path)
        inferred  = _infer_schema(path, record.get("format", ""))
        schema_kv = (body.columns
                     or {c["name"]: c["physical_type"] for c in inferred})
        result    = db.commit(collection, id, version,
                              file_hash, body.rows, schema_kv)

        if result is None:
            return ko("Commit failed — version may already be committed", 409)

        if result["skipped"]:
            log(f"Skipped {collection}/{id} — identical to latest")
            return ok({
                "collection": collection,
                "dataset_id": id,
                "version":    result["version"],
                "skipped":    True,
                "reason":     "Identical to latest committed version",
            })

        # ── sys.* metadata — written by the server, never by the task ──────
        db.set_system_metadata(collection, id, version, {
            "format":      record.get("format"),
            "rows":        body.rows,
            "hash":        file_hash,
            "task_id":     record.get("produced_by_task"),
            "job_id":      record.get("produced_by_job"),
            "committed_at": _now(),
        })

        # ── business metadata — passed by the task via ctx.meta ─────────────
        for k, v in (body.business_meta or {}).items():
            if not k.startswith("sys."):        # guard: task cannot write sys.*
                db.set_metadata(collection, id, version, k, v)

        # ── schema: upsert inferred, apply contract, detect drift ───────────
        db.upsert_schema_columns(collection, id, inferred)

        contract_result = db.apply_schema_contract(collection, id)

        diff = db.diff_schema_against_inferred(collection, id, inferred)

        # ── lineage ──────────────────────────────────────────────────────────
        if body.inputs:
            db.insert_lineage(collection, id, version,
                              [i.model_dump() for i in body.inputs])

        log(f"Committed {collection}/{id}@{version} hash={file_hash[:8]}…")

        all_warnings = diff["breaking"] + diff["warnings"]
        if contract_result["published"]:
            pub = contract_result["publish_result"] or {}
            all_warnings += pub.get("breaking_changes", []) + pub.get("warnings", [])
        if contract_result["unknown"]:
            all_warnings.append(
                f"Contract columns not yet in schema "
                f"(will apply on next commit): "
                f"{', '.join(contract_result['unknown'])}")

        data = {
            "collection":      collection,
            "dataset_id":      id,
            "version":         version,
            "path":            path,
            "hash":            file_hash,
            "rows":            body.rows,
            "skipped":         False,
            "schema_contract": {
                "applied":   contract_result["applied"],
                "published": contract_result["published"],
            },
        }

        if diff["breaking"]:
            return warn(data, ["⚠️ Schema breaking changes detected"] + all_warnings)
        if all_warnings:
            return warn(data, all_warnings)
        return ok(data)

    except Exception as e:
        log(f"❌ Commit error: {e}")
        return ko(str(e), 500)


@app.post("/datasets/{collection:path}/{id}/{version}/fail", tags=["Versions"],
          summary="Mark a reserved version as failed")
async def fail_version(collection: str, id: str, version: str):
    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)
    db.fail(collection, id, version)
    return ok({"collection": collection, "dataset_id": id,
               "version": version, "status": "failed"})


@app.delete("/datasets/{collection:path}/{id}/{version}", tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(collection: str, id: str, version: str):
    deprecated = db.deprecate(collection, id, version)
    if not deprecated:
        return ko("Version not found", 404)
    log(f"Deprecated {collection}/{id}@{version}")
    return ok({"collection": collection, "dataset_id": id,
               "version": version, "status": "deprecated"})


@app.get("/datasets/{collection:path}/{id}/{version}/preview", tags=["Versions"],
         summary="Preview rows of a local dataset version")
async def preview(collection: str, id: str, version: str,
                  limit: int = 10, offset: int = 0):
    import numpy as np

    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)

    path = record["location"]
    fmt  = (record.get("format") or "").lower()

    if record.get("source_type") and record["source_type"] != "local":
        return ko("Preview is only available for local versions. "
                  "Use resolve to get connection info for virtual sources.", 422)

    if not os.path.exists(path):
        return ko(f"File not found: {path}", 404)

    try:
        if fmt == "csv":
            df = pd.read_csv(path, skiprows=range(1, offset + 1),
                             nrows=limit, dtype=str)
        elif fmt == "parquet":
            df = pd.read_parquet(path).iloc[offset: offset + limit]
        else:
            return ko(f"Preview not supported for format '{fmt}'", 422)

        clean_data = [
            {k: _safe_json_value(v) for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]

        return ok({
            "collection": collection,
            "dataset_id": id,
            "version":    version,
            "columns":    df.columns.tolist(),
            "data":       clean_data,
            "pagination": {
                "limit":  limit,
                "offset": offset,
                "count":  len(clean_data),
            },
        })

    except Exception as e:
        log(f"❌ Preview error: {e}")
        return ko(str(e), 500)


# ===========================================================================
# Routes — Virtual datasets
# ===========================================================================

@app.post("/datasets/{collection:path}/{id}/register-virtual",
          tags=["Virtual"],
          summary="Register a virtual dataset version (no local file)",
          description=(
              "For SQL tables, S3 objects, SFTP files, or any source where "
              "the data lives externally and should NOT be copied locally. "
              "The source must be registered first via POST /sources."
          ),
          status_code=201)
async def register_virtual(collection: str, id: str,
                            body: VirtualRegisterRequest):
    src = db.get_source(body.source_id)
    if not src:
        return ko(f"Source '{body.source_id}' not found. Register it first "
                  f"via POST /sources.", 422)

    version = _version_id()

    try:
        db.ensure_collection(collection)
        db.ensure_dataset(collection, id,
                          display_name=body.display_name,
                          description=body.description,
                          owner=body.owner,
                          tags=body.tags)
        result = db.commit_virtual(collection, id, version,
                                   body.source_id, body.location,
                                   body.format, body.task_id, body.job_id)
        log(f"Virtual registered {collection}/{id}@{version} "
            f"[{src['type']}] {body.location}")
        return ok({
            "collection": collection,
            "dataset_id": id,
            "version":    version,
            "source_id":  body.source_id,
            "source_type":src["type"],
            "location":   body.location,
            "format":     body.format,
        })
    except Exception as e:
        return ko(str(e), 500)


# ===========================================================================
# Routes — Schema governance
# ===========================================================================

@app.get("/datasets/{collection:path}/{id}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(collection: str, id: str):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)
    columns = db.get_schema(collection, id)
    pii_count  = sum(1 for c in columns if c.get("pii"))
    msgs = []
    if pii_count:
        msgs.append(f"{pii_count} column(s) flagged as PII")
    unreviewed = [c["column_name"] for c in columns
                  if c.get("status") == "inferred"]
    if unreviewed:
        msgs.append(
            f"{len(unreviewed)} column(s) still 'inferred' — "
            "consider reviewing before publishing"
        )
    data = {
        "collection": collection,
        "dataset_id": id,
        "columns":    columns,
        "summary": {
            "total":      len(columns),
            "pii":        pii_count,
            "inferred":   len(unreviewed),
            "draft":      sum(1 for c in columns if c.get("status") == "draft"),
            "published":  sum(1 for c in columns if c.get("status") == "published"),
        }
    }
    return warn(data, msgs) if msgs else ok(data)


@app.patch("/datasets/{collection:path}/{id}/schema/{column_name}",
           tags=["Schema"],
           summary="Edit a single column's semantic metadata and PII flags")
async def patch_schema_column(collection: str, id: str, column_name: str,
                               body: SchemaColumnPatch,
                               editor: str = Query("anonymous")):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)

    updates = body.model_dump(exclude_none=True)
    if "nullable" in updates:
        updates["nullable"] = int(updates["nullable"])
    if "pii" in updates:
        updates["pii"] = int(updates["pii"])

    updated = db.update_schema_column(collection, id, column_name,
                                      editor, **updates)
    if not updated:
        return ko("Column not found in schema", 404)

    col = next((c for c in db.get_schema(collection, id)
                if c["column_name"] == column_name), None)
    msgs = []
    if col and col.get("pii") and not col.get("pii_type"):
        msgs.append("PII flag set but pii_type is not specified — "
                    "consider setting it to: direct | indirect | sensitive")
    return warn(col, msgs) if msgs else ok(col)


@app.post("/datasets/{collection:path}/{id}/schema/publish",
          tags=["Schema"],
          summary="Publish the schema — promotes all columns to 'published' status",
          description=(
              "After publishing, the schema becomes a contract. "
              "Future commits will produce WARN if breaking changes are detected. "
              "A full snapshot is stored in schema_history for auditing."
          ))
async def publish_schema(collection: str, id: str,
                          body: SchemaPublishRequest):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)

    result = db.publish_schema(collection, id, body.published_by)
    msgs   = result["breaking_changes"] + result["warnings"]

    data = {
        "collection":     collection,
        "dataset_id":     id,
        "published_at":   result["published_at"],
        "published_by":   body.published_by,
        "breaking_changes": result["breaking_changes"],
        "warnings":       result["warnings"],
    }

    if result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking changes vs previous published schema"] + msgs)
    return warn(data, msgs) if msgs else ok(data)


# ===========================================================================
# Routes — Schema contract
# ===========================================================================

@app.put("/datasets/{collection:path}/{id}/schema/contract",
         tags=["Schema"],
         summary="Declare a schema contract — applied automatically at every commit",
         description=(
             "Define the expected columns with their semantic types and PII flags. "
             "The contract is stored as a dataset resource and applied transparently "
             "at commit time — the task writer does not need to call patch_column "
             "or publish_schema manually. "
             "Columns already in draft/published status are NOT overwritten."
         ))
async def set_schema_contract(collection: str, id: str,
                               body: SchemaContractRequest):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)

    cols = [c.model_dump(exclude_none=True) for c in body.columns]
    result = db.set_schema_contract(collection, id, cols, body.auto_publish)
    return ok(result)


@app.get("/datasets/{collection:path}/{id}/schema/contract",
         tags=["Schema"],
         summary="Get the declared schema contract for a dataset")
async def get_schema_contract(collection: str, id: str):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)
    contract = db.get_schema_contract(collection, id)
    if not contract:
        return ko("No contract defined for this dataset", 404)
    return ok(contract)


@app.delete("/datasets/{collection:path}/{id}/schema/contract",
            tags=["Schema"],
            summary="Remove the schema contract for a dataset")
async def delete_schema_contract(collection: str, id: str):
    if not db.get_dataset(collection, id):
        return ko("Dataset not found", 404)
    deleted = db.delete_schema_contract(collection, id)
    if not deleted:
        return ko("No contract found", 404)
    return ok({"deleted": True})


# ===========================================================================
# Routes — Version metadata
# ===========================================================================

@app.get("/datasets/{collection:path}/{id}/{version}/metadata",
         tags=["Metadata"],
         summary="Get all key-value metadata for a version")
async def get_metadata(collection: str, id: str, version: str):
    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)
    return ok(db.get_metadata(collection, id, version))


@app.post("/datasets/{collection:path}/{id}/{version}/metadata",
          tags=["Metadata"],
          summary="Set a metadata key on a version")
async def set_metadata(collection: str, id: str, version: str,
                        body: MetadataSetRequest):
    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)
    db.set_metadata(collection, id, version, body.key, body.value)
    return ok({"key": body.key, "value": body.value})


@app.delete("/datasets/{collection:path}/{id}/{version}/metadata/{key}",
            tags=["Metadata"],
            summary="Delete a metadata key from a version")
async def delete_metadata(collection: str, id: str, version: str, key: str):
    record = db.get_version(collection, id, version)
    if not record:
        return ko("Version not found", 404)
    deleted = db.delete_metadata(collection, id, version, key)
    if not deleted:
        return ko("Metadata key not found", 404)
    return ok({"key": key, "deleted": True})


# ===========================================================================
# Routes — Materialize (REST API → local CSV)
# ===========================================================================

@app.post("/datasets/{collection:path}/{id}/materialize",
          tags=["Materialize"],
          summary="Fetch a REST API endpoint and store result as a local CSV version",
          status_code=201)
async def materialize(collection: str, id: str, body: MaterializeRequest):
    version = _version_id()
    path    = _local_path(collection, id, version, "csv")

    try:
        db.ensure_collection(collection)
        db.ensure_dataset(collection, id,
                          display_name=body.display_name,
                          description=body.description)
        db.reserve(collection, id, version, path,
                   "csv", body.task_id, body.job_id)

        rows, schema_cols = await _fetch_and_write(
            body.base_url, body.endpoint, body.params, path)

        if rows == 0:
            db.fail(collection, id, version)
            return ko("No records returned from endpoint", 422)

        file_hash = _compute_hash(path)
        schema_kv = {c["name"]: c["physical_type"] for c in schema_cols}
        result    = db.commit(collection, id, version,
                              file_hash, rows, schema_kv)

        if result is None:
            db.fail(collection, id, version)
            return ko("Commit failed", 409)

        if result["skipped"]:
            try:
                os.remove(path)
            except Exception:
                pass
            return ok({
                "collection": collection,
                "dataset_id": id,
                "version":    result["version"],
                "rows":       rows,
                "skipped":    True,
                "reason":     "Identical to latest committed version",
                "source_url": f"{body.base_url}{body.endpoint}",
            })

        db.upsert_schema_columns(collection, id, schema_cols)
        db.insert_lineage(collection, id, version, [{
            "collection": "__external__",
            "dataset_id": f"{body.base_url}{body.endpoint}",
            "version":    "live",
        }])

        log(f"Materialized {collection}/{id}@{version} rows={rows}")
        return ok({
            "collection": collection,
            "dataset_id": id,
            "version":    version,
            "path":       path,
            "rows":       rows,
            "hash":       file_hash,
            "skipped":    False,
            "source_url": f"{body.base_url}{body.endpoint}",
        })

    except httpx.HTTPError as e:
        db.fail(collection, id, version)
        return ko(f"HTTP error fetching source: {e}", 502)
    except Exception as e:
        log(f"❌ Materialize error: {e}")
        try:
            db.fail(collection, id, version)
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
    count = _scan(data_path, body.collection)
    return ok({"scanned": count, "data_path": data_path})


# ===========================================================================
# Entrypoint
# ===========================================================================

def main():
    if args.scan:
        _scan(args.scan_path or DATA_PATH, args.scan_collection)
        return

    log("Waluigi Catalog v2")
    log(f"  Binding  : {args.bind_address}:{args.port}")
    log(f"  URL      : http://{args.host}:{args.port}")
    log(f"  DB       : {args.db_path}")
    log(f"  Data     : {args.data_path}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()

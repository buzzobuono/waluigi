import os
import sys
import csv
import socket
import yaml
from datetime import datetime, timezone
import configargparse
import httpx
import pandas as pd
import uvicorn
import logging
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from waluigi.core.responses import ok, warn, ko
from waluigi.core.utils import _model_dump
from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _version_id, _infer_schema, _safe_json_value
from waluigi.catalog.models import *
from waluigi.sdk.connectors import ConnectorFactory
    
logger = logging.getLogger("waluigi")

app = FastAPI(
    title="Waluigi Catalog",
    description="Data Catalog service: manages source, datasets, versions, schema, lineage and metadata.",
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

try:
    db = CatalogDB(args.db_path)
    logger.info(f"Database ready: {args.db_path}")
except Exception as e:
    logger.error(f"❌ Critical DB error: {e}")
    sys.exit(1)
    

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


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
    logger.info(f"🔍 Scanning {data_path} ...")
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
                db.reserve_version(dataset_id, version, filepath, fmt,
                           "scanner", "scan")
                result = db.commit(dataset_id, version, file_hash, None,
                                   {c["name"]: c["physical_type"] for c in schema})
                if result and not result["skipped"]:
                    db.upsert_schema_columns(dataset_id, schema)
                count += 1
                logger.info(f"  ✅ {dataset_id}@{version[:19]} [{fmt}]")
            except Exception as e:
                logger.error(f"  ⚠️  Skipped {filepath}: {e}")

    logger.info(f"🏁 Scan complete — {count} dataset(s) registered.")
    return count


# Routes Folders

@app.get("/folders/{prefix:path}/", tags=["Browse"],
         summary="List datasets and virtual sub-prefixes under a prefix",
         description=(
             "Trailing slash distinguishes browse from dataset access. "
             "Returns direct child datasets and deeper virtual prefixes, "
             "exactly like S3 ListObjects with a delimiter."
         ))
async def list_folders(prefix: str):
    return ok(db.list_folders(prefix))


# Routes Sources

@app.get("/sources", tags=["Sources"],
         summary="List sources")
async def list_sources():
    return ok(db.list_sources())


@app.post("/sources", tags=["Sources"],
          summary="Register or update a source (upsert)",
          status_code=200)
async def create_source(body: SourceCreateRequest):
    existing = db.get_source(body.id)
    if existing and existing["type"] != body.type:
        return ko(f"Cannot change source type from '{existing['type']}' to '{body.type.value}' — create a new source instead", 409)
    db.upsert_source(body.id, body.type, body.config, body.description)
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


# Routes — Version Metadata

@app.get("/datasets/{dataset_id:path}/versions/{version}/metadata",
         tags=["Metadata"],
         summary="Get all metadata for a version")
async def get_metadata(dataset_id: str, version: str):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    return ok(db.get_metadata(dataset_id, version))


@app.post("/datasets/{dataset_id:path}/versions/{version}/metadata",
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


@app.delete("/datasets/{dataset_id:path}/versions/{version}/metadata/{key}",
            tags=["Metadata"],
            summary="Delete a metadata key from a version")
async def delete_metadata(dataset_id: str, version: str, key: str):
    if not db.get_version(dataset_id, version):
        return ko("Version not found", 404)
    if not db.delete_metadata(dataset_id, version, key):
        return ko("Key not found or protected (sys.*)", 404)
    return ok({"key": key, "deleted": True})


# Routes - Versions

@app.get("/datasets/{dataset_id:path}/_preview/{version}",
         tags=["Versions"],
         summary="Preview rows of Dataset Version")
async def preview(dataset_id: str, version: str,
                  limit: int = 10, offset: int = 0):

    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)

    fmt = (dataset.get("format") or "").lower()

    source_id = dataset.get("source_id")
    if not source_id:
        return ko("Dataset has no source", 404)

    source = db.get_source(source_id)
    if not source:
        return ko(f"Source '{source_id}' not found", 404)

    version_record = db.get_version(dataset_id, version)
    if not version_record:
        return ko("Version not found", 404)

    location  = version_record["location"]
    source_type = source.get("type", "local")

    try:
        connector = ConnectorFactory.get(source_type, source.get("config") or {})
        result = connector.read(location, fmt, limit=limit, offset=offset)
    except NotImplementedError as e:
        return ko(str(e), 422)
    except Exception as e:
        return ko(f"Read error: {e}", 500)

    if isinstance(result, pd.DataFrame):
        df = result
    elif isinstance(result, list):
        df = pd.DataFrame(result)
    else:
        return ko(f"Preview not supported for format '{fmt}'", 422)

    clean = [{k: _safe_json_value(v) for k, v in row.items()}
             for row in df.to_dict(orient="records")]

    return ok({
        "dataset_id": dataset_id,
        "version":    version_record["version"],
        "columns":    df.columns.tolist(),
        "rows":       clean,
        "pagination": {"limit": limit, "offset": offset, "count": len(clean)},
    })
        
@app.get("/datasets/{dataset_id:path}/versions", tags=["Versions"],
         summary="List all committed versions (newest first)")
async def list_versions(dataset_id: str):
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    return ok(db.list_versions(dataset_id))

# Routes - Datasets Schema

@app.get("/datasets/{dataset_id:path}/schema", tags=["Schema"],
         summary="Get current schema with PII flags and status per column")
async def get_schema(dataset_id: str):
    if not db.exists_dataset(dataset_id):
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
    if not db.exists_dataset(dataset_id):
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
    if not db.exists_dataset(dataset_id):
        return ko("Dataset not found", 404)
    db.publish_schema(dataset_id, body.published_by)
    return ok({"dataset_id" : dataset_id})

# Routes - Datasets

@app.get("/datasets", tags=["Datasets"],
    summary="Find datasets",
    description="status: draft | in_review | approved | deprecated"
)
async def find_datasets(status: DatasetStatus | None = Query(default=None, example=DatasetStatus.DRAFT), 
                        description: str | None = Query(default=None, example="sales dataset")):
    if not status and not description:
        return ok(db.list_datasets())
    return ok(db.find_datasets(status=status, description=description))


@app.post("/datasets", tags=["Datasets"],
          summary="Register a new dataset",
          status_code=201)
async def create_dataset(body: DatasetCreateRequest):
    if body.source_id and not db.exists_source(body.source_id):
        return ko("Source not found", 404)
    if body.id.startswith("/"):
        return ko("Dataset 'id' not valid", 400)
    existing = db.get_dataset(body.id)
    if existing and existing["format"] != body.format:
        return ko(f"Cannot change format from '{existing['format']}' to '{body.format.value}' — create a new dataset instead", 409)
    created = db.create_dataset(body.id, body.format, body.description, body.source_id)
    # FIX ME gestire upsert in db.py analogogamemte a come fatto in source
    #if not created:
    #    return ko(f"Dataset '{body.id}' already exists", 409)
    return ok(db.get_dataset(body.id))

@app.get("/datasets/{id:path}", tags=["Datasets"],
           summary="Get a dataset details")
async def get_dataset(id: str):
    dataset = db.get_dataset(id)
    if not dataset:
        return ko("Dataset not found", 404)
    msgs = []
    if dataset.get("status") != "approved":
        msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
    return warn(dataset, msgs) if msgs else ok(dataset)


@app.patch("/datasets/{id:path}", tags=["Datasets"],
           summary="Update a dataset")
async def update_dataset(id: str, body: DatasetUpdateRequest):
    updated = db.update_dataset(id, **_model_dump(body))
    if not updated:
        return ko("Dataset not found", 404)
    return ok(db.get_dataset(id))


@app.delete("/datasets/{id:path}", tags=["Datasets"],
            summary="Delete a dataset")
async def delete_source(id: str):
    deleted = db.delete_dataset(id)
    ## Fix: loop su tutte le versioni di questo dataset e esegue la cancellazione anche delle vesioni
    # la cancellazione di una versione rimuove anche il dataset fisico correlato tramite il connector specifico
    if not deleted:
        return ko("Dataset not found", 404)
    return ok({"id": id, "deleted": True})
        
######


@app.get("/datasets/{dataset_id:path}/lineage/{version}", tags=["Lineage"],
         summary="Get upstream and downstream lineage")
async def get_lineage(dataset_id: str,
                      version: str):
    record = (db.get_version(dataset_id, version) if version
              else db.get_latest_version(dataset_id))
    if not record:
        return ko("Dataset version not found", 404)

    ver = record["version"]
    return ok({
        "dataset_id": dataset_id,
        "version":    ver,
        "upstream":   db.get_upstream(dataset_id, ver),
        "downstream": db.get_downstream(dataset_id, ver),
    })



# Dataset Status

@app.post("/datasets/{dataset_id:path}/approve",
          tags=["Datasets Status"],
          summary="Approve a dataset — marks it as reviewed and publishes its schema")
async def approve_dataset(dataset_id: str, body: ApproveRequest):
    dataset = db.get_dataset(dataset_id)
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
    logger.info(f"Approved {dataset_id} by {body.approved_by}")
    if schema_result["breaking_changes"]:
        return warn(data, ["⚠️ Breaking schema changes on approval"] + msgs)
    return warn(data, msgs) if msgs else ok(data)



# Dataset Produce

@app.post("/datasets/{dataset_id:path}/_reserve", tags=["Dataset Produce"],
          summary="Reserve a new version (phase 1 of 2-phase write)",
          status_code=201)
async def dataset_reserve(dataset_id: str, body: ReserveRequest):
    try:
        dataset = db.get_dataset(dataset_id)
        if not dataset:
            return ko("Dataset not found", 404)
        source = db.get_source(dataset["source_id"])
        
        if body and body.metadata:
            existing = db.find_version_by_metadata(dataset_id, body.metadata)
            if existing:
                msg = f"Skipped {dataset_id} new version creation because of identical metadata to {existing['version']} version"
                logger.info(msg)
                return warn({
                    "dataset_id": dataset_id,
                    "version":    existing["version"],
                    "source_id":  source["id"],
                    "location":   existing["location"],
                    "skipped":    True    
                  }, [msg])
                  
        connector = ConnectorFactory.get(source["type"], source["config"])
    
        version = _version_id()
        location = connector.resolve_location(dataset_id, version, dataset["format"], DATA_PATH)
        if not db.reserve_version(dataset_id, version, location):
            return ko("Version already exists", 409)
        logger.info(f"Reserved {dataset_id}@{version}")
        return ok({"dataset_id": dataset_id,
                   "version":    version,
                   "source_id":  source["id"],    
                   "location":   location,
                   "skipped":    False })
    except Exception as e:
        return ko(str(e), 500)


@app.post("/datasets/{dataset_id:path}/_commit/{version}",
          tags=["Dataset Produce"],
          summary="Commit a reserved version (phase 1 of 2-phase write)")
async def dataset_commit(dataset_id: str, version: str, body: CommitRequest):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    source = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    if record["status"] != "reserved":
        return ko(f"Cannot commit - status is '{record['status']}'", 409)

    location = record["location"]
    if not connector.exists(location):
        return ko(f"Dataset Version not found at: {location}", 422)
    msgs = []        
    try:
        if not db.commit_version(dataset_id, version):
            raise Exception
        
        for k, v in (body.metadata or {}).items():
            db.set_metadata(dataset_id, version, k, v)

        inferred = _infer_schema(location, dataset.get("format", ""))
        db.upsert_schema_columns(dataset_id, inferred)
        diff = db.diff_schema_against_inferred(dataset_id, inferred)
        
        if body.inputs:
            db.insert_lineage(dataset_id, version,
                              [_model_dump(i) for i in body.inputs])

        logger.info(f"Committed {dataset_id}@{version}")

        all_warnings = diff["breaking"] + diff["warnings"]
        data = {
            "dataset_id": dataset_id,
            "version":    version,
            "location":   location
        }
        
        if diff["breaking"]:
            msgs.append(["Schema breaking changes"] + all_warnings)
            raise Exception
        if all_warnings:
            return warn(data, all_warnings)
            
        return ok(data)

    except Exception as e:
        msg = f"Fail to commit {dataset_id}@{version}"
        msgs.append(msg)
        logger.error(msgs, e)
        try:
            db.delete_version(dataset_id, version)
            connector.delete(location) 
            logger.info(f"Cleanup: deleted orphaned location {location}")
        except Exception as cleanup_err:
            logger.warning(f"Failed to cleanup orphaned location {location}: {cleanup_err}")
        return ko(msg, 500)
        

@app.post("/datasets/{dataset_id:path}/_fail/{version}",
          tags=["Dataset Produce"],
          summary="Mark a reserved version as failed")
async def fail_version(dataset_id: str, version: str):
    dataset = db.get_dataset(dataset_id)
    if not dataset:
        return ko("Dataset not found", 404)
    source = db.get_source(dataset["source_id"])
    connector = ConnectorFactory.get(source["type"], source["config"])
    record = db.get_version(dataset_id, version)
    if not record:
        return ko("Version not found", 404)
    location = record["location"]
    db.fail_version(dataset_id, version)
    try:
        db.delete_version(dataset_id, version)
        connector.delete(location) 
        logger.info(f"Cleanup: deleted orphaned location {location}")
    except Exception as cleanup_err:
        logger.warning(f"Failed to cleanup orphaned location {location}: {cleanup_err}")
    return ok({"dataset_id": dataset_id,
               "version":    version,
               "status":     "failed"})


# Versions

@app.delete("/datasets/{dataset_id:path}/deprecate/{version}",
            tags=["Versions"],
            summary="Deprecate a dataset version")
async def deprecate(dataset_id: str, version: str):
    if not db.deprecate(dataset_id, version):
        return ko("Version not found", 404)
    logger.info(f"Deprecated {dataset_id}@{version}")
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
        logger.info(f"Virtual {dataset_id}@{version} [{src['type']}]")
        return ok({"dataset_id":  dataset_id,
                   "version":     version,
                   "source_id":   body.source_id,
                   "source_type": src["type"],
                   "location":    body.location,
                   "format":      body.format})
    except Exception as e:
        return ko(str(e), 500)



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
        db.reserve_version(dataset_id, version, path,
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
            db.fail_version(dataset_id, version)
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
        logger.info(f"Materialized {dataset_id}@{version} rows={rows}")
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
    with open("logging.yaml") as f:
        logging.config.dictConfig(yaml.safe_load(f))

    if args.scan:
        _scan(args.scan_path or DATA_PATH, args.scan_prefix)
        return

    logger.info("Waluigi Catalog v2")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_path}")
    logger.info(f"  Data    : {args.data_path}")
    
    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)
    
if __name__ == "__main__":
    main()

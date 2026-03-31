import os
import sys
import configargparse
import socket
import uvicorn
import csv
import httpx
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from waluigi.core.catalog_db import CatalogDB
from waluigi.core.catalog_helper import CatalogHelper

app = FastAPI(
    title="Waluigi Catalog",
    description="Data catalog service — manages datasets, versions, lineage and metadata.",
    version="1.0.0"
)

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CATALOG_')
p.add('--port', type=int, default=9000)
p.add('--host', default=socket.gethostname())
p.add('--bind-address', default='0.0.0.0')
p.add('--db-path', default=os.path.join(os.getcwd(), "db/catalog.db"))
p.add('--data-path', default=os.path.join(os.getcwd(), "data"))
p.add('--scan', action='store_true', default=False, help='Scan filesystem instead of starting server')
p.add('--scan-path', default=None, help='Path to scan (default: data-path)')
p.add('--scan-namespace', default=None, help='Namespace to assign scanned datasets')
args = p.parse_args()

DATA_PATH = args.data_path
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

helper = CatalogHelper()

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out"
}


def log(msg):
    print(f"[Catalog 📦] {msg}", flush=True)


try:
    db = CatalogDB(args.db_path)
    log(f"Database pronto: {args.db_path}")
except Exception as e:
    log(f"❌ Errore critico DB: {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LineageInput(BaseModel):
    namespace: str = Field(..., example="analytics/erp/raw")
    id:        str = Field(..., example="raw_erp")
    version:   str = Field(..., example="2026-03-29T10:00:00.000000")


class ReserveRequest(BaseModel):
    format:  str               = Field("", example="csv")
    task_id: str               = Field("unknown", example="clean_erp")
    job_id:  str               = Field("unknown", example="job/global_report")
    inputs:  List[LineageInput] = Field(default_factory=list)


class CommitRequest(BaseModel):
    rows:    Optional[int]            = Field(None, example=1500)
    columns: Optional[Dict[str, Any]] = Field(None, alias="schema", example={"date": "string", "value": "float64"})

    model_config = {"populate_by_name": True}


class MetadataRequest(BaseModel):
    key:   str = Field(..., example="owner")
    value: str = Field(..., example="data-engineering")


class NamespaceUpdateRequest(BaseModel):
    description: str = Field(..., example="Raw ERP data ingested daily")


class ScanRequest(BaseModel):
    data_path: Optional[str] = Field(None, example="/data/analytics")
    namespace: Optional[str] = Field(None, example="analytics/erp/raw")

class MaterializeRequest(BaseModel):
    swagger_url: str            = Field(...,
        example="https://petstore3.swagger.io/api/v3/openapi.json")
    endpoint:    str            = Field(...,
        example="/pet/findByStatus")
    params:      Dict[str, Any] = Field(default_factory=dict,
        example={"status": "available"})
    task_id:     str            = Field("unknown", example="ingest_orders")
    job_id:      str            = Field("unknown", example="job/ingest")
        
def _dataset_path(namespace, ds_id, version, fmt):
    safe_version = version.replace(":", "-")
    ext = f".{fmt}" if fmt else ""
    dataset_dir = os.path.join(DATA_PATH, namespace, ds_id)
    os.makedirs(dataset_dir, exist_ok=True)
    return os.path.join(dataset_dir, f"{safe_version}{ext}")

def _to_dict(model):
    if hasattr(model, 'model_dump'):
        return model.model_dump()
    return model.dict()
    
def _extract_base_url(spec: dict, swagger_url: str) -> str:
    parsed = urlparse(swagger_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    # OpenAPI v3
    servers = spec.get("servers", [])
    if servers:
        url = servers[0].get("url", "").rstrip("/")
        if url.startswith("http"):
            return url
        # relative URL — prepend origin
        return f"{origin}{url}"

    # Swagger v2
    host      = spec.get("host", "")
    base_path = spec.get("basePath", "/")
    schemes   = spec.get("schemes", ["https"])
    if host:
        return f"{schemes[0]}://{host}{base_path}".rstrip("/")

    # fallback
    return origin

def _extract_items(body) -> list:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "results", "items", "records", "content",
                    "pets", "users", "orders", "entries"):
            if key in body and isinstance(body[key], list):
                return body[key]
        # single-key dict whose value is a list
        values = [v for v in body.values() if isinstance(v, list)]
        if len(values) == 1:
            return values[0]
    return []


def _next_page(body, base_url: str, endpoint: str, current_page: int):
    if not isinstance(body, dict):
        return None
    for key in ("next", "next_page", "nextPage", "nextCursor"):
        val = body.get(key)
        if val and isinstance(val, str):
            return val if val.startswith("http") else f"{base_url}{val}"
    total = body.get("total_pages") or body.get("pages") or body.get("totalPages")
    if total and current_page < int(total):
        return f"{base_url}{endpoint}"  # caller adds page param
    return None


def _flatten(obj, prefix="", sep="_") -> dict:
    out = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, key, sep))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                out[key] = str(v)          # nested array → JSON string
            else:
                out[key] = ",".join(str(i) for i in v)
        else:
            out[key] = v
    return out


async def _fetch_and_write(swagger_url: str, endpoint: str,
                           params: dict, output_path: str):
    """
    Fetch all pages from endpoint, flatten JSON, write CSV.
    Returns (rows_count, schema_dict).
    """
    async with httpx.AsyncClient(timeout=30) as client:
        spec_r = await client.get(swagger_url)
        spec_r.raise_for_status()
        spec = spec_r.json()

    base_url = _extract_base_url(spec, swagger_url)

    records  = []
    page     = 1
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
            next_url = _next_page(body, base_url, endpoint, page)
            if next_url:
                page += 1
            else:
                break

    if not records:
        return 0, {}

    fieldnames = list(dict.fromkeys(k for r in records for k in r.keys()))
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    schema = {k: "string" for k in fieldnames}
    return len(records), schema
    
# ---------------------------------------------------------------------------
# Routes — Namespaces
# ---------------------------------------------------------------------------

@app.get("/namespaces", tags=["Namespaces"],
         summary="List root namespaces")
async def list_root_namespaces():
    return JSONResponse(db.list_namespace_children(parent=None))


@app.get("/namespaces/{ns:path}/children", tags=["Namespaces"],
         summary="List children of a namespace")
async def list_namespace_children(ns: str):
    node = db.get_namespace(ns)
    if not node:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"namespace": node, "children": db.list_namespace_children(ns)})


@app.get("/namespaces/{ns:path}/datasets", tags=["Namespaces"],
         summary="List datasets in a namespace")
async def list_namespace_datasets(ns: str, recursive: bool = False):
    node = db.get_namespace(ns)
    if not node:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"namespace": ns,
                         "datasets": db.list_datasets_in_namespace(ns, recursive)})


@app.patch("/namespaces/{ns:path}", tags=["Namespaces"],
           summary="Update namespace description")
async def update_namespace(ns: str, body: NamespaceUpdateRequest):
    ok = db.update_namespace_description(ns, body.description)
    if not ok:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Routes — Datasets
# ---------------------------------------------------------------------------

@app.post("/datasets/{namespace:path}/{id}/reserve", tags=["Datasets"],
          summary="Reserve a new dataset version",
          description="Phase 1 of two-phase write. Returns the path to write the file to.")
async def reserve(namespace: str, id: str, body: ReserveRequest):
    version = helper.now_iso()
    path    = _dataset_path(namespace, id, version, body.format)
    try:
        db.ensure_namespace(namespace)
        db.reserve(namespace, id, version, path, body.format, body.task_id, body.job_id)
        if body.inputs:
            db.insert_lineage(namespace, id, version,
                              [_to_dict(i) for i in body.inputs])
        log(f"Reserved {namespace}/{id}@{version}")
        return JSONResponse(
            {"namespace": namespace, "id": id, "version": version, "path": path},
            status_code=201)
    except Exception as e:
        print(e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/datasets/{namespace:path}/{id}/{version}/commit", tags=["Datasets"],
          summary="Commit a reserved version",
          description="Phase 2. Catalog computes SHA256 hash from the file and finalizes.")
async def commit(namespace: str, id: str, version: str, body: CommitRequest):
    record = db.get_version(namespace, id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    if record["status"] != "reserved":
        return JSONResponse(
            {"error": f"cannot commit, status is '{record['status']}'"}, status_code=409)

    path = record["path"]
    if not os.path.exists(path):
        return JSONResponse({"error": f"file not found at {path}"}, status_code=422)

    try:
        file_hash = helper.compute_hash(path)
        schema = body.columns or helper.infer_schema(path, record.get("format", ""))
        ok = db.commit(namespace, id, version, file_hash, body.rows, schema)
        if not ok:
            return JSONResponse({"error": "commit failed"}, status_code=409)
        log(f"Committed {namespace}/{id}@{version} hash={file_hash[:8]}...")
        return JSONResponse({"namespace": namespace, "id": id, "version": version,
                             "path": path, "hash": file_hash, "rows": body.rows})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/datasets/{namespace:path}/{id}/{version}/fail", tags=["Datasets"],
          summary="Mark a reserved version as failed")
async def fail_version(namespace: str, id: str, version: str):
    record = db.get_version(namespace, id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    db.fail(namespace, id, version)
    return JSONResponse({"status": "failed"})


@app.post("/datasets/{namespace:path}/{id}/materialize", tags=["Datasets"],
          summary="Materialize a REST API endpoint into a dataset",
          description=(
              "Reads the swagger spec to locate the base URL, "
              "fetches all pages from the given endpoint, "
              "flattens nested JSON, writes a single CSV file "
              "and registers it in the catalog. "
              "Lineage source is recorded as the API URL."
          ))
async def materialize(namespace: str, id: str, body: MaterializeRequest):
    version = helper.now_iso()
    path    = _dataset_path(namespace, id, version, "csv")

    try:
        db.ensure_namespace(namespace)
        db.reserve(namespace, id, version, path, "csv", body.task_id, body.job_id)

        # lineage: source is the external API URL
        db.insert_lineage(namespace, id, version, [{
            "namespace": "__external__",
            "id":        f"{body.swagger_url}#{body.endpoint}",
            "version":   "live"
        }])

        rows, schema = await _fetch_and_write(
            body.swagger_url, body.endpoint, body.params, path
        )

        if rows == 0:
            db.fail(namespace, id, version)
            return JSONResponse(
                {"error": "no records returned from endpoint"}, status_code=422)

        file_hash = helper.compute_hash(path)
        db.commit(namespace, id, version, file_hash, rows, schema)

        log(f"Materialized {namespace}/{id}@{version} rows={rows} "
            f"from {body.endpoint}")

        return JSONResponse({
            "namespace": namespace,
            "id":        id,
            "version":   version,
            "path":      path,
            "rows":      rows,
            "hash":      file_hash,
            "source":    f"{body.swagger_url}#{body.endpoint}"
        }, status_code=201)

    except httpx.HTTPError as e:
        db.fail(namespace, id, version)
        log(f"❌ HTTP error during materialize: {e}")
        return JSONResponse({"error": f"HTTP error: {e}"}, status_code=502)
    except Exception as e:
        log(f"❌ Materialize failed: {e}")
        try:
            db.fail(namespace, id, version)
        except Exception:
            pass
        return JSONResponse({"error": str(e)}, status_code=500)
            
@app.get("/datasets/{namespace:path}/{id}/resolve", tags=["Datasets"],
         summary="Resolve path of latest committed version")
async def resolve_latest(namespace: str, id: str):
    record = db.get_latest(namespace, id)
    if not record:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse({"namespace": namespace, "id": id,
                         "version": record["version"], "path": record["path"]})


@app.get("/datasets/{namespace:path}/{id}/latest", tags=["Datasets"],
         summary="Get full metadata of latest committed version")
async def get_latest(namespace: str, id: str):
    record = db.get_latest(namespace, id)
    if not record:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse(record)


@app.get("/datasets/{namespace:path}/{id}/history", tags=["Datasets"],
         summary="List all committed versions")
async def history(namespace: str, id: str):
    versions = db.get_history(namespace, id)
    if not versions:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse(versions)


@app.get("/datasets/{namespace:path}/{id}/metadata", tags=["Datasets"],
         summary="Get custom metadata for a dataset")
async def get_metadata(namespace: str, id: str):
    return JSONResponse(db.get_metadata(namespace, id))


@app.post("/datasets/{namespace:path}/{id}/metadata", tags=["Datasets"],
          summary="Set a custom metadata key")
async def set_metadata(namespace: str, id: str, body: MetadataRequest):
    db.set_metadata(namespace, id, body.key, body.value)
    return JSONResponse({"status": "ok"})


@app.delete("/datasets/{namespace:path}/{id}/metadata/{key}", tags=["Datasets"],
            summary="Delete a custom metadata key")
async def delete_metadata(namespace: str, id: str, key: str):
    ok = db.delete_metadata(namespace, id, key)
    if not ok:
        return JSONResponse({"error": "key not found"}, status_code=404)
    return JSONResponse({"status": "ok"})


@app.get("/datasets/{namespace:path}/{id}/{version}", tags=["Datasets"],
         summary="Get metadata for a specific version")
async def get_version(namespace: str, id: str, version: str):
    record = db.get_version(namespace, id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    return JSONResponse(record)


@app.delete("/datasets/{namespace:path}/{id}/{version}", tags=["Datasets"],
            summary="Deprecate a dataset version")
async def deprecate(namespace: str, id: str, version: str):
    ok = db.deprecate(namespace, id, version)
    if not ok:
        return JSONResponse({"error": "version not found"}, status_code=404)
    log(f"Deprecated {namespace}/{id}@{version}")
    return JSONResponse({"status": "deprecated"})


# ---------------------------------------------------------------------------
# Routes — Lineage
# ---------------------------------------------------------------------------

@app.get("/lineage/{namespace:path}/{id}/{version}/downstream", tags=["Lineage"],
         summary="Get downstream datasets (consumers of this version)")
async def lineage_downstream(namespace: str, id: str, version: str):
    record = db.get_version(namespace, id, version)
    if not record:
        return JSONResponse({"error": "dataset version not found"}, status_code=404)
    return JSONResponse({
        "namespace": namespace, "id": id, "version": version,
        "downstream": db.get_downstream(namespace, id, version)
    })


@app.get("/lineage/{namespace:path}/{id}/{version}", tags=["Lineage"],
         summary="Get upstream datasets (sources of this version)")
async def lineage_upstream(namespace: str, id: str, version: str):
    record = db.get_version(namespace, id, version)
    if not record:
        return JSONResponse({"error": "dataset version not found"}, status_code=404)
    return JSONResponse({
        "namespace": namespace, "id": id, "version": version,
        "upstream": db.get_upstream(namespace, id, version)
    })


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def _scan(data_path, namespace=None):
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

            # la cartella padre è il dataset_id
            dataset_id = os.path.basename(root)

            # il namespace è il path relativo fino alla cartella padre
            rel = os.path.relpath(os.path.dirname(root), data_path)
            ns  = namespace if namespace else (
                rel.replace(os.sep, "/") if rel != "." else "root"
            )

            # il nome del file senza estensione è la version
            version = os.path.splitext(filename)[0].replace("-", ":", 2)

            try:
                file_hash = helper.compute_hash(filepath)
                schema    = helper.infer_schema(filepath, fmt)
                db.ensure_namespace(ns)
                db.commit_scanned(ns, dataset_id, version, filepath, fmt,
                                  file_hash, None, schema)
                count += 1
                log(f"  ✅ {ns}/{dataset_id}@{version[:19]} [{fmt}]")
            except Exception as e:
                log(f"  ⚠️ Skipped {filepath}: {e}")

    log(f"🏁 Scan complete. {count} dataset(s) registered.")
    return count


@app.post("/scan", tags=["Scanner"],
          summary="Scan a filesystem path and register all datasets found")
async def scan_api(body: ScanRequest):
    data_path = body.data_path or DATA_PATH
    if not os.path.exists(data_path):
        return JSONResponse({"error": f"path not found: {data_path}"}, status_code=400)
    count = _scan(data_path, body.namespace)
    return JSONResponse({"status": "ok", "scanned": count})
        
def main():
    if args.scan:
        _scan(args.scan_path or DATA_PATH, args.scan_namespace)
        return
        
    log(f"Waluigi Catalog:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")
    log(f"    Data: {args.data_path}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()

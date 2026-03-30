import os
import sys
import configargparse
import socket
import uvicorn
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

def get_dataset_storage_path(self, namespace, ds_id, version, fmt):
        safe_version = version.replace(":", "-")
        ext = f".{fmt}" if fmt else ""
        dataset_dir = os.path.join(self.data_path, namespace, ds_id)
        os.makedirs(dataset_dir, exist_ok=True)
        return os.path.join(dataset_dir, f"{safe_version}{ext}")


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
    path    = helper.dataset_path(namespace, id, version, body.format)
    try:
        db.ensure_namespace(namespace)
        db.reserve(namespace, id, version, path, body.format, body.task_id, body.job_id)
        if body.inputs:
            db.insert_lineage(namespace, id, version,
                              [i.model_dump() for i in body.inputs])
        log(f"Reserved {namespace}/{id}@{version}")
        return JSONResponse(
            {"namespace": namespace, "id": id, "version": version, "path": path},
            status_code=201)
    except Exception as e:
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
        schema = body.columns or _infer_schema(path, record.get("format", ""))
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

import os
import sys
import json
import hashlib
import sqlite3
import threading
import configargparse
import socket
import uvicorn
from datetime import datetime, timezone
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse

app = FastAPI()

p = configargparse.ArgParser(auto_env_var_prefix='WALUIGI_CATALOG_')
p.add('--port', type=int, default=9000)
p.add('--host', default=socket.gethostname())
p.add('--bind-address', default='0.0.0.0')
p.add('--db-path', default=os.path.join(os.getcwd(), "db/catalog.db"))
p.add('--data-path', default=os.path.join(os.getcwd(), "data"))

args = p.parse_args()

DATA_PATH = args.data_path
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc"
}


def log(msg):
    print(f"[Catalog 📦] {msg}", flush=True)


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

class CatalogDB:

    def __init__(self, db_path):
        self.db_path = db_path
        self._local = threading.local()
        self._init()

    @property
    def conn(self):
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.connection.execute("PRAGMA busy_timeout = 30000")
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection

    def _init(self):
        with self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS namespaces (
                    path TEXT PRIMARY KEY,
                    parent TEXT,
                    name TEXT,
                    description TEXT,
                    created_at TEXT
                );

                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    path TEXT NOT NULL,
                    format TEXT,
                    hash TEXT,
                    produced_by_task TEXT,
                    produced_by_job TEXT,
                    created_at TEXT,
                    committed_at TEXT,
                    rows INTEGER,
                    schema TEXT,
                    status TEXT DEFAULT 'reserved',
                    PRIMARY KEY (id, version),
                    FOREIGN KEY (namespace) REFERENCES namespaces(path)
                );

                CREATE TABLE IF NOT EXISTS dataset_metadata (
                    dataset_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT,
                    PRIMARY KEY (dataset_id, key)
                );

                CREATE TABLE IF NOT EXISTS lineage (
                    output_id TEXT NOT NULL,
                    output_version TEXT NOT NULL,
                    input_id TEXT NOT NULL,
                    input_version TEXT NOT NULL,
                    PRIMARY KEY (output_id, output_version, input_id, input_version)
                );

                CREATE INDEX IF NOT EXISTS idx_datasets_ns ON datasets(namespace);
                CREATE INDEX IF NOT EXISTS idx_datasets_id_status ON datasets(id, status);
                CREATE INDEX IF NOT EXISTS idx_lineage_output ON lineage(output_id, output_version);
                CREATE INDEX IF NOT EXISTS idx_lineage_input ON lineage(input_id, input_version);
                CREATE INDEX IF NOT EXISTS idx_ns_parent ON namespaces(parent);
            """)

    # --- Namespaces ---

    def ensure_namespace(self, path, description=None):
        """Create namespace and all intermediate nodes (like mkdir -p)."""
        parts = path.strip("/").split("/")
        with self.conn:
            for i in range(1, len(parts) + 1):
                current = "/".join(parts[:i])
                parent = "/".join(parts[:i - 1]) if i > 1 else None
                name = parts[i - 1]
                self.conn.execute("""
                    INSERT OR IGNORE INTO namespaces (path, parent, name, description, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (current, parent, name,
                      description if i == len(parts) else None,
                      _now_iso()))

    def get_namespace(self, path):
        cursor = self.conn.execute("SELECT * FROM namespaces WHERE path = ?", (path,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def list_namespace_children(self, parent=None):
        if parent is None:
            cursor = self.conn.execute(
                "SELECT * FROM namespaces WHERE parent IS NULL ORDER BY name")
        else:
            cursor = self.conn.execute(
                "SELECT * FROM namespaces WHERE parent = ? ORDER BY name", (parent,))
        return [dict(r) for r in cursor.fetchall()]

    def update_namespace_description(self, path, description):
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE namespaces SET description = ? WHERE path = ?", (description, path))
            return cursor.rowcount > 0

    def list_datasets_in_namespace(self, namespace, recursive=False):
        if namespace in [None, "root"]:
            ns_condition = "namespace IS NULL"
            ns_like_condition = "namespace LIKE ?"
            params = ["/%"] # Per il recursive in root
        else:
            ns_condition = "namespace = ?"
            ns_like_condition = "namespace LIKE ?"
            params = [namespace, f"{namespace}/%"]

        query = f"""
            WITH LatestDatasets AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY committed_at DESC) as rn
                FROM datasets
                WHERE ( {ns_condition} {"OR " + ns_like_condition if recursive else ""} )
                AND status = 'committed'
            )
            SELECT id, version, namespace, path, format, hash, rows, 
                produced_by_task, produced_by_job, committed_at, status
            FROM LatestDatasets
            WHERE rn = 1
            ORDER BY id ASC
        """
        
        final_params = [namespace] if not recursive and namespace not in [None, "root"] else params
        if namespace in [None, "root"] and not recursive:
            final_params = []

        cursor = self.conn.execute(query, final_params)
        return [dict(r) for r in cursor.fetchall()]

    # --- Datasets ---

    def reserve(self, id, version, namespace, path, format, task_id, job_id):
        with self.conn:
            self.conn.execute("""
                INSERT INTO datasets
                    (id, version, namespace, path, format,
                     produced_by_task, produced_by_job, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'reserved')
            """, (id, version, namespace, path, format, task_id, job_id, version))

    def commit(self, id, version, file_hash, rows, schema):
        with self.conn:
            cursor = self.conn.execute("""
                UPDATE datasets SET
                    hash = ?, rows = ?, schema = ?,
                    committed_at = ?, status = 'committed'
                WHERE id = ? AND version = ? AND status = 'reserved'
            """, (file_hash, rows, json.dumps(schema) if schema else None,
                  _now_iso(), id, version))
            return cursor.rowcount > 0

    def commit_scanned(self, id, version, namespace, path, format,
                       file_hash, rows, schema):
        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO datasets
                    (id, version, namespace, path, format, hash, rows, schema,
                     created_at, committed_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'committed')
            """, (id, version, namespace, path, format, file_hash, rows,
                  json.dumps(schema) if schema else None, version, version))

    def fail(self, id, version):
        with self.conn:
            self.conn.execute("""
                UPDATE datasets SET status = 'failed'
                WHERE id = ? AND version = ? AND status = 'reserved'
            """, (id, version))

    def get_latest(self, id):
        cursor = self.conn.execute("""
            SELECT * FROM datasets
            WHERE id = ? AND status = 'committed'
            ORDER BY version DESC LIMIT 1
        """, (id,))
        return self._parse(cursor.fetchone())

    def get_version(self, id, version):
        cursor = self.conn.execute(
            "SELECT * FROM datasets WHERE id = ? AND version = ?", (id, version))
        return self._parse(cursor.fetchone())

    def get_history(self, id):
        cursor = self.conn.execute("""
            SELECT * FROM datasets WHERE id = ? AND status = 'committed'
            ORDER BY version DESC
        """, (id,))
        return [self._parse(r) for r in cursor.fetchall()]

    def deprecate(self, id, version):
        with self.conn:
            cursor = self.conn.execute("""
                UPDATE datasets SET status = 'deprecated'
                WHERE id = ? AND version = ?
            """, (id, version))
            return cursor.rowcount > 0

    def insert_lineage(self, output_id, output_version, inputs):
        with self.conn:
            self.conn.executemany("""
                INSERT OR IGNORE INTO lineage
                    (output_id, output_version, input_id, input_version)
                VALUES (?, ?, ?, ?)
            """, [(output_id, output_version, i["id"], i["version"])
                  for i in inputs])

    def get_upstream(self, id, version):
        cursor = self.conn.execute("""
            SELECT l.input_id, l.input_version, d.path, d.format,
                   d.hash, d.rows, d.produced_by_task, d.produced_by_job, d.namespace
            FROM lineage l
            LEFT JOIN datasets d ON d.id = l.input_id AND d.version = l.input_version
            WHERE l.output_id = ? AND l.output_version = ?
        """, (id, version))
        return [dict(r) for r in cursor.fetchall()]

    def get_downstream(self, id, version):
        cursor = self.conn.execute("""
            SELECT l.output_id, l.output_version, d.path, d.format,
                   d.hash, d.rows, d.produced_by_task, d.produced_by_job, d.namespace
            FROM lineage l
            LEFT JOIN datasets d ON d.id = l.output_id AND d.version = l.output_version
            WHERE l.input_id = ? AND l.input_version = ?
        """, (id, version))
        return [dict(r) for r in cursor.fetchall()]

    # --- Metadata ---

    def set_metadata(self, dataset_id, key, value):
        with self.conn:
            self.conn.execute("""
                INSERT INTO dataset_metadata (dataset_id, key, value)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset_id, key) DO UPDATE SET value = excluded.value
            """, (dataset_id, key, str(value)))

    def delete_metadata(self, dataset_id, key):
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM dataset_metadata WHERE dataset_id = ? AND key = ?",
                (dataset_id, key))
            return cursor.rowcount > 0

    def get_metadata(self, dataset_id):
        cursor = self.conn.execute(
            "SELECT key, value FROM dataset_metadata WHERE dataset_id = ?",
            (dataset_id,))
        return {r["key"]: r["value"] for r in cursor.fetchall()}

    def _parse(self, row):
        if not row:
            return None
        d = dict(row)
        if d.get("schema"):
            try:
                d["schema"] = json.loads(d["schema"])
            except Exception:
                pass
        return d
    
    def list_all_datasets(self):
        cursor = self.conn.execute("""
            SELECT id, version, namespace, format, rows, status, committed_at 
            FROM datasets 
            ORDER BY committed_at DESC
        """)
        return [dict(r) for r in cursor.fetchall()]
    
try:
    db = CatalogDB(args.db_path)
    log(f"Database pronto: {args.db_path}")
except Exception as e:
    log(f"❌ Errore critico DB: {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


def _compute_hash(path):
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _infer_schema(path, fmt):
    try:
        if fmt == "parquet":
            import pyarrow.parquet as pq
            schema = pq.read_schema(path)
            return {name: str(schema.field(name).type) for name in schema.names}
        elif fmt in ("csv", "tsv"):
            import csv
            sep = "\t" if fmt == "tsv" else ","
            with open(path, newline="") as f:
                headers = next(csv.reader(f, delimiter=sep), None)
            return {h: "string" for h in headers} if headers else None
    except Exception:
        pass
    return None


def _dataset_path(namespace, id, version, fmt):
    safe_version = version.replace(":", "-")
    ext = f".{fmt}" if fmt else ""
    dataset_dir = os.path.join(DATA_PATH, namespace, id)
    os.makedirs(dataset_dir, exist_ok=True)
    return os.path.join(dataset_dir, f"{safe_version}{ext}")


# ---------------------------------------------------------------------------
# Routes — Namespaces
# ---------------------------------------------------------------------------

@app.get("/namespaces")
async def list_root_namespaces():
    return JSONResponse(db.list_namespace_children(parent=None))


@app.get("/namespaces/{ns:path}/children")
async def list_namespace_children(ns: str):
    node = db.get_namespace(ns)
    if not node:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"namespace": node, "children": db.list_namespace_children(ns)})


@app.get("/namespaces/{ns:path}/datasets")
async def list_namespace_datasets(ns: str, recursive: bool = False):
    node = db.get_namespace(ns)
    if not node:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"namespace": ns,
                         "datasets": db.list_datasets_in_namespace(ns, recursive)})


@app.patch("/namespaces/{ns:path}")
async def update_namespace(ns: str, request: Request):
    data = await request.json()
    description = data.get("description")
    if not description:
        return JSONResponse({"error": "description required"}, status_code=400)
    ok = db.update_namespace_description(ns, description)
    if not ok:
        return JSONResponse({"error": "namespace not found"}, status_code=404)
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Routes — Datasets
# ---------------------------------------------------------------------------

@app.post("/datasets/{id}/reserve")
async def reserve(id: str, request: Request):
    data = await request.json()
    namespace = data.get("namespace")
    fmt = data.get("format", "")
    task_id = data.get("task_id", "unknown")
    job_id = data.get("job_id", "unknown")
    inputs = data.get("inputs", [])

    if not namespace:
        return JSONResponse({"error": "namespace is required"}, status_code=400)

    version = _now_iso()
    path = _dataset_path(namespace, id, version, fmt)

    try:
        db.ensure_namespace(namespace)
        db.reserve(id, version, namespace, path, fmt, task_id, job_id)
        if inputs:
            db.insert_lineage(id, version, inputs)
        log(f"Reserved {namespace}/{id}@{version}")
        return JSONResponse({"id": id, "version": version, "path": path},
                            status_code=201)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/datasets/{id}/{version}/commit")
async def commit(id: str, version: str, request: Request):
    data = await request.json()
    rows = data.get("rows")
    schema = data.get("schema")

    record = db.get_version(id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    if record["status"] != "reserved":
        return JSONResponse(
            {"error": f"cannot commit, status is '{record['status']}'"}, status_code=409)

    path = record["path"]
    if not os.path.exists(path):
        return JSONResponse({"error": f"file not found at {path}"}, status_code=422)

    try:
        file_hash = _compute_hash(path)
        if not schema:
            schema = _infer_schema(path, record.get("format", ""))
        ok = db.commit(id, version, file_hash, rows, schema)
        if not ok:
            return JSONResponse({"error": "commit failed"}, status_code=409)
        log(f"Committed {id}@{version} hash={file_hash[:8]}...")
        return JSONResponse({"id": id, "version": version,
                             "path": path, "hash": file_hash, "rows": rows})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/datasets/{id}/{version}/fail")
async def fail_version(id: str, version: str):
    record = db.get_version(id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    db.fail(id, version)
    return JSONResponse({"status": "failed"})


@app.get("/datasets/{id}/resolve")
async def resolve_latest(id: str):
    record = db.get_latest(id)
    if not record:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse({"id": id, "version": record["version"], "path": record["path"]})


@app.get("/datasets/{id}/{version}/resolve")
async def resolve_version(id: str, version: str):
    record = db.get_version(id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    if record["status"] != "committed":
        return JSONResponse(
            {"error": f"version status is '{record['status']}'"}, status_code=409)
    return JSONResponse({"id": id, "version": record["version"], "path": record["path"]})


@app.get("/datasets/{id}/history")
async def history(id: str):
    versions = db.get_history(id)
    if not versions:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse(versions)


@app.get("/datasets/{id}/{version}")
async def get_version(id: str, version: str):
    record = db.get_version(id, version)
    if not record:
        return JSONResponse({"error": "version not found"}, status_code=404)
    return JSONResponse(record)


@app.get("/datasets/{id}")
async def get_latest(id: str):
    record = db.get_latest(id)
    if not record:
        return JSONResponse({"error": "dataset not found"}, status_code=404)
    return JSONResponse(record)


@app.delete("/datasets/{id}/{version}")
async def deprecate(id: str, version: str):
    ok = db.deprecate(id, version)
    if not ok:
        return JSONResponse({"error": "version not found"}, status_code=404)
    log(f"Deprecated {id}@{version}")
    return JSONResponse({"status": "deprecated"})


# ---------------------------------------------------------------------------
# Routes — Metadata
# ---------------------------------------------------------------------------

@app.get("/datasets/{id}/metadata")
async def get_metadata(id: str):
    print("***")
    return JSONResponse(db.get_metadata(id))


@app.post("/datasets/{id}/metadata")
async def set_metadata(id: str, request: Request):
    data = await request.json()
    key = data.get("key")
    value = data.get("value")
    if not key:
        return JSONResponse({"error": "key is required"}, status_code=400)
    db.set_metadata(id, key, str(value))
    return JSONResponse({"status": "ok"})


@app.delete("/datasets/{id}/metadata/{key}")
async def delete_metadata(id: str, key: str):
    ok = db.delete_metadata(id, key)
    if not ok:
        return JSONResponse({"error": "key not found"}, status_code=404)
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Routes — Lineage
# ---------------------------------------------------------------------------

@app.get("/lineage/{id}/{version}")
async def lineage_upstream(id: str, version: str):
    return JSONResponse({"id": id, "version": version,
                         "upstream": db.get_upstream(id, version)})


@app.get("/lineage/{id}/{version}/downstream")
async def lineage_downstream(id: str, version: str):
    return JSONResponse({"id": id, "version": version,
                         "downstream": db.get_downstream(id, version)})


# ---------------------------------------------------------------------------
# Routes — Scan
# ---------------------------------------------------------------------------

@app.post("/scan")
async def scan_api(request: Request):
    data = await request.json()
    data_path = data.get("data_path", DATA_PATH)
    namespace = data.get("namespace")
    if not os.path.exists(data_path):
        return JSONResponse({"error": f"path not found: {data_path}"}, status_code=400)
    count = _scan(data_path, namespace)
    return JSONResponse({"status": "ok", "scanned": count})


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
            fmt = ext.lstrip(".")

            # infer namespace from directory structure if not provided
            if namespace:
                ns = namespace
            else:
                rel = os.path.relpath(root, data_path)
                ns = rel.replace(os.sep, "/") if rel != "." else "root"

            dataset_id = os.path.splitext(filename)[0]
            version = _now_iso()

            try:
                file_hash = _compute_hash(filepath)
                schema = _infer_schema(filepath, fmt)
                db.ensure_namespace(ns)
                db.commit_scanned(
                    id=dataset_id,
                    version=version,
                    namespace=ns,
                    path=filepath,
                    format=fmt,
                    file_hash=file_hash,
                    rows=None,
                    schema=schema
                )
                count += 1
                log(f"  ✅ {ns}/{dataset_id} [{fmt}] hash={file_hash[:8]}...")
            except Exception as e:
                log(f"  ⚠️ Skipped {filepath}: {e}")

    log(f"🏁 Scan complete. {count} dataset(s) registered.")
    return count
    
@app.get("/", response_class=HTMLResponse)
async def catalog_dashboard(
    path: str = Query(None), 
    view: str = Query("explorer"), 
    ds_id: str = Query(None), 
    ver: str = Query(None)
):
    datasets = db.list_all_datasets()
    
    html = """
    <html>
    <head>
        <title>Waluigi Catalog</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body { background-color: #1a0026; color: #e0e0e0; font-family: 'Segoe UI', sans-serif; padding: 15px; margin: 0; }
            h1 { color: #d080ff; font-size: 1.8em; border-bottom: 2px solid #4b0082; padding-bottom: 10px; }
            .namespace-container { background-color: #2b0040; border-radius: 8px; margin-bottom: 25px; padding: 15px; overflow-x: auto; }
            .namespace-header { color: #ffcc00; font-weight: bold; font-size: 1.1em; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center; }
            table { border-collapse: collapse; width: 100%; min-width: 650px; background-color: #360052; }
            th { background-color: #4b0082; color: white; text-align: left; padding: 12px; }
            td { padding: 4px 8px; border-bottom: 1px solid #4b0082; font-size: 0.85em; }
            .actions { display: flex; gap: 8px; justify-content: center; flex-wrap: wrap; }
            .status-COMMITTED { color: #00ff88; font-weight: bold; }
            .status-RESERVED { color: #ffff00; font-weight: bold; animation: blink 2s infinite; }
            .status-FAILED { color: #ff4444; font-weight: bold; }
            @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
            .btn-action { background: #4b0082; color: white; padding: 6px 10px; border-radius: 4px; border: 1px solid #d080ff; cursor: pointer; font-size: 0.8em; text-decoration: none; display: inline-block;}
            .btn-action:hover { background: #d080ff; color: #1a0026; }
            .breadcrumb { color: #ffcc00; font-weight: bold; font-size: 1.1em; margin-bottom: 15px; }
            .breadcrumb a { color: #00d4ff; text-decoration: none; }
            .lineage-node { background-color: #360052; border: 1px solid #4b0082; padding: 12px; border-radius: 8px; min-width: 220px; text-align: center; }
            .lineage-focus { border: 2px solid #ffcc00 !important; background-color: #2b0040 !important; box-shadow: 0 0 15px #ffcc0033; }
            .lineage-label { color: #8a2be2; font-size: 0.75em; font-weight: bold; text-transform: uppercase; margin-bottom: 5px; }
        </style>
    </head>
    <body>
        <h1>📦 Waluigi Catalog</h1>
    """

    if view == "lineage" and ds_id and ver:
        upstream = db.get_upstream(ds_id, ver)
        downstream = db.get_downstream(ds_id, ver)
        
        html += f"""
        <div class='breadcrumb'><a href='/?view=history&ds_id={ds_id}'>⬅️ History</a> / Lineage: {ds_id}</div>
        <div style='display: flex; flex-direction: column; align-items: center; gap: 20px;'>
        """
        if upstream:
            html += "<div style='display: flex; gap: 15px; flex-wrap: wrap; justify-content: center;'>"
            for item in upstream:
                v_link = item['input_version'].replace(":", "%3A")
                html += f"""
                <a href='/?view=lineage&ds_id={item['input_id']}&ver={v_link}' class='lineage-node' style='text-decoration:none; color:inherit;'>
                    <div class='lineage-label'>Sorgente</div>
                    <b style='color:#00ff88'>{item['input_id']}</b><br><small>{item['input_version'][:19]}</small>
                </a>"""
            html += "</div><div style='color: #4b0082; font-size: 24px;'>▼</div>"

        html += f"""
        <div class='lineage-node lineage-focus'>
            <div class='lineage-label'>Focus</div>
            <b style='font-size: 1.2em; color: #00ff88;'>{ds_id}</b><br><code>{ver[:19]}</code>
            <div style='margin-top: 10px; border-top: 1px solid #4b0082; padding-top: 10px;'>
                <a href='/?view=history&ds_id={ds_id}' class='btn-action' style='border-color:#ffcc00; color:#ffcc00'>🕒 History</a>
            </div>
        </div>
        """

        if downstream:
            html += "<div style='color: #4b0082; font-size: 24px;'>▼</div>"
            html += "<div style='display: flex; gap: 15px; flex-wrap: wrap; justify-content: center;'>"
            for item in downstream:
                v_link = item['output_version'].replace(":", "%3A")
                html += f"""
                <a href='/?view=lineage&ds_id={item['output_id']}&ver={v_link}' class='lineage-node' style='text-decoration:none; color:inherit;'>
                    <div class='lineage-label'>Derivato</div>
                    <b style='color:#00ff88'>{item['output_id']}</b><br><small>{item['output_version'][:19]}</small>
                </a>"""
            html += "</div>"
        html += "</div>"

    elif view == "history" and ds_id:
        versions = db.get_history(ds_id)
        html += f"<div class='breadcrumb'><a href='/?path={path or ''}'>⬅️ Explorer</a> / {ds_id}</div>"
        html += "<div class='namespace-container'><table><tr><th>Version</th><th>Status</th><th>Rows</th><th>Format</th><th>Action</th></tr>"
        for v in versions:
            v_enc = v['version'].replace(":", "%3A")
            html += f"""
            <tr>
                <td style='font-family:monospace'>{v['version']}</td>
                <td class='status-{v['status'].upper()}'>{v['status']}</td>
                <td>{v['rows'] or '-'}</td>
                <td>{v['format']}</td>
                <td><a href='/?view=lineage&ds_id={ds_id}&ver={v_enc}' class='btn-action'>Lineage</a></td>
            </tr>"""
        html += "</table></div>"

    else:
        current_path = path.strip("/") if path else None
        sub_ns = db.list_namespace_children(current_path)
        datasets = db.list_datasets_in_namespace(current_path) if current_path else db.list_datasets_in_namespace("root")
        
        bc = "<a href='/?view=explorer'>root</a>"
        if current_path:
            acc = ""
            for p_part in current_path.split("/"):
                acc = f"{acc}/{p_part}".strip("/")
                bc += f" / <a href='/?view=explorer&path={acc}'>{p_part}</a>"
        html += f"<div class='breadcrumb'>{bc}</div><div class='namespace-container'><table>"
        html += "<tr><th>Name</th><th>Type</th><th>Latest</th><th>Format</th><th>Action</th></tr>"
        
        for ns in sub_ns:
            html += f"<tr><td><a href='/?view=explorer&path={ns['path']}' style='color:#ffcc00; text-decoration:none;'>📁 {ns['name']}</a></td><td>NS</td><td>-</td><td>-</td><td>-</td></tr>"
        
        for d in datasets:
            v_enc = d['version'].replace(":", "%3A")
            html += f"""
            <tr>
                <td><a href='/?view=history&ds_id={d['id']}&path={path or ''}' style='color:#00ff88; text-decoration:none; font-weight:bold;'>📄 {d['id']}</a></td>
                <td>DS</td>
                <td style='font-family:monospace'><small>{d['version'][:19]}</small></td>
                <td>{d['format']}</td>
                <td><a href='/?view=lineage&ds_id={d['id']}&ver={v_enc}' class='btn-action'>Lineage</a></td>
            </tr>"""
        html += "</table></div>"

    html += "</body></html>"
    return html
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse as _ap

    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        sp = _ap.ArgumentParser(prog="wlcatalog scan")
        sp.add_argument("scan")
        sp.add_argument("--data-path", default=DATA_PATH)
        sp.add_argument("--namespace", default=None)
        sargs = sp.parse_args()
        _scan(sargs.data_path, sargs.namespace)
        return

    log(f"Waluigi Catalog:")
    log(f"    Binding: {args.bind_address}:{args.port}")
    log(f"    URL: http://{args.host}:{args.port}")
    log(f"    DB: {args.db_path}")
    log(f"    Data: {args.data_path}")
    uvicorn.run(app, host=args.bind_address, port=args.port)


if __name__ == "__main__":
    main()

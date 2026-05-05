import sqlite3
import threading
import json
from datetime import datetime, timezone
from waluigi.catalog.entities import _dataset, _version, _source


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
    
def _user() -> str:
    return "admin"
    
class CatalogDB:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local  = threading.local()
        self._init()

    @staticmethod
    def _row(row) -> dict | None:
        return dict(row) if row is not None else None

    @staticmethod
    def _rows(cur) -> list[dict]:
        return [dict(r) for r in cur.fetchall()]

    @property
    def conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "connection"):
            c = sqlite3.connect(self.db_path, check_same_thread=False)
            c.execute("PRAGMA busy_timeout = 30000")
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA foreign_keys = ON")
            c.row_factory = sqlite3.Row
            self._local.connection = c
        return self._local.connection

    def _init(self):
        with self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS sources (
                    id          TEXT PRIMARY KEY,
                    description TEXT,
                    type        TEXT NOT NULL, -- type: local | s3 | sql | sftp | api
                    config      TEXT NOT NULL DEFAULT '{}',  -- JSON
                    username    TEXT NOT NULL,
                    createdate  TEXT NOT NULL,
                    updatedate  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS datasets (
                    id           TEXT PRIMARY KEY,
                    format       TEXT NOT NULL,
                    description  TEXT,
                    status       TEXT NOT NULL DEFAULT 'draft',
                    source_id    TEXT REFERENCES sources(id),
                    dq_suite     TEXT,          -- path to DQ suite YAML on server (optional)
                    username     TEXT NOT NULL,
                    createdate   TEXT NOT NULL,
                    updatedate   TEXT NOT NULL
                );

                -- location semantics depend on source type:
                --   local → absolute file path
                --   s3    → s3://bucket/key
                --   sql   → table or SELECT query
                --   sftp  → remote absolute path
                --   api   → base_url#endpoint
                CREATE TABLE IF NOT EXISTS versions (
                    dataset_id       TEXT NOT NULL REFERENCES datasets(id),
                    version          TEXT NOT NULL,
                    location         TEXT NOT NULL,
                    status           TEXT NOT NULL DEFAULT 'reserved',
                    username         TEXT NOT NULL,
                    createdate       TEXT NOT NULL,
                    updatedate       TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, version)
                );

                -- One row per column per dataset (not per version).
                -- status lifecycle: inferred → draft → published
                CREATE TABLE IF NOT EXISTS schema_columns (
                    dataset_id     TEXT NOT NULL REFERENCES datasets(id),
                    column_name    TEXT NOT NULL,
                    physical_type  TEXT,
                    logical_type   TEXT,
                    nullable       INTEGER NOT NULL DEFAULT 1,
                    pii            INTEGER NOT NULL DEFAULT 0,
                    pii_type       TEXT    NOT NULL DEFAULT 'none', -- none | direct | indirect | sensitive
                    pii_notes      TEXT,
                    description    TEXT,
                    status         TEXT NOT NULL DEFAULT 'inferred',
                    username       TEXT NOT NULL,
                    createdate     TEXT NOT NULL,
                    updatedate     TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, column_name)
                );

                -- DQ expectations: one row per rule applied to a dataset.
                -- Replaces file-based dq_suite. Rules catalogue stays on FS.
                CREATE TABLE IF NOT EXISTS expectations (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    rule_id     TEXT NOT NULL,
                    inputs      TEXT NOT NULL DEFAULT '{}',  -- JSON {placeholder: "this.column"}
                    params      TEXT NOT NULL DEFAULT '{}',  -- JSON {param_name: value}
                    tolerance   REAL NOT NULL DEFAULT 1.0,
                    position    INTEGER NOT NULL DEFAULT 0,
                    username    TEXT NOT NULL,
                    createdate  TEXT NOT NULL,
                    updatedate  TEXT NOT NULL
                );

                -- Chart definitions: one row per chart per dataset.
                CREATE TABLE IF NOT EXISTS charts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    title       TEXT NOT NULL,
                    spec        TEXT NOT NULL DEFAULT '{}',  -- JSON chart spec
                    position    INTEGER NOT NULL DEFAULT 0,
                    username    TEXT NOT NULL,
                    createdate  TEXT NOT NULL,
                    updatedate  TEXT NOT NULL
                );

                -- DQ run results: one row per committed version.
                -- Replaces sys.dq.* version_metadata keys.
                CREATE TABLE IF NOT EXISTS dq_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    version     TEXT NOT NULL,
                    score       REAL NOT NULL DEFAULT 0,
                    passed      INTEGER NOT NULL DEFAULT 0,
                    total       INTEGER NOT NULL DEFAULT 0,
                    success     INTEGER NOT NULL DEFAULT 0,
                    details     TEXT NOT NULL DEFAULT '[]',  -- JSON [{rule_id, success, score, error}]
                    error       TEXT,
                    createdate  TEXT NOT NULL,
                    UNIQUE (dataset_id, version)
                );

                CREATE TABLE IF NOT EXISTS lineage (
                    output_dataset  TEXT NOT NULL,
                    output_version  TEXT NOT NULL,
                    input_dataset   TEXT NOT NULL,
                    input_version   TEXT NOT NULL,
                    username        TEXT NOT NULL,
                    createdate      TEXT NOT NULL,
                    updatedate      TEXT NOT NULL,
                    PRIMARY KEY (output_dataset, output_version,
                                 input_dataset,  input_version)
                );

                -- sys.* keys are written by the server only.
                -- Plain keys are free business metadata from the task.
                CREATE TABLE IF NOT EXISTS version_metadata (
                    dataset_id TEXT NOT NULL,
                    version    TEXT NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT,
                    username        TEXT NOT NULL,
                    createdate      TEXT NOT NULL,
                    updatedate      TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, version, key)
                );

            """)
            # migrations for existing databases
            for stmt in [
                "ALTER TABLE datasets ADD COLUMN dq_suite TEXT",
                """CREATE TABLE IF NOT EXISTS expectations (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    rule_id     TEXT NOT NULL,
                    inputs      TEXT NOT NULL DEFAULT '{}',
                    params      TEXT NOT NULL DEFAULT '{}',
                    tolerance   REAL NOT NULL DEFAULT 1.0,
                    position    INTEGER NOT NULL DEFAULT 0,
                    username    TEXT NOT NULL,
                    createdate  TEXT NOT NULL,
                    updatedate  TEXT NOT NULL
                )""",
                """CREATE TABLE IF NOT EXISTS dq_results (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    version     TEXT NOT NULL,
                    score       REAL NOT NULL DEFAULT 0,
                    passed      INTEGER NOT NULL DEFAULT 0,
                    total       INTEGER NOT NULL DEFAULT 0,
                    success     INTEGER NOT NULL DEFAULT 0,
                    details     TEXT NOT NULL DEFAULT '[]',
                    error       TEXT,
                    createdate  TEXT NOT NULL,
                    UNIQUE (dataset_id, version)
                )""",
                """CREATE TABLE IF NOT EXISTS charts (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id  TEXT NOT NULL REFERENCES datasets(id),
                    title       TEXT NOT NULL,
                    spec        TEXT NOT NULL DEFAULT '{}',
                    position    INTEGER NOT NULL DEFAULT 0,
                    username    TEXT NOT NULL,
                    createdate  TEXT NOT NULL,
                    updatedate  TEXT NOT NULL
                )""",
            ]:
                try:
                    self.conn.execute(stmt)
                except Exception:
                    pass


    # Folders
    
    def list_folders(self, prefix: str) -> dict:
        prefix = prefix.rstrip("/") + "/"
        prefix = prefix.lstrip("/")
        cur = self.conn.execute("""
            SELECT *
            FROM datasets
            WHERE id LIKE ?
            ORDER BY id
        """, (f"{prefix}%",))
    
        all_rows = cur.fetchall()
        datasets, sub_prefixes = [], set()
    
        for row in all_rows:
            d = _dataset(row)
            rest = d["id"][len(prefix):]
            if "/" not in rest:
                datasets.append(d)
            else:
                sub = prefix + rest.split("/")[0] + "/"
                sub_prefixes.add(sub)
    
        return {
            "prefix":   prefix,
            "datasets": datasets,
            "prefixes": sorted(sub_prefixes),
        }
        
    # Sources
    
    def list_sources(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM sources ORDER BY id")
        return [_source(r) for r in cur.fetchall()]
    
    def create_source(self, id: str, type: str, config: dict,
                      description: str = None) -> bool:
        now = _now()
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO sources
                        (id, description, type, config, username, createdate, updatedate)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (id, description, type, json.dumps(config), _user(), now, now))
            return True
        except sqlite3.IntegrityError:
            return False
    
    def exists_source(self, id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM sources WHERE id = ?", (id,))
        return cur.fetchone() is not None
    
    def get_source(self, id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM sources WHERE id = ?", (id,))
        return _source(cur.fetchone())
    
    def update_source(self, id: str, **kwargs) -> bool:
        allowed = {"type", "config", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "config" in updates:
            updates["config"] = json.dumps(updates["config"])
        updates["updatedate"] = _now()
        updates["username"]= _user()
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE sources SET {cols} WHERE id = ?", vals)
            return cur.rowcount > 0
    
    def upsert_source(self, id: str, type: str, config: dict,
                  description: str = None) -> None:
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT INTO sources (id, type, config, description, username, createdate, updatedate)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    config      = excluded.config,
                    description = excluded.description,
                    username    = excluded.username,
                    updatedate  = excluded.updatedate
            """, (id, type, json.dumps(config), description, _user(), now, now))
            
    def delete_source(self, id: str) -> bool:
        with self.conn:
            cur = self.conn.execute("DELETE FROM sources WHERE id = ?", (id,))
            return cur.rowcount > 0
    
    # Datasets
    
    def list_datasets(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM datasets ORDER BY id")
        return [_dataset(r) for r in cur.fetchall()]

    def find_datasets(self, status: str, description: str) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM datasets WHERE status = ? and description LIKE ? ORDER BY id", (status, description))
        return [_dataset(r) for r in cur.fetchall()]

    def create_dataset(self, id: str, format: str, description: str = None,
                       source_id: str = "local", dq_suite: str = None) -> bool:
        now = _now()
        with self.conn:
            cur = self.conn.execute(
                "INSERT INTO datasets (id, format, description, status, source_id, dq_suite, username, createdate, updatedate) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING",
                (id, format, description, 'draft', source_id, dq_suite, _user(), now, now))
            return cur.rowcount > 0
  
    def exists_dataset(self, id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM datasets WHERE id = ?", (id,))
        return cur.fetchone() is not None
    
    def get_dataset(self, id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM datasets WHERE id = ?", (id,))
        return _dataset(cur.fetchone())
        
    def update_dataset(self, id: str, **kwargs) -> bool:
        allowed = {"description", "status", "dq_suite"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        updates["updatedate"] = _now()
        updates["username"]= _user()
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE datasets SET {cols} WHERE id = ?", vals)
            return cur.rowcount > 0
   
    def delete_dataset(self, id: str) -> bool:
        with self.conn:
            self.conn.execute("DELETE FROM versions WHERE dataset_id = ?", (id,))
            cur = self.conn.execute("DELETE FROM datasets WHERE id = ?", (id,))
            return cur.rowcount > 0
    
    # Versions        
    
    def list_versions(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT *
            FROM versions
            WHERE dataset_id = ? 
            --AND status = 'committed'
            ORDER BY createdate DESC
        """, (dataset_id,))
        rows = cur.fetchall()
        return [_version({**dict(r), "id": dataset_id}) for r in rows]
 
 
    def get_version(self, dataset_id: str, version: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT *
            FROM versions
            WHERE dataset_id = ? AND version = ?
        """, (dataset_id, version))
        return _version(cur.fetchone())

    def get_latest_version(self, dataset_id: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT * FROM versions
            WHERE dataset_id = ? AND status = 'committed'
            ORDER BY updatedate DESC LIMIT 1
        """, (dataset_id,))
        return _version(cur.fetchone())
     
    def find_version_by_metadata(self, dataset_id: str, metadata: dict) -> dict | None:
        if metadata is None:
            return None

        cur = self.conn.execute("""
            SELECT * FROM versions 
            WHERE dataset_id = ? AND status = 'committed'
            ORDER BY updatedate DESC LIMIT 1
        """, (dataset_id,))
        
        row = cur.fetchone()
        if not row:
            return None
            
        version_id = row["version"]

        existing_meta = self.get_metadata(dataset_id, version_id)
        existing_meta_user = {k: v for k, v in existing_meta.items() if not k.startswith("sys.")}

        target_meta = {k: str(v) for k, v in metadata.items()}

        if existing_meta_user == target_meta:
            return _version(row)
            
        return None

    def reserve_version(self, dataset_id: str, version: str, location: str) -> bool:
        now = _now()
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO versions
                        (dataset_id, version, location, status, 
                        username, createdate, updatedate)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (dataset_id, version, location, 'reserved', _user(), now, now))
            return True
        except sqlite3.IntegrityError as e:
            return False

    def commit_version(self, dataset_id: str, version: str) -> dict | None:
        with self.conn:
            now = _now()
            cur = self.conn.execute("""
                UPDATE versions SET
                    updatedate = ?, 
                    status = 'committed'
                WHERE dataset_id = ? AND version = ? AND status = 'reserved'
            """, (now, dataset_id, version))
            if cur.rowcount == 0:
                return False
            return True
    
    def fail_version(self, dataset_id: str, version: str):
        now = _now()
        with self.conn:
            cur = self.conn.execute("""
                UPDATE versions SET 
                    updatedate = ?, 
                    status = 'failed'
                WHERE dataset_id = ? AND version = ? AND status = 'reserved'
            """, (now, dataset_id, version))
            if cur.rowcount == 0:
                return False
            return True

    def delete_version(self, dataset_id: str, version: str) -> dict | None:
        with self.conn:
            cur = self.conn.execute("""
                DELETE FROM versions
                    WHERE dataset_id = ? AND version = ?
            """, (dataset_id, version))    
            if cur.rowcount == 0:
                return False
            return True
                
    # Version metadata
    
    def set_metadata(self, dataset_id: str, version: str,
                     key: str, value: str):
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT INTO version_metadata (dataset_id, version, key, value, username, createdate, updatedate)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id, version, key)
                DO UPDATE SET value = excluded.value
            """, (dataset_id, version, key, str(value), _user(), now, now))
            
    def delete_metadata(self, dataset_id: str, version: str, key: str) -> bool:
        if key.startswith("sys."):
            return False   # sys.* keys are immutable
        with self.conn:
            cur = self.conn.execute("""
                DELETE FROM version_metadata
                WHERE dataset_id = ? AND version = ? AND key = ?
            """, (dataset_id, version, key))
            return cur.rowcount > 0

    def get_metadata(self, dataset_id: str, version: str) -> dict:
        cur = self.conn.execute("""
            SELECT key, value FROM version_metadata
            WHERE dataset_id = ? AND version = ?
            ORDER BY key
        """, (dataset_id, version))
        return {r["key"]: r["value"] for r in cur.fetchall()}
            
    # Dataset Schema
    
    def upsert_schema_columns(self, dataset_id: str, columns: list[dict]):
        now = _now()
        with self.conn:
            for col in columns:
                self.conn.execute("""
                    INSERT INTO schema_columns (dataset_id , column_name, physical_type, logical_type, nullable, pii, pii_type, pii_notes, description, status, username, createdate, updatedate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(dataset_id, column_name) DO UPDATE SET
                        physical_type  = excluded.physical_type,
                        updatedate = excluded.updatedate
                    WHERE schema_columns.status = 'inferred'
                """, (dataset_id , col["name"], col.get("physical_type"), col.get("logical_type"), 1, 0, 'none', '', '', 'inferred', _user(), now, now) )
    
    def get_schema(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM schema_columns
            WHERE dataset_id = ?
            ORDER BY column_name
        """, (dataset_id,))
        return self._rows(cur)

    def update_schema_column(self, dataset_id: str, column_name: str, **kwargs) -> bool:
        allowed = {
            "logical_type", "nullable", "pii", "pii_type",
            "pii_notes", "description"
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        if "nullable" in updates:
            updates["nullable"] = int(updates["nullable"])
        if "pii" in updates:
            updates["pii"] = int(updates["pii"])

        updates["username"] = _user()
        updates["updatedate"] = _now()

        set_parts = [f"{k} = ?" for k in updates]
        set_parts.append(
            "status = CASE WHEN status = 'published' THEN 'published' ELSE 'draft' END"
        )
        vals = list(updates.values()) + [dataset_id, column_name]

        with self.conn:
            cur = self.conn.execute(
                f"UPDATE schema_columns SET {', '.join(set_parts)} "
                f"WHERE dataset_id = ? AND column_name = ?", vals)
            return cur.rowcount > 0

    def publish_schema(self, dataset_id: str, publisher: str) -> dict:
        """Promote all columns to published."""
        now = _now()
        with self.conn:
            self.conn.execute("""
                UPDATE schema_columns
                SET status = 'published', updatedate = ?
                WHERE dataset_id = ? AND status IN ('inferred', 'draft')
            """, (now, dataset_id))

    def approve_schema_column(self, dataset_id: str, column_name: str) -> bool:
        """Promote a single column to published."""
        now = _now()
        with self.conn:
            cur = self.conn.execute("""
                UPDATE schema_columns
                SET status = 'published', username = ?, updatedate = ?
                WHERE dataset_id = ? AND column_name = ?
            """, (_user(), now, dataset_id, column_name))
            return cur.rowcount > 0

    def delete_schema_column(self, dataset_id: str, column_name: str) -> bool:
        """Remove a column from the schema definition."""
        with self.conn:
            cur = self.conn.execute("""
                DELETE FROM schema_columns
                WHERE dataset_id = ? AND column_name = ?
            """, (dataset_id, column_name))
            return cur.rowcount > 0
            
    # Dataset Expectations

    def list_expectations(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM expectations
            WHERE dataset_id = ?
            ORDER BY position, id
        """, (dataset_id,))
        rows = self._rows(cur)
        for r in rows:
            r["inputs"] = json.loads(r.get("inputs") or "{}")
            r["params"] = json.loads(r.get("params") or "{}")
        return rows

    def add_expectation(self, dataset_id: str, rule_id: str,
                        inputs: dict, params: dict,
                        tolerance: float = 1.0, position: int = 0) -> dict:
        now = _now()
        with self.conn:
            cur = self.conn.execute("""
                INSERT INTO expectations
                    (dataset_id, rule_id, inputs, params, tolerance, position,
                     username, createdate, updatedate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (dataset_id, rule_id, json.dumps(inputs), json.dumps(params),
                  tolerance, position, _user(), now, now))
            row_id = cur.lastrowid
        return self.get_expectation(dataset_id, row_id)

    def get_expectation(self, dataset_id: str, exp_id: int) -> dict | None:
        cur = self.conn.execute("""
            SELECT * FROM expectations WHERE dataset_id = ? AND id = ?
        """, (dataset_id, exp_id))
        row = self._row(cur.fetchone())
        if row:
            row["inputs"] = json.loads(row.get("inputs") or "{}")
            row["params"] = json.loads(row.get("params") or "{}")
        return row

    def update_expectation(self, dataset_id: str, exp_id: int, **kwargs) -> bool:
        allowed = {"rule_id", "inputs", "params", "tolerance", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "inputs" in updates:
            updates["inputs"] = json.dumps(updates["inputs"])
        if "params" in updates:
            updates["params"] = json.dumps(updates["params"])
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [dataset_id, exp_id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE expectations SET {cols} WHERE dataset_id = ? AND id = ?", vals)
            return cur.rowcount > 0

    def delete_expectation(self, dataset_id: str, exp_id: int) -> bool:
        with self.conn:
            cur = self.conn.execute(
                "DELETE FROM expectations WHERE dataset_id = ? AND id = ?",
                (dataset_id, exp_id))
            return cur.rowcount > 0

    # DQ Results

    def save_dq_result(self, dataset_id: str, version: str,
                       score: float, passed: int, total: int,
                       success: bool, details: list,
                       error: str = None) -> dict:
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT INTO dq_results
                    (dataset_id, version, score, passed, total, success, details, error, createdate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id, version) DO UPDATE SET
                    score    = excluded.score,
                    passed   = excluded.passed,
                    total    = excluded.total,
                    success  = excluded.success,
                    details  = excluded.details,
                    error    = excluded.error,
                    createdate = excluded.createdate
            """, (dataset_id, version, score, passed, total,
                  int(success), json.dumps(details), error, now))
        return self.get_dq_result(dataset_id, version)

    def get_dq_result(self, dataset_id: str, version: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT * FROM dq_results WHERE dataset_id = ? AND version = ?
        """, (dataset_id, version))
        row = self._row(cur.fetchone())
        if row:
            row["details"] = json.loads(row.get("details") or "[]")
            row["success"] = bool(row["success"])
        return row

    def list_dq_results(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM dq_results WHERE dataset_id = ?
            ORDER BY createdate DESC
        """, (dataset_id,))
        rows = self._rows(cur)
        for r in rows:
            r["details"] = json.loads(r.get("details") or "[]")
            r["success"] = bool(r["success"])
        return rows

    # Charts

    def list_charts(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM charts WHERE dataset_id = ?
            ORDER BY position, id
        """, (dataset_id,))
        rows = self._rows(cur)
        for r in rows:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return rows

    def add_chart(self, dataset_id: str, title: str, spec: dict,
                  position: int = 0) -> dict:
        now = _now()
        with self.conn:
            cur = self.conn.execute("""
                INSERT INTO charts (dataset_id, title, spec, position, username, createdate, updatedate)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (dataset_id, title, json.dumps(spec), position, _user(), now, now))
            row_id = cur.lastrowid
        return self.get_chart(dataset_id, row_id)

    def get_chart(self, dataset_id: str, chart_id: int) -> dict | None:
        cur = self.conn.execute("""
            SELECT * FROM charts WHERE dataset_id = ? AND id = ?
        """, (dataset_id, chart_id))
        row = self._row(cur.fetchone())
        if row:
            row["spec"] = json.loads(row.get("spec") or "{}")
        return row

    def update_chart(self, dataset_id: str, chart_id: int, **kwargs) -> bool:
        allowed = {"title", "spec", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "spec" in updates:
            updates["spec"] = json.dumps(updates["spec"])
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [dataset_id, chart_id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE charts SET {cols} WHERE dataset_id = ? AND id = ?", vals)
            return cur.rowcount > 0

    def delete_chart(self, dataset_id: str, chart_id: int) -> bool:
        with self.conn:
            cur = self.conn.execute(
                "DELETE FROM charts WHERE dataset_id = ? AND id = ?",
                (dataset_id, chart_id))
            return cur.rowcount > 0

    def diff_schema_against_inferred(self, dataset_id: str,
                                      inferred: list[dict]) -> dict:
        """Compare freshly inferred columns against published schema."""
        published = self.conn.execute("""
            SELECT * FROM schema_columns
            WHERE dataset_id = ? AND status = 'published'
        """, (dataset_id,)).fetchall()

        if not published:
            return {"breaking": [], "warnings": []}

        pub = {dict(r)["column_name"]: dict(r) for r in published}
        inf = {c["name"]: c for c in inferred}

        breaking, warnings = [], []
        for col, meta in pub.items():
            if col not in inf:
                breaking.append(f"Published column '{col}' missing in new data")
            elif inf[col].get("physical_type") != meta["physical_type"]:
                breaking.append(
                    f"Type change on '{col}': "
                    f"{meta['physical_type']} → {inf[col].get('physical_type')}")
        return {"breaking": breaking, "warnings": warnings}


    # ------
    def set_in_review(self, id: str) -> bool:
        """Promote dataset from draft to in_review. No-op if approved."""
        with self.conn:
            cur = self.conn.execute("""
                UPDATE datasets SET status = 'in_review'
                WHERE id = ? AND status = 'draft'
            """, (id,))
            return cur.rowcount > 0

    def approve_dataset(self, id: str, approved_by: str) -> bool:
        """Approve dataset and publish its schema atomically."""
        now = _now()
        with self.conn:
            cur = self.conn.execute("""
                UPDATE datasets
                SET status = 'approved', approved_by = ?, approved_at = ?
                WHERE id = ? AND status != 'deprecated'
            """, (approved_by, now, id))
            return cur.rowcount > 0
    
    def commit_virtual(self, dataset_id: str, version: str, source_id: str,
                       location: str, fmt: str,
                       task_id: str, job_id: str) -> dict:
        """Register a virtual version (no local file, no hash)."""
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO versions
                    (dataset_id, version, source_id, location, format,
                     produced_by_task, produced_by_job,
                     status, created_at, committed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'committed', ?, ?)
            """, (dataset_id, version, source_id, location, fmt,
                  task_id, job_id, now, now))
        return {"skipped": False, "version": version}



    def deprecate(self, dataset_id: str, version: str) -> bool:
        with self.conn:
            cur = self.conn.execute("""
                UPDATE versions SET status = 'deprecated'
                WHERE dataset_id = ? AND version = ?
            """, (dataset_id, version))
            return cur.rowcount > 0
    
    
    
    # Lineage
   
    def insert_lineage(self, out_id: str, out_ver: str,
                       inputs: list[dict]):
        """inputs: [{"dataset_id": ..., "version": ...}]"""
        with self.conn:
            self.conn.executemany("""
                INSERT OR IGNORE INTO lineage
                    (output_dataset, output_version, input_dataset, input_version)
                VALUES (?, ?, ?, ?)
            """, [(out_id, out_ver, i["dataset_id"], i["version"])
                  for i in inputs])

    def get_upstream(self, dataset_id: str, version: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT l.input_dataset  AS dataset_id,
                   l.input_version  AS version,
                   v.location, v.format, v.source_id, v.rows, v.hash,
                   v.produced_by_task, v.produced_by_job
            FROM lineage l
            LEFT JOIN versions v
                ON v.dataset_id = l.input_dataset
                AND v.version   = l.input_version
            WHERE l.output_dataset = ? AND l.output_version = ?
        """, (dataset_id, version))
        return self._rows(cur)

    def get_downstream(self, dataset_id: str, version: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT l.output_dataset AS dataset_id,
                   l.output_version AS version,
                   v.location, v.format, v.source_id, v.rows, v.hash,
                   v.produced_by_task, v.produced_by_job
            FROM lineage l
            LEFT JOIN versions v
                ON v.dataset_id = l.output_dataset
                AND v.version   = l.output_version
            WHERE l.input_dataset = ? AND l.input_version = ?
        """, (dataset_id, version))
        return self._rows(cur)
    
    

    
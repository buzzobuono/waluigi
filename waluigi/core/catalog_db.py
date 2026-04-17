import sqlite3
import threading
import json
from datetime import datetime, timezone
from waluigi.core.entities import _dataset, _version, _source


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
    
def _user() -> str:
    return "admin"
    
class CatalogDB:
    """
    SQLite backend for the Waluigi Catalog v2.

    Dataset identity
    ----------------
    Every dataset has a single `id` which is a slash-separated path,
    e.g. "sales/raw/sales_raw". There are no separate collection entities.
    Navigation is virtual — listing by prefix, exactly like S3.

    Concepts
    --------
    dataset         – logical entity identified by a full path id
    source          – physical connector (local | s3 | sql | sftp | api)
    version         – immutable snapshot of a dataset
    schema_columns  – per-column semantic state (inferred → draft → published)
    schema_history  – append-only publish snapshots for auditing
    lineage         – directed graph of dataset dependencies
    version_metadata – key-value tags; sys.* keys are reserved for the server
    """

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local  = threading.local()
        self._init()

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
                -- ── Sources ──────────────────────────────────────────────────
                -- Describes HOW to reach data physically.
                -- type: local | s3 | sql | sftp | api
                CREATE TABLE IF NOT EXISTS sources (
                    id          TEXT PRIMARY KEY,
                    description TEXT,
                    type        TEXT NOT NULL,
                    config      TEXT NOT NULL DEFAULT '{}',  -- JSON
                    username     TEXT,
                    createdate   TEXT NOT NULL,
                    updatedate   TEXT NOT NULL
                );

                -- ── Datasets ─────────────────────────────────────────────────
                -- id is the full slash-separated path, e.g. "sales/raw/sales_raw"
                CREATE TABLE IF NOT EXISTS datasets (
                    id           TEXT PRIMARY KEY,
                    description  TEXT,
                    tags         TEXT NOT NULL DEFAULT '[]',  -- JSON array
                    status       TEXT NOT NULL DEFAULT 'draft',
                    username     TEXT,
                    createdate   TEXT NOT NULL,
                    updatedate   TEXT NOT NULL
                );

                -- ── Versions ─────────────────────────────────────────────────
                -- location semantics depend on source type:
                --   local → absolute file path
                --   s3    → s3://bucket/key
                --   sql   → table or SELECT query
                --   sftp  → remote absolute path
                --   api   → base_url#endpoint
                CREATE TABLE IF NOT EXISTS versions (
                    dataset_id       TEXT NOT NULL REFERENCES datasets(id),
                    version          TEXT NOT NULL,
                    source_id        TEXT REFERENCES sources(id),
                    location         TEXT NOT NULL,
                    format           TEXT,
                    hash             TEXT,
                    rows             INTEGER,
                    task_id          TEXT,
                    job_id           TEXT,
                    status           TEXT NOT NULL DEFAULT 'reserved',
                    username         TEXT,
                    createdate       TEXT NOT NULL,
                    updatedate       TEXT NOT NULL,
                    PRIMARY KEY (dataset_id, version)
                );

                -- ── Schema columns ────────────────────────────────────────────
                -- One row per column per dataset (not per version).
                -- status lifecycle: inferred → draft → published
                -- pii_type:        none | direct | indirect | sensitive
                CREATE TABLE IF NOT EXISTS schema_columns (
                    dataset_id     TEXT NOT NULL REFERENCES datasets(id),
                    column_name    TEXT NOT NULL,
                    physical_type  TEXT,
                    logical_type   TEXT,
                    nullable       INTEGER NOT NULL DEFAULT 1,
                    pii            INTEGER NOT NULL DEFAULT 0,
                    pii_type       TEXT    NOT NULL DEFAULT 'none',
                    pii_notes      TEXT,
                    description    TEXT,
                    tags           TEXT NOT NULL DEFAULT '[]',
                    status         TEXT NOT NULL DEFAULT 'inferred',
                    last_edited_by TEXT,
                    last_edited_at TEXT,
                    PRIMARY KEY (dataset_id, column_name)
                );

                -- ── Schema history ────────────────────────────────────────────
                -- Append-only log of every publish event.
                CREATE TABLE IF NOT EXISTS schema_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id   TEXT NOT NULL,
                    snapshot     TEXT NOT NULL,     -- JSON of all columns
                    published_by TEXT,
                    published_at TEXT NOT NULL
                );

                -- ── Lineage ───────────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS lineage (
                    output_dataset  TEXT NOT NULL,
                    output_version  TEXT NOT NULL,
                    input_dataset   TEXT NOT NULL,
                    input_version   TEXT NOT NULL,
                    PRIMARY KEY (output_dataset, output_version,
                                 input_dataset,  input_version)
                );

                -- ── Version metadata ──────────────────────────────────────────
                -- sys.* keys are written by the server only.
                -- Plain keys are free business metadata from the task.
                CREATE TABLE IF NOT EXISTS version_metadata (
                    dataset_id TEXT NOT NULL,
                    version    TEXT NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT,
                    PRIMARY KEY (dataset_id, version, key)
                );

            """)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    
    def _row(self, row) -> dict | None:
        if row is None:
            return None
        d = dict(row)
        for field in ("tags", "config"):
            if field in d and d[field]:
                try:
                    d[field] = json.loads(d[field])
                except Exception:
                    pass
        return d

    def _rows(self, cursor) -> list[dict]:
        return [self._row(r) for r in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

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

    def get_source(self, id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM sources WHERE id = ?", (id,))
        return _source(cur.fetchone())
    
    def list_sources(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM sources ORDER BY id")
        return [_source(r) for r in cur.fetchall()]
    
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
    
    def delete_source(self, id: str) -> bool:
        with self.conn:
            cur = self.conn.execute("DELETE FROM sources WHERE id = ?", (id,))
            return cur.rowcount > 0
    
    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------

    def create_dataset(self, id: str, description: str = None,
                       tags: list = None) -> bool:
        now = _now()
        with self.conn:
            cur = self.conn.execute("INSERT INTO datasets (id, description, tags, status, username, createdate, updatedate) VALUES (?, ?, ?, 'draft', ?, ?, ?) ON CONFLICT(id) DO NOTHING",
                                     (id, description, json.dumps(tags or []), _user(), now, now))
            return cur.rowcount > 0
  
    def exists_dataset(self, id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM datasets WHERE id = ?", (id,))
        return cur.fetchone() is not None
    
    def get_dataset(self, id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM datasets WHERE d.id = ?", (id,))
        return _dataset(cur.fetchone())
    
    def list_datasets(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM datasets ORDER BY id")
        return [_dataset(r) for r in cur.fetchall()]

    def find_datasets(self, status: str, description: str) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM datasets WHERE status = ? and description LIKE ? ORDER BY d.i", (status, description))
        return _dataset(cur)

    def update_dataset(self, id: str, **kwargs) -> bool:
        allowed = {"type", "config", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "config" in updates:
            updates["config"] = json.dumps(updates["config"])
        updates["updatedate"] = _now()
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE sources SET {cols} WHERE id = ?", vals)
            return cur.rowcount > 0
   
    def get_dataset______(self, id: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT d.*,
                   v.version,
                   v.status         AS version_status,
                   v.format,
                   v.rows,
                   v.committed_at,
                   v.source_id,
                   s.type           AS source_type
            FROM datasets d
            LEFT JOIN versions v
                ON v.dataset_id = d.id
                AND v.version = (
                    SELECT version FROM versions
                    WHERE dataset_id = d.id AND status = 'committed'
                    ORDER BY committed_at DESC LIMIT 1
                )
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE d.id = ?
        """, (id,))
        return _dataset(cur.fetchone())
    
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

    def update_dataset(self, id: str, **kwargs) -> bool:
        allowed = {"display_name", "description", "owner", "tags"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "tags" in updates:
            updates["tags"] = json.dumps(updates["tags"])
        # Any manual edit promotes status to in_review
        # (unless already approved — approved datasets stay approved
        #  until explicitly re-approved after schema changes)
        updates["status"] = (
            "CASE WHEN status = 'approved' THEN 'in_review' ELSE "
            "CASE WHEN status = 'draft' THEN 'in_review' ELSE status END END"
        )
        set_parts = []
        vals = []
        for k, v in updates.items():
            if k == "status":
                set_parts.append(f"status = ({v})")
            else:
                set_parts.append(f"{k} = ?")
                vals.append(v)
        vals.append(id)
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE datasets SET {', '.join(set_parts)} WHERE id = ?",
                vals)
            return cur.rowcount > 0

    def list_prefix(self, prefix: str) -> dict:
        prefix = prefix.rstrip("/") + "/"
        prefix = prefix.lstrip("/")
        cur = self.conn.execute("""
            SELECT d.*
            FROM datasets d
            WHERE d.id LIKE ?
            ORDER BY d.id
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
    
    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def reserve(self, dataset_id: str, version: str, location: str,
                fmt: str, task_id: str, job_id: str,
                source_id: str = None) -> bool:
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO versions
                        (dataset_id, version, source_id, location, format,
                         produced_by_task, produced_by_job, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?)
                """, (dataset_id, version, source_id, location, fmt,
                      task_id, job_id, _now()))
            return True
        except sqlite3.IntegrityError as e:
            print(e)
            return False

    def commit(self, dataset_id: str, version: str, file_hash: str,
               rows: int | None, schema: dict | None) -> dict | None:
        """
        Returns:
            {"skipped": True,  "version": existing_version}  identical to latest
            {"skipped": False, "version": version}            committed
            None                                              wrong status
        """
        with self.conn:
            latest = self.conn.execute("""
                SELECT version, hash FROM versions
                WHERE dataset_id = ? AND status = 'committed'
                ORDER BY committed_at DESC LIMIT 1
            """, (dataset_id,)).fetchone()

            if latest and dict(latest)["hash"] == file_hash:
                self.conn.execute("""
                    DELETE FROM versions
                    WHERE dataset_id = ? AND version = ? AND status = 'reserved'
                """, (dataset_id, version))
                return {"skipped": True, "version": dict(latest)["version"]}

            cur = self.conn.execute("""
                UPDATE versions SET
                    hash = ?, rows = ?, 
                    committed_at = ?, status = 'committed'
                WHERE dataset_id = ? AND version = ? AND status = 'reserved'
            """, (file_hash, rows,
                  _now(), dataset_id, version))

            if cur.rowcount == 0:
                return None
            return {"skipped": False, "version": version}

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

    def fail(self, dataset_id: str, version: str):
        with self.conn:
            self.conn.execute("""
                UPDATE versions SET status = 'failed'
                WHERE dataset_id = ? AND version = ? AND status = 'reserved'
            """, (dataset_id, version))

    def deprecate(self, dataset_id: str, version: str) -> bool:
        with self.conn:
            cur = self.conn.execute("""
                UPDATE versions SET status = 'deprecated'
                WHERE dataset_id = ? AND version = ?
            """, (dataset_id, version))
            return cur.rowcount > 0

    def get_version(self, dataset_id: str, version: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT v.*, s.type AS source_type, s.config AS source_config
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.dataset_id = ? AND v.version = ?
        """, (dataset_id, version))
        return _version(cur.fetchone())

    def get_latest(self, dataset_id: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT v.*, s.type AS source_type, s.config AS source_config
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.dataset_id = ? AND v.status = 'committed'
            ORDER BY v.committed_at DESC LIMIT 1
        """, (dataset_id,))
        return self._row(cur.fetchone())
        
    def get_history(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT v.*,
                   s.type           AS source_type
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.dataset_id = ? AND v.status = 'committed'
            ORDER BY v.committed_at DESC
        """, (dataset_id,))
        rows = cur.fetchall()
        return [_version({**dict(r), "id": dataset_id}) for r in rows]
            
    # ------------------------------------------------------------------
    # Schema columns
    # ------------------------------------------------------------------

    def upsert_schema_columns(self, dataset_id: str, columns: list[dict]):
        """
        Insert inferred columns. Columns already in draft/published
        have their physical_type refreshed but status is preserved.
        """
        now = _now()
        with self.conn:
            for col in columns:
                self.conn.execute("""
                    INSERT INTO schema_columns
                        (dataset_id, column_name, physical_type, logical_type,
                         nullable, pii, pii_type, status, last_edited_at)
                    VALUES (?, ?, ?, ?, 1, 0, 'none', 'inferred', ?)
                    ON CONFLICT(dataset_id, column_name) DO UPDATE SET
                        physical_type  = excluded.physical_type,
                        last_edited_at = excluded.last_edited_at
                    WHERE schema_columns.status = 'inferred'
                """, (dataset_id, col["name"],
                      col.get("physical_type"), col.get("logical_type"), now))

    def get_schema(self, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM schema_columns
            WHERE dataset_id = ?
            ORDER BY column_name
        """, (dataset_id,))
        return self._rows(cur)

    def update_schema_column(self, dataset_id: str, column_name: str,
                              editor: str, **kwargs) -> bool:
        allowed = {
            "logical_type", "nullable", "pii", "pii_type",
            "pii_notes", "description", "tags"
        }
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False

        if "tags" in updates and isinstance(updates["tags"], list):
            updates["tags"] = json.dumps(updates["tags"])
        if "nullable" in updates:
            updates["nullable"] = int(updates["nullable"])
        if "pii" in updates:
            updates["pii"] = int(updates["pii"])

        updates["last_edited_by"] = editor
        updates["last_edited_at"] = _now()

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
        """
        Promote all columns to published and snapshot into schema_history.
        Returns diff vs previous published snapshot.
        """
        now = _now()
        with self.conn:
            self.conn.execute("""
                UPDATE schema_columns
                SET status = 'published', last_edited_at = ?
                WHERE dataset_id = ? AND status IN ('inferred', 'draft')
            """, (now, dataset_id))

            current = self.get_schema(dataset_id)

            prev_row = self.conn.execute("""
                SELECT snapshot FROM schema_history
                WHERE dataset_id = ?
                ORDER BY published_at DESC LIMIT 1
            """, (dataset_id,)).fetchone()

            self.conn.execute("""
                INSERT INTO schema_history
                    (dataset_id, snapshot, published_by, published_at)
                VALUES (?, ?, ?, ?)
            """, (dataset_id, json.dumps(current), publisher, now))

        breaking, warnings = [], []
        if prev_row:
            prev = {c["column_name"]: c
                    for c in json.loads(dict(prev_row)["snapshot"])}
            curr = {c["column_name"]: c for c in current}

            for col, meta in prev.items():
                if col not in curr:
                    breaking.append(f"Column removed: '{col}'")
                elif curr[col]["logical_type"] != meta["logical_type"]:
                    breaking.append(
                        f"Type changed on '{col}': "
                        f"{meta['logical_type']} → {curr[col]['logical_type']}")
                elif not meta["nullable"] and curr[col]["nullable"]:
                    warnings.append(f"'{col}' changed from NOT NULL to nullable")
                elif meta["pii"] and not curr[col]["pii"]:
                    warnings.append(
                        f"PII flag removed from '{col}' — verify intentional")
            for col in curr:
                if col not in prev:
                    warnings.append(f"New column added: '{col}'")

        return {"published_at":    now,
                "breaking_changes": breaking,
                "warnings":         warnings}

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
                warnings.append(
                    f"Physical type drift on '{col}': "
                    f"{meta['physical_type']} → {inf[col].get('physical_type')}")
        return {"breaking": breaking, "warnings": warnings}

    # ------------------------------------------------------------------
    # Lineage
    # ------------------------------------------------------------------

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
    
    

    # ------------------------------------------------------------------
    # Version metadata
    # ------------------------------------------------------------------

    def set_metadata(self, dataset_id: str, version: str,
                     key: str, value: str):
        with self.conn:
            self.conn.execute("""
                INSERT INTO version_metadata (dataset_id, version, key, value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(dataset_id, version, key)
                DO UPDATE SET value = excluded.value
            """, (dataset_id, version, key, str(value)))

    def set_system_metadata(self, dataset_id: str, version: str,
                            fields: dict):
        """Write sys.* metadata — keys are prefixed with 'sys.' automatically."""
        with self.conn:
            self.conn.executemany("""
                INSERT INTO version_metadata (dataset_id, version, key, value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(dataset_id, version, key)
                DO UPDATE SET value = excluded.value
            """, [(dataset_id, version, f"sys.{k}", str(v))
                  for k, v in fields.items() if v is not None])

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

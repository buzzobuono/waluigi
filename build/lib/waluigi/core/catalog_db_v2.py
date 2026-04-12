import sqlite3
import threading
import json
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CatalogDB:
    """
    SQLite backend for the Waluigi Catalog v2.

    Concepts
    --------
    collection      – hierarchical logical folder
    source          – physical connector (sql, s3, sftp, local, api)
    dataset         – logical entity inside a collection
    version         – immutable snapshot of a dataset
    schema_contract – declared column definitions applied automatically at commit
    schema_columns  – per-column semantic state (inferred → draft → published)
    lineage         – directed graph of dataset dependencies
    version_metadata – key-value tags; sys.* keys are reserved for the server
    """

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
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
                -- ── Collections ──────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS collections (
                    path        TEXT PRIMARY KEY,
                    parent      TEXT,
                    name        TEXT NOT NULL,
                    description TEXT,
                    tags        TEXT DEFAULT '[]',
                    owner       TEXT,
                    created_at  TEXT NOT NULL
                );

                -- ── Sources ───────────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS sources (
                    id          TEXT PRIMARY KEY,
                    type        TEXT NOT NULL,
                    config      TEXT NOT NULL DEFAULT '{}',
                    description TEXT,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );

                -- ── Datasets ─────────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS datasets (
                    collection   TEXT NOT NULL REFERENCES collections(path),
                    id           TEXT NOT NULL,
                    display_name TEXT,
                    description  TEXT,
                    tags         TEXT DEFAULT '[]',
                    owner        TEXT,
                    created_at   TEXT NOT NULL,
                    PRIMARY KEY (collection, id)
                );

                -- ── Versions ─────────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS versions (
                    collection       TEXT NOT NULL,
                    dataset_id       TEXT NOT NULL,
                    version          TEXT NOT NULL,
                    source_id        TEXT REFERENCES sources(id),
                    location         TEXT NOT NULL,
                    format           TEXT,
                    hash             TEXT,
                    rows             INTEGER,
                    schema_snapshot  TEXT,
                    produced_by_task TEXT,
                    produced_by_job  TEXT,
                    status           TEXT NOT NULL DEFAULT 'reserved',
                    created_at       TEXT NOT NULL,
                    committed_at     TEXT,
                    PRIMARY KEY (collection, dataset_id, version),
                    FOREIGN KEY (collection, dataset_id)
                        REFERENCES datasets(collection, id)
                );

                -- ── Schema contract ───────────────────────────────────────────
                -- Declared once per dataset. Applied automatically at every commit.
                -- auto_publish: if true and all columns are covered, publish schema
                --               automatically after applying the contract.
                CREATE TABLE IF NOT EXISTS schema_contracts (
                    collection   TEXT NOT NULL,
                    dataset_id   TEXT NOT NULL,
                    columns      TEXT NOT NULL DEFAULT '[]',  -- JSON array of column defs
                    auto_publish INTEGER NOT NULL DEFAULT 1,
                    created_at   TEXT NOT NULL,
                    updated_at   TEXT NOT NULL,
                    PRIMARY KEY (collection, dataset_id),
                    FOREIGN KEY (collection, dataset_id)
                        REFERENCES datasets(collection, id)
                );

                -- ── Schema columns ────────────────────────────────────────────
                -- One row per column per dataset (NOT per version).
                -- status: inferred → draft → published
                -- pii_type: none | direct | indirect | sensitive
                CREATE TABLE IF NOT EXISTS schema_columns (
                    collection     TEXT NOT NULL,
                    dataset_id     TEXT NOT NULL,
                    column_name    TEXT NOT NULL,
                    physical_type  TEXT,
                    logical_type   TEXT,
                    nullable       INTEGER DEFAULT 1,
                    pii            INTEGER DEFAULT 0,
                    pii_type       TEXT DEFAULT 'none',
                    pii_notes      TEXT,
                    description    TEXT,
                    tags           TEXT DEFAULT '[]',
                    status         TEXT DEFAULT 'inferred',
                    last_edited_by TEXT,
                    last_edited_at TEXT,
                    PRIMARY KEY (collection, dataset_id, column_name),
                    FOREIGN KEY (collection, dataset_id)
                        REFERENCES datasets(collection, id)
                );

                -- ── Schema history ────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS schema_history (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection   TEXT NOT NULL,
                    dataset_id   TEXT NOT NULL,
                    snapshot     TEXT NOT NULL,
                    published_by TEXT,
                    published_at TEXT NOT NULL
                );

                -- ── Lineage ───────────────────────────────────────────────────
                CREATE TABLE IF NOT EXISTS lineage (
                    output_collection TEXT NOT NULL,
                    output_dataset    TEXT NOT NULL,
                    output_version    TEXT NOT NULL,
                    input_collection  TEXT NOT NULL,
                    input_dataset     TEXT NOT NULL,
                    input_version     TEXT NOT NULL,
                    PRIMARY KEY (
                        output_collection, output_dataset, output_version,
                        input_collection,  input_dataset,  input_version
                    )
                );

                -- ── Version metadata ──────────────────────────────────────────
                -- Keys prefixed with "sys." are written by the server only.
                -- Keys without prefix are free business metadata from the task.
                CREATE TABLE IF NOT EXISTS version_metadata (
                    collection TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    version    TEXT NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT,
                    PRIMARY KEY (collection, dataset_id, version, key)
                );

                -- ── Indexes ───────────────────────────────────────────────────
                CREATE INDEX IF NOT EXISTS idx_collections_parent
                    ON collections(parent);
                CREATE INDEX IF NOT EXISTS idx_versions_status
                    ON versions(collection, dataset_id, status);
                CREATE INDEX IF NOT EXISTS idx_versions_committed
                    ON versions(collection, dataset_id, committed_at);
                CREATE INDEX IF NOT EXISTS idx_lineage_output
                    ON lineage(output_collection, output_dataset, output_version);
                CREATE INDEX IF NOT EXISTS idx_lineage_input
                    ON lineage(input_collection, input_dataset, input_version);
            """)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row(self, row) -> dict | None:
        if row is None:
            return None
        d = dict(row)
        for field in ("tags", "config", "schema_snapshot", "columns"):
            if field in d and d[field]:
                try:
                    d[field] = json.loads(d[field])
                except Exception:
                    pass
        return d

    def _rows(self, cursor) -> list[dict]:
        return [self._row(r) for r in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Collections
    # ------------------------------------------------------------------

    def ensure_collection(self, path: str, description: str = None,
                          owner: str = None, tags: list = None):
        parts = path.strip("/").split("/")
        with self.conn:
            for i in range(1, len(parts) + 1):
                current = "/".join(parts[:i])
                parent  = "/".join(parts[:i - 1]) if i > 1 else None
                name    = parts[i - 1]
                is_leaf = (i == len(parts))
                self.conn.execute("""
                    INSERT OR IGNORE INTO collections
                        (path, parent, name, description, tags, owner, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (current, parent, name,
                      description if is_leaf else None,
                      json.dumps(tags or []),
                      owner if is_leaf else None,
                      _now()))

    def get_collection(self, path: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM collections WHERE path = ?", (path,))
        return self._row(cur.fetchone())

    def update_collection(self, path: str, **kwargs) -> bool:
        allowed = {"description", "tags", "owner"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "tags" in updates:
            updates["tags"] = json.dumps(updates["tags"])
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [path]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE collections SET {cols} WHERE path = ?", vals)
            return cur.rowcount > 0

    def list_collection_children(self, parent: str | None = None) -> list[dict]:
        if parent is None:
            cur = self.conn.execute(
                "SELECT * FROM collections WHERE parent IS NULL ORDER BY name")
        else:
            cur = self.conn.execute(
                "SELECT * FROM collections WHERE parent = ? ORDER BY name", (parent,))
        return self._rows(cur)

    def list_datasets_in_collection(self, collection: str,
                                    recursive: bool = False) -> list[dict]:
        pattern = f"{collection}/%" if recursive else None
        query = """
            SELECT d.*,
                   v.version AS latest_version,
                   v.format, v.rows, v.status AS version_status,
                   v.committed_at, v.source_id, v.location
            FROM datasets d
            LEFT JOIN versions v
                ON v.collection = d.collection AND v.dataset_id = d.id
                AND v.version = (
                    SELECT version FROM versions
                    WHERE collection = d.collection AND dataset_id = d.id
                      AND status = 'committed'
                    ORDER BY committed_at DESC LIMIT 1
                )
            WHERE d.collection = ?
            {extra}
            ORDER BY d.collection, d.id
        """
        if recursive:
            cur = self.conn.execute(
                query.format(extra="OR d.collection LIKE ?"),
                (collection, pattern))
        else:
            cur = self.conn.execute(query.format(extra=""), (collection,))
        return self._rows(cur)

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    def create_source(self, id: str, type: str, config: dict,
                      description: str = None) -> bool:
        now = _now()
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO sources (id, type, config, description, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (id, type, json.dumps(config), description, now, now))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_source(self, id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM sources WHERE id = ?", (id,))
        return self._row(cur.fetchone())

    def list_sources(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM sources ORDER BY id")
        return self._rows(cur)

    def update_source(self, id: str, **kwargs) -> bool:
        allowed = {"type", "config", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "config" in updates:
            updates["config"] = json.dumps(updates["config"])
        updates["updated_at"] = _now()
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

    def ensure_dataset(self, collection: str, id: str,
                       display_name: str = None, description: str = None,
                       owner: str = None, tags: list = None) -> bool:
        with self.conn:
            cur = self.conn.execute("""
                INSERT INTO datasets
                    (collection, id, display_name, description, tags, owner, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(collection, id) DO NOTHING
            """, (collection, id, display_name, description,
                  json.dumps(tags or []), owner, _now()))
            return cur.rowcount > 0

    def get_dataset(self, collection: str, id: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT * FROM datasets WHERE collection = ? AND id = ?", (collection, id))
        return self._row(cur.fetchone())

    def update_dataset(self, collection: str, id: str, **kwargs) -> bool:
        allowed = {"display_name", "description", "owner", "tags"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "tags" in updates:
            updates["tags"] = json.dumps(updates["tags"])
        cols = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [collection, id]
        with self.conn:
            cur = self.conn.execute(
                f"UPDATE datasets SET {cols} WHERE collection = ? AND id = ?", vals)
            return cur.rowcount > 0

    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def reserve(self, collection: str, dataset_id: str, version: str,
                location: str, fmt: str, task_id: str, job_id: str,
                source_id: str = None) -> bool:
        try:
            with self.conn:
                self.conn.execute("""
                    INSERT INTO versions
                        (collection, dataset_id, version, source_id,
                         location, format, produced_by_task, produced_by_job,
                         status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?)
                """, (collection, dataset_id, version, source_id,
                      location, fmt, task_id, job_id, _now()))
            return True
        except sqlite3.IntegrityError:
            return False

    def commit(self, collection: str, dataset_id: str, version: str,
               file_hash: str, rows: int | None,
               schema: dict | None) -> dict | None:
        """
        Returns:
            {"skipped": True,  "version": existing_version}
            {"skipped": False, "version": version}
            None  →  nothing to commit (wrong status)
        """
        with self.conn:
            latest = self.conn.execute("""
                SELECT version, hash FROM versions
                WHERE collection = ? AND dataset_id = ? AND status = 'committed'
                ORDER BY committed_at DESC LIMIT 1
            """, (collection, dataset_id)).fetchone()

            if latest and dict(latest)["hash"] == file_hash:
                self.conn.execute("""
                    DELETE FROM versions
                    WHERE collection = ? AND dataset_id = ? AND version = ?
                      AND status = 'reserved'
                """, (collection, dataset_id, version))
                return {"skipped": True, "version": dict(latest)["version"]}

            cur = self.conn.execute("""
                UPDATE versions SET
                    hash = ?, rows = ?, schema_snapshot = ?,
                    committed_at = ?, status = 'committed'
                WHERE collection = ? AND dataset_id = ? AND version = ?
                  AND status = 'reserved'
            """, (file_hash, rows,
                  json.dumps(schema) if schema else None,
                  _now(), collection, dataset_id, version))

            if cur.rowcount == 0:
                return None
            return {"skipped": False, "version": version}

    def commit_virtual(self, collection: str, dataset_id: str, version: str,
                       source_id: str, location: str, fmt: str,
                       task_id: str, job_id: str) -> dict:
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO versions
                    (collection, dataset_id, version, source_id, location, format,
                     produced_by_task, produced_by_job,
                     status, created_at, committed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'committed', ?, ?)
            """, (collection, dataset_id, version, source_id, location, fmt,
                  task_id, job_id, now, now))
        return {"skipped": False, "version": version}

    def fail(self, collection: str, dataset_id: str, version: str):
        with self.conn:
            self.conn.execute("""
                UPDATE versions SET status = 'failed'
                WHERE collection = ? AND dataset_id = ? AND version = ?
                  AND status = 'reserved'
            """, (collection, dataset_id, version))

    def deprecate(self, collection: str, dataset_id: str, version: str) -> bool:
        with self.conn:
            cur = self.conn.execute("""
                UPDATE versions SET status = 'deprecated'
                WHERE collection = ? AND dataset_id = ? AND version = ?
            """, (collection, dataset_id, version))
            return cur.rowcount > 0

    def get_version(self, collection: str, dataset_id: str,
                    version: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT v.*, s.type AS source_type, s.config AS source_config
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.collection = ? AND v.dataset_id = ? AND v.version = ?
        """, (collection, dataset_id, version))
        return self._row(cur.fetchone())

    def get_latest(self, collection: str, dataset_id: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT v.*, s.type AS source_type, s.config AS source_config
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.collection = ? AND v.dataset_id = ? AND v.status = 'committed'
            ORDER BY v.committed_at DESC LIMIT 1
        """, (collection, dataset_id))
        return self._row(cur.fetchone())

    def get_history(self, collection: str, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT v.*, s.type AS source_type
            FROM versions v
            LEFT JOIN sources s ON s.id = v.source_id
            WHERE v.collection = ? AND v.dataset_id = ?
              AND v.status = 'committed'
            ORDER BY v.committed_at DESC
        """, (collection, dataset_id))
        return self._rows(cur)

    # ------------------------------------------------------------------
    # Schema contract
    # ------------------------------------------------------------------

    def set_schema_contract(self, collection: str, dataset_id: str,
                            columns: list[dict],
                            auto_publish: bool = True) -> dict:
        """
        Declare the expected schema for a dataset.
        Stored as a resource; applied automatically at every commit.
        columns: list of dicts with keys:
            name (required), logical_type, nullable, pii, pii_type,
            pii_notes, description, tags
        """
        now = _now()
        with self.conn:
            self.conn.execute("""
                INSERT INTO schema_contracts
                    (collection, dataset_id, columns, auto_publish,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(collection, dataset_id) DO UPDATE SET
                    columns      = excluded.columns,
                    auto_publish = excluded.auto_publish,
                    updated_at   = excluded.updated_at
            """, (collection, dataset_id,
                  json.dumps(columns), int(auto_publish), now, now))
        return {"collection": collection, "dataset_id": dataset_id,
                "columns": columns, "auto_publish": auto_publish,
                "updated_at": now}

    def get_schema_contract(self, collection: str,
                            dataset_id: str) -> dict | None:
        cur = self.conn.execute("""
            SELECT * FROM schema_contracts
            WHERE collection = ? AND dataset_id = ?
        """, (collection, dataset_id))
        return self._row(cur.fetchone())

    def delete_schema_contract(self, collection: str, dataset_id: str) -> bool:
        with self.conn:
            cur = self.conn.execute("""
                DELETE FROM schema_contracts
                WHERE collection = ? AND dataset_id = ?
            """, (collection, dataset_id))
            return cur.rowcount > 0

    def apply_schema_contract(self, collection: str,
                               dataset_id: str) -> dict:
        """
        Apply the registered contract to schema_columns.
        - Columns in the contract that are still 'inferred' get promoted
          to 'draft' with the declared metadata.
        - Columns already in 'draft' or 'published' are NOT touched
          (human edits are preserved).
        - If auto_publish=True and no 'inferred' columns remain after
          applying the contract, the schema is published automatically.

        Returns:
            {
              "applied": [col_names patched],
              "skipped": [col_names already draft/published],
              "unknown": [col_names in contract but not in schema yet],
              "published": bool,
              "publish_result": {...} | None
            }
        """
        contract = self.get_schema_contract(collection, dataset_id)
        if not contract:
            return {"applied": [], "skipped": [], "unknown": [],
                    "published": False, "publish_result": None}

        contract_cols = {c["name"]: c for c in contract["columns"]}
        current_schema = {c["column_name"]: c
                          for c in self.get_schema(collection, dataset_id)}

        applied, skipped, unknown = [], [], []
        now = _now()

        with self.conn:
            for name, defn in contract_cols.items():
                if name not in current_schema:
                    unknown.append(name)
                    continue

                col = current_schema[name]
                if col["status"] in ("draft", "published"):
                    skipped.append(name)
                    continue

                # Build update from contract definition
                allowed = {
                    "logical_type", "nullable", "pii",
                    "pii_type", "pii_notes", "description", "tags"
                }
                updates = {k: v for k, v in defn.items()
                           if k in allowed and v is not None}

                if not updates:
                    skipped.append(name)
                    continue

                if "tags" in updates and isinstance(updates["tags"], list):
                    updates["tags"] = json.dumps(updates["tags"])
                if "nullable" in updates:
                    updates["nullable"] = int(updates["nullable"])
                if "pii" in updates:
                    updates["pii"] = int(updates["pii"])

                set_parts = [f"{k} = ?" for k in updates]
                vals = list(updates.values())
                set_parts += ["status = 'draft'",
                              "last_edited_by = 'contract'",
                              f"last_edited_at = '{now}'"]
                vals += [collection, dataset_id, name]

                self.conn.execute(
                    f"UPDATE schema_columns SET {', '.join(set_parts)} "
                    f"WHERE collection = ? AND dataset_id = ? "
                    f"  AND column_name = ? AND status = 'inferred'",
                    vals
                )
                applied.append(name)

        # Auto-publish if configured and no inferred columns remain
        publish_result = None
        published = False

        if contract["auto_publish"]:
            remaining_inferred = [
                c for c in self.get_schema(collection, dataset_id)
                if c["status"] == "inferred"
            ]
            if not remaining_inferred:
                publish_result = self.publish_schema(
                    collection, dataset_id, publisher="contract")
                published = True

        return {
            "applied":         applied,
            "skipped":         skipped,
            "unknown":         unknown,
            "published":       published,
            "publish_result":  publish_result,
        }

    # ------------------------------------------------------------------
    # Schema columns
    # ------------------------------------------------------------------

    def upsert_schema_columns(self, collection: str, dataset_id: str,
                               columns: list[dict]):
        """
        Insert inferred columns. Columns already in draft/published
        have their physical_type refreshed but status is preserved.
        """
        now = _now()
        with self.conn:
            for col in columns:
                self.conn.execute("""
                    INSERT INTO schema_columns
                        (collection, dataset_id, column_name,
                         physical_type, logical_type, nullable,
                         pii, pii_type, status, last_edited_at)
                    VALUES (?, ?, ?, ?, ?, 1, 0, 'none', 'inferred', ?)
                    ON CONFLICT(collection, dataset_id, column_name)
                    DO UPDATE SET
                        physical_type  = excluded.physical_type,
                        last_edited_at = excluded.last_edited_at
                    WHERE schema_columns.status = 'inferred'
                """, (collection, dataset_id, col["name"],
                      col.get("physical_type"), col.get("logical_type"), now))

    def get_schema(self, collection: str, dataset_id: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT * FROM schema_columns
            WHERE collection = ? AND dataset_id = ?
            ORDER BY column_name
        """, (collection, dataset_id))
        return self._rows(cur)

    def update_schema_column(self, collection: str, dataset_id: str,
                              column_name: str, editor: str,
                              **kwargs) -> bool:
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
        vals = list(updates.values()) + [collection, dataset_id, column_name]

        with self.conn:
            cur = self.conn.execute(
                f"UPDATE schema_columns SET {', '.join(set_parts)} "
                f"WHERE collection = ? AND dataset_id = ? AND column_name = ?",
                vals)
            return cur.rowcount > 0

    def publish_schema(self, collection: str, dataset_id: str,
                       publisher: str) -> dict:
        """
        Promote all columns to published and snapshot into schema_history.
        Returns diff vs previous published snapshot.
        """
        now = _now()
        with self.conn:
            self.conn.execute("""
                UPDATE schema_columns
                SET status = 'published', last_edited_at = ?
                WHERE collection = ? AND dataset_id = ?
                  AND status IN ('inferred', 'draft')
            """, (now, collection, dataset_id))

            current = self.get_schema(collection, dataset_id)

            prev_row = self.conn.execute("""
                SELECT snapshot FROM schema_history
                WHERE collection = ? AND dataset_id = ?
                ORDER BY published_at DESC LIMIT 1
            """, (collection, dataset_id)).fetchone()

            self.conn.execute("""
                INSERT INTO schema_history
                    (collection, dataset_id, snapshot, published_by, published_at)
                VALUES (?, ?, ?, ?, ?)
            """, (collection, dataset_id, json.dumps(current), publisher, now))

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

        return {"published_at": now,
                "breaking_changes": breaking,
                "warnings": warnings}

    def diff_schema_against_inferred(self, collection: str, dataset_id: str,
                                      inferred: list[dict]) -> dict:
        published = self.conn.execute("""
            SELECT * FROM schema_columns
            WHERE collection = ? AND dataset_id = ? AND status = 'published'
        """, (collection, dataset_id)).fetchall()

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

    def insert_lineage(self, out_col: str, out_id: str, out_ver: str,
                       inputs: list[dict]):
        with self.conn:
            self.conn.executemany("""
                INSERT OR IGNORE INTO lineage
                    (output_collection, output_dataset, output_version,
                     input_collection,  input_dataset,  input_version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(out_col, out_id, out_ver,
                   i["collection"], i["dataset_id"], i["version"])
                  for i in inputs])

    def get_upstream(self, collection: str, dataset_id: str,
                     version: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT l.input_collection AS collection,
                   l.input_dataset    AS dataset_id,
                   l.input_version    AS version,
                   v.location, v.format, v.source_id, v.rows, v.hash,
                   v.produced_by_task, v.produced_by_job
            FROM lineage l
            LEFT JOIN versions v
                ON  v.collection = l.input_collection
                AND v.dataset_id = l.input_dataset
                AND v.version    = l.input_version
            WHERE l.output_collection = ?
              AND l.output_dataset    = ?
              AND l.output_version    = ?
        """, (collection, dataset_id, version))
        return self._rows(cur)

    def get_downstream(self, collection: str, dataset_id: str,
                       version: str) -> list[dict]:
        cur = self.conn.execute("""
            SELECT l.output_collection AS collection,
                   l.output_dataset    AS dataset_id,
                   l.output_version    AS version,
                   v.location, v.format, v.source_id, v.rows, v.hash,
                   v.produced_by_task, v.produced_by_job
            FROM lineage l
            LEFT JOIN versions v
                ON  v.collection = l.output_collection
                AND v.dataset_id = l.output_dataset
                AND v.version    = l.output_version
            WHERE l.input_collection = ?
              AND l.input_dataset    = ?
              AND l.input_version    = ?
        """, (collection, dataset_id, version))
        return self._rows(cur)

    # ------------------------------------------------------------------
    # Version metadata
    # ------------------------------------------------------------------

    def set_metadata(self, collection: str, dataset_id: str, version: str,
                     key: str, value: str):
        with self.conn:
            self.conn.execute("""
                INSERT INTO version_metadata
                    (collection, dataset_id, version, key, value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(collection, dataset_id, version, key)
                DO UPDATE SET value = excluded.value
            """, (collection, dataset_id, version, key, str(value)))

    def set_system_metadata(self, collection: str, dataset_id: str,
                            version: str, fields: dict):
        """Write sys.* metadata. Keys are automatically prefixed with 'sys.'"""
        with self.conn:
            self.conn.executemany("""
                INSERT INTO version_metadata
                    (collection, dataset_id, version, key, value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(collection, dataset_id, version, key)
                DO UPDATE SET value = excluded.value
            """, [(collection, dataset_id, version, f"sys.{k}", str(v))
                  for k, v in fields.items() if v is not None])

    def delete_metadata(self, collection: str, dataset_id: str,
                        version: str, key: str) -> bool:
        if key.startswith("sys."):
            return False   # sys.* keys are immutable
        with self.conn:
            cur = self.conn.execute("""
                DELETE FROM version_metadata
                WHERE collection = ? AND dataset_id = ? AND version = ? AND key = ?
            """, (collection, dataset_id, version, key))
            return cur.rowcount > 0

    def get_metadata(self, collection: str, dataset_id: str,
                     version: str) -> dict:
        cur = self.conn.execute("""
            SELECT key, value FROM version_metadata
            WHERE collection = ? AND dataset_id = ? AND version = ?
            ORDER BY key
        """, (collection, dataset_id, version))
        return {r["key"]: r["value"] for r in cur.fetchall()}

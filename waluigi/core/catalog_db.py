import sqlite3
import threading
import json
from datetime import datetime
from waluigi.core.catalog_helper import CatalogHelper


class CatalogDB:

    def __init__(self, db_path):
        self.helper = CatalogHelper()
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
                    path        TEXT PRIMARY KEY,
                    parent      TEXT,
                    name        TEXT,
                    description TEXT,
                    created_at  TEXT
                );

                CREATE TABLE IF NOT EXISTS datasets (
                    namespace        TEXT NOT NULL,
                    id               TEXT NOT NULL,
                    version          TEXT NOT NULL,
                    path             TEXT NOT NULL,
                    format           TEXT,
                    hash             TEXT,
                    produced_by_task TEXT,
                    produced_by_job  TEXT,
                    created_at       TEXT,
                    committed_at     TEXT,
                    rows             INTEGER,
                    schema           TEXT,
                    status           TEXT DEFAULT 'reserved',
                    PRIMARY KEY (namespace, id, version),
                    FOREIGN KEY (namespace) REFERENCES namespaces(path)
                );

                CREATE TABLE IF NOT EXISTS dataset_metadata (
                    namespace  TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    version    TEXT NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT,
                    PRIMARY KEY (namespace, dataset_id, version, key)
                );

                CREATE TABLE IF NOT EXISTS lineage (
                    output_ns      TEXT NOT NULL,
                    output_id      TEXT NOT NULL,
                    output_version TEXT NOT NULL,
                    input_ns       TEXT NOT NULL,
                    input_id       TEXT NOT NULL,
                    input_version  TEXT NOT NULL,
                    PRIMARY KEY (output_ns, output_id, output_version,
                                 input_ns,  input_id,  input_version)
                );

                CREATE INDEX IF NOT EXISTS idx_datasets_ns     ON datasets(namespace);
                CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(namespace, id, status);
                CREATE INDEX IF NOT EXISTS idx_lineage_output  ON lineage(output_ns, output_id, output_version);
                CREATE INDEX IF NOT EXISTS idx_lineage_input   ON lineage(input_ns,  input_id,  input_version);
                CREATE INDEX IF NOT EXISTS idx_ns_parent       ON namespaces(parent);
            """)

    
    # --- Namespaces ---

    def ensure_namespace(self, path, description=None):
        parts = path.strip("/").split("/")
        with self.conn:
            for i in range(1, len(parts) + 1):
                current = "/".join(parts[:i])
                parent  = "/".join(parts[:i - 1]) if i > 1 else None
                name    = parts[i - 1]
                self.conn.execute("""
                    INSERT OR IGNORE INTO namespaces (path, parent, name, description, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (current, parent, name,
                      description if i == len(parts) else None,
                      self.helper.now_iso()))

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
        if recursive:
            cursor = self.conn.execute("""
                WITH latest AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY namespace, id ORDER BY committed_at DESC
                    ) AS rn
                    FROM datasets
                    WHERE (namespace = ? OR namespace LIKE ?) AND status = 'committed'
                )
                SELECT namespace, id, version, path, format, hash, rows,
                       produced_by_task, produced_by_job, committed_at, status
                FROM latest WHERE rn = 1
                ORDER BY namespace, id
            """, (namespace, f"{namespace}/%"))
        else:
            cursor = self.conn.execute("""
                WITH latest AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY namespace, id ORDER BY committed_at DESC
                    ) AS rn
                    FROM datasets
                    WHERE namespace = ? AND status = 'committed'
                )
                SELECT namespace, id, version, path, format, hash, rows,
                       produced_by_task, produced_by_job, committed_at, status
                FROM latest WHERE rn = 1
                ORDER BY id
            """, (namespace,))
        return [dict(r) for r in cursor.fetchall()]

    # --- Datasets ---

    def reserve(self, namespace, id, version, path, fmt, task_id, job_id):
        with self.conn:
            self.conn.execute("""
                INSERT INTO datasets
                    (namespace, id, version, path, format,
                     produced_by_task, produced_by_job, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'reserved')
            """, (namespace, id, version, path, fmt, task_id, job_id, version))

    def commit_(self, namespace, id, version, file_hash, rows, schema):
        with self.conn:
            cursor = self.conn.execute("""
                UPDATE datasets SET
                    hash = ?, rows = ?, schema = ?,
                    committed_at = ?, status = 'committed'
                WHERE namespace = ? AND id = ? AND version = ? AND status = 'reserved'
            """, (file_hash, rows, json.dumps(schema) if schema else None,
                  self.helper.now_iso(), namespace, id, version))
            return cursor.rowcount > 0

    def commit(self, namespace, id, version, file_hash, rows, schema):
        with self.conn:
            # get latest committed version only
            latest = self.conn.execute("""
            SELECT version, hash FROM datasets
            WHERE namespace = ? AND id = ? AND status = 'committed'
            ORDER BY version DESC LIMIT 1
            """, (namespace, id)).fetchone()

            if latest and dict(latest)["hash"] == file_hash:
                # identical to latest — drop reserved slot and return existing
                self.conn.execute("""
                DELETE FROM datasets
                WHERE namespace = ? AND id = ? AND version = ? AND status = 'reserved'
                """, (namespace, id, version))
                return {"skipped": True, "version": dict(latest)["version"]}

            # different from latest (or no latest exists) — commit normally
            cursor = self.conn.execute("""
            UPDATE datasets SET
                hash = ?, rows = ?, schema = ?,
                committed_at = ?, status = 'committed'
            WHERE namespace = ? AND id = ? AND version = ? AND status = 'reserved'
            """, (file_hash, rows,
              json.dumps(schema) if schema else None,
              self.helper.now_iso(), namespace, id, version))

            if cursor.rowcount == 0:
                return None

            return {"skipped": False, "version": version}


    def commit_scanned(self, namespace, id, version, path, fmt, file_hash, rows, schema):
        with self.conn:
            self.conn.execute("""
                INSERT OR IGNORE INTO datasets
                    (namespace, id, version, path, format, hash, rows, schema,
                     created_at, committed_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'committed')
            """, (namespace, id, version, path, fmt, file_hash, rows,
                  json.dumps(schema) if schema else None, version, version))

    def fail(self, namespace, id, version):
        with self.conn:
            self.conn.execute("""
                UPDATE datasets SET status = 'failed'
                WHERE namespace = ? AND id = ? AND version = ? AND status = 'reserved'
            """, (namespace, id, version))

    def get_latest(self, namespace, id):
        cursor = self.conn.execute("""
            SELECT * FROM datasets
            WHERE namespace = ? AND id = ? AND status = 'committed'
            ORDER BY version DESC LIMIT 1
        """, (namespace, id))
        return self._parse(cursor.fetchone())

    def get_version(self, namespace, id, version):
        cursor = self.conn.execute("""
            SELECT * FROM datasets WHERE namespace = ? AND id = ? AND version = ?
        """, (namespace, id, version))
        return self._parse(cursor.fetchone())

    def get_history(self, namespace, id):
        cursor = self.conn.execute("""
            SELECT * FROM datasets
            WHERE namespace = ? AND id = ? AND status = 'committed'
            ORDER BY version DESC
        """, (namespace, id))
        return [self._parse(r) for r in cursor.fetchall()]

    def deprecate(self, namespace, id, version):
        with self.conn:
            cursor = self.conn.execute("""
                UPDATE datasets SET status = 'deprecated'
                WHERE namespace = ? AND id = ? AND version = ?
            """, (namespace, id, version))
            return cursor.rowcount > 0

    def insert_lineage(self, out_ns, out_id, out_ver, inputs):
        """inputs: list of {"namespace": ..., "id": ..., "version": ...}"""
        with self.conn:
            self.conn.executemany("""
                INSERT OR IGNORE INTO lineage
                    (output_ns, output_id, output_version, input_ns, input_id, input_version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(out_ns, out_id, out_ver, i["namespace"], i["id"], i["version"])
                  for i in inputs])

    def get_upstream(self, namespace, id, version):
        cursor = self.conn.execute("""
            SELECT l.input_ns AS namespace, l.input_id AS id, l.input_version AS version,
                   d.path, d.format, d.hash, d.rows,
                   d.produced_by_task, d.produced_by_job
            FROM lineage l
            LEFT JOIN datasets d
                ON  d.namespace = l.input_ns
                AND d.id        = l.input_id
                AND d.version   = l.input_version
            WHERE l.output_ns = ? AND l.output_id = ? AND l.output_version = ?
        """, (namespace, id, version))
        return [dict(r) for r in cursor.fetchall()]

    def get_downstream(self, namespace, id, version):
        cursor = self.conn.execute("""
            SELECT l.output_ns AS namespace, l.output_id AS id, l.output_version AS version,
                   d.path, d.format, d.hash, d.rows,
                   d.produced_by_task, d.produced_by_job
            FROM lineage l
            LEFT JOIN datasets d
                ON  d.namespace = l.output_ns
                AND d.id        = l.output_id
                AND d.version   = l.output_version
            WHERE l.input_ns = ? AND l.input_id = ? AND l.input_version = ?
        """, (namespace, id, version))
        return [dict(r) for r in cursor.fetchall()]

    # --- Metadata ---

    def set_metadata(self, namespace, dataset_id, version, key, value):
        with self.conn:
            self.conn.execute("""
                INSERT INTO dataset_metadata (namespace, dataset_id, version, key, value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(namespace, dataset_id, version, key)
                DO UPDATE SET value = excluded.value
                """, (namespace, dataset_id, version, key, str(value)))

    def delete_metadata(self, namespace, dataset_id, version, key):
        with self.conn:
            cursor = self.conn.execute("""
                DELETE FROM dataset_metadata
                WHERE namespace = ? AND dataset_id = ? AND version = ? AND key = ?
            """, (namespace, dataset_id, version, key))
            return cursor.rowcount > 0

    def get_metadata(self, namespace, dataset_id, version):
        cursor = self.conn.execute("""
            SELECT key, value FROM dataset_metadata
            WHERE namespace = ? AND dataset_id = ? AND version = ?
        """, (namespace, dataset_id, version))
        return {r["key"]: r["value"] for r in cursor.fetchall()}
            
    def list_all_datasets(self):
        cursor = self.conn.execute("""
            SELECT namespace, id, version, format, rows, status, committed_at
            FROM datasets ORDER BY committed_at DESC
        """)
        return [dict(r) for r in cursor.fetchall()]

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

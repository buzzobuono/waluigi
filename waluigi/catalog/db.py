import json
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine, event, text,
    MetaData, Table, Column, Text, Integer, Float,
    PrimaryKeyConstraint, UniqueConstraint, ForeignKey,
)

from waluigi.catalog.entities import Source, Dataset, Version, Expectation


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _user() -> str:
    return "admin"


_meta = MetaData()

_t_sources = Table("sources", _meta,
    Column("id",          Text, primary_key=True),
    Column("description", Text),
    Column("type",        Text, nullable=False),
    Column("config",      Text, nullable=False, default="{}"),
    Column("username",    Text, nullable=False),
    Column("createdate",  Text, nullable=False),
    Column("updatedate",  Text, nullable=False),
)

_t_datasets = Table("datasets", _meta,
    Column("id",          Text, primary_key=True),
    Column("format",      Text, nullable=False),
    Column("description", Text),
    Column("status",      Text, nullable=False, default="draft"),
    Column("source_id",   Text, ForeignKey("sources.id")),
    Column("dq_suite",    Text),
    Column("username",    Text, nullable=False),
    Column("createdate",  Text, nullable=False),
    Column("updatedate",  Text, nullable=False),
    Column("approved_by", Text),
    Column("approved_at", Text),
)

_t_versions = Table("versions", _meta,
    Column("dataset_id",  Text, nullable=False),
    Column("version",     Text, nullable=False),
    Column("location",    Text, nullable=False),
    Column("status",      Text, nullable=False, default="reserved"),
    Column("username",    Text, nullable=False),
    Column("createdate",  Text, nullable=False),
    Column("updatedate",  Text, nullable=False),
    PrimaryKeyConstraint("dataset_id", "version"),
)

_t_schema_columns = Table("schema_columns", _meta,
    Column("dataset_id",    Text, nullable=False),
    Column("column_name",   Text, nullable=False),
    Column("physical_type", Text),
    Column("logical_type",  Text),
    Column("nullable",      Integer, nullable=False, default=1),
    Column("pii",           Integer, nullable=False, default=0),
    Column("pii_type",      Text, nullable=False, default="none"),
    Column("pii_notes",     Text),
    Column("description",   Text),
    Column("status",        Text, nullable=False, default="inferred"),
    Column("username",      Text, nullable=False),
    Column("createdate",    Text, nullable=False),
    Column("updatedate",    Text, nullable=False),
    PrimaryKeyConstraint("dataset_id", "column_name"),
)

_t_expectations = Table("expectations", _meta,
    Column("id",         Integer, primary_key=True, autoincrement=True),
    Column("dataset_id", Text, nullable=False),
    Column("rule_id",    Text, nullable=False),
    Column("inputs",     Text, nullable=False, default="{}"),
    Column("params",     Text, nullable=False, default="{}"),
    Column("tolerance",  Float, nullable=False, default=1.0),
    Column("position",   Integer, nullable=False, default=0),
    Column("username",   Text, nullable=False),
    Column("createdate", Text, nullable=False),
    Column("updatedate", Text, nullable=False),
)

_t_charts = Table("charts", _meta,
    Column("id",         Integer, primary_key=True, autoincrement=True),
    Column("dataset_id", Text, nullable=False),
    Column("key",        Text, nullable=False),
    Column("title",      Text, nullable=False),
    Column("spec",       Text, nullable=False, default="{}"),
    Column("position",   Integer, nullable=False, default=0),
    Column("username",   Text, nullable=False),
    Column("createdate", Text, nullable=False),
    Column("updatedate", Text, nullable=False),
    UniqueConstraint("dataset_id", "key"),
)

_t_dq_results = Table("dq_results", _meta,
    Column("id",         Integer, primary_key=True, autoincrement=True),
    Column("dataset_id", Text, nullable=False),
    Column("version",    Text, nullable=False),
    Column("score",      Float, nullable=False, default=0),
    Column("passed",     Integer, nullable=False, default=0),
    Column("total",      Integer, nullable=False, default=0),
    Column("success",    Integer, nullable=False, default=0),
    Column("details",    Text, nullable=False, default="[]"),
    Column("error",      Text),
    Column("createdate", Text, nullable=False),
    UniqueConstraint("dataset_id", "version"),
)

_t_lineage = Table("lineage", _meta,
    Column("output_dataset", Text, nullable=False),
    Column("output_version", Text, nullable=False),
    Column("input_dataset",  Text, nullable=False),
    Column("input_version",  Text, nullable=False),
    Column("username",       Text, nullable=False),
    Column("createdate",     Text, nullable=False),
    Column("updatedate",     Text, nullable=False),
    PrimaryKeyConstraint(
        "output_dataset", "output_version", "input_dataset", "input_version"),
)

_t_version_metadata = Table("version_metadata", _meta,
    Column("dataset_id", Text, nullable=False),
    Column("version",    Text, nullable=False),
    Column("key",        Text, nullable=False),
    Column("value",      Text),
    Column("username",   Text, nullable=False),
    Column("createdate", Text, nullable=False),
    Column("updatedate", Text, nullable=False),
    PrimaryKeyConstraint("dataset_id", "version", "key"),
)


class CatalogDB:

    def __init__(self, url: str):
        kwargs = {}
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}
        self.engine = create_engine(url, **kwargs)
        self._setup_pragmas()
        self._init()

    def _setup_pragmas(self):
        if self.engine.dialect.name != "sqlite":
            return

        @event.listens_for(self.engine, "connect")
        def _set_pragmas(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA busy_timeout=30000")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

    def _init(self):
        _meta.create_all(self.engine)
        # Ensure columns added after initial schema
        with self.engine.begin() as conn:
            for ddl in (
                "ALTER TABLE datasets ADD COLUMN approved_by TEXT",
                "ALTER TABLE datasets ADD COLUMN approved_at TEXT",
            ):
                try:
                    conn.execute(text(ddl))
                except Exception:
                    pass

    @staticmethod
    def _row(row) -> dict | None:
        return dict(row._mapping) if row is not None else None

    @staticmethod
    def _rows(rows) -> list[dict]:
        return [dict(r._mapping) for r in rows]

    def _upsert_stmt(self, table, values: dict,
                     conflict_cols: list[str], update_cols: list[str]):
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as _insert
        else:
            from sqlalchemy.dialects.sqlite import insert as _insert
        stmt = _insert(table).values(**values)
        return stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_={k: getattr(stmt.excluded, k) for k in update_cols},
        )

    def _insert_ignore_stmt(self, table, values: dict, conflict_cols: list[str]):
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as _insert
        else:
            from sqlalchemy.dialects.sqlite import insert as _insert
        stmt = _insert(table).values(**values)
        return stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    # ── Folders ───────────────────────────────────────────────────────────────

    def list_folders(self, prefix: str) -> dict:
        prefix = prefix.rstrip("/") + "/"
        prefix = prefix.lstrip("/")
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM datasets WHERE id LIKE :pat ORDER BY id"),
                {"pat": f"{prefix}%"},
            ).fetchall()

        datasets, sub_prefixes = [], set()
        for row in rows:
            d = Dataset.from_row(row)
            rest = d.id[len(prefix):]
            if "/" not in rest:
                datasets.append(d)
            else:
                sub = prefix + rest.split("/")[0] + "/"
                sub_prefixes.add(sub)

        return {
            "prefix":   prefix,
            "datasets": [d.to_dict() for d in datasets],
            "prefixes": sorted(sub_prefixes),
        }

    # ── Sources ───────────────────────────────────────────────────────────────

    def list_sources(self) -> list[Source]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM sources ORDER BY id")).fetchall()
        return [Source.from_row(r) for r in rows]

    def create_source(self, id: str, type: str, config: dict,
                      description: str = None) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._insert_ignore_stmt(_t_sources, {
                "id": id, "description": description, "type": type,
                "config": json.dumps(config), "username": _user(),
                "createdate": now, "updatedate": now,
            }, ["id"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def exists_source(self, id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM sources WHERE id = :id"), {"id": id}
            ).fetchone()
        return row is not None

    def get_source(self, id: str) -> Source | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM sources WHERE id = :id"), {"id": id}
            ).fetchone()
        return Source.from_row(row)

    def update_source(self, id: str, **kwargs) -> bool:
        allowed = {"type", "config", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "config" in updates:
            updates["config"] = json.dumps(updates["config"])
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = :{k}" for k in updates)
        updates["_id"] = id
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE sources SET {cols} WHERE id = :_id"), updates
            )
        return result.rowcount > 0

    def upsert_source(self, id: str, type: str, config: dict,
                      description: str = None) -> None:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._upsert_stmt(
                _t_sources,
                {"id": id, "type": type, "config": json.dumps(config),
                 "description": description, "username": _user(),
                 "createdate": now, "updatedate": now},
                ["id"],
                ["config", "description", "username", "updatedate"],
            )
            conn.execute(stmt)

    def delete_source(self, id: str) -> bool:
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("DELETE FROM sources WHERE id = :id"), {"id": id}
                )
            return result.rowcount > 0
        except Exception as e:
            if "FOREIGN KEY" in str(e) or "foreign key" in str(e).lower():
                raise ValueError(
                    f"Source '{id}' is still referenced by one or more datasets"
                ) from e
            raise

    # ── Datasets ──────────────────────────────────────────────────────────────

    def list_datasets(self) -> list[Dataset]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM datasets ORDER BY id")).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def find_datasets(self, status: str = None, description: str = None) -> list[Dataset]:
        clauses, params = [], {}
        if status:
            clauses.append("status = :status")
            params["status"] = status
        if description:
            clauses.append("description LIKE :desc")
            params["desc"] = description
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT * FROM datasets {where} ORDER BY id"),
                params,
            ).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def create_dataset(self, id: str, format: str, description: str = None,
                       source_id: str = "local", dq_suite: str = None) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._insert_ignore_stmt(_t_datasets, {
                "id": id, "format": format, "description": description,
                "status": "draft", "source_id": source_id, "dq_suite": dq_suite,
                "username": _user(), "createdate": now, "updatedate": now,
            }, ["id"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def exists_dataset(self, id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM datasets WHERE id = :id"), {"id": id}
            ).fetchone()
        return row is not None

    def get_dataset(self, id: str) -> Dataset | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM datasets WHERE id = :id"), {"id": id}
            ).fetchone()
        return Dataset.from_row(row)

    def update_dataset(self, id: str, **kwargs) -> bool:
        allowed = {"description", "status", "dq_suite"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = :{k}" for k in updates)
        updates["_id"] = id
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE datasets SET {cols} WHERE id = :_id"), updates
            )
        return result.rowcount > 0

    def delete_dataset(self, id: str) -> bool:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM versions WHERE dataset_id = :id"), {"id": id})
            result = conn.execute(text("DELETE FROM datasets WHERE id = :id"), {"id": id})
        return result.rowcount > 0

    # ── Versions ──────────────────────────────────────────────────────────────

    def list_versions(self, dataset_id: str) -> list[Version]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM versions WHERE dataset_id = :did"
                     " ORDER BY createdate DESC"),
                {"did": dataset_id},
            ).fetchall()
        return [Version.from_row(r) for r in rows]

    def get_version(self, dataset_id: str, version: str) -> Version | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchone()
        return Version.from_row(row)

    def get_latest_version(self, dataset_id: str) -> Version | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND status = 'committed'"
                     " ORDER BY updatedate DESC LIMIT 1"),
                {"did": dataset_id},
            ).fetchone()
        return Version.from_row(row)

    def find_version_by_metadata(self, dataset_id: str,
                                  metadata: dict) -> Version | None:
        if metadata is None:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND status = 'committed'"
                     " ORDER BY updatedate DESC LIMIT 1"),
                {"did": dataset_id},
            ).fetchone()
        if not row:
            return None
        version_id = dict(row._mapping)["version"]
        existing_meta = self.get_metadata(dataset_id, version_id)
        existing_meta_user = {
            k: v for k, v in existing_meta.items() if not k.startswith("sys.")
        }
        target_meta = {k: str(v) for k, v in metadata.items()}
        if existing_meta_user == target_meta:
            return Version.from_row(row)
        return None

    def reserve_version(self, dataset_id: str, version: str,
                        location: str) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._insert_ignore_stmt(_t_versions, {
                "dataset_id": dataset_id, "version": version, "location": location,
                "status": "reserved", "username": _user(),
                "createdate": now, "updatedate": now,
            }, ["dataset_id", "version"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def commit_version(self, dataset_id: str, version: str) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'committed', updatedate = :now"
                     " WHERE dataset_id = :did AND version = :ver"
                     " AND status = 'reserved'"),
                {"now": now, "did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def fail_version(self, dataset_id: str, version: str) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'failed', updatedate = :now"
                     " WHERE dataset_id = :did AND version = :ver"
                     " AND status = 'reserved'"),
                {"now": now, "did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def delete_version(self, dataset_id: str, version: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM versions WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    # ── Version metadata ──────────────────────────────────────────────────────

    def set_metadata(self, dataset_id: str, version: str,
                     key: str, value: str):
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._upsert_stmt(
                _t_version_metadata,
                {"dataset_id": dataset_id, "version": version, "key": key,
                 "value": str(value), "username": _user(),
                 "createdate": now, "updatedate": now},
                ["dataset_id", "version", "key"],
                ["value", "updatedate"],
            )
            conn.execute(stmt)

    def delete_metadata(self, dataset_id: str, version: str, key: str) -> bool:
        if key.startswith("sys."):
            return False
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM version_metadata"
                     " WHERE dataset_id = :did AND version = :ver AND key = :key"),
                {"did": dataset_id, "ver": version, "key": key},
            )
        return result.rowcount > 0

    def get_metadata(self, dataset_id: str, version: str) -> dict:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT key, value FROM version_metadata"
                     " WHERE dataset_id = :did AND version = :ver ORDER BY key"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return {dict(r._mapping)["key"]: dict(r._mapping)["value"] for r in rows}

    # ── Schema ────────────────────────────────────────────────────────────────

    def upsert_schema_columns(self, dataset_id: str, columns: list[dict]):
        now = _now()
        with self.engine.begin() as conn:
            for col in columns:
                stmt = self._upsert_stmt(
                    _t_schema_columns,
                    {"dataset_id": dataset_id, "column_name": col["name"],
                     "physical_type": col.get("physical_type"),
                     "logical_type": col.get("logical_type"),
                     "nullable": 1, "pii": 0, "pii_type": "none",
                     "pii_notes": "", "description": "", "status": "inferred",
                     "username": _user(), "createdate": now, "updatedate": now},
                    ["dataset_id", "column_name"],
                    ["physical_type", "updatedate"],
                )
                conn.execute(stmt)

    def upsert_schema_column(self, dataset_id: str, column_name: str,
                             **kwargs) -> dict | None:
        allowed = {"logical_type", "nullable", "pii", "pii_type",
                   "pii_notes", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if "nullable" in updates:
            updates["nullable"] = int(updates["nullable"])
        if "pii" in updates:
            updates["pii"] = int(updates["pii"])

        now = _now()
        set_parts = [f"{k} = :{k}" for k in updates]
        set_parts += [
            "username = :_usr", "updatedate = :_now",
            "status = CASE WHEN status = 'published' THEN 'published' ELSE 'draft' END",
        ]
        params = dict(updates)
        params.update({"_usr": _user(), "_now": now,
                       "_did": dataset_id, "_col": column_name})

        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE schema_columns SET {', '.join(set_parts)}"
                     f" WHERE dataset_id = :_did AND column_name = :_col"),
                params,
            )
            if result.rowcount == 0:
                conn.execute(text("""
                    INSERT INTO schema_columns
                        (dataset_id, column_name, physical_type, logical_type,
                         nullable, pii, pii_type, pii_notes, description,
                         status, username, createdate, updatedate)
                    VALUES
                        (:did, :col, NULL, :lt, :nullable, :pii,
                         :pii_type, :pii_notes, :description,
                         'draft', :usr, :now, :now)
                """), {
                    "did": dataset_id, "col": column_name,
                    "lt":          updates.get("logical_type"),
                    "nullable":    int(updates.get("nullable", True)),
                    "pii":         int(updates.get("pii", False)),
                    "pii_type":    updates.get("pii_type", "none"),
                    "pii_notes":   updates.get("pii_notes", ""),
                    "description": updates.get("description", ""),
                    "usr": _user(), "now": now,
                })

        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM schema_columns"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"did": dataset_id, "col": column_name},
            ).fetchone()
        return self._row(row)

    def get_schema(self, dataset_id: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM schema_columns WHERE dataset_id = :did"
                     " ORDER BY column_name"),
                {"did": dataset_id},
            ).fetchall()
        return self._rows(rows)

    def update_schema_column(self, dataset_id: str, column_name: str,
                             **kwargs) -> bool:
        allowed = {"logical_type", "nullable", "pii", "pii_type",
                   "pii_notes", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "nullable" in updates:
            updates["nullable"] = int(updates["nullable"])
        if "pii" in updates:
            updates["pii"] = int(updates["pii"])
        updates["_usr"] = _user()
        updates["_now"] = _now()
        set_parts = [f"{k} = :{k}" for k in updates if not k.startswith("_")]
        set_parts += [
            "username = :_usr", "updatedate = :_now",
            "status = CASE WHEN status = 'published' THEN 'published' ELSE 'draft' END",
        ]
        updates["_did"] = dataset_id
        updates["_col"] = column_name
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE schema_columns SET {', '.join(set_parts)}"
                     f" WHERE dataset_id = :_did AND column_name = :_col"),
                updates,
            )
        return result.rowcount > 0

    def publish_schema(self, dataset_id: str, publisher: str) -> dict:
        now = _now()
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE schema_columns SET status = 'published', updatedate = :now"
                     " WHERE dataset_id = :did AND status IN ('inferred', 'draft')"),
                {"now": now, "did": dataset_id},
            )
        return {"published_at": now, "breaking_changes": [], "warnings": []}

    def approve_schema_column(self, dataset_id: str, column_name: str) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE schema_columns"
                     " SET status = 'published', username = :usr, updatedate = :now"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"usr": _user(), "now": now, "did": dataset_id, "col": column_name},
            )
        return result.rowcount > 0

    def delete_schema_column(self, dataset_id: str, column_name: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM schema_columns"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"did": dataset_id, "col": column_name},
            )
        return result.rowcount > 0

    # ── Expectations ──────────────────────────────────────────────────────────

    def list_expectations(self, dataset_id: str) -> list[Expectation]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM expectations WHERE dataset_id = :did"
                     " ORDER BY position, id"),
                {"did": dataset_id},
            ).fetchall()
        return [Expectation.from_row(r) for r in rows]

    def add_expectation(self, dataset_id: str, rule_id: str,
                        inputs: dict, params: dict,
                        tolerance: float = 1.0,
                        position: int = 0) -> Expectation:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("INSERT INTO expectations"
                     " (dataset_id, rule_id, inputs, params, tolerance, position,"
                     "  username, createdate, updatedate)"
                     " VALUES (:did, :rule, :inputs, :params, :tol, :pos,"
                     "         :usr, :now, :now)"),
                {"did": dataset_id, "rule": rule_id,
                 "inputs": json.dumps(inputs), "params": json.dumps(params),
                 "tol": tolerance, "pos": position,
                 "usr": _user(), "now": now},
            )
            row_id = result.lastrowid
        return self.get_expectation(dataset_id, row_id)

    def get_expectation(self, dataset_id: str, exp_id: int) -> Expectation | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM expectations"
                     " WHERE dataset_id = :did AND id = :eid"),
                {"did": dataset_id, "eid": exp_id},
            ).fetchone()
        return Expectation.from_row(row)

    def update_expectation(self, dataset_id: str, exp_id: int,
                           **kwargs) -> bool:
        allowed = {"rule_id", "inputs", "params", "tolerance", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "inputs" in updates:
            updates["inputs"] = json.dumps(updates["inputs"])
        if "params" in updates:
            updates["params"] = json.dumps(updates["params"])
        updates["_now"] = _now()
        updates["_usr"] = _user()
        updates["_did"] = dataset_id
        updates["_eid"] = exp_id
        cols = ", ".join(f"{k} = :{k}" for k in updates if not k.startswith("_"))
        cols += ", updatedate = :_now, username = :_usr"
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE expectations SET {cols}"
                     f" WHERE dataset_id = :_did AND id = :_eid"),
                updates,
            )
        return result.rowcount > 0

    def delete_expectation(self, dataset_id: str, exp_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM expectations WHERE dataset_id = :did AND id = :eid"),
                {"did": dataset_id, "eid": exp_id},
            )
        return result.rowcount > 0

    # ── DQ Results ────────────────────────────────────────────────────────────

    def save_dq_result(self, dataset_id: str, version: str,
                       score: float, passed: int, total: int,
                       success: bool, details: list,
                       error: str = None) -> dict:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._upsert_stmt(
                _t_dq_results,
                {"dataset_id": dataset_id, "version": version,
                 "score": score, "passed": passed, "total": total,
                 "success": int(success), "details": json.dumps(details),
                 "error": error, "createdate": now},
                ["dataset_id", "version"],
                ["score", "passed", "total", "success", "details",
                 "error", "createdate"],
            )
            conn.execute(stmt)
        return self.get_dq_result(dataset_id, version)

    def get_dq_result(self, dataset_id: str, version: str) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM dq_results"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchone()
        r = self._row(row)
        if r:
            r["details"] = json.loads(r.get("details") or "[]")
            r["success"] = bool(r["success"])
        return r

    def list_dq_results(self, dataset_id: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM dq_results WHERE dataset_id = :did"
                     " ORDER BY createdate DESC"),
                {"did": dataset_id},
            ).fetchall()
        result = self._rows(rows)
        for r in result:
            r["details"] = json.loads(r.get("details") or "[]")
            r["success"] = bool(r["success"])
        return result

    # ── Charts ────────────────────────────────────────────────────────────────

    def list_charts(self, dataset_id: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did"
                     " ORDER BY position, id"),
                {"did": dataset_id},
            ).fetchall()
        result = self._rows(rows)
        for r in result:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return result

    def add_chart(self, dataset_id: str, key: str, title: str,
                  spec: dict, position: int = 0) -> dict:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("INSERT INTO charts"
                     " (dataset_id, key, title, spec, position,"
                     "  username, createdate, updatedate)"
                     " VALUES (:did, :key, :title, :spec, :pos,"
                     "         :usr, :now, :now)"),
                {"did": dataset_id, "key": key, "title": title,
                 "spec": json.dumps(spec), "pos": position,
                 "usr": _user(), "now": now},
            )
            row_id = result.lastrowid
        return self.get_chart(dataset_id, row_id)

    def get_chart(self, dataset_id: str, chart_id: int) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did AND id = :cid"),
                {"did": dataset_id, "cid": chart_id},
            ).fetchone()
        r = self._row(row)
        if r:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return r

    def get_chart_by_key(self, dataset_id: str, key: str) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did AND key = :key"),
                {"did": dataset_id, "key": key},
            ).fetchone()
        r = self._row(row)
        if r:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return r

    def update_chart(self, dataset_id: str, chart_id: int, **kwargs) -> bool:
        allowed = {"key", "title", "spec", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "spec" in updates:
            updates["spec"] = json.dumps(updates["spec"])
        updates["_now"] = _now()
        updates["_usr"] = _user()
        updates["_did"] = dataset_id
        updates["_cid"] = chart_id
        cols = ", ".join(f"{k} = :{k}" for k in updates if not k.startswith("_"))
        cols += ", updatedate = :_now, username = :_usr"
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE charts SET {cols}"
                     f" WHERE dataset_id = :_did AND id = :_cid"),
                updates,
            )
        return result.rowcount > 0

    def delete_chart(self, dataset_id: str, chart_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM charts WHERE dataset_id = :did AND id = :cid"),
                {"did": dataset_id, "cid": chart_id},
            )
        return result.rowcount > 0

    def diff_schema_against_inferred(self, dataset_id: str,
                                      inferred: list[dict]) -> dict:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM schema_columns"
                     " WHERE dataset_id = :did AND status = 'published'"),
                {"did": dataset_id},
            ).fetchall()
        if not rows:
            return {"breaking": [], "warnings": []}

        pub = {dict(r._mapping)["column_name"]: dict(r._mapping) for r in rows}
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

    # ── Dataset lifecycle ─────────────────────────────────────────────────────

    def set_in_review(self, id: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE datasets SET status = 'in_review'"
                     " WHERE id = :id AND status = 'draft'"),
                {"id": id},
            )
        return result.rowcount > 0

    def approve_dataset(self, id: str, approved_by: str) -> bool:
        now = _now()
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE datasets"
                     " SET status = 'approved', approved_by = :by, approved_at = :at"
                     " WHERE id = :id AND status != 'deprecated'"),
                {"by": approved_by, "at": now, "id": id},
            )
        return result.rowcount > 0

    def commit_virtual(self, dataset_id: str, version: str,
                       location: str) -> dict:
        now = _now()
        with self.engine.begin() as conn:
            stmt = self._upsert_stmt(
                _t_versions,
                {"dataset_id": dataset_id, "version": version, "location": location,
                 "status": "committed", "username": _user(),
                 "createdate": now, "updatedate": now},
                ["dataset_id", "version"],
                ["location", "status", "username", "updatedate"],
            )
            conn.execute(stmt)
        return {"skipped": False, "version": version}

    def deprecate(self, dataset_id: str, version: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'deprecated'"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    # ── Lineage ───────────────────────────────────────────────────────────────

    def insert_lineage(self, out_id: str, out_ver: str,
                       inputs: list[dict]):
        now = _now()
        with self.engine.begin() as conn:
            for i in inputs:
                stmt = self._insert_ignore_stmt(_t_lineage, {
                    "output_dataset": out_id, "output_version": out_ver,
                    "input_dataset": i["dataset_id"], "input_version": i["version"],
                    "username": _user(), "createdate": now, "updatedate": now,
                }, ["output_dataset", "output_version",
                    "input_dataset", "input_version"])
                conn.execute(stmt)

    def get_upstream(self, dataset_id: str, version: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT l.input_dataset AS dataset_id,"
                     "       l.input_version AS version"
                     " FROM lineage l"
                     " WHERE l.output_dataset = :did AND l.output_version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return self._rows(rows)

    def get_downstream(self, dataset_id: str, version: str) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT l.output_dataset AS dataset_id,"
                     "       l.output_version AS version"
                     " FROM lineage l"
                     " WHERE l.input_dataset = :did AND l.input_version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return self._rows(rows)

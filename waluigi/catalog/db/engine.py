from datetime import datetime, timezone

from sqlalchemy import (
    create_engine, event, text,
    MetaData, Table, Column, Text, Integer, Float,
    PrimaryKeyConstraint, UniqueConstraint, ForeignKey,
)


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


def create_catalog_engine(url: str):
    kwargs = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(url, **kwargs)

    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def _set_pragmas(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA journal_mode=WAL")
            dbapi_conn.execute("PRAGMA busy_timeout=30000")
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

    _meta.create_all(engine)
    with engine.begin() as conn:
        for ddl in (
            "ALTER TABLE datasets ADD COLUMN approved_by TEXT",
            "ALTER TABLE datasets ADD COLUMN approved_at TEXT",
        ):
            try:
                conn.execute(text(ddl))
            except Exception:
                pass

    return engine

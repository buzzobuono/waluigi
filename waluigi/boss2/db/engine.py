from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, event,
    MetaData, Table, Column, Index, Text, Integer, Float, DateTime,
    PrimaryKeyConstraint,
)

_meta = MetaData()

_t_tasks = Table("tasks", _meta,
    Column("namespace",   Text, nullable=False),
    Column("id",          Text, nullable=False),
    Column("parent_id",   Text),
    Column("params",      Text),
    Column("attributes",  Text),
    Column("status",      Text, nullable=False, default="PENDING"),
    Column("last_update", DateTime),
    Column("job_id",      Text),
    PrimaryKeyConstraint("namespace", "id"),
)

_t_jobs = Table("jobs", _meta,
    Column("namespace",    Text, nullable=False),
    Column("job_id",       Text, nullable=False),
    Column("kind",         Text, nullable=False, default="Job"),
    Column("metadata",     Text),
    Column("spec",         Text),
    Column("status",       Text, nullable=False, default="PENDING"),
    Column("started_at",   DateTime),
    Column("locked_by",    Text),
    Column("locked_until", DateTime),
    PrimaryKeyConstraint("namespace", "job_id"),
)

_t_workers = Table("workers", _meta,
    Column("url",        Text, primary_key=True),
    Column("status",     Text, nullable=False, default="ALIVE"),
    Column("max_slots",  Integer),
    Column("free_slots", Integer),
    Column("last_seen",  DateTime),
)

_t_resources = Table("resources", _meta,
    Column("name",   Text, primary_key=True),
    Column("amount", Float, nullable=False),
    Column("usage",  Float, nullable=False, default=0.0),
)

_t_task_logs = Table("task_logs", _meta,
    Column("id",        Integer, primary_key=True, autoincrement=True),
    Column("namespace", Text, nullable=False),
    Column("task_id",   Text, nullable=False),
    Column("timestamp", DateTime),
    Column("message",   Text),
    Column("boss_id",   Text),
)

Index("idx_logs_ns_task", _t_task_logs.c.namespace, _t_task_logs.c.task_id)


def create_boss_engine(url: str):
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

    # Add kind column to existing DBs that predate this field
    if engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            cols = [r[1] for r in conn.execute(
                __import__("sqlalchemy").text("PRAGMA table_info(jobs)")
            ).fetchall()]
            if "kind" not in cols:
                conn.execute(__import__("sqlalchemy").text(
                    "ALTER TABLE jobs ADD COLUMN kind TEXT NOT NULL DEFAULT 'Job'"
                ))

    from sqlalchemy import select, insert
    with engine.begin() as conn:
        if not conn.execute(select(_t_resources)).fetchone():
            conn.execute(insert(_t_resources).values(name="coin", amount=2.0, usage=0.0))

    return engine

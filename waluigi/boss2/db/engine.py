from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, event, text,
    MetaData, Table, Column, Index, Text, Integer, Float, DateTime,
    ForeignKey,
)

_meta = MetaData()

_t_tasks = Table("tasks", _meta,
    Column("id",          Text, primary_key=True),
    Column("namespace",   Text),
    Column("parent_id",   Text),
    Column("params",      Text),
    Column("attributes",  Text),
    Column("status",      Text, nullable=False, default="PENDING"),
    Column("last_update", DateTime),
    Column("job_id",      Text),
)

_t_jobs = Table("jobs", _meta,
    Column("job_id",       Text, primary_key=True),
    Column("metadata",     Text),
    Column("spec",         Text),
    Column("status",       Text, nullable=False, default="PENDING"),
    Column("started_at",   DateTime),
    Column("locked_by",    Text),
    Column("locked_until", DateTime),
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
    Column("task_id",   Text, ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False),
    Column("timestamp", DateTime),
    Column("message",   Text),
    Column("boss_id",   Text),
)

Index("idx_logs_task_id", _t_task_logs.c.task_id)


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

    # Seed a default resource pool if the table is empty
    from sqlalchemy import select, insert
    with engine.begin() as conn:
        if not conn.execute(select(_t_resources)).fetchone():
            conn.execute(insert(_t_resources).values(name="coin", amount=2.0, usage=0.0))

    return engine

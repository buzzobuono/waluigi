from sqlalchemy import (
    create_engine, event,
    MetaData, Table, Column, Index, Text, Integer, Float,
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
    Column("last_update", Text),
    Column("job_id",      Text),
    PrimaryKeyConstraint("namespace", "id"),
)

_t_jobs = Table("jobs", _meta,
    Column("namespace",         Text, nullable=False),
    Column("job_id",            Text, nullable=False),
    Column("execution_policy",  Text, nullable=False, default="Ephemeral"),
    Column("concurrency_policy", Text, nullable=False, default="Forbid"),
    Column("metadata",          Text),
    Column("spec",              Text),
    Column("status",            Text, nullable=False, default="PENDING"),
    Column("started_at",        Text),
    Column("locked_by",         Text),
    Column("locked_until",      Text),
    PrimaryKeyConstraint("namespace", "job_id"),
)

_t_workers = Table("workers", _meta,
    Column("url",        Text, primary_key=True),
    Column("status",     Text, nullable=False, default="ALIVE"),
    Column("max_slots",  Integer),
    Column("free_slots", Integer),
    Column("last_seen",  Text),
)

_t_resources = Table("resources", _meta,
    Column("namespace", Text, nullable=False),
    Column("name",      Text, nullable=False),
    Column("amount",    Float, nullable=False),
    Column("usage",     Float, nullable=False, default=0.0),
    PrimaryKeyConstraint("namespace", "name"),
)

_t_task_logs = Table("task_logs", _meta,
    Column("id",        Integer, primary_key=True, autoincrement=True),
    Column("namespace", Text, nullable=False),
    Column("task_id",   Text, nullable=False),
    Column("timestamp", Text),
    Column("message",   Text),
    Column("boss_id",   Text),
)

Index("idx_logs_ns_task", _t_task_logs.c.namespace, _t_task_logs.c.task_id)

_t_namespaces = Table("namespaces", _meta,
    Column("name",        Text, primary_key=True),
    Column("description", Text, nullable=False, default=""),
    Column("created_at",  Text, nullable=False),
)

_t_task_defnitions = Table("task_definitions", _meta,
    Column("namespace",    Text, nullable=False),
    Column("id",       Text, nullable=False),
    Column("kind",         Text, nullable=False, default="Job"),
    Column("metadata",     Text),
    Column("spec",         Text),
    PrimaryKeyConstraint("namespace", "id"),
)

_t_job_definitions = Table("job_definitions", _meta,
    Column("namespace", Text, nullable=False),
    Column("id",        Text, nullable=False),
    Column("metadata",  Text),
    Column("spec",      Text),
    PrimaryKeyConstraint("namespace", "id"),
)

_t_task_deps = Table("task_deps", _meta,
    Column("namespace", Text, nullable=False),
    Column("task_id",   Text, nullable=False),
    Column("dep_id",    Text, nullable=False),
    PrimaryKeyConstraint("namespace", "task_id", "dep_id"),
)

_t_cron_jobs = Table("cron_jobs", _meta,
    Column("namespace",  Text,    nullable=False),
    Column("id",         Text,    nullable=False),
    Column("spec",       Text,    nullable=False, default="{}"),
    Column("enabled",    Integer, nullable=False, default=1),
    Column("last_fire",  Text),
    PrimaryKeyConstraint("namespace", "id"),
)


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

    return engine

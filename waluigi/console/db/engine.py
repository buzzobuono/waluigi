from sqlalchemy import (
    create_engine, event, text,
    MetaData, Table, Column, Text,
)

_meta = MetaData()

_t_users = Table("users", _meta,
    Column("userid",        Text, primary_key=True),
    Column("username",      Text, nullable=False),
    Column("password_hash", Text, nullable=False),
    Column("namespaces",    Text, nullable=False, default="[]"),
    Column("createdate",    Text, nullable=False),
    Column("updatedate",    Text, nullable=False),
)


def create_console_engine(url: str):
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

    # Idempotent column addition for databases predating the namespaces column
    if engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            try:
                conn.execute(text(
                    "ALTER TABLE users ADD COLUMN namespaces TEXT NOT NULL DEFAULT '[]'"
                ))
            except Exception:
                pass

    return engine

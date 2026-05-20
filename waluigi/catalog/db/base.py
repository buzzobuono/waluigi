from __future__ import annotations
from contextvars import ContextVar
from contextlib import contextmanager
from functools import wraps

_tx: ContextVar = ContextVar('_catalog_tx', default=None)
_engine = None


def _set_engine(engine) -> None:
    global _engine
    _engine = engine


def atomic(func):
    """Wrap a service method in a single DB transaction (propagation=REQUIRED).

    If a transaction is already active in the current context (nested @atomic
    call), the existing connection is reused and no new transaction is opened.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if _tx.get() is not None:
            return func(*args, **kwargs)
        with _engine.begin() as conn:
            token = _tx.set(conn)
            try:
                return func(*args, **kwargs)
            finally:
                _tx.reset(token)
    return wrapper


class BaseRepository:

    def __init__(self, engine):
        self.engine = engine

    @contextmanager
    def _conn(self):
        """Yield the active transaction connection, or open a new one."""
        existing = _tx.get()
        if existing is not None:
            yield existing
        else:
            with self.engine.begin() as conn:
                yield conn

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

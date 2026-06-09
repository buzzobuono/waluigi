from __future__ import annotations
from contextvars import ContextVar
from contextlib import contextmanager
from functools import wraps
from contextlib import nullcontext

_tx: ContextVar = ContextVar("_boss_tx", default=None)
_engine = None


def _set_engine(engine) -> None:
    global _engine
    _engine = engine


def atomic(func):
    """Wrap a service method in a single DB transaction (propagation=REQUIRED)."""
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

    def _select_for_update(self, stmt):
        """Add FOR UPDATE on PostgreSQL. No-op on SQLite (use a threading.Lock instead)."""
        if self.engine.dialect.name == "postgresql":
            return stmt.with_for_update()
        return stmt

    def _upsert_stmt(self, table, values: dict,
                     conflict_cols: list[str], update_cols: list[str]):
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as _insert
        else:
            from sqlalchemy.dialects.sqlite import insert as _insert
        stmt = _insert(table).values(**values)
        if not update_cols:
            return stmt.on_conflict_do_nothing(index_elements=conflict_cols)
        return stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_={k: getattr(stmt.excluded, k) for k in update_cols},
        )

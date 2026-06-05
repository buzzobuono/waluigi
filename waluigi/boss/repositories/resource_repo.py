from __future__ import annotations
import threading
from contextlib import nullcontext
from sqlalchemy import select, update, delete, case

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_resources

_r = _t_resources.c

# Used only by update_limits() — serialises concurrent admin writes on SQLite.
# acquire() does not need it: it uses a conditional UPDATE with no prior SELECT.
_sqlite_lock = threading.Lock()


class _AcquireFailed(Exception):
    pass


class ResourceRepository(BaseRepository):

    @property
    def _resource_lock(self):
        """For update_limits() only.
        SQLite: process-wide lock (no FOR UPDATE, single-Boss constraint).
        PostgreSQL: no-op (SELECT FOR UPDATE handles cross-process serialization)."""
        if self.engine.dialect.name == "sqlite":
            return _sqlite_lock
        return nullcontext()

    def list(self, namespace: str) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(
                    select(_t_resources).where(_r.namespace == namespace)
                ).fetchall()
            )

    def acquire(self, namespace: str, resources: dict) -> bool:
        """
        Check-and-reserve via conditional UPDATE — no Python lock, no prior SELECT.

        UPDATE ... WHERE usage + amount <= total_amount is atomic on both SQLite and
        PostgreSQL: rowcount 0 means the pool is not defined or exhausted.
        Resources are sorted to guarantee a consistent lock order on PostgreSQL and
        avoid deadlocks between concurrent acquire() calls.
        The transaction is always independent (engine.begin()) so a rollback on
        partial failure never affects an outer @atomic context.
        """
        if not resources:
            return True
        try:
            with self.engine.begin() as conn:
                for name, amount in sorted(resources.items()):
                    result = conn.execute(
                        update(_t_resources)
                        .where(_r.namespace == namespace)
                        .where(_r.name == name)
                        .where(_r.usage + float(amount) <= _r.amount)
                        .values(usage=_r.usage + float(amount))
                    )
                    if result.rowcount == 0:
                        raise _AcquireFailed()
            return True
        except _AcquireFailed:
            return False

    def release(self, namespace: str, resources: dict) -> None:
        # Single atomic UPDATE per resource — no read-modify-write, no lock needed.
        if not resources:
            return
        with self._conn() as conn:
            for name, amount in resources.items():
                conn.execute(
                    update(_t_resources)
                    .where(_r.namespace == namespace)
                    .where(_r.name == name)
                    .values(usage=case(
                        (_r.usage > float(amount), _r.usage - float(amount)),
                        else_=0.0,
                    ))
                )

    def update_limits(self, namespace: str, limits: dict) -> tuple[bool, str]:
        """Replace the resource pool for a namespace. Rejects if in-use resources would be violated."""
        with self._resource_lock:
            with self._conn() as conn:
                stmt = self._select_for_update(
                    select(_t_resources).where(_r.namespace == namespace)
                )
                rows = self._rows(conn.execute(stmt).fetchall())
                current = {r["name"]: (r["amount"], r["usage"]) for r in rows}

                for name, (_, usage) in current.items():
                    if name not in limits and usage > 0:
                        return False, f"Resource '{name}' in use ({usage}), cannot remove"

                for name, new_amount in limits.items():
                    new_amount = float(new_amount)
                    if name in current:
                        _, usage = current[name]
                        if new_amount < usage:
                            return False, (
                                f"Resource '{name}' current usage ({usage}) > new limit ({new_amount})"
                            )

                for name, new_amount in limits.items():
                    stmt = self._upsert_stmt(
                        _t_resources,
                        values={"namespace": namespace, "name": name,
                                "amount": float(new_amount), "usage": 0.0},
                        conflict_cols=["namespace", "name"],
                        update_cols=["amount"],
                    )
                    conn.execute(stmt)

                for name in current:
                    if name not in limits:
                        conn.execute(
                            delete(_t_resources)
                            .where(_r.namespace == namespace)
                            .where(_r.name == name)
                        )

                return True, "Resources updated"

from __future__ import annotations
import threading
from contextlib import nullcontext
from sqlalchemy import select, update, delete, case

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_resources

_r = _t_resources.c

# Process-wide write serializer for SQLite (single-Boss constraint).
# PostgreSQL uses SELECT FOR UPDATE instead — no Python lock needed.
_sqlite_lock = threading.Lock()


class ResourceRepository(BaseRepository):

    @property
    def _resource_lock(self):
        """SQLite: process-wide lock (no FOR UPDATE support, single-Boss only).
        PostgreSQL: no-op (FOR UPDATE on the SELECT handles cross-process serialization)."""
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
        Check-and-reserve atomically.
        If a resource pool required by the task is not defined in the namespace,
        the task is blocked (returns False) until the pool is created.
        Tasks with no resource requirements (empty dict) always run freely.
        """
        if not resources:
            return True
        with self._resource_lock:
            with self._conn() as conn:
                stmt = self._select_for_update(
                    select(_r.name, _r.amount, _r.usage)
                    .where(_r.namespace == namespace)
                    .where(_r.name.in_(list(resources.keys())))
                )
                rows = {r[0]: (r[1], r[2]) for r in conn.execute(stmt).fetchall()}
                for name, amount in resources.items():
                    if name not in rows:
                        return False  # pool not defined → task cannot run
                    cap, used = rows[name]
                    if used + float(amount) > cap:
                        return False  # pool exhausted
                for name, amount in resources.items():
                    conn.execute(
                        update(_t_resources)
                        .where(_r.namespace == namespace)
                        .where(_r.name == name)
                        .values(usage=_r.usage + float(amount))
                    )
                return True

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

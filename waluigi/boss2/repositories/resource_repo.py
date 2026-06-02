from __future__ import annotations
import threading
from sqlalchemy import select, update, delete, case

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_resources

_r = _t_resources.c


class ResourceRepository(BaseRepository):
    # Serialises acquire/release within a single Boss process.
    # For multi-Boss deployments use PostgreSQL (SERIALIZABLE isolation handles it).
    _lock = threading.Lock()

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
        with self._lock:
            with self._conn() as conn:
                for name, amount in resources.items():
                    row = conn.execute(
                        select(_r.amount, _r.usage)
                        .where(_r.namespace == namespace)
                        .where(_r.name == name)
                    ).fetchone()
                    if row is None:
                        return False  # pool not defined → task cannot run
                    if row[1] + float(amount) > row[0]:
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
        if not resources:
            return
        with self._lock:
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
        with self._lock:
            with self._conn() as conn:
                rows = self._rows(conn.execute(
                    select(_t_resources).where(_r.namespace == namespace)
                ).fetchall())
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

from __future__ import annotations
import threading
from sqlalchemy import select, update, delete, case

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_resources


class ResourceRepository(BaseRepository):
    # Serialises acquire/release within a single Boss process.
    # For multi-Boss deployments use PostgreSQL (SERIALIZABLE isolation handles it).
    _lock = threading.Lock()

    def list(self) -> list[dict]:
        with self._conn() as conn:
            return self._rows(conn.execute(select(_t_resources)).fetchall())

    def acquire(self, resources: dict) -> bool:
        """Check-and-reserve all requested resources atomically. Returns False if any insufficient."""
        with self._lock:
            with self._conn() as conn:
                for name, amount in resources.items():
                    row = conn.execute(
                        select(_t_resources.c.amount, _t_resources.c.usage)
                        .where(_t_resources.c.name == name)
                    ).fetchone()
                    if not row or (row[1] + float(amount) > row[0]):
                        return False
                for name, amount in resources.items():
                    conn.execute(
                        update(_t_resources)
                        .where(_t_resources.c.name == name)
                        .values(usage=_t_resources.c.usage + float(amount))
                    )
                return True

    def release(self, resources: dict) -> None:
        with self._lock:
            with self._conn() as conn:
                for name, amount in resources.items():
                    conn.execute(
                        update(_t_resources)
                        .where(_t_resources.c.name == name)
                        .values(usage=case(
                            (_t_resources.c.usage > float(amount),
                             _t_resources.c.usage - float(amount)),
                            else_=0.0,
                        ))
                    )

    def update_limits(self, limits: dict) -> tuple[bool, str]:
        """Replace the resource pool definition. Rejects if resources in use would be violated."""
        with self._lock:
            with self._conn() as conn:
                rows = conn.execute(select(_t_resources)).fetchall()
                current = {r[0]: (r[1], r[2]) for r in rows}   # name → (amount, usage)

                for name, (_, usage) in current.items():
                    if name not in limits and usage > 0:
                        return False, f"Risorse occupate: '{name}' in uso ({usage}), impossibile rimuovere"

                for name, new_amount in limits.items():
                    new_amount = float(new_amount)
                    if name in current:
                        _, usage = current[name]
                        if new_amount < usage:
                            return False, (
                                f"Risorse occupate: '{name}' uso attuale ({usage}) > richiesto ({new_amount})"
                            )

                for name, new_amount in limits.items():
                    stmt = self._upsert_stmt(
                        _t_resources,
                        values={"name": name, "amount": float(new_amount), "usage": 0.0},
                        conflict_cols=["name"],
                        update_cols=["amount"],
                    )
                    conn.execute(stmt)

                for name in current:
                    if name not in limits:
                        conn.execute(delete(_t_resources).where(_t_resources.c.name == name))

                return True, "Risorse aggiornate con successo"

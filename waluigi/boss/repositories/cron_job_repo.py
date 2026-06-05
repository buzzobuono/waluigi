from __future__ import annotations
import json
from sqlalchemy import select, update, delete, and_, or_

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_cron_jobs

_cj = _t_cron_jobs


class CronJobRepository(BaseRepository):

    def list_enabled(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_cj).where(_cj.c.enabled == 1)
            ).fetchall()
            return [self._parse(r) for r in rows]

    def list(self, namespace: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_cj).where(_cj.c.namespace == namespace)
            ).fetchall()
            return [self._parse(r) for r in rows]

    def get(self, namespace: str, id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_cj).where(and_(_cj.c.namespace == namespace, _cj.c.id == id))
            ).fetchone()
            return self._parse(row) if row else None

    def upsert(self, namespace: str, id: str, spec: dict, enabled: bool = True) -> None:
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_cron_jobs,
                values={
                    "namespace": namespace,
                    "id":        id,
                    "spec":      json.dumps(spec),
                    "enabled":   1 if enabled else 0,
                },
                conflict_cols=["namespace", "id"],
                update_cols=["spec", "enabled"],
            )
            conn.execute(stmt)

    def delete(self, namespace: str, id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                delete(_cj).where(and_(_cj.c.namespace == namespace, _cj.c.id == id))
            )
            return result.rowcount > 0

    def set_enabled(self, namespace: str, id: str, enabled: bool) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_cj)
                .where(and_(_cj.c.namespace == namespace, _cj.c.id == id))
                .values(enabled=1 if enabled else 0)
            )
            return result.rowcount > 0

    def try_claim_fire(self, namespace: str, id: str,
                       expected_last_fire: str | None, new_last_fire: str) -> bool:
        """Atomic compare-and-swap on last_fire. Returns True if this caller won the claim."""
        with self._conn() as conn:
            result = conn.execute(
                update(_cj)
                .where(and_(
                    _cj.c.namespace == namespace,
                    _cj.c.id        == id,
                    (
                        _cj.c.last_fire == None
                        if expected_last_fire is None
                        else or_(
                            _cj.c.last_fire == None,
                            _cj.c.last_fire == expected_last_fire,
                        )
                    ),
                ))
                .values(last_fire=new_last_fire)
            )
            return result.rowcount == 1

    def set_last_fire(self, namespace: str, id: str, ts: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_cj)
                .where(and_(_cj.c.namespace == namespace, _cj.c.id == id))
                .values(last_fire=ts)
            )

    @staticmethod
    def _parse(row) -> dict:
        d = dict(row._mapping)
        d["spec"]    = json.loads(d["spec"]) if isinstance(d["spec"], str) else (d["spec"] or {})
        d["enabled"] = bool(d["enabled"])
        return d

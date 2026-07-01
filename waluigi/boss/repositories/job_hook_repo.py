from __future__ import annotations
import json
from datetime import datetime, timezone
from sqlalchemy import select, update, delete, and_

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_job_hooks

_jh = _t_job_hooks


class JobHookRepository(BaseRepository):

    def list(self, namespace: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_jh).where(_jh.c.namespace == namespace)
            ).fetchall()
            return [self._parse(r) for r in rows]

    def list_enabled_for_job(self, namespace: str, job_name: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_jh).where(and_(_jh.c.namespace == namespace, _jh.c.enabled == 1))
            ).fetchall()
        result = []
        for row in rows:
            h = self._parse(row)
            if h.get("spec", {}).get("watch", {}).get("job") == job_name:
                result.append(h)
        return result

    def get(self, namespace: str, id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_jh).where(and_(_jh.c.namespace == namespace, _jh.c.id == id))
            ).fetchone()
            return self._parse(row) if row else None

    def upsert(self, namespace: str, id: str, spec: dict, enabled: bool = True) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_job_hooks,
                values={
                    "namespace":  namespace,
                    "id":         id,
                    "spec":       json.dumps(spec),
                    "enabled":    1 if enabled else 0,
                    "created_at": now,
                },
                conflict_cols=["namespace", "id"],
                update_cols=["spec", "enabled"],
            )
            conn.execute(stmt)

    def delete(self, namespace: str, id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                delete(_jh).where(and_(_jh.c.namespace == namespace, _jh.c.id == id))
            )
            return result.rowcount > 0

    def set_enabled(self, namespace: str, id: str, enabled: bool) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_jh)
                .where(and_(_jh.c.namespace == namespace, _jh.c.id == id))
                .values(enabled=1 if enabled else 0)
            )
            return result.rowcount > 0

    @staticmethod
    def _parse(row) -> dict:
        d = dict(row._mapping)
        d["spec"]    = json.loads(d["spec"]) if isinstance(d["spec"], str) else (d["spec"] or {})
        d["enabled"] = bool(d["enabled"])
        return d

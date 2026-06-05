from __future__ import annotations
import json
from sqlalchemy import select, delete, and_

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_job_definitions

_jd = _t_job_definitions


class JobDefinitionRepository(BaseRepository):

    def list(self, namespace: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_jd).where(_jd.c.namespace == namespace)
            ).fetchall()
            return [self._parse(r) for r in rows]

    def get(self, namespace: str, id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_jd).where(and_(_jd.c.namespace == namespace, _jd.c.id == id))
            ).fetchone()
            return self._parse(row) if row else None

    def upsert(self, namespace: str, id: str, metadata: dict, spec: dict) -> None:
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_job_definitions,
                values={
                    "namespace": namespace,
                    "id":        id,
                    "metadata":  json.dumps(metadata),
                    "spec":      json.dumps(spec),
                },
                conflict_cols=["namespace", "id"],
                update_cols=["metadata", "spec"],
            )
            conn.execute(stmt)

    def delete(self, namespace: str, id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                delete(_jd).where(and_(_jd.c.namespace == namespace, _jd.c.id == id))
            )
            return result.rowcount > 0

    @staticmethod
    def _parse(row) -> dict:
        d = dict(row._mapping)
        d["metadata"] = json.loads(d["metadata"]) if isinstance(d["metadata"], str) else (d["metadata"] or {})
        d["spec"]     = json.loads(d["spec"])     if isinstance(d["spec"],     str) else (d["spec"]     or {})
        return d

from __future__ import annotations
import json
from datetime import datetime, timezone
from sqlalchemy import select, delete, and_

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_secrets

_s = _t_secrets.c


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SecretRepository(BaseRepository):

    def list_names(self, namespace: str) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_s.name).where(_s.namespace == namespace)
            ).fetchall()
            return [r[0] for r in rows]

    def get_keys(self, namespace: str, name: str) -> list[str] | None:
        """Return the key names of a secret group (no values)."""
        with self._conn() as conn:
            row = conn.execute(
                select(_s.data, _s.createdate, _s.updatedate).where(
                    and_(_s.namespace == namespace, _s.name == name)
                )
            ).fetchone()
        if row is None:
            return None
        try:
            d = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
            return {"name": name, "keys": list(d.keys()),
                    "createdate": row[1], "updatedate": row[2]}
        except (json.JSONDecodeError, TypeError):
            return {"name": name, "keys": [], "createdate": row[1], "updatedate": row[2]}

    def upsert(self, namespace: str, name: str, data: dict) -> None:
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_secrets,
                values={
                    "namespace":  namespace,
                    "name":       name,
                    "data":       json.dumps(data),
                    "createdate": now,
                    "updatedate": now,
                },
                conflict_cols=["namespace", "name"],
                update_cols=["data", "updatedate"],
            )
            conn.execute(stmt)

    def delete(self, namespace: str, name: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                delete(_t_secrets).where(and_(
                    _s.namespace == namespace,
                    _s.name == name,
                ))
            )
            return result.rowcount > 0

    def get_all_for_namespace(self, namespace: str) -> dict[str, str]:
        """Return merged flat dict of all key→value pairs for the namespace."""
        with self._conn() as conn:
            rows = conn.execute(
                select(_s.data).where(_s.namespace == namespace)
            ).fetchall()
        merged: dict[str, str] = {}
        for row in rows:
            try:
                d = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                merged.update({k: str(v) for k, v in d.items()})
            except (json.JSONDecodeError, TypeError):
                pass
        return merged

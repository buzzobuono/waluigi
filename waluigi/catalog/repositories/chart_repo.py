from __future__ import annotations
import json

from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user


class ChartRepository(BaseRepository):

    def list(self, dataset_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did"
                     " ORDER BY position, id"),
                {"did": dataset_id},
            ).fetchall()
        result = self._rows(rows)
        for r in result:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return result

    def get(self, dataset_id: str, chart_id: int) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did AND id = :cid"),
                {"did": dataset_id, "cid": chart_id},
            ).fetchone()
        r = self._row(row)
        if r:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return r

    def get_by_key(self, dataset_id: str, key: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM charts WHERE dataset_id = :did AND key = :key"),
                {"did": dataset_id, "key": key},
            ).fetchone()
        r = self._row(row)
        if r:
            r["spec"] = json.loads(r.get("spec") or "{}")
        return r

    def add(self, dataset_id: str, key: str, title: str,
            spec: dict, position: int = 0) -> dict:
        now = _now()
        with self._conn() as conn:
            row_id = conn.execute(
                text("INSERT INTO charts"
                     " (dataset_id, key, title, spec, position,"
                     "  username, createdate, updatedate)"
                     " VALUES (:did, :key, :title, :spec, :pos,"
                     "         :usr, :now, :now)"
                     " RETURNING id"),
                {"did": dataset_id, "key": key, "title": title,
                 "spec": json.dumps(spec), "pos": position,
                 "usr": _user(), "now": now},
            ).scalar()
        return self.get(dataset_id, row_id)

    def update(self, dataset_id: str, chart_id: int, **kwargs) -> bool:
        allowed = {"key", "title", "spec", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "spec" in updates:
            updates["spec"] = json.dumps(updates["spec"])
        updates["_now"] = _now()
        updates["_usr"] = _user()
        updates["_did"] = dataset_id
        updates["_cid"] = chart_id
        cols = ", ".join(f"{k} = :{k}" for k in updates if not k.startswith("_"))
        cols += ", updatedate = :_now, username = :_usr"
        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE charts SET {cols}"
                     f" WHERE dataset_id = :_did AND id = :_cid"),
                updates,
            )
        return result.rowcount > 0

    def delete(self, dataset_id: str, chart_id: int) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("DELETE FROM charts WHERE dataset_id = :did AND id = :cid"),
                {"did": dataset_id, "cid": chart_id},
            )
        return result.rowcount > 0

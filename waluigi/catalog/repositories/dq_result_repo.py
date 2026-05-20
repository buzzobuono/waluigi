from __future__ import annotations
import json

from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _t_dq_results


class DQResultRepository(BaseRepository):

    def save(self, dataset_id: str, version: str,
             score: float, passed: int, total: int,
             success: bool, details: list,
             error: str = None) -> dict:
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_dq_results,
                {"dataset_id": dataset_id, "version": version,
                 "score": score, "passed": passed, "total": total,
                 "success": int(success), "details": json.dumps(details),
                 "error": error, "createdate": now},
                ["dataset_id", "version"],
                ["score", "passed", "total", "success", "details",
                 "error", "createdate"],
            )
            conn.execute(stmt)
        return self.get(dataset_id, version)

    def get(self, dataset_id: str, version: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM dq_results"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchone()
        r = self._row(row)
        if r:
            r["details"] = json.loads(r.get("details") or "[]")
            r["success"] = bool(r["success"])
        return r

    def list(self, dataset_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM dq_results WHERE dataset_id = :did"
                     " ORDER BY createdate DESC"),
                {"did": dataset_id},
            ).fetchall()
        result = self._rows(rows)
        for r in result:
            r["details"] = json.loads(r.get("details") or "[]")
            r["success"] = bool(r["success"])
        return result

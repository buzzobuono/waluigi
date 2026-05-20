from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_datasets, _t_versions
from waluigi.catalog.entities import Dataset


class DatasetRepository(BaseRepository):

    def list(self) -> list[Dataset]:
        with self._conn() as conn:
            rows = conn.execute(text("SELECT * FROM datasets ORDER BY id")).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def find(self, status: str = None, description: str = None) -> list[Dataset]:
        clauses, params = [], {}
        if status:
            clauses.append("status = :status")
            params["status"] = status
        if description:
            clauses.append("description LIKE :desc")
            params["desc"] = description
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._conn() as conn:
            rows = conn.execute(
                text(f"SELECT * FROM datasets {where} ORDER BY id"), params
            ).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def get(self, id: str) -> Dataset | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM datasets WHERE id = :id"), {"id": id}
            ).fetchone()
        return Dataset.from_row(row)

    def exists(self, id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT 1 FROM datasets WHERE id = :id"), {"id": id}
            ).fetchone()
        return row is not None

    def create(self, id: str, format: str, description: str = None,
               source_id: str = "local", dq_suite: str = None) -> bool:
        now = _now()
        with self._conn() as conn:
            stmt = self._insert_ignore_stmt(_t_datasets, {
                "id": id, "format": format, "description": description,
                "status": "draft", "source_id": source_id, "dq_suite": dq_suite,
                "username": _user(), "createdate": now, "updatedate": now,
            }, ["id"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def update(self, id: str, **kwargs) -> bool:
        allowed = {"description", "status", "dq_suite"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = :{k}" for k in updates)
        updates["_id"] = id
        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE datasets SET {cols} WHERE id = :_id"), updates
            )
        return result.rowcount > 0

    def delete(self, id: str) -> bool:
        with self._conn() as conn:
            conn.execute(
                text("DELETE FROM versions WHERE dataset_id = :id"), {"id": id}
            )
            result = conn.execute(
                text("DELETE FROM datasets WHERE id = :id"), {"id": id}
            )
        return result.rowcount > 0

    def set_in_review(self, id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE datasets SET status = 'in_review'"
                     " WHERE id = :id AND status = 'draft'"),
                {"id": id},
            )
        return result.rowcount > 0

    def approve(self, id: str, approved_by: str) -> bool:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE datasets"
                     " SET status = 'approved', approved_by = :by, approved_at = :at"
                     " WHERE id = :id AND status != 'deprecated'"),
                {"by": approved_by, "at": now, "id": id},
            )
        return result.rowcount > 0

    def commit_virtual(self, dataset_id: str, version: str,
                       location: str) -> dict:
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_versions,
                {"dataset_id": dataset_id, "version": version, "location": location,
                 "status": "committed", "username": _user(),
                 "createdate": now, "updatedate": now},
                ["dataset_id", "version"],
                ["location", "status", "username", "updatedate"],
            )
            conn.execute(stmt)
        return {"skipped": False, "version": version}

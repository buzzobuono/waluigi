from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_datasets, _t_versions
from waluigi.catalog.entities import Dataset


class DatasetRepository(BaseRepository):

    def list(self, namespace: str) -> list[Dataset]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM datasets WHERE namespace = :ns ORDER BY id"),
                {"ns": namespace},
            ).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def find(self, namespace: str, status: str = None,
             description: str = None) -> list[Dataset]:
        clauses = ["namespace = :ns"]
        params  = {"ns": namespace}
        if status:
            clauses.append("status = :status")
            params["status"] = status
        if description:
            clauses.append("description LIKE :desc")
            params["desc"] = description
        where = "WHERE " + " AND ".join(clauses)
        with self._conn() as conn:
            rows = conn.execute(
                text(f"SELECT * FROM datasets {where} ORDER BY id"), params
            ).fetchall()
        return [Dataset.from_row(r) for r in rows]

    def get(self, namespace: str, id: str) -> Dataset | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM datasets WHERE namespace = :ns AND id = :id"),
                {"ns": namespace, "id": id},
            ).fetchone()
        return Dataset.from_row(row)

    def exists(self, namespace: str, id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT 1 FROM datasets WHERE namespace = :ns AND id = :id"),
                {"ns": namespace, "id": id},
            ).fetchone()
        return row is not None

    def create(self, namespace: str, id: str, format: str,
               description: str = None, source_id: str = None,
               dq_suite: str = None) -> bool:
        now = _now()
        with self._conn() as conn:
            stmt = self._insert_ignore_stmt(_t_datasets, {
                "namespace": namespace, "id": id, "format": format,
                "description": description, "status": "draft",
                "source_id": source_id, "dq_suite": dq_suite,
                "username": _user(), "createdate": now, "updatedate": now,
            }, ["namespace", "id"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def update(self, namespace: str, id: str, **kwargs) -> bool:
        allowed = {"description", "status", "dq_suite"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = :{k}" for k in updates)
        updates["_ns"] = namespace
        updates["_id"] = id
        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE datasets SET {cols} WHERE namespace = :_ns AND id = :_id"),
                updates,
            )
        return result.rowcount > 0

    def delete(self, namespace: str, id: str) -> bool:
        browse_path = f"{namespace}/{id}"
        with self._conn() as conn:
            conn.execute(
                text("DELETE FROM versions WHERE dataset_id = :path"),
                {"path": browse_path},
            )
            result = conn.execute(
                text("DELETE FROM datasets WHERE namespace = :ns AND id = :id"),
                {"ns": namespace, "id": id},
            )
        return result.rowcount > 0

    def set_in_review(self, namespace: str, id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE datasets SET status = 'in_review'"
                     " WHERE namespace = :ns AND id = :id AND status = 'draft'"),
                {"ns": namespace, "id": id},
            )
        return result.rowcount > 0

    def approve(self, namespace: str, id: str, approved_by: str) -> bool:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE datasets"
                     " SET status = 'approved', approved_by = :by, approved_at = :at"
                     " WHERE namespace = :ns AND id = :id AND status != 'deprecated'"),
                {"by": approved_by, "at": now, "ns": namespace, "id": id},
            )
        return result.rowcount > 0

    def commit_virtual(self, namespace: str, id: str,
                       version: str, location: str) -> dict:
        browse_path = f"{namespace}/{id}"
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_versions,
                {"dataset_id": browse_path, "version": version,
                 "location": location, "status": "committed",
                 "username": _user(), "createdate": now, "updatedate": now},
                ["dataset_id", "version"],
                ["location", "status", "username", "updatedate"],
            )
            conn.execute(stmt)
        return {"skipped": False, "version": version}

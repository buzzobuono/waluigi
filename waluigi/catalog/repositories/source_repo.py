from __future__ import annotations
import json

from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_sources
from waluigi.catalog.entities import Source


class SourceRepository(BaseRepository):

    def list(self) -> list[Source]:
        with self._conn() as conn:
            rows = conn.execute(text("SELECT * FROM sources ORDER BY id")).fetchall()
        return [Source.from_row(r) for r in rows]

    def get(self, id: str) -> Source | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM sources WHERE id = :id"), {"id": id}
            ).fetchone()
        return Source.from_row(row)

    def exists(self, id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT 1 FROM sources WHERE id = :id"), {"id": id}
            ).fetchone()
        return row is not None

    def create(self, id: str, type: str, config: dict,
               description: str = None) -> bool:
        now = _now()
        with self._conn() as conn:
            stmt = self._insert_ignore_stmt(_t_sources, {
                "id": id, "description": description, "type": type,
                "config": json.dumps(config), "username": _user(),
                "createdate": now, "updatedate": now,
            }, ["id"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def update(self, id: str, **kwargs) -> bool:
        allowed = {"type", "config", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "config" in updates:
            updates["config"] = json.dumps(updates["config"])
        updates["updatedate"] = _now()
        updates["username"] = _user()
        cols = ", ".join(f"{k} = :{k}" for k in updates)
        updates["_id"] = id
        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE sources SET {cols} WHERE id = :_id"), updates
            )
        return result.rowcount > 0

    def upsert(self, id: str, type: str, config: dict,
               description: str = None) -> None:
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_sources,
                {"id": id, "type": type, "config": json.dumps(config),
                 "description": description, "username": _user(),
                 "createdate": now, "updatedate": now},
                ["id"],
                ["config", "description", "username", "updatedate"],
            )
            conn.execute(stmt)

    def delete(self, id: str) -> bool:
        try:
            with self._conn() as conn:
                result = conn.execute(
                    text("DELETE FROM sources WHERE id = :id"), {"id": id}
                )
            return result.rowcount > 0
        except Exception as e:
            if "FOREIGN KEY" in str(e) or "foreign key" in str(e).lower():
                raise ValueError(
                    f"Source '{id}' is still referenced by one or more datasets"
                ) from e
            raise

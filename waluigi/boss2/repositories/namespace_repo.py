from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import select, delete

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_namespaces

_n = _t_namespaces.c


class NamespaceRepository(BaseRepository):

    def exists(self, name: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                select(_n.name).where(_n.name == name)
            ).fetchone()
            return row is not None

    def get(self, name: str) -> dict | None:
        with self._conn() as conn:
            return self._row(
                conn.execute(
                    select(_t_namespaces).where(_n.name == name)
                ).fetchone()
            )

    def list(self) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(select(_t_namespaces)).fetchall()
            )

    def create(self, name: str, description: str = "") -> None:
        stmt = self._upsert_stmt(
            _t_namespaces,
            values={"name": name, "description": description,
                    "created_at": datetime.now(timezone.utc)},
            conflict_cols=["name"],
            update_cols=["description"],
        )
        with self._conn() as conn:
            conn.execute(stmt)

    def delete(self, name: str) -> None:
        with self._conn() as conn:
            conn.execute(delete(_t_namespaces).where(_n.name == name))

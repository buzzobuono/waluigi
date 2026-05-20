from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_version_metadata


class MetadataRepository(BaseRepository):

    def get(self, dataset_id: str, version: str) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT key, value FROM version_metadata"
                     " WHERE dataset_id = :did AND version = :ver ORDER BY key"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return {dict(r._mapping)["key"]: dict(r._mapping)["value"] for r in rows}

    def set(self, dataset_id: str, version: str, key: str, value: str) -> None:
        now = _now()
        with self._conn() as conn:
            stmt = self._upsert_stmt(
                _t_version_metadata,
                {"dataset_id": dataset_id, "version": version, "key": key,
                 "value": str(value), "username": _user(),
                 "createdate": now, "updatedate": now},
                ["dataset_id", "version", "key"],
                ["value", "updatedate"],
            )
            conn.execute(stmt)

    def delete(self, dataset_id: str, version: str, key: str) -> bool:
        if key.startswith("sys."):
            return False
        with self._conn() as conn:
            result = conn.execute(
                text("DELETE FROM version_metadata"
                     " WHERE dataset_id = :did AND version = :ver AND key = :key"),
                {"did": dataset_id, "ver": version, "key": key},
            )
        return result.rowcount > 0

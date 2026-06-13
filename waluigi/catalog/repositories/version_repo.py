from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_versions
from waluigi.catalog.entities import Version


class VersionRepository(BaseRepository):

    def list(self, dataset_id: str) -> list[Version]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM versions WHERE dataset_id = :did"
                     " AND status = 'committed' ORDER BY createdate DESC"),
                {"did": dataset_id},
            ).fetchall()
        return [Version.from_row(r) for r in rows]

    def get(self, dataset_id: str, version: str) -> Version | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchone()
        return Version.from_row(row)

    def get_latest(self, dataset_id: str) -> Version | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND status = 'committed'"
                     " ORDER BY updatedate DESC LIMIT 1"),
                {"did": dataset_id},
            ).fetchone()
        return Version.from_row(row)

    def find_by_metadata(self, dataset_id: str,
                         metadata: dict,
                         get_metadata_fn) -> Version | None:
        if metadata is None:
            return None
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM versions"
                     " WHERE dataset_id = :did AND status = 'committed'"
                     " ORDER BY updatedate DESC LIMIT 1"),
                {"did": dataset_id},
            ).fetchone()
        if not row:
            return None
        version_id = dict(row._mapping)["version"]
        existing_meta = get_metadata_fn(dataset_id, version_id)
        existing_meta_user = {
            k: v for k, v in existing_meta.items() if not k.startswith("sys.")
        }
        target_meta = {k: str(v) for k, v in metadata.items()}
        if existing_meta_user == target_meta:
            return Version.from_row(row)
        return None

    def reserve(self, dataset_id: str, version: str, location: str) -> bool:
        now = _now()
        with self._conn() as conn:
            stmt = self._insert_ignore_stmt(_t_versions, {
                "dataset_id": dataset_id, "version": version, "location": location,
                "status": "reserved", "username": _user(),
                "createdate": now, "updatedate": now,
            }, ["dataset_id", "version"])
            result = conn.execute(stmt)
        return result.rowcount > 0

    def commit(self, dataset_id: str, version: str) -> bool:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'committed', updatedate = :now"
                     " WHERE dataset_id = :did AND version = :ver"
                     " AND status = 'reserved'"),
                {"now": now, "did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def fail(self, dataset_id: str, version: str) -> bool:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'failed', updatedate = :now"
                     " WHERE dataset_id = :did AND version = :ver"
                     " AND status = 'reserved'"),
                {"now": now, "did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def delete(self, dataset_id: str, version: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("DELETE FROM versions WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def delete_hard(self, dataset_id: str, version: str) -> bool:
        with self._conn() as conn:
            conn.execute(
                text("DELETE FROM version_metadata"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
            conn.execute(
                text("DELETE FROM dq_results"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
            conn.execute(
                text("DELETE FROM lineage"
                     " WHERE (output_dataset = :did AND output_version = :ver)"
                     "    OR (input_dataset  = :did AND input_version  = :ver)"),
                {"did": dataset_id, "ver": version},
            )
            result = conn.execute(
                text("DELETE FROM versions WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

    def deprecate(self, dataset_id: str, version: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE versions SET status = 'deprecated'"
                     " WHERE dataset_id = :did AND version = :ver"),
                {"did": dataset_id, "ver": version},
            )
        return result.rowcount > 0

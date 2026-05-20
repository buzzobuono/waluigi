from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_lineage


class LineageRepository(BaseRepository):

    def insert(self, out_id: str, out_ver: str, inputs: list[dict]) -> None:
        now = _now()
        with self._conn() as conn:
            for i in inputs:
                stmt = self._insert_ignore_stmt(_t_lineage, {
                    "output_dataset": out_id, "output_version": out_ver,
                    "input_dataset": i["dataset_id"], "input_version": i["version"],
                    "username": _user(), "createdate": now, "updatedate": now,
                }, ["output_dataset", "output_version",
                    "input_dataset", "input_version"])
                conn.execute(stmt)

    def get_upstream(self, dataset_id: str, version: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT l.input_dataset AS dataset_id,"
                     "       l.input_version AS version"
                     " FROM lineage l"
                     " WHERE l.output_dataset = :did AND l.output_version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return self._rows(rows)

    def get_downstream(self, dataset_id: str, version: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT l.output_dataset AS dataset_id,"
                     "       l.output_version AS version"
                     " FROM lineage l"
                     " WHERE l.input_dataset = :did AND l.input_version = :ver"),
                {"did": dataset_id, "ver": version},
            ).fetchall()
        return self._rows(rows)

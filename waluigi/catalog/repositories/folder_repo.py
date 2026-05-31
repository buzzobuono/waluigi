from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.entities import Dataset


class FolderRepository(BaseRepository):

    def list_folders(self, namespace: str, prefix: str) -> dict:
        prefix = prefix.rstrip("/") + "/"
        prefix = prefix.lstrip("/")
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM datasets"
                     " WHERE namespace = :ns AND id LIKE :pat ORDER BY id"),
                {"ns": namespace, "pat": f"{prefix}%"},
            ).fetchall()

        datasets, sub_prefixes = [], set()
        for row in rows:
            d = Dataset.from_row(row)
            rest = d.id[len(prefix):]
            if "/" not in rest:
                datasets.append(d)
            else:
                sub = prefix + rest.split("/")[0] + "/"
                sub_prefixes.add(sub)

        return {
            "prefix":   prefix,
            "datasets": [d.to_dict() for d in datasets],
            "prefixes": sorted(sub_prefixes),
        }

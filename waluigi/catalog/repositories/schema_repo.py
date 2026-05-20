from __future__ import annotations
from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user, _t_schema_columns


class SchemaRepository(BaseRepository):

    def get(self, dataset_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM schema_columns WHERE dataset_id = :did"
                     " ORDER BY column_name"),
                {"did": dataset_id},
            ).fetchall()
        return self._rows(rows)

    def upsert_columns(self, dataset_id: str, columns: list[dict]) -> None:
        now = _now()
        with self._conn() as conn:
            for col in columns:
                stmt = self._upsert_stmt(
                    _t_schema_columns,
                    {"dataset_id": dataset_id, "column_name": col["name"],
                     "physical_type": col.get("physical_type"),
                     "logical_type": col.get("logical_type"),
                     "nullable": 1, "pii": 0, "pii_type": "none",
                     "pii_notes": "", "description": "", "status": "inferred",
                     "username": _user(), "createdate": now, "updatedate": now},
                    ["dataset_id", "column_name"],
                    ["physical_type", "updatedate"],
                )
                conn.execute(stmt)

    def upsert_column(self, dataset_id: str, column_name: str,
                      **kwargs) -> dict | None:
        allowed = {"logical_type", "nullable", "pii", "pii_type",
                   "pii_notes", "description"}
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if "nullable" in updates:
            updates["nullable"] = int(updates["nullable"])
        if "pii" in updates:
            updates["pii"] = int(updates["pii"])

        now = _now()
        set_parts = [f"{k} = :{k}" for k in updates]
        set_parts += [
            "username = :_usr", "updatedate = :_now",
            "status = CASE WHEN status = 'published' THEN 'published' ELSE 'draft' END",
        ]
        params = dict(updates)
        params.update({"_usr": _user(), "_now": now,
                       "_did": dataset_id, "_col": column_name})

        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE schema_columns SET {', '.join(set_parts)}"
                     f" WHERE dataset_id = :_did AND column_name = :_col"),
                params,
            )
            if result.rowcount == 0:
                conn.execute(text("""
                    INSERT INTO schema_columns
                        (dataset_id, column_name, physical_type, logical_type,
                         nullable, pii, pii_type, pii_notes, description,
                         status, username, createdate, updatedate)
                    VALUES
                        (:did, :col, NULL, :lt, :nullable, :pii,
                         :pii_type, :pii_notes, :description,
                         'draft', :usr, :now, :now)
                """), {
                    "did": dataset_id, "col": column_name,
                    "lt":          updates.get("logical_type"),
                    "nullable":    int(updates.get("nullable", True)),
                    "pii":         int(updates.get("pii", False)),
                    "pii_type":    updates.get("pii_type", "none"),
                    "pii_notes":   updates.get("pii_notes", ""),
                    "description": updates.get("description", ""),
                    "usr": _user(), "now": now,
                })

        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM schema_columns"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"did": dataset_id, "col": column_name},
            ).fetchone()
        return self._row(row)

    def publish(self, dataset_id: str, publisher: str) -> dict:
        now = _now()
        with self._conn() as conn:
            conn.execute(
                text("UPDATE schema_columns SET status = 'published', updatedate = :now"
                     " WHERE dataset_id = :did AND status IN ('inferred', 'draft')"),
                {"now": now, "did": dataset_id},
            )
        return {"published_at": now, "breaking_changes": [], "warnings": []}

    def approve_column(self, dataset_id: str, column_name: str) -> bool:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("UPDATE schema_columns"
                     " SET status = 'published', username = :usr, updatedate = :now"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"usr": _user(), "now": now, "did": dataset_id, "col": column_name},
            )
        return result.rowcount > 0

    def delete_column(self, dataset_id: str, column_name: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("DELETE FROM schema_columns"
                     " WHERE dataset_id = :did AND column_name = :col"),
                {"did": dataset_id, "col": column_name},
            )
        return result.rowcount > 0

    def diff_against_inferred(self, dataset_id: str,
                               inferred: list[dict]) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM schema_columns"
                     " WHERE dataset_id = :did AND status = 'published'"),
                {"did": dataset_id},
            ).fetchall()
        if not rows:
            return {"breaking": [], "warnings": []}

        pub = {dict(r._mapping)["column_name"]: dict(r._mapping) for r in rows}
        inf = {c["name"]: c for c in inferred}

        breaking, warnings = [], []
        for col, meta in pub.items():
            if col not in inf:
                breaking.append(f"Published column '{col}' missing in new data")
            elif inf[col].get("physical_type") != meta["physical_type"]:
                breaking.append(
                    f"Type change on '{col}': "
                    f"{meta['physical_type']} → {inf[col].get('physical_type')}")
        return {"breaking": breaking, "warnings": warnings}

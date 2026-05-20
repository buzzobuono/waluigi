from __future__ import annotations
import json

from sqlalchemy import text

from waluigi.catalog.db.base import BaseRepository
from waluigi.catalog.db.engine import _now, _user
from waluigi.catalog.entities import Expectation


class ExpectationRepository(BaseRepository):

    def list(self, dataset_id: str) -> list[Expectation]:
        with self._conn() as conn:
            rows = conn.execute(
                text("SELECT * FROM expectations WHERE dataset_id = :did"
                     " ORDER BY position, id"),
                {"did": dataset_id},
            ).fetchall()
        return [Expectation.from_row(r) for r in rows]

    def get(self, dataset_id: str, exp_id: int) -> Expectation | None:
        with self._conn() as conn:
            row = conn.execute(
                text("SELECT * FROM expectations"
                     " WHERE dataset_id = :did AND id = :eid"),
                {"did": dataset_id, "eid": exp_id},
            ).fetchone()
        return Expectation.from_row(row)

    def add(self, dataset_id: str, rule_id: str, inputs: dict, params: dict,
            tolerance: float = 1.0, position: int = 0) -> Expectation:
        now = _now()
        with self._conn() as conn:
            result = conn.execute(
                text("INSERT INTO expectations"
                     " (dataset_id, rule_id, inputs, params, tolerance, position,"
                     "  username, createdate, updatedate)"
                     " VALUES (:did, :rule, :inputs, :params, :tol, :pos,"
                     "         :usr, :now, :now)"),
                {"did": dataset_id, "rule": rule_id,
                 "inputs": json.dumps(inputs), "params": json.dumps(params),
                 "tol": tolerance, "pos": position,
                 "usr": _user(), "now": now},
            )
            row_id = result.lastrowid
        return self.get(dataset_id, row_id)

    def update(self, dataset_id: str, exp_id: int, **kwargs) -> bool:
        allowed = {"rule_id", "inputs", "params", "tolerance", "position"}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        if not updates:
            return False
        if "inputs" in updates:
            updates["inputs"] = json.dumps(updates["inputs"])
        if "params" in updates:
            updates["params"] = json.dumps(updates["params"])
        updates["_now"] = _now()
        updates["_usr"] = _user()
        updates["_did"] = dataset_id
        updates["_eid"] = exp_id
        cols = ", ".join(f"{k} = :{k}" for k in updates if not k.startswith("_"))
        cols += ", updatedate = :_now, username = :_usr"
        with self._conn() as conn:
            result = conn.execute(
                text(f"UPDATE expectations SET {cols}"
                     f" WHERE dataset_id = :_did AND id = :_eid"),
                updates,
            )
        return result.rowcount > 0

    def delete(self, dataset_id: str, exp_id: int) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                text("DELETE FROM expectations WHERE dataset_id = :did AND id = :eid"),
                {"did": dataset_id, "eid": exp_id},
            )
        return result.rowcount > 0

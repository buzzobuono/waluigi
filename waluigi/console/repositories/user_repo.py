from __future__ import annotations
import json
from sqlalchemy import select, update, delete

from waluigi.console.db.base import BaseRepository
from waluigi.console.db.engine import _t_users


class UserRepository(BaseRepository):

    def get(self, userid: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_users).where(_t_users.c.userid == userid)
            ).fetchone()
            return self._row(row)

    def list(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                select(_t_users).order_by(_t_users.c.createdate.desc())
            ).fetchall()
            return self._rows(rows)

    def create(self, userid: str, username: str, password_hash: str,
               namespaces: list[str], now: str) -> None:
        with self._conn() as conn:
            conn.execute(_t_users.insert().values(
                userid=userid,
                username=username,
                password_hash=password_hash,
                namespaces=json.dumps(namespaces),
                createdate=now,
                updatedate=now,
            ))

    def update(self, userid: str, values: dict) -> int:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_users)
                .where(_t_users.c.userid == userid)
                .values(**values)
            )
            return result.rowcount

    def delete(self, userid: str) -> int:
        with self._conn() as conn:
            result = conn.execute(
                delete(_t_users).where(_t_users.c.userid == userid)
            )
            return result.rowcount

    def exists(self, userid: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_users.c.userid).where(_t_users.c.userid == userid)
            ).fetchone()
            return row is not None

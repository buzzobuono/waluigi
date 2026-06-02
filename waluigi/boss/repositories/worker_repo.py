from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import select, update, delete, and_, case

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_workers


class WorkerRepository(BaseRepository):

    def register(self, url: str, max_slots: int, free_slots: int) -> None:
        stmt = self._upsert_stmt(
            _t_workers,
            values={
                "url":        url,
                "max_slots":  max_slots,
                "free_slots": free_slots,
                "status":     "ALIVE",
                "last_seen":  datetime.now(timezone.utc),
            },
            conflict_cols=["url"],
            update_cols=["max_slots", "free_slots", "status", "last_seen"],
        )
        with self._conn() as conn:
            conn.execute(stmt)

    def list(self) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(select(_t_workers).order_by(_t_workers.c.last_seen.asc())).fetchall()
            )

    def get_available(self) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(
                    select(_t_workers)
                    .where(_t_workers.c.free_slots > 0)
                    .order_by(_t_workers.c.free_slots.desc(), _t_workers.c.last_seen.asc())
                ).fetchall()
            )

    def get_slots(self, url: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_workers.c.max_slots, _t_workers.c.free_slots)
                .where(_t_workers.c.url == url)
            ).fetchone()
            return {"max_slots": row[0], "free_slots": row[1]} if row else None

    def acquire_slot(self, url: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_workers)
                .where(and_(
                    _t_workers.c.url == url,
                    _t_workers.c.free_slots > 0,
                    _t_workers.c.status == "ALIVE",
                ))
                .values(free_slots=_t_workers.c.free_slots - 1)
            )
            return result.rowcount > 0

    def release_slot(self, url: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_workers)
                .where(and_(_t_workers.c.url == url, _t_workers.c.status == "ALIVE"))
                .values(free_slots=case(
                    (_t_workers.c.free_slots < _t_workers.c.max_slots,
                     _t_workers.c.free_slots + 1),
                    else_=_t_workers.c.max_slots,
                ))
            )

    def delete(self, url: str) -> None:
        with self._conn() as conn:
            conn.execute(delete(_t_workers).where(_t_workers.c.url == url))

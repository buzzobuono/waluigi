from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import select, insert, and_

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_task_logs


class LogRepository(BaseRepository):

    def insert_many(self, namespace: str, task_id: str, lines: list[str], worker_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                insert(_t_task_logs),
                [
                    {"namespace": namespace, "task_id": task_id,
                     "message": line, "boss_id": worker_id, "timestamp": now}
                    for line in lines
                ],
            )

    def get(self, namespace: str, task_id: str, limit: int = 20) -> list[dict]:
        with self._conn() as conn:
            subq = (
                select(_t_task_logs)
                .where(and_(
                    _t_task_logs.c.namespace == namespace,
                    _t_task_logs.c.task_id == task_id,
                ))
                .order_by(_t_task_logs.c.id.desc())
                .limit(limit)
                .subquery()
            )
            rows = conn.execute(select(subq).order_by(subq.c.id.asc())).fetchall()
            return [
                {
                    "id":        r.id,
                    "timestamp": r.timestamp,
                    "worker_id": r.boss_id,
                    "message":   r.message,
                }
                for r in rows
            ]

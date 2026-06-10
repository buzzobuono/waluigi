from __future__ import annotations
from sqlalchemy import select, delete

from waluigi.boss.db.base import BaseRepository
from waluigi.boss.db.engine import _t_task_deps


class TaskDepsRepository(BaseRepository):

    def add(self, namespace: str, task_id: str, dep_id: str) -> None:
        stmt = self._upsert_stmt(
            _t_task_deps,
            values={"namespace": namespace, "task_id": task_id, "dep_id": dep_id},
            conflict_cols=["namespace", "task_id", "dep_id"],
            update_cols=[],
        )
        with self._conn() as conn:
            conn.execute(stmt)

    def list_by_namespace(self, namespace: str) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(
                    select(_t_task_deps).where(_t_task_deps.c.namespace == namespace)
                ).fetchall()
            )

    def list_by_tasks(self, namespace: str, task_ids: list[str]) -> list[dict]:
        if not task_ids:
            return []
        with self._conn() as conn:
            return self._rows(
                conn.execute(
                    select(_t_task_deps).where(
                        (_t_task_deps.c.namespace == namespace) &
                        (_t_task_deps.c.task_id.in_(task_ids))
                    )
                ).fetchall()
            )

    def delete_by_task(self, namespace: str, task_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                delete(_t_task_deps).where(
                    (_t_task_deps.c.namespace == namespace) &
                    (_t_task_deps.c.task_id == task_id)
                )
            )

    def delete_by_namespace(self, namespace: str) -> None:
        with self._conn() as conn:
            conn.execute(
                delete(_t_task_deps).where(_t_task_deps.c.namespace == namespace)
            )

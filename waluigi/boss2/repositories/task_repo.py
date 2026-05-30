from __future__ import annotations
from datetime import datetime, timezone
from typing import Literal
from sqlalchemy import select, update, delete, func, and_

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_tasks


class TaskRepository(BaseRepository):

    def get_status(self, task_id: str, params_hash: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_tasks.c.status).where(
                    and_(_t_tasks.c.id == task_id, _t_tasks.c.params == params_hash)
                )
            ).fetchone()
            return row[0] if row else None

    def register(self, task_id: str, namespace: str, parent_id: str | None,
                 params: str, attributes: str, job_id: str) -> None:
        stmt = self._upsert_stmt(
            _t_tasks,
            values={
                "id":          task_id,
                "namespace":   namespace,
                "parent_id":   parent_id,
                "params":      params,
                "attributes":  attributes,
                "status":      "PENDING",
                "last_update": datetime.now(timezone.utc),
                "job_id":      job_id,
            },
            conflict_cols=["id"],
            update_cols=["namespace", "parent_id", "job_id", "last_update"],
        )
        with self._conn() as conn:
            conn.execute(stmt)

    def try_lock(self, task_id: str) -> bool:
        """Atomically transition status to RUNNING. Returns False if already RUNNING."""
        with self._conn() as conn:
            result = conn.execute(
                update(_t_tasks)
                .where(and_(_t_tasks.c.id == task_id, _t_tasks.c.status != "RUNNING"))
                .values(status="RUNNING", last_update=datetime.now(timezone.utc))
            )
            return result.rowcount > 0

    def update(self, task_id: str, namespace: str, params: str,
               attributes: str, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_tasks)
                .where(_t_tasks.c.id == task_id)
                .values(
                    status=status,
                    last_update=datetime.now(timezone.utc),
                    namespace=namespace,
                    params=params,
                    attributes=attributes,
                )
            )

    def list_tasks(
        self,
        *,
        job_id: str | None = None,
        namespace: str | None = None,
        order: Literal["asc", "desc"] = "desc",
    ) -> list[dict]:
        q = select(_t_tasks)
        if job_id is not None:
            q = q.where(_t_tasks.c.job_id == job_id)
        if namespace is not None:
            q = q.where(_t_tasks.c.namespace == namespace)
        sort = _t_tasks.c.last_update.asc() if order == "asc" else _t_tasks.c.last_update.desc()
        q = q.order_by(sort)
        with self._conn() as conn:
            return self._rows(conn.execute(q).fetchall())
                
    def reset(self, task_id: str) -> None:
        with self._conn() as conn:
            conn.execute(update(_t_tasks).where(_t_tasks.c.id == task_id).values(status="PENDING"))
            
    def delete(self, task_id: str) -> None:
        with self._conn() as conn:
            conn.execute(delete(_t_tasks).where(_t_tasks.c.id == task_id))



    def list_namespaces(self) -> list[dict]:
        with self._conn() as conn:
            return self._rows(
                conn.execute(
                    select(_t_tasks.c.namespace, func.count().label("task_count"))
                    .group_by(_t_tasks.c.namespace)
                ).fetchall()
            )
    
    def delete_namespace(self, namespace: str) -> None:
        with self._conn() as conn:
            conn.execute(delete(_t_tasks).where(_t_tasks.c.namespace == namespace))

    def reset_namespace(self, namespace: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_tasks).where(_t_tasks.c.namespace == namespace).values(status="PENDING")
            )
    
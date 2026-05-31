from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update, delete, and_, or_, case

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_jobs, _t_tasks


class JobRepository(BaseRepository):

    def create(self, namespace: str, job_id: str, kind: str, metadata: dict, spec: dict) -> None:
        with self._conn() as conn:
            existing = conn.execute(
                select(_t_jobs.c.status).where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                ))
            ).fetchone()
            if existing is None:
                conn.execute(_t_jobs.insert().values(
                    namespace=namespace,
                    job_id=job_id,
                    kind=kind,
                    metadata=json.dumps(metadata),
                    spec=json.dumps(spec),
                    status="PENDING",
                ))
            elif existing[0] not in ("RUNNING", "READY"):
                conn.execute(
                    update(_t_jobs)
                    .where(and_(_t_jobs.c.namespace == namespace, _t_jobs.c.job_id == job_id))
                    .values(
                        kind=kind,
                        metadata=json.dumps(metadata),
                        spec=json.dumps(spec),
                        status="PENDING",
                        locked_by=None,
                        locked_until=None,
                    )
                )

    def list_runnable_ids(self) -> list[tuple[str, str]]:
        now = datetime.now(timezone.utc)
        with self._conn() as conn:
            rows = conn.execute(
                select(_t_jobs.c.namespace, _t_jobs.c.job_id).where(and_(
                    _t_jobs.c.status.notin_(["SUCCESS", "FAILED", "CANCELLED", "PAUSED"]),
                    or_(_t_jobs.c.locked_until == None, _t_jobs.c.locked_until < now),
                ))
            ).fetchall()
            return [(r[0], r[1]) for r in rows]

    def claim(self, boss_id: str, namespace: str, job_id: str) -> dict | None:
        now = datetime.now(timezone.utc)
        lock_until = now + timedelta(seconds=60)
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.notin_(["SUCCESS", "FAILED", "CANCELLED"]),
                    or_(_t_jobs.c.locked_until == None, _t_jobs.c.locked_until < now),
                ))
                .values(
                    locked_by=boss_id,
                    locked_until=lock_until,
                    status="RUNNING",
                    started_at=case(
                        (_t_jobs.c.started_at == None, now),
                        else_=_t_jobs.c.started_at,
                    ),
                )
            )
            if result.rowcount == 0:
                return None
            row = conn.execute(
                select(_t_jobs).where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                ))
            ).fetchone()
            return {
                "namespace": row.namespace,
                "job_id":    row.job_id,
                "metadata":  json.loads(row.metadata),
                "spec":      json.loads(row.spec),
            }

    def update_status(self, namespace: str, job_id: str, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_jobs)
                .where(and_(_t_jobs.c.namespace == namespace, _t_jobs.c.job_id == job_id))
                .values(status=status, locked_by=None, locked_until=None)
            )

    def get_status(self, namespace: str, job_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_jobs.c.status).where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                ))
            ).fetchone()
            return row[0] if row else None

    def release(self, namespace: str, job_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_jobs)
                .where(and_(_t_jobs.c.namespace == namespace, _t_jobs.c.job_id == job_id))
                .values(locked_by=None, locked_until=None)
            )

    def get(self, namespace: str, job_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_jobs).where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                ))
            ).fetchone()
            if row is None:
                return None
            d = dict(row._mapping)
            d["metadata"] = json.loads(d["metadata"]) if isinstance(d["metadata"], str) else (d["metadata"] or {})
            d["spec"]     = json.loads(d["spec"])     if isinstance(d["spec"],     str) else (d["spec"]     or {})
            return d

    def list(self, namespace: str | None = None) -> list[dict]:
        with self._conn() as conn:
            q = select(_t_jobs)
            if namespace is not None:
                q = q.where(_t_jobs.c.namespace == namespace)
            return self._rows(conn.execute(q).fetchall())

    def cancel(self, namespace: str, job_id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.notin_(["SUCCESS", "FAILED", "CANCELLED"]),
                ))
                .values(status="CANCELLED", locked_by=None, locked_until=None)
            )
            return result.rowcount > 0

    def reset(self, namespace: str, job_id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["FAILED", "CANCELLED"]),
                ))
                .values(status="PENDING", locked_by=None, locked_until=None)
            )
            if result.rowcount > 0:
                conn.execute(
                    update(_t_tasks)
                    .where(and_(
                        _t_tasks.c.namespace == namespace,
                        _t_tasks.c.job_id == job_id,
                        _t_tasks.c.status == "FAILED",
                    ))
                    .values(status="PENDING")
                )
            return result.rowcount > 0

    def pause(self, namespace: str, job_id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["PENDING", "RUNNING"]),
                ))
                .values(status="PAUSED", locked_by=None, locked_until=None)
            )
            return result.rowcount > 0

    def resume(self, namespace: str, job_id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status == "PAUSED",
                ))
                .values(status="PENDING")
            )
            return result.rowcount > 0

    def delete(self, namespace: str, job_id: str) -> bool:
        with self._conn() as conn:
            conn.execute(
                delete(_t_tasks).where(and_(
                    _t_tasks.c.namespace == namespace,
                    _t_tasks.c.job_id == job_id,
                ))
            )
            result = conn.execute(
                delete(_t_jobs).where(and_(
                    _t_jobs.c.namespace == namespace,
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["SUCCESS", "FAILED", "CANCELLED"]),
                ))
            )
            return result.rowcount > 0

    def reset_namespace(self, namespace: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_jobs)
                .where(_t_jobs.c.namespace == namespace)
                .values(status="PENDING", locked_by=None, locked_until=None)
            )

    def delete_namespace(self, namespace: str) -> None:
        with self._conn() as conn:
            conn.execute(delete(_t_jobs).where(_t_jobs.c.namespace == namespace))

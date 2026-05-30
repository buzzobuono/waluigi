from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update, delete, and_, or_, case

from waluigi.boss2.db.base import BaseRepository
from waluigi.boss2.db.engine import _t_jobs, _t_tasks


class JobRepository(BaseRepository):

    def create(self, job_id: str, metadata: dict, spec: dict) -> None:
        """Insert or update a job. Does NOT update if the job is currently RUNNING or READY."""
        with self._conn() as conn:
            existing = conn.execute(
                select(_t_jobs.c.status).where(_t_jobs.c.job_id == job_id)
            ).fetchone()
            if existing is None:
                conn.execute(_t_jobs.insert().values(
                    job_id=job_id,
                    metadata=json.dumps(metadata),
                    spec=json.dumps(spec),
                    status="PENDING",
                ))
            elif existing[0] not in ("RUNNING", "READY"):
                conn.execute(
                    update(_t_jobs)
                    .where(_t_jobs.c.job_id == job_id)
                    .values(
                        metadata=json.dumps(metadata),
                        spec=json.dumps(spec),
                        status="PENDING",
                        locked_by=None,
                        locked_until=None,
                    )
                )

    def list_runnable_ids(self) -> list[str]:
        now = datetime.now(timezone.utc)
        with self._conn() as conn:
            rows = conn.execute(
                select(_t_jobs.c.job_id).where(
                    and_(
                        _t_jobs.c.status.notin_(["SUCCESS", "FAILED", "CANCELLED", "PAUSED"]),
                        or_(_t_jobs.c.locked_until == None, _t_jobs.c.locked_until < now),
                    )
                )
            ).fetchall()
            return [r[0] for r in rows]

    def claim(self, boss_id: str, job_id: str) -> dict | None:
        """Atomically lock a job for this boss. Returns job data or None if already claimed."""
        now = datetime.now(timezone.utc)
        lock_until = now + timedelta(seconds=60)
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
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
                select(_t_jobs).where(_t_jobs.c.job_id == job_id)
            ).fetchone()
            return {
                "job_id":   row.job_id,
                "metadata": json.loads(row.metadata),
                "spec":     json.loads(row.spec),
            }

    def update_status(self, job_id: str, status: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_jobs)
                .where(_t_jobs.c.job_id == job_id)
                .values(status=status, locked_by=None, locked_until=None)
            )

    def get_status(self, job_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                select(_t_jobs.c.status).where(_t_jobs.c.job_id == job_id)
            ).fetchone()
            return row[0] if row else None

    def release(self, job_id: str) -> None:
        with self._conn() as conn:
            conn.execute(
                update(_t_jobs)
                .where(_t_jobs.c.job_id == job_id)
                .values(locked_by=None, locked_until=None)
            )

    def list(self, status: str | None = None) -> list[dict]:
        with self._conn() as conn:
            q = select(_t_jobs)
            if status:
                q = q.where(_t_jobs.c.status == status)
            return self._rows(conn.execute(q).fetchall())

    def cancel(self, job_id: str) -> bool:
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.notin_(["SUCCESS", "FAILED", "CANCELLED"]),
                ))
                .values(status="CANCELLED", locked_by=None, locked_until=None)
            )
            return result.rowcount > 0

    def reset(self, job_id: str) -> bool:
        """Reset a terminal job (FAILED/CANCELLED) to PENDING and reset its FAILED tasks."""
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["FAILED", "CANCELLED"]),
                ))
                .values(status="PENDING", locked_by=None, locked_until=None)
            )
            if result.rowcount > 0:
                conn.execute(
                    update(_t_tasks)
                    .where(and_(
                        _t_tasks.c.job_id == job_id,
                        _t_tasks.c.status == "FAILED",
                    ))
                    .values(status="PENDING")
                )
            return result.rowcount > 0

    def pause(self, job_id: str) -> bool:
        """Pause an active job (PENDING/RUNNING). The planner will skip it."""
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["PENDING", "RUNNING"]),
                ))
                .values(status="PAUSED", locked_by=None, locked_until=None)
            )
            return result.rowcount > 0

    def resume(self, job_id: str) -> bool:
        """Resume a paused job back to PENDING."""
        with self._conn() as conn:
            result = conn.execute(
                update(_t_jobs)
                .where(and_(
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status == "PAUSED",
                ))
                .values(status="PENDING")
            )
            return result.rowcount > 0

    def delete(self, job_id: str) -> bool:
        """Delete job and its tasks only if the job is in a terminal state."""
        with self._conn() as conn:
            terminal = select(_t_jobs.c.job_id).where(
                and_(
                    _t_jobs.c.job_id == job_id,
                    _t_jobs.c.status.in_(["SUCCESS", "FAILED", "CANCELLED"]),
                )
            )
            conn.execute(
                delete(_t_tasks).where(_t_tasks.c.job_id.in_(terminal))
            )
            result = conn.execute(
                delete(_t_jobs).where(
                    and_(
                        _t_jobs.c.job_id == job_id,
                        _t_jobs.c.status.in_(["SUCCESS", "FAILED", "CANCELLED"]),
                    )
                )
            )
            return result.rowcount > 0

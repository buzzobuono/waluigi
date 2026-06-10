from __future__ import annotations
import logging
from fastapi import Depends

from waluigi.boss.db import BossDB

logger = logging.getLogger("waluigi")

_db: BossDB | None = None


def init_db(url: str) -> None:
    global _db
    _db = BossDB(url)
    logger.info(f"Boss DB ready: {url}")


def get_db() -> BossDB:
    return _db


# ── Repository dependencies ───────────────────────────────────────────────────

def tasks_repository(db=Depends(get_db)):
    return db.tasks

def jobs_repository(db=Depends(get_db)):
    return db.jobs

def workers_repository(db=Depends(get_db)):
    return db.workers

def resources_repository(db=Depends(get_db)):
    return db.resources

def logs_repository(db=Depends(get_db)):
    return db.logs


# ── Service dependencies ──────────────────────────────────────────────────────

def task_service(db=Depends(get_db)):
    from waluigi.boss.services.task_service import TaskService
    return TaskService(db.tasks, db.task_deps)

def namespaces_repository(db=Depends(get_db)):
    return db.namespaces

def namespace_service(db=Depends(get_db)):
    from waluigi.boss.services.namespace_service import NamespaceService
    return NamespaceService(db.namespaces, db.tasks, db.jobs, db.task_deps, db.logs)


def job_service(db=Depends(get_db)):
    from waluigi.boss.services.job_service import JobService
    return JobService(db.jobs)

def worker_service(db=Depends(get_db)):
    from waluigi.boss.services.worker_service import WorkerService
    return WorkerService(db.workers)

def resource_service(db=Depends(get_db)):
    from waluigi.boss.services.resource_service import ResourceService
    return ResourceService(db.resources)

def log_service(db=Depends(get_db)):
    from waluigi.boss.services.log_service import LogService
    return LogService(db.logs)

def update_service(db=Depends(get_db)):
    from waluigi.boss.services.update_service import UpdateService
    return UpdateService(db.tasks, db.resources, db.workers)

def task_definition_service(db=Depends(get_db)):
    from waluigi.boss.services.task_definition_service import TaskDefinitionService
    return TaskDefinitionService(db.task_definitions)

def job_definition_service(db=Depends(get_db)):
    from waluigi.boss.services.job_definition_service import JobDefinitionService
    return JobDefinitionService(db.job_definitions)

def cron_job_service(db=Depends(get_db)):
    from waluigi.boss.services.cron_job_service import CronJobService
    return CronJobService(db.cron_jobs)

def boss_engine(db=Depends(get_db)):
    from waluigi.boss.engine import BossEngine
    return BossEngine(db.tasks, db.workers, db.resources, db.task_definitions, db.task_deps)

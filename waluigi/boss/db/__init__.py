from waluigi.boss.db.base import _set_engine
from waluigi.boss.db.engine import create_boss_engine


class BossDB:
    """Registry: initialises one shared engine and exposes typed repositories."""

    def __init__(self, url: str):
        from waluigi.boss.repositories.task_repo import TaskRepository
        from waluigi.boss.repositories.job_repo import JobRepository
        from waluigi.boss.repositories.worker_repo import WorkerRepository
        from waluigi.boss.repositories.resource_repo import ResourceRepository
        from waluigi.boss.repositories.log_repo import LogRepository
        from waluigi.boss.repositories.task_definition_repo import TaskDefinitionRepository
        from waluigi.boss.repositories.namespace_repo import NamespaceRepository

        engine = create_boss_engine(url)
        _set_engine(engine)

        self.tasks            = TaskRepository(engine)
        self.jobs             = JobRepository(engine)
        self.workers          = WorkerRepository(engine)
        self.resources        = ResourceRepository(engine)
        self.logs             = LogRepository(engine)
        self.task_definitions = TaskDefinitionRepository(engine)
        self.namespaces       = NamespaceRepository(engine)

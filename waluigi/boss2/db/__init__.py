from waluigi.boss2.db.base import _set_engine
from waluigi.boss2.db.engine import create_boss_engine


class BossDB:
    """Registry: initialises one shared engine and exposes typed repositories."""

    def __init__(self, url: str):
        from waluigi.boss2.repositories.task_repo import TaskRepository
        from waluigi.boss2.repositories.job_repo import JobRepository
        from waluigi.boss2.repositories.worker_repo import WorkerRepository
        from waluigi.boss2.repositories.resource_repo import ResourceRepository
        from waluigi.boss2.repositories.log_repo import LogRepository
        from waluigi.boss2.repositories.task_definition_repo import TaskDefinitionRepository
        from waluigi.boss2.repositories.namespace_repo import NamespaceRepository

        engine = create_boss_engine(url)
        _set_engine(engine)

        self.tasks            = TaskRepository(engine)
        self.jobs             = JobRepository(engine)
        self.workers          = WorkerRepository(engine)
        self.resources        = ResourceRepository(engine)
        self.logs             = LogRepository(engine)
        self.task_definitions = TaskDefinitionRepository(engine)
        self.namespaces       = NamespaceRepository(engine)

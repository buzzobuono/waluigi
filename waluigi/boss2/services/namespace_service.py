from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository
from waluigi.boss2.repositories.job_repo import JobRepository


class NamespaceService:

    def __init__(self, task_repo: TaskRepository, job_repo: JobRepository):
        self.tasks = task_repo
        self.jobs  = job_repo

    def list_namespaces(self) -> list[dict]:
        return self.tasks.list_namespaces()

    def reset_namespace(self, namespace: str) -> None:
        self.tasks.reset_namespace(namespace)
        self.jobs.reset_namespace(namespace)

    def delete_namespace(self, namespace: str) -> None:
        self.tasks.delete_namespace(namespace)
        self.jobs.delete_namespace(namespace)

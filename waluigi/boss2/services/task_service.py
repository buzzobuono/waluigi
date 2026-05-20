from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository


class TaskService:

    def __init__(self, repo: TaskRepository):
        self.repo = repo

    def list(self) -> list[dict]:
        return self.repo.list_all()

    def list_by_job(self, job_id: str) -> list[dict]:
        return self.repo.list_by_job(job_id)

    def list_namespaces(self) -> list[dict]:
        return self.repo.list_namespaces()

    def reset(self, task_id: str) -> None:
        self.repo.reset(task_id)

    def reset_namespace(self, namespace: str) -> None:
        self.repo.reset_namespace(namespace)

    def delete(self, task_id: str) -> None:
        self.repo.delete(task_id)

    def delete_namespace(self, namespace: str) -> None:
        self.repo.delete_namespace(namespace)

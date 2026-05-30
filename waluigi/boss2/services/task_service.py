from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository


class TaskService:

    def __init__(self, repo: TaskRepository):
        self.repo = repo

    def list_tasks(self, *, namespace: str, job_id: str | None = None) -> list[dict]:
        return self.repo.list_tasks(namespace=namespace, job_id=job_id)

    def reset(self, namespace: str, task_id: str) -> None:
        self.repo.reset(namespace, task_id)

    def delete(self, namespace: str, task_id: str) -> None:
        self.repo.delete(namespace, task_id)

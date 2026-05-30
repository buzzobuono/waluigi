from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository


class TaskService:

    def __init__(self, repo: TaskRepository):
        self.repo = repo

    def list_tasks(
        self,
        *,
        job_id: str | None = None,
        namespace: str | None = None,
    ) -> list[dict]:
        return self.repo.list_tasks(job_id=job_id, namespace=namespace)
        
    def reset(self, task_id: str) -> None:
        self.repo.reset(task_id)
        
    def delete(self, task_id: str) -> None:
        self.repo.delete(task_id)
        
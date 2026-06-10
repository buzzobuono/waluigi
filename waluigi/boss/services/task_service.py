from __future__ import annotations
from waluigi.boss.repositories.task_repo import TaskRepository
from waluigi.boss.repositories.task_deps_repo import TaskDepsRepository


class TaskService:

    def __init__(self, repo: TaskRepository, deps_repo: TaskDepsRepository):
        self.repo      = repo
        self.deps_repo = deps_repo

    def list_tasks(self, *, namespace: str, job_id: str | None = None) -> list[dict]:
        tasks = self.repo.list_tasks(namespace=namespace, job_id=job_id)

        task_ids = [t["id"] for t in tasks]
        deps = self.deps_repo.list_by_tasks(namespace, task_ids)
        deps_by_task: dict[str, list[str]] = {}
        for row in deps:
            deps_by_task.setdefault(row["task_id"], []).append(row["dep_id"])

        for t in tasks:
            t["requires"] = deps_by_task.get(t["id"], [])

        return tasks

    def reset(self, namespace: str, task_id: str) -> None:
        self.repo.reset(namespace, task_id)

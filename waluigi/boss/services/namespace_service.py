from __future__ import annotations
from waluigi.boss.repositories.namespace_repo import NamespaceRepository
from waluigi.boss.repositories.task_repo import TaskRepository
from waluigi.boss.repositories.task_deps_repo import TaskDepsRepository
from waluigi.boss.repositories.job_repo import JobRepository


class NamespaceService:

    def __init__(self, ns_repo: NamespaceRepository,
                 task_repo: TaskRepository, job_repo: JobRepository,
                 task_deps_repo: TaskDepsRepository | None = None,
                 log_repo=None):
        self.namespaces = ns_repo
        self.tasks      = task_repo
        self.jobs       = job_repo
        self.task_deps  = task_deps_repo
        self.logs       = log_repo

    def list_namespaces(self) -> list[dict]:
        rows = self.namespaces.list()
        # attach task count from tasks table
        counts = {r["namespace"]: r["task_count"]
                  for r in self.tasks.list_namespaces()}
        return [
            {
                "namespace":   r["name"],
                "description": r["description"],
                "task_count":  counts.get(r["name"], 0),
            }
            for r in rows
        ]

    def create_namespace(self, name: str, description: str = "") -> dict:
        self.namespaces.create(name, description)
        return {"namespace": name, "description": description}

    def exists(self, name: str) -> bool:
        return self.namespaces.exists(name)

    def reset_namespace(self, namespace: str) -> None:
        self.tasks.reset_namespace(namespace)
        self.jobs.reset_namespace(namespace)

    def delete_namespace(self, namespace: str) -> None:
        if self.task_deps:
            self.task_deps.delete_by_namespace(namespace)
        if self.logs:
            self.logs.delete_by_namespace(namespace)
        self.tasks.delete_namespace(namespace)
        self.jobs.delete_namespace(namespace)
        self.namespaces.delete(namespace)

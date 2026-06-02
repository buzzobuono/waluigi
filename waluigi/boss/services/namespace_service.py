from __future__ import annotations
from waluigi.boss.repositories.namespace_repo import NamespaceRepository
from waluigi.boss.repositories.task_repo import TaskRepository
from waluigi.boss.repositories.job_repo import JobRepository


class NamespaceService:

    def __init__(self, ns_repo: NamespaceRepository,
                 task_repo: TaskRepository, job_repo: JobRepository):
        self.namespaces = ns_repo
        self.tasks      = task_repo
        self.jobs       = job_repo

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
        self.tasks.delete_namespace(namespace)
        self.jobs.delete_namespace(namespace)
        self.namespaces.delete(namespace)

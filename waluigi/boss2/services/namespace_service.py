from __future__ import annotations
from waluigi.boss2.repositories.task_repo import TaskRepository


class NamespaceService:

    def __init__(self, repo: TaskRepository):
        self.repo = repo
        
    def list_namespaces(self) -> list[dict]:
        return self.repo.list_namespaces()
        
    def reset_namespace(self, namespace: str) -> None:
        self.repo.reset_namespace(namespace)
        
    def delete_namespace(self, namespace: str) -> None:
        self.repo.delete_namespace(namespace)

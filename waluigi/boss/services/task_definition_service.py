from __future__ import annotations

from waluigi.boss.repositories.task_definition_repo import TaskDefinitionRepository


class TaskDefinitionService:

    def __init__(self, repo: TaskDefinitionRepository):
        self._repo = repo

    def list(self, namespace: str) -> list[dict]:
        return self._repo.list(namespace)

    def get(self, namespace: str, id: str) -> dict | None:
        return self._repo.get(namespace, id)

    def upsert(self, namespace: str, id: str, kind: str, metadata: dict, spec: dict) -> None:
        self._repo.upsert(namespace, id, kind, metadata, spec)

    def delete(self, namespace: str, id: str) -> bool:
        return self._repo.delete(namespace, id)

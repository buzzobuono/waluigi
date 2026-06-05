from __future__ import annotations
from waluigi.boss.repositories.job_definition_repo import JobDefinitionRepository


class JobDefinitionService:

    def __init__(self, repo: JobDefinitionRepository):
        self._repo = repo

    def list(self, namespace: str) -> list[dict]:
        return self._repo.list(namespace)

    def get(self, namespace: str, id: str) -> dict | None:
        return self._repo.get(namespace, id)

    def upsert(self, namespace: str, id: str, metadata: dict, spec: dict) -> None:
        self._repo.upsert(namespace, id, metadata, spec)

    def delete(self, namespace: str, id: str) -> bool:
        return self._repo.delete(namespace, id)

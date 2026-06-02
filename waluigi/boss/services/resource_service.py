from __future__ import annotations
from waluigi.boss.repositories.resource_repo import ResourceRepository


class ResourceService:

    def __init__(self, repo: ResourceRepository):
        self.repo = repo

    def list(self, namespace: str) -> list[dict]:
        return self.repo.list(namespace)

    def apply(self, namespace: str, spec: dict) -> tuple[bool, str]:
        return self.repo.update_limits(namespace, spec)

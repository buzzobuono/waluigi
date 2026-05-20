from __future__ import annotations
from waluigi.boss2.repositories.resource_repo import ResourceRepository


class ResourceService:

    def __init__(self, repo: ResourceRepository):
        self.repo = repo

    def list(self) -> list[dict]:
        return self.repo.list()

    def apply(self, spec: dict) -> tuple[bool, str]:
        return self.repo.update_limits(spec)

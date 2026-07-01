from __future__ import annotations
from waluigi.boss.repositories.job_hook_repo import JobHookRepository


class JobHookService:

    def __init__(self, repo: JobHookRepository):
        self.repo = repo

    def list(self, namespace: str) -> list[dict]:
        return self.repo.list(namespace)

    def list_enabled_for_job(self, namespace: str, job_name: str) -> list[dict]:
        return self.repo.list_enabled_for_job(namespace, job_name)

    def get(self, namespace: str, id: str) -> dict | None:
        return self.repo.get(namespace, id)

    def upsert(self, namespace: str, id: str, spec: dict, enabled: bool = True) -> None:
        self.repo.upsert(namespace, id, spec, enabled)

    def delete(self, namespace: str, id: str) -> bool:
        return self.repo.delete(namespace, id)

    def set_enabled(self, namespace: str, id: str, enabled: bool) -> bool:
        return self.repo.set_enabled(namespace, id, enabled)

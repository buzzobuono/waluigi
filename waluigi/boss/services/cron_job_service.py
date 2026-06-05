from __future__ import annotations
from waluigi.boss.repositories.cron_job_repo import CronJobRepository


class CronJobService:

    def __init__(self, repo: CronJobRepository):
        self._repo = repo

    def list(self, namespace: str) -> list[dict]:
        return self._repo.list(namespace)

    def list_enabled(self) -> list[dict]:
        return self._repo.list_enabled()

    def get(self, namespace: str, id: str) -> dict | None:
        return self._repo.get(namespace, id)

    def upsert(self, namespace: str, id: str, spec: dict, enabled: bool = True) -> None:
        self._repo.upsert(namespace, id, spec, enabled)

    def delete(self, namespace: str, id: str) -> bool:
        return self._repo.delete(namespace, id)

    def set_enabled(self, namespace: str, id: str, enabled: bool) -> bool:
        return self._repo.set_enabled(namespace, id, enabled)

    def try_claim_fire(self, namespace: str, id: str,
                       expected_last_fire: str | None, new_last_fire: str) -> bool:
        return self._repo.try_claim_fire(namespace, id, expected_last_fire, new_last_fire)

    def set_last_fire(self, namespace: str, id: str, ts: str) -> None:
        self._repo.set_last_fire(namespace, id, ts)

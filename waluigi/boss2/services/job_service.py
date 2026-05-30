from __future__ import annotations
from waluigi.boss2.repositories.job_repo import JobRepository


class JobService:

    def __init__(self, repo: JobRepository):
        self.repo = repo

    def create(self, namespace: str, job_id: str, metadata: dict, spec: dict) -> None:
        self.repo.create(namespace, job_id, metadata, spec)

    def list_runnable_ids(self) -> list[tuple[str, str]]:
        return self.repo.list_runnable_ids()

    def claim(self, boss_id: str, namespace: str, job_id: str) -> dict | None:
        return self.repo.claim(boss_id, namespace, job_id)

    def update_status(self, namespace: str, job_id: str, status: str) -> None:
        self.repo.update_status(namespace, job_id, status)

    def get_status(self, namespace: str, job_id: str) -> str | None:
        return self.repo.get_status(namespace, job_id)

    def release(self, namespace: str, job_id: str) -> None:
        self.repo.release(namespace, job_id)

    def list(self, namespace: str | None = None) -> list[dict]:
        return self.repo.list(namespace)

    def cancel(self, namespace: str, job_id: str) -> bool:
        return self.repo.cancel(namespace, job_id)

    def reset(self, namespace: str, job_id: str) -> bool:
        return self.repo.reset(namespace, job_id)

    def pause(self, namespace: str, job_id: str) -> bool:
        return self.repo.pause(namespace, job_id)

    def resume(self, namespace: str, job_id: str) -> bool:
        return self.repo.resume(namespace, job_id)

    def delete(self, namespace: str, job_id: str) -> bool:
        return self.repo.delete(namespace, job_id)

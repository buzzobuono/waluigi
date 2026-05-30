from __future__ import annotations
from waluigi.boss2.repositories.job_repo import JobRepository


class JobService:

    def __init__(self, repo: JobRepository):
        self.repo = repo

    def create(self, job_id: str, metadata: dict, spec: dict) -> None:
        self.repo.create(job_id, metadata, spec)

    def list_runnable_ids(self) -> list[str]:
        return self.repo.list_runnable_ids()

    def claim(self, boss_id: str, job_id: str) -> dict | None:
        return self.repo.claim(boss_id, job_id)

    def update_status(self, job_id: str, status: str) -> None:
        self.repo.update_status(job_id, status)

    def get_status(self, job_id: str) -> str | None:
        return self.repo.get_status(job_id)

    def release(self, job_id: str) -> None:
        self.repo.release(job_id)

    def list(self, status: str | None = None) -> list[dict]:
        return self.repo.list(status)

    def reset(self, job_id: str) -> bool:
        return self.repo.reset(job_id)

    def pause(self, job_id: str) -> bool:
        return self.repo.pause(job_id)

    def resume(self, job_id: str) -> bool:
        return self.repo.resume(job_id)

    def cancel(self, job_id: str) -> bool:
        return self.repo.cancel(job_id)

    def delete(self, job_id: str) -> bool:
        return self.repo.delete(job_id)

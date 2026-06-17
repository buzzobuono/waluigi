from __future__ import annotations
from waluigi.boss.repositories.worker_repo import WorkerRepository


class WorkerService:

    def __init__(self, repo: WorkerRepository):
        self.repo = repo

    def register(self, url: str, max_slots: int, free_slots: int) -> None:
        self.repo.register(url, max_slots, free_slots)

    def list(self) -> list[dict]:
        return self.repo.list()

    def remove(self, url: str) -> None:
        self.repo.delete(url)

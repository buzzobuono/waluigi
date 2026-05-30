from __future__ import annotations
from waluigi.boss2.repositories.log_repo import LogRepository


class LogService:

    def __init__(self, repo: LogRepository):
        self.repo = repo

    def append(self, namespace: str, task_id: str, lines: list[str], worker_id: str) -> None:
        self.repo.insert_many(namespace, task_id, lines, worker_id)

    def get(self, namespace: str, task_id: str, limit: int = 20) -> list[dict]:
        return self.repo.get(namespace, task_id, limit)

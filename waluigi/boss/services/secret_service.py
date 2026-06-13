from __future__ import annotations
from waluigi.boss.repositories.secret_repo import SecretRepository


class SecretService:

    def __init__(self, repo: SecretRepository):
        self.repo = repo

    def list_names(self, namespace: str) -> list[str]:
        return self.repo.list_names(namespace)

    def upsert(self, namespace: str, name: str, data: dict) -> None:
        self.repo.upsert(namespace, name, data)

    def delete(self, namespace: str, name: str) -> bool:
        return self.repo.delete(namespace, name)

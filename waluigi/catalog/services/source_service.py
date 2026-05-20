import logging

from waluigi.catalog.repositories.source_repo import SourceRepository

logger = logging.getLogger("waluigi")


class SourceService:

    def __init__(self, repo: SourceRepository):
        self.repo = repo

    def list(self) -> list[dict]:
        return [s.to_dict() for s in self.repo.list()]

    def get(self, id: str) -> dict | None:
        source = self.repo.get(id)
        return source.to_dict() if source else None

    def upsert(self, id: str, source_type: str, config: dict,
               description: str | None) -> dict:
        existing = self.repo.get(id)
        if existing and existing.type != source_type:
            raise ValueError(
                f"Cannot change source type from '{existing.type}' "
                f"to '{source_type}' — create a new source instead"
            )
        self.repo.upsert(id, source_type, config, description)
        return self.repo.get(id).to_dict()

    def update(self, id: str, **kwargs) -> dict | None:
        if not self.repo.update(id, **kwargs):
            return None
        return self.repo.get(id).to_dict()

    def delete(self, id: str) -> bool:
        return self.repo.delete(id)

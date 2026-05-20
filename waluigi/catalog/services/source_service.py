import logging

from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.api.schemas import SourceResponse

logger = logging.getLogger("waluigi")


class SourceService:

    def __init__(self, repo: SourceRepository):
        self.repo = repo

    def list(self) -> list[SourceResponse]:
        return [SourceResponse.from_entity(s) for s in self.repo.list()]

    def get(self, id: str) -> SourceResponse | None:
        source = self.repo.get(id)
        return SourceResponse.from_entity(source) if source else None

    def upsert(self, id: str, source_type: str, config: dict,
               description: str | None) -> SourceResponse:
        existing = self.repo.get(id)
        if existing and existing.type != source_type:
            raise ValueError(
                f"Cannot change source type from '{existing.type}' "
                f"to '{source_type}' — create a new source instead"
            )
        self.repo.upsert(id, source_type, config, description)
        return SourceResponse.from_entity(self.repo.get(id))

    def update(self, id: str, **kwargs) -> SourceResponse | None:
        if not self.repo.update(id, **kwargs):
            return None
        return SourceResponse.from_entity(self.repo.get(id))

    def delete(self, id: str) -> bool:
        return self.repo.delete(id)

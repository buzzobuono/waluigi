import logging

from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.api.schemas import SourceResponse

logger = logging.getLogger("waluigi")


class SourceService:

    def __init__(self, source_repository: SourceRepository):
        self.source_repository = source_repository

    def list(self) -> list[SourceResponse]:
        return [SourceResponse.from_entity(s) for s in self.source_repository.list()]

    def get(self, id: str) -> SourceResponse | None:
        source = self.source_repository.get(id)
        return SourceResponse.from_entity(source) if source else None

    def upsert(self, id: str, source_type: str, config: dict,
               description: str | None) -> SourceResponse:
        existing = self.source_repository.get(id)
        if existing and existing.type != source_type:
            raise ValueError(
                f"Cannot change source type from '{existing.type}' "
                f"to '{source_type}' — create a new source instead"
            )
        self.source_repository.upsert(id, source_type, config, description)
        return SourceResponse.from_entity(self.source_repository.get(id))

    def update(self, id: str, **kwargs) -> SourceResponse | None:
        if not self.source_repository.update(id, **kwargs):
            return None
        return SourceResponse.from_entity(self.source_repository.get(id))

    def delete(self, id: str) -> bool:
        return self.source_repository.delete(id)

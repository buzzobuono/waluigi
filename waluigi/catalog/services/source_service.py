import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class SourceService:

    def __init__(self, db: CatalogDB):
        self.db = db

    def list(self) -> list[dict]:
        
        return self.db.list_sources()

    def get(self, id: str) -> dict | None:
        return self.db.get_source(id)

    def upsert(self, id: str, source_type: str, config: dict,
               description: str | None) -> dict:
        """Create or update a source. Raises ValueError if type would change."""
        existing = self.db.get_source(id)
        if existing and existing.type != source_type:
            raise ValueError(
                f"Cannot change source type from '{existing.type}' "
                f"to '{source_type}' — create a new source instead"
            )
        self.db.upsert_source(id, source_type, config, description)
        return self.db.get_source(id)

    def update(self, id: str, **kwargs) -> dict | None:
        """Update source fields. Returns updated source or None if not found."""
        if not self.db.update_source(id, **kwargs):
            return None
        return self.db.get_source(id)

    def delete(self, id: str) -> bool:
        """Delete a source. Raises ValueError if referenced by datasets."""
        return self.db.delete_source(id)

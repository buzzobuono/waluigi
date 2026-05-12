import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class LineageService:

    def __init__(self, db: CatalogDB):
        self.db = db

    def get_lineage(self, dataset_id: str, version: str) -> dict:
        """Returns lineage dict. Raises ValueError if version not found."""
        record = (self.db.get_version(dataset_id, version) if version
                  else self.db.get_latest_version(dataset_id))
        if not record:
            raise ValueError("Dataset version not found")
        ver = record.version
        return {
            "dataset_id": dataset_id,
            "version":    ver,
            "upstream":   self.db.get_upstream(dataset_id, ver),
            "downstream": self.db.get_downstream(dataset_id, ver),
        }

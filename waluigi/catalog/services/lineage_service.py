import logging

from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.lineage_repo import LineageRepository

logger = logging.getLogger("waluigi")


class LineageService:

    def __init__(self, versions: VersionRepository, lineage: LineageRepository):
        self.versions = versions
        self.lineage  = lineage

    def get_lineage(self, dataset_id: str, version: str) -> dict:
        record = (self.versions.get(dataset_id, version) if version
                  else self.versions.get_latest(dataset_id))
        if not record:
            raise ValueError("Dataset version not found")
        ver = record.version
        return {
            "dataset_id": dataset_id,
            "version":    ver,
            "upstream":   self.lineage.get_upstream(dataset_id, ver),
            "downstream": self.lineage.get_downstream(dataset_id, ver),
        }

import logging

from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.lineage_repo import LineageRepository

logger = logging.getLogger("waluigi")


class LineageService:

    def __init__(self, versions_repository: VersionRepository, lineage_repository: LineageRepository):
        self.versions_repository = versions_repository
        self.lineage_repository  = lineage_repository

    def get_lineage(self, dataset_id: str, version: str) -> dict:
        record = (self.versions_repository.get(dataset_id, version) if version
                  else self.versions_repository.get_latest(dataset_id))
        if not record:
            raise ValueError("Dataset version not found")
        ver = record.version
        return {
            "dataset_id": dataset_id,
            "version":    ver,
            "upstream":   self.lineage_repository.get_upstream(dataset_id, ver),
            "downstream": self.lineage_repository.get_downstream(dataset_id, ver),
        }

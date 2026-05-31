import logging

from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.lineage_repo import LineageRepository

logger = logging.getLogger("waluigi")


class LineageService:

    def __init__(self, versions_repository: VersionRepository,
                 lineage_repository: LineageRepository):
        self.versions_repository = versions_repository
        self.lineage_repository  = lineage_repository

    def get_lineage(self, namespace: str, dataset_id: str, version: str) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        record = (self.versions_repository.get(browse_path, version) if version
                  else self.versions_repository.get_latest(browse_path))
        if not record:
            raise ValueError("Dataset version not found")
        ver = record.version
        return {
            "dataset_id": browse_path,
            "version":    ver,
            "upstream":   self.lineage_repository.get_upstream(browse_path, ver),
            "downstream": self.lineage_repository.get_downstream(browse_path, ver),
        }

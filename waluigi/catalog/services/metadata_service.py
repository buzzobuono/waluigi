import logging

from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.metadata_repo import MetadataRepository

logger = logging.getLogger("waluigi")


class MetadataService:

    def __init__(self, versions_repository: VersionRepository,
                 metadata_repository: MetadataRepository):
        self.versions_repository = versions_repository
        self.metadata_repository = metadata_repository

    def get_version_metadata(self, namespace: str, dataset_id: str,
                             version: str) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        if not self.versions_repository.get(browse_path, version):
            raise ValueError("Version not found")
        return self.metadata_repository.get(browse_path, version)

    def set_version_metadata(self, namespace: str, dataset_id: str,
                             version: str, key: str, value: str) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        if not self.versions_repository.get(browse_path, version):
            raise ValueError("Version not found")
        if key.startswith("sys."):
            raise ValueError("sys.* keys are reserved for the server")
        self.metadata_repository.set(browse_path, version, key, value)
        return {"key": key, "value": value}

    def delete_version_metadata(self, namespace: str, dataset_id: str,
                                version: str, key: str) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        if not self.versions_repository.get(browse_path, version):
            raise ValueError("Version not found")
        if not self.metadata_repository.delete(browse_path, version, key):
            raise ValueError("Key not found or protected (sys.*)")
        return {"key": key, "deleted": True}

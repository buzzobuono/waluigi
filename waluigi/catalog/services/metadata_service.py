import logging

from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.metadata_repo import MetadataRepository

logger = logging.getLogger("waluigi")


class MetadataService:

    def __init__(self, versions: VersionRepository, metadata: MetadataRepository):
        self.versions = versions
        self.metadata = metadata

    def get_version_metadata(self, dataset_id: str, version: str) -> dict:
        if not self.versions.get(dataset_id, version):
            raise ValueError("Version not found")
        return self.metadata.get(dataset_id, version)

    def set_version_metadata(self, dataset_id: str, version: str,
                             key: str, value: str) -> dict:
        if not self.versions.get(dataset_id, version):
            raise ValueError("Version not found")
        if key.startswith("sys."):
            raise ValueError("sys.* keys are reserved for the server")
        self.metadata.set(dataset_id, version, key, value)
        return {"key": key, "value": value}

    def delete_version_metadata(self, dataset_id: str, version: str,
                                key: str) -> dict:
        if not self.versions.get(dataset_id, version):
            raise ValueError("Version not found")
        if not self.metadata.delete(dataset_id, version, key):
            raise ValueError("Key not found or protected (sys.*)")
        return {"key": key, "deleted": True}

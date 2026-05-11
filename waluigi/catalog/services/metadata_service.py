import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class MetadataService:

    def __init__(self, db: CatalogDB):
        self.db = db

    # ── Version metadata ──────────────────────────────────────────────────────

    def get_version_metadata(self, dataset_id: str, version: str) -> dict:
        """Raises ValueError if version not found."""
        if not self.db.get_version(dataset_id, version):
            raise ValueError("Version not found")
        return self.db.get_metadata(dataset_id, version)

    def set_version_metadata(self, dataset_id: str, version: str,
                             key: str, value: str) -> dict:
        """Raises ValueError if version not found or key is sys.*."""
        if not self.db.get_version(dataset_id, version):
            raise ValueError("Version not found")
        if key.startswith("sys."):
            raise ValueError("sys.* keys are reserved for the server")
        self.db.set_metadata(dataset_id, version, key, value)
        return {"key": key, "value": value}

    def delete_version_metadata(self, dataset_id: str, version: str,
                                key: str) -> dict:
        """Raises ValueError if version not found or key not found/protected."""
        if not self.db.get_version(dataset_id, version):
            raise ValueError("Version not found")
        if not self.db.delete_metadata(dataset_id, version, key):
            raise ValueError("Key not found or protected (sys.*)")
        return {"key": key, "deleted": True}



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

    # ── Expectations ──────────────────────────────────────────────────────────

    def list_expectations(self, dataset_id: str) -> list:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.list_expectations(dataset_id)

    def add_expectation(self, dataset_id: str, rule_id: str, inputs: dict,
                        params: dict, tolerance: float, position: int) -> dict:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.add_expectation(
            dataset_id, rule_id, inputs, params, tolerance, position)

    def update_expectation(self, dataset_id: str, exp_id: int,
                           **updates) -> dict:
        """Raises ValueError if dataset or expectation not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.update_expectation(dataset_id, exp_id, **updates):
            raise ValueError("Expectation not found")
        return self.db.get_expectation(dataset_id, exp_id)

    def delete_expectation(self, dataset_id: str, exp_id: int) -> dict:
        """Raises ValueError if dataset or expectation not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.delete_expectation(dataset_id, exp_id):
            raise ValueError("Expectation not found")
        return {"deleted": exp_id}


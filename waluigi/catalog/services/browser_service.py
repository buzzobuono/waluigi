import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class CatalogBrowserService:

    def __init__(self, db: CatalogDB):
        self.db = db

    # ── Folders ───────────────────────────────────────────────────────────────

    def list_folders(self, prefix: str) -> list:
        return self.db.list_folders(prefix)

    # ── Lineage ───────────────────────────────────────────────────────────────

    def get_lineage(self, dataset_id: str, version: str) -> dict:
        """Returns lineage dict. Raises ValueError if version not found."""
        record = (self.db.get_version(dataset_id, version) if version
                  else self.db.get_latest_version(dataset_id))
        if not record:
            raise ValueError("Dataset version not found")
        ver = record["version"]
        return {
            "dataset_id": dataset_id,
            "version":    ver,
            "upstream":   self.db.get_upstream(dataset_id, ver),
            "downstream": self.db.get_downstream(dataset_id, ver),
        }

    # ── DQ Results ────────────────────────────────────────────────────────────

    def list_dq_results(self, dataset_id: str) -> list:
        """Raises ValueError if dataset not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.list_dq_results(dataset_id)

    def get_dq_result(self, dataset_id: str, version: str) -> dict:
        """Raises ValueError if dataset or result not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        row = self.db.get_dq_result(dataset_id, version)
        if not row:
            raise ValueError("No DQ result for this version")
        return row

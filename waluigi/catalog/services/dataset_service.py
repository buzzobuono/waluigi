import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class DatasetService:

    def __init__(self, db: CatalogDB):
        self.db = db

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def find(self, status=None, description=None) -> list:
        if not status and not description:
            return self.db.list_datasets()
        return self.db.find_datasets(status=status, description=description)

    def get(self, id: str) -> tuple[dict, list]:
        """Returns (dataset, warnings). Raises ValueError if not found."""
        dataset = self.db.get_dataset(id)
        if not dataset:
            raise ValueError("Dataset not found")
        msgs = []
        if dataset.get("status") != "approved":
            msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
        return dataset, msgs

    def create(self, id: str, fmt: str, description=None,
               source_id=None, dq_suite=None) -> dict:
        """Create or idempotently upsert a dataset. Raises ValueError on conflicts."""
        source_id = source_id or None  # normalise "" → None (avoids FK violation)
        if source_id and not self.db.exists_source(source_id):
            raise ValueError("Source not found")
        if id.startswith("/"):
            raise ValueError("Dataset 'id' not valid")
        existing = self.db.get_dataset(id)
        if existing and existing["format"] != fmt:
            raise ValueError(
                f"Cannot change format from '{existing['format']}' "
                f"to '{fmt}' — create a new dataset instead"
            )
        self.db.create_dataset(id, fmt, description, source_id, dq_suite)
        return self.db.get_dataset(id)

    def update(self, id: str, **kwargs) -> dict | None:
        """Returns updated dataset or None if not found."""
        if not self.db.update_dataset(id, **kwargs):
            return None
        return self.db.get_dataset(id)

    def delete(self, id: str) -> bool:
        return self.db.delete_dataset(id)

    # ── Status lifecycle ──────────────────────────────────────────────────────

    def approve(self, dataset_id: str, approved_by: str,
                notes: str | None = None) -> tuple[dict, list]:
        """Approve dataset and publish schema. Returns (data, warnings).

        Raises ValueError for not-found / invalid state.
        Raises RuntimeError on unexpected DB failure.
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        if dataset.get("status") == "deprecated":
            raise ValueError("Cannot approve a deprecated dataset")

        schema_result = self.db.publish_schema(dataset_id, publisher=approved_by)
        if not self.db.approve_dataset(dataset_id, approved_by):
            raise RuntimeError("Approval failed")

        msgs = schema_result["breaking_changes"] + schema_result["warnings"]
        data = {
            "dataset_id":          dataset_id,
            "status":              "approved",
            "approved_by":         approved_by,
            "notes":               notes,
            "schema_published_at": schema_result["published_at"],
            "breaking_changes":    schema_result["breaking_changes"],
            "warnings":            schema_result["warnings"],
        }
        logger.info(f"Approved {dataset_id} by {approved_by}")
        return data, msgs

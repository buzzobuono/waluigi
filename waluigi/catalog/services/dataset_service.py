import logging

from waluigi.catalog.db.base import atomic
from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository

logger = logging.getLogger("waluigi")


class DatasetService:

    def __init__(self, datasets: DatasetRepository, sources: SourceRepository,
                 schema: SchemaRepository):
        self.datasets = datasets
        self.sources  = sources
        self.schema   = schema

    def find(self, status=None, description=None) -> list:
        if not status and not description:
            return self.datasets.list()
        return self.datasets.find(status=status, description=description)

    def get(self, id: str) -> tuple[dict, list]:
        dataset = self.datasets.get(id)
        if not dataset:
            raise ValueError("Dataset not found")
        msgs = []
        if dataset.status != "approved":
            msgs.append(f"Dataset status is '{dataset.status}' — not yet approved")
        return dataset, msgs

    def create(self, id: str, fmt: str, description=None,
               source_id=None, dq_suite=None) -> dict:
        source_id = source_id or None
        if source_id and not self.sources.exists(source_id):
            raise ValueError("Source not found")
        if id.startswith("/"):
            raise ValueError("Dataset 'id' not valid")
        existing = self.datasets.get(id)
        if existing and existing.format != fmt:
            raise ValueError(
                f"Cannot change format from '{existing.format}' "
                f"to '{fmt}' — create a new dataset instead"
            )
        self.datasets.create(id, fmt, description, source_id, dq_suite)
        return self.datasets.get(id)

    def update(self, id: str, **kwargs) -> dict | None:
        if not self.datasets.update(id, **kwargs):
            return None
        return self.datasets.get(id)

    def delete(self, id: str) -> bool:
        return self.datasets.delete(id)

    @atomic
    def approve(self, dataset_id: str, approved_by: str,
                notes: str | None = None) -> tuple[dict, list]:
        dataset = self.datasets.get(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        if dataset.status == "deprecated":
            raise ValueError("Cannot approve a deprecated dataset")

        schema_result = self.schema.publish(dataset_id, publisher=approved_by)
        if not self.datasets.approve(dataset_id, approved_by):
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

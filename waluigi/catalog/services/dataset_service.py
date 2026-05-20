import logging

from waluigi.catalog.db.base import atomic
from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository
from waluigi.catalog.api.schemas import DatasetResponse

logger = logging.getLogger("waluigi")


class DatasetService:

    def __init__(self, datasets_repository: DatasetRepository, sources_repository: SourceRepository,
                 schema_repository: SchemaRepository):
        self.datasets_repository = datasets_repository
        self.sources_repository  = sources_repository
        self.schema_repository   = schema_repository

    def find(self, status=None, description=None) -> list[DatasetResponse]:
        datasets = self.datasets_repository.list() if not status and not description \
                   else self.datasets_repository.find(status=status, description=description)
        return [DatasetResponse.from_entity(d) for d in datasets]

    def get(self, id: str) -> tuple[DatasetResponse, list]:
        dataset = self.datasets_repository.get(id)
        if not dataset:
            raise ValueError("Dataset not found")
        msgs = []
        if dataset.status != "approved":
            msgs.append(f"Dataset status is '{dataset.status}' — not yet approved")
        return DatasetResponse.from_entity(dataset), msgs

    def create(self, id: str, fmt: str, description=None,
               source_id=None, dq_suite=None) -> DatasetResponse:
        source_id = source_id or None
        if source_id and not self.sources_repository.exists(source_id):
            raise ValueError("Source not found")
        if id.startswith("/"):
            raise ValueError("Dataset 'id' not valid")
        existing = self.datasets_repository.get(id)
        if existing and existing.format != fmt:
            raise ValueError(
                f"Cannot change format from '{existing.format}' "
                f"to '{fmt}' — create a new dataset instead"
            )
        self.datasets_repository.create(id, fmt, description, source_id, dq_suite)
        return DatasetResponse.from_entity(self.datasets_repository.get(id))

    def update(self, id: str, **kwargs) -> DatasetResponse | None:
        if not self.datasets_repository.update(id, **kwargs):
            return None
        return DatasetResponse.from_entity(self.datasets_repository.get(id))

    def delete(self, id: str) -> bool:
        return self.datasets_repository.delete(id)

    @atomic
    def approve(self, dataset_id: str, approved_by: str,
                notes: str | None = None) -> tuple[dict, list]:
        dataset = self.datasets_repository.get(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        if dataset.status == "deprecated":
            raise ValueError("Cannot approve a deprecated dataset")

        schema_result = self.schema_repository.publish(dataset_id, publisher=approved_by)
        if not self.datasets_repository.approve(dataset_id, approved_by):
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

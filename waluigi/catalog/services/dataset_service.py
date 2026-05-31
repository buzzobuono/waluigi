import logging

from waluigi.catalog.db.base import atomic
from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.source_repo import SourceRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository
from waluigi.catalog.api.schemas import DatasetResponse

logger = logging.getLogger("waluigi")


class DatasetService:

    def __init__(self, datasets_repository: DatasetRepository,
                 sources_repository: SourceRepository,
                 schema_repository: SchemaRepository):
        self.datasets_repository = datasets_repository
        self.sources_repository  = sources_repository
        self.schema_repository   = schema_repository

    def find(self, namespace: str, status=None,
             description=None) -> list[DatasetResponse]:
        datasets = (
            self.datasets_repository.list(namespace)
            if not status and not description
            else self.datasets_repository.find(namespace, status=status,
                                               description=description)
        )
        return [DatasetResponse.from_entity(d) for d in datasets]

    def get(self, namespace: str, id: str) -> tuple[DatasetResponse, list]:
        dataset = self.datasets_repository.get(namespace, id)
        if not dataset:
            raise ValueError("Dataset not found")
        msgs = []
        if dataset.status != "approved":
            msgs.append(f"Dataset status is '{dataset.status}' — not yet approved")
        return DatasetResponse.from_entity(dataset), msgs

    def create(self, namespace: str, id: str, fmt: str,
               description=None, source_id: str = None,
               dq_suite=None) -> DatasetResponse:
        if not source_id:
            raise ValueError("source_id is required")
        if not self.sources_repository.exists(namespace, source_id):
            raise ValueError("Source not found")
        if id.startswith("/"):
            raise ValueError("Dataset 'id' not valid")
        existing = self.datasets_repository.get(namespace, id)
        if existing and existing.format != fmt:
            raise ValueError(
                f"Cannot change format from '{existing.format}' "
                f"to '{fmt}' — create a new dataset instead"
            )
        self.datasets_repository.create(namespace, id, fmt, description,
                                        source_id, dq_suite)
        return DatasetResponse.from_entity(
            self.datasets_repository.get(namespace, id))

    def update(self, namespace: str, id: str, **kwargs) -> DatasetResponse | None:
        if not self.datasets_repository.update(namespace, id, **kwargs):
            return None
        return DatasetResponse.from_entity(
            self.datasets_repository.get(namespace, id))

    def delete(self, namespace: str, id: str) -> bool:
        return self.datasets_repository.delete(namespace, id)

    @atomic
    def approve(self, namespace: str, dataset_id: str, approved_by: str,
                notes: str | None = None) -> tuple[dict, list]:
        dataset = self.datasets_repository.get(namespace, dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        if dataset.status == "deprecated":
            raise ValueError("Cannot approve a deprecated dataset")

        browse_path = f"{namespace}/{dataset_id}"
        schema_result = self.schema_repository.publish(browse_path,
                                                       publisher=approved_by)
        if not self.datasets_repository.approve(namespace, dataset_id, approved_by):
            raise RuntimeError("Approval failed")

        msgs = schema_result["breaking_changes"] + schema_result["warnings"]
        data = {
            "dataset_id":          browse_path,
            "status":              "approved",
            "approved_by":         approved_by,
            "notes":               notes,
            "schema_published_at": schema_result["published_at"],
            "breaking_changes":    schema_result["breaking_changes"],
            "warnings":            schema_result["warnings"],
        }
        logger.info(f"Approved {browse_path} by {approved_by}")
        return data, msgs

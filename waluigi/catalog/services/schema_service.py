import logging

from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository

logger = logging.getLogger("waluigi")


class SchemaService:

    def __init__(self, datasets_repository: DatasetRepository, schema_repository: SchemaRepository):
        self.datasets_repository = datasets_repository
        self.schema_repository   = schema_repository

    def get_schema(self, dataset_id: str) -> tuple[dict, list]:
        if not self.datasets_repository.exists(dataset_id):
            raise ValueError("Dataset not found")
        columns   = self.schema_repository.get(dataset_id)
        pii_count = sum(1 for c in columns if c.get("pii"))
        inferred  = [c["column_name"] for c in columns
                     if c.get("status") == "inferred"]
        msgs = []
        if pii_count:
            msgs.append(f"{pii_count} column(s) flagged as PII")
        if inferred:
            msgs.append(
                f"{len(inferred)} column(s) still 'inferred' — "
                "review before publishing")
        data = {
            "dataset_id": dataset_id,
            "columns":    columns,
            "summary": {
                "total":     len(columns),
                "pii":       pii_count,
                "inferred":  len(inferred),
                "draft":     sum(1 for c in columns if c.get("status") == "draft"),
                "published": sum(1 for c in columns if c.get("status") == "published"),
            },
        }
        return data, msgs

    def patch_column(self, dataset_id: str, column_name: str,
                     **updates) -> tuple[dict, list]:
        if not self.datasets_repository.exists(dataset_id):
            raise ValueError("Dataset not found")
        col = self.schema_repository.upsert_column(dataset_id, column_name, **updates)
        self.datasets_repository.set_in_review(dataset_id)
        msgs = []
        if col and col.get("pii") and col.get("pii_type") == "none":
            msgs.append("PII flag set but pii_type is 'none' — "
                        "set it to: direct | indirect | sensitive")
        return col, msgs

    def approve_column(self, dataset_id: str, column_name: str) -> dict:
        if not self.datasets_repository.exists(dataset_id):
            raise ValueError("Dataset not found")
        if not self.schema_repository.approve_column(dataset_id, column_name):
            raise ValueError("Column not found in schema")
        return next((c for c in self.schema_repository.get(dataset_id)
                     if c["column_name"] == column_name), None)

    def delete_column(self, dataset_id: str, column_name: str) -> dict:
        if not self.datasets_repository.exists(dataset_id):
            raise ValueError("Dataset not found")
        if not self.schema_repository.delete_column(dataset_id, column_name):
            raise ValueError("Column not found in schema")
        return {"column_name": column_name, "deleted": True}

    def publish_schema(self, dataset_id: str, published_by: str) -> dict:
        if not self.datasets_repository.exists(dataset_id):
            raise ValueError("Dataset not found")
        self.schema_repository.publish(dataset_id, published_by)
        return {"dataset_id": dataset_id}

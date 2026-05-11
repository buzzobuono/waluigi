import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class SchemaService:

    def __init__(self, db: CatalogDB):
        self.db = db

    def get_schema(self, dataset_id: str) -> tuple[dict, list]:
        """Returns (data, warnings). Raises ValueError if dataset not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        columns   = self.db.get_schema(dataset_id)
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
        """Returns (col, warnings). Raises ValueError if dataset not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        col = self.db.upsert_schema_column(dataset_id, column_name, **updates)
        self.db.set_in_review(dataset_id)
        msgs = []
        if col and col.get("pii") and col.get("pii_type") == "none":
            msgs.append("PII flag set but pii_type is 'none' — "
                        "set it to: direct | indirect | sensitive")
        return col, msgs

    def approve_column(self, dataset_id: str, column_name: str) -> dict:
        """Raises ValueError if dataset not found or column not in schema."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.approve_schema_column(dataset_id, column_name):
            raise ValueError("Column not found in schema")
        return next((c for c in self.db.get_schema(dataset_id)
                     if c["column_name"] == column_name), None)

    def delete_column(self, dataset_id: str, column_name: str) -> dict:
        """Raises ValueError if dataset not found or column not in schema."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.delete_schema_column(dataset_id, column_name):
            raise ValueError("Column not found in schema")
        return {"column_name": column_name, "deleted": True}

    def publish_schema(self, dataset_id: str, published_by: str) -> dict:
        """Raises ValueError if dataset not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        self.db.publish_schema(dataset_id, published_by)
        return {"dataset_id": dataset_id}

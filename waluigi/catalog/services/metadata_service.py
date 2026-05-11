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

    # ── Schema ────────────────────────────────────────────────────────────────

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

    # ── Charts (CRUD) ─────────────────────────────────────────────────────────

    def list_charts(self, dataset_id: str) -> list:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.list_charts(dataset_id)

    def get_chart(self, dataset_id: str, chart_id: int) -> dict | None:
        return self.db.get_chart(dataset_id, chart_id)

    def get_chart_by_key(self, dataset_id: str, key: str) -> dict | None:
        return self.db.get_chart_by_key(dataset_id, key)

    def add_chart(self, dataset_id: str, key: str, title: str,
                  spec: dict, position: int) -> dict:
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.add_chart(dataset_id, key, title, spec, position)

    def update_chart(self, dataset_id: str, chart_id: int, **updates) -> dict:
        """Raises ValueError if dataset or chart not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.update_chart(dataset_id, chart_id, **updates):
            raise ValueError("Chart not found")
        return self.db.get_chart(dataset_id, chart_id)

    def delete_chart(self, dataset_id: str, chart_id: int) -> dict:
        """Raises ValueError if dataset or chart not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        if not self.db.delete_chart(dataset_id, chart_id):
            raise ValueError("Chart not found")
        return {"deleted": chart_id}

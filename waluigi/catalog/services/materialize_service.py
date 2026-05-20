import os
import csv
import logging
import httpx

from waluigi.catalog.db.base import atomic
from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.version_repo import VersionRepository
from waluigi.catalog.repositories.schema_repo import SchemaRepository
from waluigi.catalog.repositories.lineage_repo import LineageRepository
from waluigi.catalog.repositories.metadata_repo import MetadataRepository
from waluigi.catalog.utils import _version_id

logger = logging.getLogger("waluigi")


class MaterializeService:

    def __init__(self,
                 datasets:  DatasetRepository,
                 versions:  VersionRepository,
                 schema:    SchemaRepository,
                 lineage:   LineageRepository,
                 metadata:  MetadataRepository,
                 data_path: str):
        self.datasets  = datasets
        self.versions  = versions
        self.schema    = schema
        self.lineage   = lineage
        self.metadata  = metadata
        self.data_path = data_path

    @atomic
    async def materialize(self, dataset_id: str,
                          base_url: str, endpoint: str, params: dict,
                          display_name: str | None = None,
                          description: str | None = None,
                          task_id: str | None = None,
                          job_id: str | None = None) -> dict:
        version = _version_id()
        path    = self.local_path(dataset_id, version, "csv")

        self.datasets.create(dataset_id, "csv", description=description)
        self.versions.reserve(dataset_id, version, path)

        try:
            rows, schema_cols = await self.fetch_and_write(
                base_url, endpoint, params, path)
        except httpx.HTTPError:
            self.versions.fail(dataset_id, version)
            raise

        if rows == 0:
            self.versions.fail(dataset_id, version)
            raise ValueError("No records returned from endpoint")

        if not self.versions.commit(dataset_id, version):
            self.versions.fail(dataset_id, version)
            raise RuntimeError("Commit failed")

        self.schema.upsert_columns(dataset_id, schema_cols)
        self.lineage.insert(dataset_id, version, [{
            "dataset_id": f"__external__/{base_url}{endpoint}",
            "version":    "live",
        }])
        if task_id:
            self.metadata.set(dataset_id, version, "sys.produced_by_task", task_id)
        if job_id:
            self.metadata.set(dataset_id, version, "sys.produced_by_job", job_id)

        logger.info(f"Materialized {dataset_id}@{version} rows={rows}")
        return {
            "dataset_id": dataset_id,
            "version":    version,
            "path":       path,
            "rows":       rows,
            "source_url": f"{base_url}{endpoint}",
        }

    def local_path(self, dataset_id: str, version: str, fmt: str) -> str:
        safe_id  = dataset_id.replace("/", os.sep)
        dir_path = os.path.join(self.data_path, safe_id)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f"{version}.{fmt}")

    async def fetch_and_write(self, base_url: str, endpoint: str,
                              params: dict, output_path: str) -> tuple[int, list[dict]]:
        records, page = [], 1
        next_url = f"{base_url}{endpoint}"
        async with httpx.AsyncClient(timeout=30) as client:
            while next_url:
                call_params = {**params, "page": page} if page > 1 else params
                r = await client.get(next_url, params=call_params)
                r.raise_for_status()
                body  = r.json()
                items = self._extract_items(body)
                if not items:
                    break
                records.extend([self._flatten(item) for item in items])
                next_url = self._next_url(body, base_url, endpoint, page)
                if next_url:
                    page += 1
                else:
                    break

        if not records:
            return 0, []

        fieldnames = list(dict.fromkeys(k for row in records for k in row.keys()))
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)

        schema_cols = [
            {"name": k, "physical_type": "string", "logical_type": "string"}
            for k in fieldnames
        ]
        return len(records), schema_cols

    @staticmethod
    def _extract_items(body) -> list:
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("data", "results", "items", "records",
                        "content", "entries", "rows"):
                if key in body and isinstance(body[key], list):
                    return body[key]
            values = [v for v in body.values() if isinstance(v, list)]
            if len(values) == 1:
                return values[0]
        return []

    @staticmethod
    def _next_url(body, base_url: str, endpoint: str, page: int) -> str | None:
        if not isinstance(body, dict):
            return None
        for key in ("next", "next_page", "nextPage", "nextCursor"):
            val = body.get(key)
            if val and isinstance(val, str):
                return val if val.startswith("http") else f"{base_url}{val}"
        total = body.get("total_pages") or body.get("pages") or body.get("totalPages")
        if total and page < int(total):
            return f"{base_url}{endpoint}"
        return None

    @staticmethod
    def _flatten(obj, prefix="", sep="_") -> dict:
        out = {}
        for k, v in obj.items():
            key = f"{prefix}{sep}{k}" if prefix else k
            if isinstance(v, dict):
                out.update(MaterializeService._flatten(v, key, sep))
            elif isinstance(v, list):
                out[key] = (str(v) if (v and isinstance(v[0], dict))
                            else ",".join(str(i) for i in v))
            else:
                out[key] = v
        return out

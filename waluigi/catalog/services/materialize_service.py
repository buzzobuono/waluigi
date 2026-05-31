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
                 datasets_repository:  DatasetRepository,
                 versions_repository:  VersionRepository,
                 schema_repository:    SchemaRepository,
                 lineage_repository:   LineageRepository,
                 metadata_repository:  MetadataRepository,
                 data_path: str):
        self.datasets_repository  = datasets_repository
        self.versions_repository  = versions_repository
        self.schema_repository    = schema_repository
        self.lineage_repository   = lineage_repository
        self.metadata_repository  = metadata_repository
        self.data_path = data_path

    @atomic
    async def materialize(self, namespace: str, dataset_id: str,
                          source_id: str,
                          base_url: str, endpoint: str, params: dict,
                          display_name: str | None = None,
                          description: str | None = None,
                          task_id: str | None = None,
                          job_id: str | None = None) -> dict:
        browse_path = f"{namespace}/{dataset_id}"
        version = _version_id()
        path    = self.local_path(browse_path, version, "csv")

        self.datasets_repository.create(namespace, dataset_id, "csv",
                                        description=description,
                                        source_id=source_id)
        self.versions_repository.reserve(browse_path, version, path)

        try:
            rows, schema_cols = await self.fetch_and_write(
                base_url, endpoint, params, path)
        except httpx.HTTPError:
            self.versions_repository.fail(browse_path, version)
            raise

        if rows == 0:
            self.versions_repository.fail(browse_path, version)
            raise ValueError("No records returned from endpoint")

        if not self.versions_repository.commit(browse_path, version):
            self.versions_repository.fail(browse_path, version)
            raise RuntimeError("Commit failed")

        self.schema_repository.upsert_columns(browse_path, schema_cols)
        self.lineage_repository.insert(browse_path, version, [{
            "dataset_id": f"__external__/{base_url}{endpoint}",
            "version":    "live",
        }])
        if task_id:
            self.metadata_repository.set(browse_path, version,
                                         "sys.produced_by_task", task_id)
        if job_id:
            self.metadata_repository.set(browse_path, version,
                                         "sys.produced_by_job", job_id)

        logger.info(f"Materialized {browse_path}@{version} rows={rows}")
        return {
            "dataset_id": browse_path,
            "version":    version,
            "path":       path,
            "rows":       rows,
            "source_url": f"{base_url}{endpoint}",
        }

    def local_path(self, browse_path: str, version: str, fmt: str) -> str:
        safe_id  = browse_path.replace("/", os.sep)
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

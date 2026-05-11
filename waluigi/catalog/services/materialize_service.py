import os
import csv
import logging
import httpx

from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _version_id

logger = logging.getLogger("waluigi")


class MaterializeService:

    def __init__(self, db: CatalogDB, data_path: str):
        self.db        = db
        self.data_path = data_path

    # ── Materialize (REST API → local CSV) ────────────────────────────────────

    async def materialize(self, dataset_id: str,
                          base_url: str, endpoint: str, params: dict,
                          display_name: str | None = None,
                          description: str | None = None,
                          task_id: str | None = None,
                          job_id: str | None = None) -> dict:
        """Fetch a paginated REST API and persist as a local CSV version.

        Raises ValueError on empty response or commit failure.
        Re-raises httpx.HTTPError after failing the reserved version (for 502 mapping).
        """
        version = _version_id()
        path    = self.local_path(dataset_id, version, "csv")

        self.db.create_dataset(dataset_id,
                               display_name=display_name,
                               description=description)
        self.db.reserve_version(dataset_id, version, path)

        try:
            rows, schema_cols = await self.fetch_and_write(
                base_url, endpoint, params, path)
        except httpx.HTTPError:
            self.db.fail_version(dataset_id, version)
            raise

        if rows == 0:
            self.db.fail_version(dataset_id, version)
            raise ValueError("No records returned from endpoint")

        if not self.db.commit_version(dataset_id, version):
            self.db.fail_version(dataset_id, version)
            raise RuntimeError("Commit failed")

        self.db.upsert_schema_columns(dataset_id, schema_cols)
        self.db.insert_lineage(dataset_id, version, [{
            "dataset_id": f"__external__/{base_url}{endpoint}",
            "version":    "live",
        }])
        if task_id:
            self.db.set_metadata(dataset_id, version,
                                 "sys.produced_by_task", task_id)
        if job_id:
            self.db.set_metadata(dataset_id, version,
                                 "sys.produced_by_job", job_id)

        logger.info(f"Materialized {dataset_id}@{version} rows={rows}")
        return {
            "dataset_id": dataset_id,
            "version":    version,
            "path":       path,
            "rows":       rows,
            "source_url": f"{base_url}{endpoint}",
        }

    # ── Filesystem helper ─────────────────────────────────────────────────────

    def local_path(self, dataset_id: str, version: str, fmt: str) -> str:
        """Return an absolute path for a locally-stored dataset version file."""
        safe_id  = dataset_id.replace("/", os.sep)
        dir_path = os.path.join(self.data_path, safe_id)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f"{version}.{fmt}")

    # ── REST API fetch helper ─────────────────────────────────────────────────

    async def fetch_and_write(self, base_url: str, endpoint: str,
                              params: dict, output_path: str) -> tuple[int, list[dict]]:
        """Fetch a paginated REST API and write all records to a CSV file.

        Returns (row_count, schema_cols) where schema_cols is a list of
        {name, physical_type, logical_type} dicts inferred from the field names.
        """
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

    # ── Private parsing helpers ───────────────────────────────────────────────

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

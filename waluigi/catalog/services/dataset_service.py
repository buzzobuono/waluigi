import os
import csv
import logging
import httpx

from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _infer_schema

logger = logging.getLogger("waluigi")

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out",
}


class DatasetService:

    def __init__(self, db: CatalogDB, data_path: str):
        self.db        = db
        self.data_path = data_path

    # ── Filesystem helpers ────────────────────────────────────────────────────

    def local_path(self, dataset_id: str, version: str, fmt: str) -> str:
        """Return an absolute path for a locally-stored dataset version file."""
        safe_id  = dataset_id.replace("/", os.sep)
        dir_path = os.path.join(self.data_path, safe_id)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, f"{version}.{fmt}")

    # ── Scanner ───────────────────────────────────────────────────────────────

    def scan(self, data_path: str, prefix: str | None = None) -> int:
        """Walk data_path and register every scannable file as a dataset+version."""
        logger.info(f"🔍 Scanning {data_path} ...")
        count = 0
        for root, dirs, files in os.walk(data_path):
            dirs.sort()
            for filename in sorted(files):
                ext = os.path.splitext(filename)[1].lower()
                if ext not in SCANNABLE_EXTENSIONS:
                    continue

                filepath = os.path.join(root, filename)
                fmt      = ext.lstrip(".")
                rel_dir  = os.path.relpath(root, data_path).replace(os.sep, "/")
                name     = os.path.splitext(filename)[0]
                version  = name.replace("-", ":", 2)

                if prefix:
                    dataset_id = f"{prefix.strip('/')}/{rel_dir}/{name}".replace("//", "/")
                else:
                    dataset_id = f"{rel_dir}/{name}".replace("//", "/")

                try:
                    schema = _infer_schema(filepath, fmt)
                    self.db.create_dataset(dataset_id, fmt)
                    self.db.reserve_version(dataset_id, version, filepath,
                                            "scanner", "scan")
                    committed = self.db.commit_version(dataset_id, version)
                    if committed:
                        self.db.upsert_schema_columns(dataset_id, schema)
                    count += 1
                    logger.info(f"  ✅ {dataset_id}@{version[:19]} [{fmt}]")
                except Exception as e:
                    logger.error(f"  ⚠️  Skipped {filepath}: {e}")

        logger.info(f"🏁 Scan complete — {count} dataset(s) registered.")
        return count

    # ── REST API materializer ─────────────────────────────────────────────────

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
                out.update(DatasetService._flatten(v, key, sep))
            elif isinstance(v, list):
                out[key] = (str(v) if (v and isinstance(v[0], dict))
                            else ",".join(str(i) for i in v))
            else:
                out[key] = v
        return out

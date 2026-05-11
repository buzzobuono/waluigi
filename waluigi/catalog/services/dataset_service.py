import os
import csv
import logging
import httpx
import pandas as pd

from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _infer_schema, _version_id, _safe_json_value
from waluigi.sdk.connectors import ConnectorFactory

logger = logging.getLogger("waluigi")

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out",
}


class DatasetService:

    def __init__(self, db: CatalogDB, data_path: str, dq_service=None):
        self.db         = db
        self.data_path  = data_path
        self.dq_service = dq_service

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def find(self, status=None, description=None) -> list:
        if not status and not description:
            return self.db.list_datasets()
        return self.db.find_datasets(status=status, description=description)

    def get(self, id: str) -> tuple[dict, list]:
        """Returns (dataset, warnings). Raises ValueError if not found."""
        dataset = self.db.get_dataset(id)
        if not dataset:
            raise ValueError("Dataset not found")
        msgs = []
        if dataset.get("status") != "approved":
            msgs.append(f"Dataset status is '{dataset.get('status')}' — not yet approved")
        return dataset, msgs

    def create(self, id: str, fmt: str, description=None,
               source_id=None, dq_suite=None) -> dict:
        """Create or idempotently upsert a dataset. Raises ValueError on conflicts."""
        source_id = source_id or None  # normalise "" → None (avoids FK violation)
        if source_id and not self.db.exists_source(source_id):
            raise ValueError("Source not found")
        if id.startswith("/"):
            raise ValueError("Dataset 'id' not valid")
        existing = self.db.get_dataset(id)
        if existing and existing["format"] != fmt:
            raise ValueError(
                f"Cannot change format from '{existing['format']}' "
                f"to '{fmt}' — create a new dataset instead"
            )
        self.db.create_dataset(id, fmt, description, source_id, dq_suite)
        return self.db.get_dataset(id)

    def update(self, id: str, **kwargs) -> dict | None:
        """Returns updated dataset or None if not found."""
        if not self.db.update_dataset(id, **kwargs):
            return None
        return self.db.get_dataset(id)

    def delete(self, id: str) -> bool:
        return self.db.delete_dataset(id)

    # ── Status lifecycle ──────────────────────────────────────────────────────

    def approve(self, dataset_id: str, approved_by: str,
                notes: str | None = None) -> tuple[dict, list]:
        """Approve dataset and publish schema. Returns (data, warnings).

        Raises ValueError for not-found / invalid state.
        Raises RuntimeError on unexpected DB failure.
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        if dataset.get("status") == "deprecated":
            raise ValueError("Cannot approve a deprecated dataset")

        schema_result = self.db.publish_schema(dataset_id, publisher=approved_by)
        if not self.db.approve_dataset(dataset_id, approved_by):
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

    # ── Version lifecycle ─────────────────────────────────────────────────────

    def list_versions(self, dataset_id: str) -> list:
        """Raises ValueError if dataset not found."""
        if not self.db.exists_dataset(dataset_id):
            raise ValueError("Dataset not found")
        return self.db.list_versions(dataset_id)

    def deprecate(self, dataset_id: str, version: str) -> dict:
        """Raises ValueError if version not found."""
        if not self.db.deprecate(dataset_id, version):
            raise ValueError("Version not found")
        logger.info(f"Deprecated {dataset_id}@{version}")
        return {"dataset_id": dataset_id, "version": version, "status": "deprecated"}

    # ── Version produce (2-phase write) ───────────────────────────────────────

    def reserve(self, dataset_id: str, metadata: dict | None = None,
                force: bool = False) -> tuple[dict, bool]:
        """Reserve a new version slot. Returns (result, skipped).

        Raises ValueError if dataset / source not found or version already exists.
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        source = self.db.get_source(dataset["source_id"])
        if not source:
            raise ValueError(f"Source '{dataset['source_id']}' not found")

        if not force and metadata:
            existing = self.db.find_version_by_metadata(dataset_id, metadata)
            if existing:
                msg = (f"Skipped {dataset_id} new version creation because of "
                       f"identical metadata to {existing['version']} version")
                logger.info(msg)
                return {
                    "dataset_id": dataset_id,
                    "version":    existing["version"],
                    "source_id":  source["id"],
                    "location":   existing["location"],
                    "skipped":    True,
                    "_skip_msg":  msg,
                }, True

        connector = ConnectorFactory.get(source["type"], source["config"])
        version   = _version_id()
        location  = connector.resolve_location(
            dataset_id, version, dataset["format"], self.data_path)
        if not self.db.reserve_version(dataset_id, version, location):
            raise ValueError("Version already exists")
        logger.info(f"Reserved {dataset_id}@{version}")
        return {
            "dataset_id": dataset_id,
            "version":    version,
            "source_id":  source["id"],
            "location":   location,
            "skipped":    False,
        }, False

    def commit(self, dataset_id: str, version: str,
               metadata: dict | None = None,
               task_id: str | None = None,
               job_id: str | None = None,
               inputs: list | None = None) -> tuple[dict, list]:
        """Commit a reserved version. Returns (data, warnings).

        Raises ValueError for not-found / bad state / missing file.
        Raises RuntimeError on commit failure (after cleanup).
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")

        source    = self.db.get_source(dataset["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])

        record = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")
        if record["status"] != "reserved":
            raise ValueError(f"Cannot commit - status is '{record['status']}'")

        location = record["location"]
        if not connector.exists(location):
            raise ValueError(f"Dataset Version not found at: {location}")

        try:
            if not self.db.commit_version(dataset_id, version):
                raise RuntimeError("commit_version returned False")

            for k, v in (metadata or {}).items():
                self.db.set_metadata(dataset_id, version, k, v)
            if task_id:
                self.db.set_metadata(dataset_id, version,
                                     "sys.produced_by_task", task_id)
            if job_id:
                self.db.set_metadata(dataset_id, version,
                                     "sys.produced_by_job", job_id)

            inferred = connector.infer_schema(location)
            self.db.upsert_schema_columns(dataset_id, inferred)
            diff = self.db.diff_schema_against_inferred(dataset_id, inferred)

            if inputs:
                self.db.insert_lineage(dataset_id, version, inputs)

            dq_result    = None
            expectations = self.db.list_expectations(dataset_id)
            if expectations and self.dq_service:
                dq_result = self.dq_service.run_on_commit(
                    dataset_id, version, connector, location,
                    dataset["format"], expectations,
                )

            logger.info(f"Committed {dataset_id}@{version}")

            data     = {"dataset_id": dataset_id, "version": version,
                        "location": location, "dq": dq_result}
            warnings = diff["breaking"] + diff["warnings"]
            if diff["breaking"]:
                msg = f"Schema breaking changes detected on {dataset_id}@{version}"
                logger.warning(msg)
                warnings = [msg] + warnings
            return data, warnings

        except Exception as e:
            msg = f"Failed to commit {dataset_id}@{version}: {e}"
            logger.error(msg)
            try:
                self.db.delete_version(dataset_id, version)
                connector.delete(location)
                logger.info(f"Cleanup: deleted orphaned location {location}")
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup {location}: {cleanup_err}")
            raise RuntimeError(msg) from e

    def fail(self, dataset_id: str, version: str) -> dict:
        """Mark a reserved version as failed and clean up storage."""
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")
        source    = self.db.get_source(dataset["source_id"])
        connector = ConnectorFactory.get(source["type"], source["config"])
        record    = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")
        location = record["location"]
        self.db.fail_version(dataset_id, version)
        try:
            connector.delete(location)
            self.db.delete_version(dataset_id, version)
            logger.info(f"Cleanup: deleted orphaned location {location}")
        except Exception as cleanup_err:
            logger.warning(f"Failed to cleanup {location}: {cleanup_err}")
        return {"dataset_id": dataset_id, "version": version, "status": "failed"}

    # ── Preview ───────────────────────────────────────────────────────────────

    def preview(self, dataset_id: str, version: str,
                limit: int = 10, offset: int = 0) -> dict:
        """Return a preview of rows for a dataset version.

        Raises ValueError for not-found conditions.
        Raises NotImplementedError for unsupported formats.
        """
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            raise ValueError("Dataset not found")

        fmt       = (dataset.get("format") or "").lower()
        source_id = dataset.get("source_id")
        if not source_id:
            raise ValueError("Dataset has no source")

        source = self.db.get_source(source_id)
        if not source:
            raise ValueError(f"Source '{source_id}' not found")

        record = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")

        connector = ConnectorFactory.get(source["type"], source.get("config") or {})
        result    = connector.read(record["location"], fmt, limit=limit, offset=offset)

        if isinstance(result, pd.DataFrame):
            df = result
        elif isinstance(result, list):
            df = pd.DataFrame(result)
        else:
            raise NotImplementedError(f"Preview not supported for format '{fmt}'")

        clean = [{k: _safe_json_value(v) for k, v in row.items()}
                 for row in df.to_dict(orient="records")]
        return {
            "dataset_id": dataset_id,
            "version":    record["version"],
            "columns":    df.columns.tolist(),
            "rows":       clean,
            "pagination": {"limit": limit, "offset": offset, "count": len(clean)},
        }

    # ── Virtual datasets ──────────────────────────────────────────────────────

    def register_virtual(self, dataset_id: str, source_id: str,
                         location: str, fmt: str,
                         display_name: str | None = None,
                         description: str | None = None,
                         owner: str | None = None,
                         tags=None,
                         task_id: str | None = None,
                         job_id: str | None = None) -> dict:
        """Register a virtual dataset version (no physical file).

        Raises ValueError if source not found.
        """
        src = self.db.get_source(source_id)
        if not src:
            raise ValueError(
                f"Source '{source_id}' not found. "
                "Register it first via POST /sources."
            )
        version = _version_id()
        self.db.create_dataset(dataset_id, display_name=display_name,
                               description=description, owner=owner, tags=tags)
        self.db.commit_virtual(dataset_id, version, location)
        if task_id:
            self.db.set_metadata(dataset_id, version,
                                 "sys.produced_by_task", task_id)
        if job_id:
            self.db.set_metadata(dataset_id, version,
                                 "sys.produced_by_job", job_id)
        logger.info(f"Virtual {dataset_id}@{version} [{src['type']}]")
        return {
            "dataset_id":  dataset_id,
            "version":     version,
            "source_id":   source_id,
            "source_type": src["type"],
            "location":    location,
            "format":      fmt,
        }

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
                out.update(DatasetService._flatten(v, key, sep))
            elif isinstance(v, list):
                out[key] = (str(v) if (v and isinstance(v[0], dict))
                            else ",".join(str(i) for i in v))
            else:
                out[key] = v
        return out

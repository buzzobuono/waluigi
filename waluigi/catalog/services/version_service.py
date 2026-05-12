import os
import logging
import pandas as pd

from waluigi.catalog.db import CatalogDB
from waluigi.catalog.utils import _infer_schema, _version_id, _safe_json_value
from waluigi.sdk.connectors import ConnectorFactory

logger = logging.getLogger("waluigi")

SCANNABLE_EXTENSIONS = {
    ".parquet", ".csv", ".tsv", ".json", ".xls", ".xlsx",
    ".sas7bdat", ".pkl", ".pickle", ".feather", ".orc", ".out",
}


class VersionService:

    def __init__(self, db: CatalogDB, data_path: str, dq_service=None):
        self.db         = db
        self.data_path  = data_path
        self.dq_service = dq_service

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
        source = self.db.get_source(dataset.source_id)
        if not source:
            raise ValueError(f"Source '{dataset.source_id}' not found")

        if not force and metadata:
            existing = self.db.find_version_by_metadata(dataset_id, metadata)
            if existing:
                msg = (f"Skipped {dataset_id} new version creation because of "
                       f"identical metadata to {existing.version} version")
                logger.info(msg)
                return {
                    "dataset_id": dataset_id,
                    "version":    existing.version,
                    "source_id":  source.id,
                    "location":   existing.location,
                    "skipped":    True,
                    "_skip_msg":  msg,
                }, True

        connector = ConnectorFactory.get(source.type, source.config)
        version   = _version_id()
        location  = connector.resolve_location(
            dataset_id, version, dataset.format, self.data_path)
        if not self.db.reserve_version(dataset_id, version, location):
            raise ValueError("Version already exists")
        logger.info(f"Reserved {dataset_id}@{version}")
        return {
            "dataset_id": dataset_id,
            "version":    version,
            "source_id":  source.id,
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

        source    = self.db.get_source(dataset.source_id)
        connector = ConnectorFactory.get(source.type, source.config)

        record = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")
        if record.status != "reserved":
            raise ValueError(f"Cannot commit - status is '{record.status}'")

        location = record.location
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
                    dataset.format, expectations,
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
        source    = self.db.get_source(dataset.source_id)
        connector = ConnectorFactory.get(source.type, source.config)
        record    = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")
        location = record.location
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

        fmt       = (dataset.format or "").lower()
        source_id = dataset.source_id
        if not source_id:
            raise ValueError("Dataset has no source")

        source = self.db.get_source(source_id)
        if not source:
            raise ValueError(f"Source '{source_id}' not found")

        record = self.db.get_version(dataset_id, version)
        if not record:
            raise ValueError("Version not found")

        connector = ConnectorFactory.get(source.type, source.config or {})
        result    = connector.read(record.location, fmt, limit=limit, offset=offset)

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
            "version":    record.version,
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
        logger.info(f"Virtual {dataset_id}@{version} [{src.type}]")
        return {
            "dataset_id":  dataset_id,
            "version":     version,
            "source_id":   source_id,
            "source_type": src.type,
            "location":    location,
            "format":      fmt,
        }

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


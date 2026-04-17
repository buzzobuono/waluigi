"""
waluigi.sdk.catalog

Format-agnostic data catalog SDK for Waluigi Catalog v2.

Dataset identity
----------------
Every dataset has a single slash-separated id, e.g. "sales/raw/sales_raw".
No collections — navigation is virtual, exactly like S3.

Usage:

    from waluigi.sdk.catalog import catalog

    # WRITE
    with catalog.produce("sales/raw/sales_raw", format="csv") as ctx:
        writer.writerows(rows)
        ctx.rows = len(rows)
        ctx.meta["source"] = "SAP_EXTRACT"

    # READ
    info = catalog.resolve("sales/raw/sales_raw")
    df = pd.read_csv(info.path)

    # BROWSE (S3-style)
    result = catalog.browse("sales/raw/")
    # result["datasets"]  → direct child datasets
    # result["prefixes"]  → virtual sub-prefixes

    # VIRTUAL
    catalog.register_virtual("finance/erp/fatture_pg",
        source_id="pg-dwh",
        location="SELECT * FROM finance.fatture",
        format="sql")

    # SCHEMA (data steward)
    schema = catalog.get_schema("sales/raw/sales_raw")
    catalog.patch_column("sales/raw/sales_raw", "email",
                         pii=True, pii_type="direct")
    catalog.publish_schema("sales/raw/sales_raw", published_by="mario.rossi")

Environment variables:
    WALUIGI_CATALOG_URL   default: http://localhost:9000
    WALUIGI_TASK_ID
    WALUIGI_JOB_ID
"""

import os
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests


# ---------------------------------------------------------------------------
# Exceptions & warnings
# ---------------------------------------------------------------------------

class CatalogError(Exception):
    """Raised when the catalog returns result=KO."""


class CatalogWarning(UserWarning):
    """Raised (as a warning) when the catalog returns result=WARN."""


# ---------------------------------------------------------------------------
# Response wrapper
# ---------------------------------------------------------------------------

@dataclass
class ResolveInfo:
    """Typed result of catalog.resolve()."""
    dataset_id:   str
    version:      str
    source_type:  str
    format:       Optional[str]
    rows:         Optional[int]
    committed_at: Optional[str]
    pii_columns:  List[str] = field(default_factory=list)
    # local
    path:         Optional[str] = None
    # sftp
    remote_path:  Optional[str] = None
    # s3
    uri:          Optional[str] = None
    endpoint_url: Optional[str] = None
    region:       Optional[str] = None
    # sql
    dsn:          Optional[str] = None
    query:        Optional[str] = None
    # api
    url:          Optional[str] = None

    @classmethod
    def from_response(cls, data: dict) -> "ResolveInfo":
        ci = data.get("connection_info", {})
        return cls(
            dataset_id=data["dataset_id"],
            version=data["version"],
            source_type=data["source_type"],
            format=data.get("format"),
            rows=data.get("rows"),
            committed_at=data.get("committed_at"),
            pii_columns=data.get("pii_columns", []),
            path=ci.get("path"),
            remote_path=ci.get("remote_path"),
            uri=ci.get("uri"),
            endpoint_url=ci.get("endpoint_url"),
            region=ci.get("region"),
            dsn=ci.get("dsn"),
            query=ci.get("query"),
            url=ci.get("url"),
        )


# ---------------------------------------------------------------------------
# Context manager returned by catalog.produce()
# ---------------------------------------------------------------------------

class DatasetWriter:

    def __init__(self, client: "CatalogClient",
                 dataset_id: str, version: str, path: str,
                 inputs: List[dict] = None):
        self._client     = client
        self._dataset_id = dataset_id
        self._version    = version
        self._inputs     = inputs or []
        self.path               = path
        self.rows: Optional[int]     = None
        self.columns: Optional[dict] = None
        self.meta: Dict[str, str]    = {}
        self.skipped                 = False
        self.committed_version       = version

    def __enter__(self) -> "DatasetWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self._client._post(
                    f"/datasets/{self._dataset_id}/fail/{self._version}",
                    json={}, unwrap=False)
            except Exception:
                pass
            return False

        result = self._client._post(
            f"/datasets/{self._dataset_id}/commit/{self._version}",
            json={
                "rows":          self.rows,
                "columns":       self.columns,
                "inputs":        self._inputs,
                "business_meta": self.meta,
            },
        )
        self.skipped           = result.get("skipped", False)
        self.committed_version = result.get("version", self._version)
        return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class CatalogClient:

    def __init__(self, url: str = None):
        self.url = (
            url
            or os.environ.get("WALUIGI_CATALOG_URL", "http://localhost:9000")
        ).rstrip("/")
        self._task_id = os.environ.get("WALUIGI_TASK_ID", "unknown")
        self._job_id  = os.environ.get("WALUIGI_JOB_ID",  "unknown")

    # ------------------------------------------------------------------
    # Lineage helpers
    # ------------------------------------------------------------------

    def ref(self, dataset_id: str, version: str = None) -> dict:
        """
        Build a lineage input ref.
        If version is omitted, resolves the latest committed version.
        """
        if version is None:
            version = self.last_version(dataset_id)
        return {"dataset_id": dataset_id, "version": version}

    # ------------------------------------------------------------------
    # Browse (S3-style)
    # ------------------------------------------------------------------

    def browse(self, prefix: str = "") -> dict:
        """
        List datasets and virtual sub-prefixes under a prefix.
        Use trailing slash: browse("sales/raw/")
        Returns {"prefix", "datasets", "prefixes"}.
        """
        prefix = prefix.rstrip("/")
        if prefix:
            return self._get(f"/datasets/{prefix}/")
        return self._get("/datasets/")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def resolve(self, dataset_id: str) -> ResolveInfo:
        """Return connection info for the latest committed version."""
        data = self._get(f"/datasets/{dataset_id}/resolve")
        return ResolveInfo.from_response(data)

    def last_version(self, dataset_id: str) -> str:
        """Return the latest committed version string."""
        data = self._get(f"/datasets/{dataset_id}/versions")
        versions = data.get("versions", [])
        if not versions:
            raise CatalogError(f"No committed versions for {dataset_id}")
        return versions[0]["version"]

    def get_dataset(self, dataset_id: str) -> dict:
        """Return dataset entity + latest version metadata."""
        return self._get(f"/datasets/{dataset_id}")

    def history(self, dataset_id: str) -> List[dict]:
        """Return all committed versions (newest first)."""
        data = self._get(f"/datasets/{dataset_id}/versions")
        return data.get("versions", [])

    def lineage(self, dataset_id: str, version: str = None) -> dict:
        """Return upstream and downstream lineage."""
        params = f"?version={version}" if version else ""
        return self._get(f"/datasets/{dataset_id}/lineage{params}")

    def preview(self, dataset_id: str, version: str,
                limit: int = 10, offset: int = 0) -> dict:
        """Return a paginated row preview for a local version."""
        return self._get(
            f"/datasets/{dataset_id}/preview/{version}"
            f"?limit={limit}&offset={offset}")

    # ------------------------------------------------------------------
    # Write — local (2-phase)
    # ------------------------------------------------------------------

    def produce(self, dataset_id: str,
                format: str = "",
                inputs: List[dict] = None,
                display_name: str = None,
                description: str = None,
                owner: str = None,
                tags: List[str] = None) -> DatasetWriter:
        """
        Reserve a new local version and return a context manager.

            with catalog.produce("sales/raw/sales_raw", format="csv") as ctx:
                writer.writerows(rows)
                ctx.rows = len(rows)
                ctx.meta["source"] = "SAP_EXTRACT"
        """
        r = self._post(
            f"/datasets/{dataset_id}/reserve",
            json={
                "format":       format,
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
                "owner":        owner,
                "tags":         tags,
            },
        )
        return DatasetWriter(self, dataset_id,
                             r["version"], r["path"],
                             inputs=inputs or [])

    # ------------------------------------------------------------------
    # Write — virtual
    # ------------------------------------------------------------------

    def register_virtual(self, dataset_id: str,
                         source_id: str, location: str,
                         format: str = "",
                         display_name: str = None,
                         description: str = None,
                         owner: str = None,
                         tags: List[str] = None) -> dict:
        """Register a version that lives in an external source."""
        return self._post(
            f"/datasets/{dataset_id}/register-virtual",
            json={
                "source_id":    source_id,
                "location":     location,
                "format":       format,
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
                "owner":        owner,
                "tags":         tags,
            },
        )

    # ------------------------------------------------------------------
    # Write — materialize REST API → local CSV
    # ------------------------------------------------------------------

    def materialize(self, dataset_id: str,
                    base_url: str, endpoint: str,
                    params: Dict[str, Any] = None,
                    display_name: str = None,
                    description: str = None) -> dict:
        """Fetch a REST API endpoint and store result as a local CSV version."""
        return self._post(
            f"/datasets/{dataset_id}/materialize",
            json={
                "base_url":     base_url,
                "endpoint":     endpoint,
                "params":       params or {},
                "task_id":      self._task_id,
                "job_id":       self._job_id,
                "display_name": display_name,
                "description":  description,
            },
        )

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    def register_source(self, id: str, type: str,
                        config: Dict[str, Any],
                        description: str = None) -> dict:
        """Register a physical connector (local | sql | s3 | sftp | api)."""
        return self._post("/sources", json={
            "id":          id,
            "type":        type,
            "config":      config,
            "description": description,
        })

    def get_source(self, id: str) -> dict:
        return self._get(f"/sources/{id}")

    def list_sources(self) -> List[dict]:
        return self._get("/sources")

    # ------------------------------------------------------------------
    # Schema (data steward operations)
    # ------------------------------------------------------------------

    def get_schema(self, dataset_id: str) -> List[dict]:
        """Return current schema columns with PII flags and status."""
        data = self._get(f"/datasets/{dataset_id}/schema")
        return data.get("columns", [])

    def patch_column(self, dataset_id: str, column_name: str,
                     editor: str = "sdk", **kwargs) -> dict:
        """
        Edit semantic metadata for a single column.
        kwargs: logical_type, nullable, pii, pii_type,
                pii_notes, description, tags
        """
        return self._patch(
            f"/datasets/{dataset_id}/schema/{column_name}?editor={editor}",
            json=kwargs,
        )

    def publish_schema(self, dataset_id: str,
                       published_by: str = "sdk") -> dict:
        """Promote all columns to published. Returns diff vs previous snapshot."""
        return self._post(
            f"/datasets/{dataset_id}/schema/publish",
            json={"published_by": published_by},
        )

    def approve(self, dataset_id: str,
                approved_by: str, notes: str = "") -> dict:
        """
        Approve a dataset — marks it as reviewed and publishes its schema.
        Should be called by a data steward after reviewing schema and PII flags.

            catalog.approve("sales/raw/sales_raw",
                            approved_by="mario.rossi",
                            notes="PII verified, schema confirmed")
        """
        return self._post(
            f"/datasets/{dataset_id}/approve",
            json={"approved_by": approved_by, "notes": notes},
        )

    def list_by_status(self, status: str = "in_review") -> list:
        """
        List datasets by approval status.
        status: draft | in_review | approved | deprecated
        """
        data = self._get(f"/datasets?status={status}")
        return data.get("datasets", [])

    # ------------------------------------------------------------------
    # Version metadata
    # ------------------------------------------------------------------

    def set_metadata(self, dataset_id: str, version: str,
                     key: str, value: str):
        """Set a business metadata key on a specific version."""
        self._post(
            f"/datasets/{dataset_id}/metadata/{version}",
            json={"key": key, "value": value},
        )

    def get_metadata(self, dataset_id: str, version: str) -> dict:
        """Return all metadata (sys.* and business) for a version."""
        return self._get(f"/datasets/{dataset_id}/metadata/{version}")

    def delete_metadata(self, dataset_id: str, version: str, key: str):
        """Delete a business metadata key (sys.* keys are protected)."""
        self._delete(f"/datasets/{dataset_id}/metadata/{version}/{key}")

    # ------------------------------------------------------------------
    # Internal HTTP + contract unwrapping
    # ------------------------------------------------------------------

    def _unwrap(self, response: requests.Response,
                unwrap: bool = True) -> Any:
        response.raise_for_status()
        body = response.json()
        if not unwrap:
            return body
        diagnostic = body.get("diagnostic", {})
        result     = diagnostic.get("result", "OK")
        messages   = diagnostic.get("messages", [])
        data       = body.get("data")
        if result == "KO":
            raise CatalogError(
                "; ".join(messages) if messages else "Unknown error")
        if result == "WARN" and messages:
            warnings.warn(
                f"[Catalog WARN] {'; '.join(messages)}",
                CatalogWarning, stacklevel=3)
        return data

    def _get(self, path: str) -> Any:
        return self._unwrap(requests.get(f"{self.url}{path}"))

    #def _post(self, path: str, json: dict = None,
    #          unwrap: bool = True) -> Any:
    #    return self._unwrap(
    #        requests.post(f"{self.url}{path}", json=json), unwrap=unwrap)
    
    def _post(self, path: str, json: dict = None, unwrap: bool = True) -> Any:
        resp = requests.post(f"{self.url}{path}", json=json)
        #print("STATUS:", resp.status_code)
        #print("BODY:", resp.text)
        return self._unwrap(resp, unwrap=unwrap)
    
    def _patch(self, path: str, json: dict = None) -> Any:
        return self._unwrap(requests.patch(f"{self.url}{path}", json=json))

    def _delete(self, path: str) -> Any:
        return self._unwrap(requests.delete(f"{self.url}{path}"))


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

catalog = CatalogClient()
